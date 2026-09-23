//! Pure parsing of irssi autolog files into archive entries.
//!
//! No I/O: a log file's path and text go in, classified entries come out.
//!
//! The layout is irssi's own setting:
//!
//! ```text
//! autolog_path = "~/irclogs/$tag/%Y/%m/%d/$0.log"
//! ```
//!
//! The network (`$tag`), target (`$0`, a channel or nick) and date come only
//! from the path; a line carries just `%H:%M`.
//!
//! | class | form |
//! |---|---|
//! | message | `HH:MM <nick> text` |
//! | notice, server | `HH:MM !server [* ]text` |
//! | notice, user | `HH:MM -nick(user@host)- text` |
//! | event | `HH:MM -!- text` |
//! | event, OTR | `HH:MM OTR: text` |
//! | action | `HH:MM  * nick text` |
//! | *log opened/closed* | `--- Log opened <date>` |
//! | *day changed* | `--- Day changed <date>` |
//!
//! Classes are matched narrowly, and anything unrecognised is returned in
//! [`Parsed::unparsed`] for the caller to report.

/// A calendar date, as components. No arithmetic is needed: every date here is
/// read, never computed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

/// When a line was logged, to the minute irssi recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp {
    pub date: Date,
    pub hour: u32,
    pub minute: u32,
}

impl std::fmt::Display for Timestamp {
    /// A MariaDB `DATETIME` literal, seconds always zero.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:00",
            self.date.year, self.date.month, self.date.day, self.hour, self.minute
        )
    }
}

/// What a logged line is. All four are kept; the reader decides what to show.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Message,
    Action,
    Event,
    Notice,
}

impl Kind {
    /// The value stored in the `irc_messages.kind` ENUM.
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Message => "message",
            Kind::Action => "action",
            Kind::Event => "event",
            Kind::Notice => "notice",
        }
    }
}

/// One recognised line.
///
/// `line_no` counts physical lines from 1, skipped ones included. It is part of
/// the dedupe key, stable because irssi only appends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub line_no: u32,
    pub at: Timestamp,
    pub kind: Kind,
    /// A nick, or for a server [`Kind::Notice`] the server. `None` for an event.
    pub nick: Option<String>,
    pub text: String,
}

/// The result of reading one log file: what was understood, and the line
/// numbers of what was not.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Parsed {
    pub entries: Vec<Entry>,
    pub unparsed: Vec<u32>,
}

/// A log file's identity, recovered from its path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPath {
    pub network: String,
    pub target: String,
    pub date: Date,
}

impl LogPath {
    /// Channels start with `#`; a nick cannot.
    pub fn is_channel(&self) -> bool {
        self.target.starts_with('#')
    }
}

/// Split a path relative to the irclogs root into network, target and date.
///
/// Exactly five components. Some old files have four, with no network; matching
/// the last five would read `irclogs/2014/03/09/x.log` as network `irclogs`.
/// The caller counts those and leaves them alone.
pub fn parse_path(rel: &str) -> Option<LogPath> {
    let parts: Vec<&str> = rel
        .split('/')
        .filter(|p| !p.is_empty() && *p != ".")
        .collect();
    let [network, year, month, day, file] = parts[..] else {
        return None;
    };
    let date = Date {
        year: year.parse().ok().filter(|_| year.len() == 4)?,
        month: month.parse().ok().filter(|m| (1..=12).contains(m))?,
        day: day.parse().ok().filter(|d| (1..=31).contains(d))?,
    };
    Some(LogPath {
        network: network.to_string(),
        target: file.strip_suffix(".log")?.to_string(),
        date,
    })
}

/// Read one log file's text, starting from the date its path gave.
pub fn parse_log(start: Date, text: &str) -> Parsed {
    let mut out = Parsed::default();
    let mut date = start;

    for (i, line) in text.lines().enumerate() {
        let line_no = i as u32 + 1;
        if line.trim().is_empty() {
            continue;
        }

        if let Some(marker) = line.strip_prefix("--- ") {
            // Opened/closed markers carry nothing; a day change moves the date.
            if let Some(rest) = marker.strip_prefix("Day changed ") {
                match parse_day_changed(rest) {
                    Some(d) => date = d,
                    None => out.unparsed.push(line_no),
                }
            } else if !marker.starts_with("Log opened") && !marker.starts_with("Log closed") {
                out.unparsed.push(line_no);
            }
            continue;
        }

        match parse_entry(date, line_no, line) {
            Some(entry) => out.entries.push(entry),
            None => out.unparsed.push(line_no),
        }
    }
    out
}

/// `Fri Aug 15 2026` — weekday, month name, day, year.
fn parse_day_changed(rest: &str) -> Option<Date> {
    let [_weekday, month, day, year] = rest.split_whitespace().collect::<Vec<_>>()[..] else {
        return None;
    };
    let month = match month {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    Some(Date {
        year: year.parse().ok()?,
        month,
        day: day.parse().ok().filter(|d| (1..=31).contains(d))?,
    })
}

fn parse_entry(date: Date, line_no: u32, line: &str) -> Option<Entry> {
    let (hour, minute, rest) = split_time(line)?;
    let at = Timestamp { date, hour, minute };
    let entry = |kind, nick: Option<&str>, text: &str| {
        Some(Entry {
            line_no,
            at,
            kind,
            nick: nick.map(str::to_string),
            text: text.to_string(),
        })
    };

    // `<nick> text`, split on the first `>`.
    if let Some(rest) = rest.strip_prefix('<') {
        let (nick, text) = rest.split_once('>')?;
        // irssi pads the mode column: `< nick>` for a plain speaker, `<@nick>`
        // for an op.
        return entry(
            Kind::Message,
            Some(nick.trim_start_matches([' ', '@', '+', '%', '&', '~'])),
            text.strip_prefix(' ').unwrap_or(text),
        );
    }
    // ` * nick text`: the clock's space is already consumed.
    if let Some(rest) = rest.strip_prefix(" * ") {
        let (nick, text) = rest.split_once(' ').unwrap_or((rest, ""));
        return entry(Kind::Action, Some(nick), text);
    }
    // `-!- somebody has joined`, kept whole.
    if let Some(rest) = rest.strip_prefix("-!- ") {
        return entry(Kind::Event, None, rest);
    }
    // `!server text`; the leading `*** ` is usual but optional.
    if let Some(rest) = rest.strip_prefix('!') {
        let (server, text) = rest.split_once(' ')?;
        return entry(
            Kind::Notice,
            Some(server),
            text.strip_prefix("*** ").unwrap_or(text),
        );
    }
    // `-nick(user@host)- text`, a notice from a person; the hostmask is dropped.
    // After `-!- `, which shares the dash.
    if let Some(rest) = rest.strip_prefix('-') {
        let (who, text) = rest.split_once("- ")?;
        let nick = who.split_once('(').map_or(who, |(nick, _host)| nick);
        return entry(Kind::Notice, Some(nick), text);
    }
    // `[notice(nick)] text`, an older irssi theme's form.
    if let Some(rest) = rest.strip_prefix("[notice(") {
        let (nick, text) = rest.split_once(")] ")?;
        return entry(Kind::Notice, Some(nick), text);
    }
    // The OTR plugin's status lines. Literal, not `word:`, so the next plugin's
    // output is reported rather than absorbed.
    if rest.starts_with("OTR: ") {
        return entry(Kind::Event, None, rest);
    }
    None
}

/// `HH:MM ` at the head of a line, validated as a real clock time.
///
/// An invalid time would make MariaDB reject the row mid-import.
fn split_time(line: &str) -> Option<(u32, u32, &str)> {
    let (clock, rest) = line.split_at_checked(6)?;
    let (hour, minute) = clock.strip_suffix(' ')?.split_once(':')?;
    let hour = hour.parse().ok().filter(|h| *h < 24u32)?;
    let minute = minute.parse().ok().filter(|m| *m < 60u32)?;
    Some((hour, minute, rest))
}
