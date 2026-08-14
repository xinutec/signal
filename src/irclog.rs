//! Pure parsing of irssi autolog files into archive entries.
//!
//! No I/O, like [`crate::parse`]: a log file's path and its text go in,
//! classified entries come out, so the fiddly part — which of five shapes a
//! line is, and which day it belongs to — is unit-testable without a
//! filesystem or a database.
//!
//! The layout is fixed by irssi's own setting rather than by convention:
//!
//! ```text
//! autolog_path = "~/irclogs/$tag/%Y/%m/%d/$0.log"
//! ```
//!
//! So the **network** (`$tag`) and the **target** (`$0` — a channel or a nick)
//! are recoverable only from the path, and so is the date: a logged line
//! carries irssi's default `timestamp_format` of `%H:%M` and nothing more.
//!
//! ⚠ **The line classes were measured, not assumed** — one network's whole
//! tree, 966,039 lines, 2026-08-14:
//!
//! | class | form | count |
//! |---|---|---|
//! | message | `HH:MM <nick> text` | 425,748 |
//! | notice, server | `HH:MM !server [*** ]text` | 385,012 |
//! | *log opened* | `--- Log opened <date>` | 52,894 |
//! | *log closed* | `--- Log closed <date>` | 52,812 |
//! | event | `HH:MM -!- text` | 45,600 |
//! | action | `HH:MM  * nick text` | 3,495 |
//! | event, OTR | `HH:MM OTR: text` | 288 |
//! | *day changed* | `--- Day changed <date>` | 162 |
//! | notice, user | `HH:MM -nick(user@host)- text` | 25 |
//! | unrecognised | — | 3 |
//!
//! Server notices very nearly outnumber conversation, and three of the classes
//! were not in the format as anybody had written it down: the bare notice with
//! no `***`, the notice from a person, and the OTR plugin's status lines. Each
//! was found by counting the corpus rather than by reading about it.
//!
//! Which is why **nothing here drops a line silently**. Anything unrecognised
//! comes back in [`Parsed::unparsed`] by line number for the caller to report,
//! and the classes are matched narrowly on purpose — a rule general enough to
//! absorb the next surprise would also hide it.

/// A calendar date, as components.
///
/// Not a `chrono::NaiveDate` because nothing here does date *arithmetic* — the
/// path supplies one date and a `--- Day changed` marker supplies the next,
/// both absolute. Taking on a date dependency to buy a `format!` we can write
/// ourselves would be the larger change.
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
    /// A MariaDB `DATETIME` literal. Seconds are zero because `%H:%M` is all the
    /// precision there is; two messages in one minute are ordered by insertion,
    /// which is the order the file already had.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:00",
            self.date.year, self.date.month, self.date.day, self.hour, self.minute
        )
    }
}

/// What a logged line *is*. The archive keeps all four and lets the reader
/// decide what to show — a notice is not conversation, but it is the record of
/// why a conversation stopped.
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
/// `line_no` counts *physical* lines from 1, skipped ones included, because it
/// is half the dedupe key: irssi's autolog only ever appends, so a line's
/// number within its file never moves and re-importing writes nothing twice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub line_no: u32,
    pub at: Timestamp,
    pub kind: Kind,
    /// Who said it: a nick, or for a [`Kind::Notice`] the server that sent it.
    /// `None` for an event, which is about somebody rather than by them.
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
    /// Channels start with `#`; anything else is a private conversation. This
    /// is IRC's own rule, not a heuristic — a nick may not begin with `#`.
    pub fn is_channel(&self) -> bool {
        self.target.starts_with('#')
    }
}

/// Split a path **relative to the irclogs root** into network, target and date.
///
/// ⚠ **Exactly five components, and that is load-bearing.** 57 of 89,474 files
/// sit at `<YYYY>/<MM>/<DD>/<target>.log` with no network at all — they predate
/// the `$tag` in `autolog_path`. Matching on the *last* five components instead
/// would read `irclogs/2014/03/09/x.log` as the network `irclogs`, filing
/// somebody's conversation under a server that does not exist. Four components
/// is a legacy file; the caller counts them and leaves them alone.
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
            // `Log opened` / `Log closed` bracket every session and say nothing
            // the entries do not; a day change moves the clock and must not be
            // missed.
            if let Some(rest) = marker.strip_prefix("Day changed ") {
                match parse_day_changed(rest) {
                    Some(d) => date = d,
                    // Not skipped: leaving the date as it was would file a whole
                    // day of conversation under yesterday, silently.
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

    // `<nick> text`. Split on the FIRST `>` so a message *about* IRC syntax is
    // not re-parsed as one.
    if let Some(rest) = rest.strip_prefix('<') {
        let (nick, text) = rest.split_once('>')?;
        // ⚠ The space belongs in this set: irssi writes the channel mode in a
        // fixed column, so an ordinary speaker is `< nick>` and an op is
        // `<@nick>`. 323,570 of the measured messages are the padded form
        // against 102,178 unpadded — reading the space as part of the name
        // splits every unopped participant into a second person.
        return entry(
            Kind::Message,
            Some(nick.trim_start_matches([' ', '@', '+', '%', '&', '~'])),
            text.strip_prefix(' ').unwrap_or(text),
        );
    }
    // ` * nick text` — one space already consumed after the clock, so the
    // action's own second space is what is left. This is the only thing that
    // distinguishes it.
    if let Some(rest) = rest.strip_prefix(" * ") {
        let (nick, text) = rest.split_once(' ').unwrap_or((rest, ""));
        return entry(Kind::Action, Some(nick), text);
    }
    // `-!- somebody has joined` — kept whole. Splitting join from part here
    // would invent structure nothing consumes.
    if let Some(rest) = rest.strip_prefix("-!- ") {
        return entry(Kind::Event, None, rest);
    }
    // `!server text`, where the text of a status notice conventionally opens
    // with `***`. 32,712 measured carry it and 79 do not, so it is decoration
    // and stripping it is what keeps those 79 from being reported as a mystery.
    if let Some(rest) = rest.strip_prefix('!') {
        let (server, text) = rest.split_once(' ')?;
        return entry(
            Kind::Notice,
            Some(server),
            text.strip_prefix("*** ").unwrap_or(text),
        );
    }
    // `-nick(user@host)- text` — a notice from a person. The hostmask names a
    // connection rather than a correspondent, and the archive keys people by
    // nick, so it is dropped. Checked after `-!- `, which shares the leading
    // dash.
    if let Some(rest) = rest.strip_prefix('-') {
        let (who, text) = rest.split_once("- ")?;
        let nick = who.split_once('(').map_or(who, |(nick, _host)| nick);
        return entry(Kind::Notice, Some(nick), text);
    }
    // `[notice(nick)] text` — the same thing an older irssi theme wrote before
    // the dashed form. 3 measured, all from 2013.
    if let Some(rest) = rest.strip_prefix("[notice(") {
        let (nick, text) = rest.split_once(")] ")?;
        return entry(Kind::Notice, Some(nick), text);
    }
    // The OTR plugin logs its status into the conversation it protects (288
    // measured, all private). ⚠ Matched literally rather than as `word:`: a
    // general rule would swallow the next plugin's output as a known class
    // instead of reporting it, which is how this one stayed invisible until the
    // corpus was counted.
    if rest.starts_with("OTR: ") {
        return entry(Kind::Event, None, rest);
    }
    None
}

/// `HH:MM ` at the head of a line, validated as a real clock time.
///
/// `24:00` and `21:60` do not occur in a real log, but accepting one would put
/// a row in the archive that MariaDB then rejects as a `DATETIME` — halfway
/// through an import, with everything before it already written.
fn split_time(line: &str) -> Option<(u32, u32, &str)> {
    let (clock, rest) = line.split_at_checked(6)?;
    let (hour, minute) = clock.strip_suffix(' ')?.split_once(':')?;
    let hour = hour.parse().ok().filter(|h| *h < 24u32)?;
    let minute = minute.parse().ok().filter(|m| *m < 60u32)?;
    Some((hour, minute, rest))
}
