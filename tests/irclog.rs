//! Unit tests for the irssi autolog parser. Run with `cargo test`.
//!
//! Every fixture here is synthetic. The real logs are private conversation with
//! named people and this repository is public, so the fixtures reproduce the
//! *shapes* measured against the live tree (2026-08-14, 64,038 timestamped
//! lines in one network's 2026 logs) and none of its content.

use signal_archiver::irclog::{Date, Entry, Kind, parse_log, parse_path};

fn d(year: i32, month: u32, day: u32) -> Date {
    Date { year, month, day }
}

/// The parser is handed the date from the path, because a log line carries
/// `%H:%M` and nothing more.
const DAY: Date = Date {
    year: 2026,
    month: 8,
    day: 14,
};

fn only(text: &str) -> Entry {
    let parsed = parse_log(DAY, text);
    assert!(
        parsed.unparsed.is_empty(),
        "line went unrecognised: {:?}",
        parsed.unparsed
    );
    assert_eq!(parsed.entries.len(), 1, "expected exactly one entry");
    parsed.entries.into_iter().next().expect("one entry")
}

// ---------------------------------------------------------------- the path

/// `autolog_path = "~/irclogs/$tag/%Y/%m/%d/$0.log"` — the network and the
/// target are *only* recoverable from the path, so this is not a convenience.
#[test]
fn a_path_yields_network_target_and_date() {
    let p = parse_path("xinutec/2026/08/14/#chan.log").expect("parses");
    assert_eq!(p.network, "xinutec");
    assert_eq!(p.target, "#chan");
    assert_eq!(p.date, d(2026, 8, 14));
}

#[test]
fn a_dm_target_is_the_nick_with_no_marker() {
    let p = parse_path("xinutec/2026/08/14/somebody.log").expect("parses");
    assert_eq!(p.target, "somebody");
    assert!(!p.is_channel());
}

#[test]
fn a_channel_target_is_marked_by_its_own_name() {
    assert!(
        parse_path("xinutec/2026/08/14/#chan.log")
            .expect("parses")
            .is_channel()
    );
}

/// 57 of 89,474 log files sit at `<YYYY>/<MM>/<DD>/<target>.log` with no
/// network component — they predate the `$tag` in `autolog_path`. Returning
/// None lets the caller count them; guessing a network would file somebody's
/// conversation under the wrong server.
#[test]
fn a_path_without_a_network_is_refused_rather_than_guessed() {
    assert!(parse_path("2014/03/09/somebody.log").is_none());
}

#[test]
fn a_leading_directory_prefix_does_not_confuse_the_path() {
    let p = parse_path("./xinutec/2026/08/14/#chan.log").expect("parses");
    assert_eq!(p.network, "xinutec");
}

// ------------------------------------------------------------- the classes

/// `HH:MM <nick> text` — 30,074 of 64,038 measured lines.
#[test]
fn a_message_carries_its_nick_and_text() {
    let e = only("21:05 <alice> hello there");
    assert_eq!(e.kind, Kind::Message);
    assert_eq!(e.nick.as_deref(), Some("alice"));
    assert_eq!(e.text, "hello there");
    assert_eq!(e.at.hour, 21);
    assert_eq!(e.at.minute, 5);
    assert_eq!(e.at.date, DAY);
}

/// Channel ops are logged `<@nick>`, voice `<+nick>`. The prefix is a mode on
/// the channel, not part of anybody's name — keeping it would file the same
/// person under two names the day they are opped.
///
/// ⚠ **A space is one of those modes: it is the column with no mode in it.**
/// 323,570 of the 425,748 measured messages are `< nick>` — every ordinary
/// person speaking in a channel — against 102,178 unpadded. Treating the space
/// as part of the name split every unopped participant into two people, and put
/// 13,465 of the archive's owner's own messages under somebody else.
#[test]
fn a_mode_prefix_is_not_part_of_the_nick() {
    for (line, nick) in [
        ("21:05 <@alice> hi", "alice"),
        ("21:05 <+bob> hi", "bob"),
        ("21:05 <%carol> hi", "carol"),
        ("21:05 <~dave> hi", "dave"),
        ("21:05 <&erin> hi", "erin"),
        ("21:05 < frank> hi", "frank"),
    ] {
        let e = only(line);
        assert_eq!(e.nick.as_deref(), Some(nick), "for {line}");
    }
}

/// The same person, opped and not, is one person.
#[test]
fn the_padded_and_opped_forms_of_one_nick_agree() {
    assert_eq!(
        only("21:05 < alice> hi").nick,
        only("21:05 <@alice> hi").nick
    );
}

/// The text is taken whole after the first `> `, so a message *about* IRC
/// syntax is not re-parsed as one.
#[test]
fn angle_brackets_inside_a_message_stay_in_the_message() {
    let e = only("21:05 <alice> try <@bob> or -!- for the event form");
    assert_eq!(e.nick.as_deref(), Some("alice"));
    assert_eq!(e.text, "try <@bob> or -!- for the event form");
}

#[test]
fn an_empty_message_is_still_a_message() {
    let e = only("21:05 <alice> ");
    assert_eq!(e.kind, Kind::Message);
    assert_eq!(e.text, "");
}

#[test]
fn utf8_survives_intact() {
    let e = only("21:05 <alice> größer — 日本語 🎉");
    assert_eq!(e.text, "größer — 日本語 🎉");
}

/// `HH:MM  * nick text` — note the *two* spaces, which is what distinguishes an
/// action from everything else. 157 measured.
#[test]
fn an_action_is_the_two_space_star_form() {
    let e = only("21:05  * alice waves");
    assert_eq!(e.kind, Kind::Action);
    assert_eq!(e.nick.as_deref(), Some("alice"));
    assert_eq!(e.text, "waves");
}

/// `HH:MM -!- …` — joins, parts, quits, nick changes, modes. 1,092 measured.
/// Kept whole: the app shows them as one grey line if it shows them at all, and
/// splitting join from part here would be inventing structure nothing consumes.
#[test]
fn an_event_has_no_nick_and_keeps_its_whole_text() {
    let e = only("21:05 -!- alice [alice@example.invalid] has joined #chan");
    assert_eq!(e.kind, Kind::Event);
    assert_eq!(e.nick, None);
    assert_eq!(e.text, "alice [alice@example.invalid] has joined #chan");
}

/// `HH:MM !server *** text` — 32,712 measured, **the largest class of all**,
/// larger than actual messages. A parser that dropped what it did not recognise
/// would silently discard half the corpus and look like it worked.
#[test]
fn a_server_notice_is_recognised_and_attributed_to_the_server() {
    let e = only("21:05 !irc.example.invalid *** You are now logged in");
    assert_eq!(e.kind, Kind::Notice);
    assert_eq!(e.nick.as_deref(), Some("irc.example.invalid"));
    assert_eq!(e.text, "You are now logged in");
}

/// The `***` is decoration, not structure: 79 notices across the measured tree
/// carry none. Requiring it cost those 79 lines, which is how it was found —
/// by counting the real corpus, not by reading the format.
#[test]
fn a_server_notice_without_the_stars_is_still_a_notice() {
    let e = only("21:05 !irc.example.invalid Closing link");
    assert_eq!(e.kind, Kind::Notice);
    assert_eq!(e.nick.as_deref(), Some("irc.example.invalid"));
    assert_eq!(e.text, "Closing link");
}

/// A notice from a *person* rather than a server: `-nick(user@host)- text`.
/// 21 measured. The hostmask is dropped — it identifies a connection, not a
/// correspondent, and the archive keys people by nick.
#[test]
fn a_user_notice_keeps_the_nick_and_drops_the_hostmask() {
    let e = only("21:05 -alice(alice@example.invalid)- ping");
    assert_eq!(e.kind, Kind::Notice);
    assert_eq!(e.nick.as_deref(), Some("alice"));
    assert_eq!(e.text, "ping");
}

/// An older irssi theme wrote a person's notice as `[notice(nick)] text`. Three
/// lines in the measured tree, all from 2013, and they are the entire remainder
/// — with this the corpus classifies completely.
#[test]
fn the_older_bracketed_notice_form_is_also_a_notice() {
    let e = only("21:05 [notice(alice)] ping");
    assert_eq!(e.kind, Kind::Notice);
    assert_eq!(e.nick.as_deref(), Some("alice"));
    assert_eq!(e.text, "ping");
}

/// The OTR plugin logs its status into the conversation it protects: 288 lines,
/// all in private logs. Grey lines about the conversation rather than in it, so
/// they are events — the same thing a join is, from the reader's side.
#[test]
fn otr_plugin_status_is_an_event_and_keeps_its_prefix() {
    let e = only("21:05 OTR: Private conversation started");
    assert_eq!(e.kind, Kind::Event);
    assert_eq!(e.nick, None);
    assert_eq!(e.text, "OTR: Private conversation started");
}

/// ⚠ The OTR prefix is matched *literally*, and that is the point. Generalising
/// to `word: text` would swallow the next plugin's output as a known class
/// instead of reporting it, which is exactly how the 288 OTR lines and the 79
/// bare notices stayed invisible until somebody counted.
#[test]
fn another_plugins_prefix_is_reported_rather_than_swallowed() {
    let parsed = parse_log(DAY, "21:05 SOMEPLUGIN: went secure");
    assert!(parsed.entries.is_empty());
    assert_eq!(parsed.unparsed, vec![1]);
}

// -------------------------------------------------------------- the markers

#[test]
fn log_open_and_close_markers_are_not_entries() {
    let parsed = parse_log(
        DAY,
        "--- Log opened Thu Aug 14 08:00:00 2026\n\
         21:05 <alice> hi\n\
         --- Log closed Thu Aug 14 23:59:59 2026\n",
    );
    assert!(parsed.unparsed.is_empty());
    assert_eq!(parsed.entries.len(), 1);
}

/// The one that matters. irssi holds a log file open across midnight — 12 such
/// markers in one network's 2026 logs — so lines after it belong to the *next*
/// day even though the path still says the old one. Without this they are
/// timestamped a day early and sort into the wrong place forever.
#[test]
fn a_day_changed_marker_moves_the_date_on() {
    let parsed = parse_log(
        DAY,
        "23:59 <alice> before midnight\n\
         --- Day changed Fri Aug 15 2026\n\
         00:01 <alice> after midnight\n",
    );
    assert!(parsed.unparsed.is_empty());
    let dates: Vec<Date> = parsed.entries.iter().map(|e| e.at.date).collect();
    assert_eq!(dates, vec![d(2026, 8, 14), d(2026, 8, 15)]);
}

#[test]
fn every_month_name_is_understood() {
    for (n, name) in [
        (1, "Jan"),
        (2, "Feb"),
        (3, "Mar"),
        (4, "Apr"),
        (5, "May"),
        (6, "Jun"),
        (7, "Jul"),
        (8, "Aug"),
        (9, "Sep"),
        (10, "Oct"),
        (11, "Nov"),
        (12, "Dec"),
    ] {
        let text = format!("--- Day changed Wed {name} 03 2027\n00:01 <alice> hi\n");
        let parsed = parse_log(DAY, &text);
        assert!(parsed.unparsed.is_empty(), "{name} went unrecognised");
        assert_eq!(parsed.entries[0].at.date, d(2027, n, 3), "for {name}");
    }
}

/// A marker we cannot read must not silently leave the date as it was — that
/// would file a day of conversation under yesterday with nothing to show for
/// it. It is reported instead.
#[test]
fn an_unreadable_day_changed_marker_is_reported_not_ignored() {
    let parsed = parse_log(DAY, "--- Day changed Sometime In Marchtember\n");
    assert_eq!(parsed.unparsed, vec![1]);
    assert!(parsed.entries.is_empty());
}

// ---------------------------------------------------------------- the shape

/// Line numbers are the dedupe key: the ingester keys on
/// `(conversation, file date, line number)` because irssi's autolog only ever
/// appends, so a line's number is stable and re-running imports nothing twice.
/// They must therefore count *physical* lines, including the ones skipped.
#[test]
fn line_numbers_count_physical_lines_including_skipped_ones() {
    let parsed = parse_log(
        DAY,
        "--- Log opened Thu Aug 14 08:00:00 2026\n\
         21:05 <alice> first\n\
         \n\
         21:06 <bob> second\n",
    );
    assert_eq!(
        parsed.entries.iter().map(|e| e.line_no).collect::<Vec<_>>(),
        vec![2, 4]
    );
}

#[test]
fn blank_lines_are_skipped_without_being_reported() {
    let parsed = parse_log(DAY, "\n   \n21:05 <alice> hi\n");
    assert!(parsed.unparsed.is_empty());
    assert_eq!(parsed.entries.len(), 1);
}

/// Anything that matches no known shape is surfaced by line number rather than
/// dropped. Half this corpus was a class nobody had written down; the next
/// unknown class should announce itself rather than quietly vanish.
#[test]
fn an_unknown_shape_is_reported_by_line_number() {
    let parsed = parse_log(DAY, "21:05 <alice> fine\nnot a log line at all\n");
    assert_eq!(parsed.entries.len(), 1);
    assert_eq!(parsed.unparsed, vec![2]);
}

#[test]
fn a_missing_trailing_newline_does_not_lose_the_last_line() {
    let parsed = parse_log(DAY, "21:05 <alice> hi");
    assert_eq!(parsed.entries.len(), 1);
}

#[test]
fn an_empty_file_yields_nothing_and_complains_about_nothing() {
    let parsed = parse_log(DAY, "");
    assert!(parsed.entries.is_empty());
    assert!(parsed.unparsed.is_empty());
}

/// A time that is not a time is not a message. `24:00` and `21:60` never occur
/// in a real log, but accepting them would put a row in the archive that
/// MariaDB then refuses as a DATETIME, mid-import.
#[test]
fn an_impossible_clock_time_is_refused() {
    for line in ["24:00 <alice> hi", "21:60 <alice> hi", "99:99 <alice> hi"] {
        let parsed = parse_log(DAY, line);
        assert!(parsed.entries.is_empty(), "accepted {line}");
        assert_eq!(parsed.unparsed, vec![1], "did not report {line}");
    }
}

/// What the ingester binds to a MariaDB `DATETIME`. Seconds are zero because
/// irssi's default `timestamp_format` is `%H:%M` and there is no more precision
/// to be had — two messages in the same minute are ordered by insertion, which
/// is the order the file already had.
#[test]
fn a_timestamp_renders_as_a_mariadb_datetime() {
    let e = only("09:05 <alice> hi");
    assert_eq!(e.at.to_string(), "2026-08-14 09:05:00");
}
