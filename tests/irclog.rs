//! Unit tests for the irssi autolog parser. Run with `cargo test`.
//!
//! Fixtures are synthetic: the real logs are private and this repository is
//! public. They reproduce the shapes of the live tree, not its content.

use signal_archiver::irclog::{Date, Entry, Kind, parse_log, parse_path};

fn d(year: i32, month: u32, day: u32) -> Date {
    Date { year, month, day }
}

/// The date comes from the path; a line carries only `%H:%M`.
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

/// `autolog_path = "~/irclogs/$tag/%Y/%m/%d/$0.log"`: network and target come
/// only from the path.
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

/// Old files have no network component. `None` lets the caller count them.
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

/// `HH:MM <nick> text`.
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

/// `<@nick>` and `<+nick>` carry a channel mode, not part of the name. So does
/// the padding space in `< nick>`, the form of every unmoded speaker.
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

#[test]
fn the_padded_and_opped_forms_of_one_nick_agree() {
    assert_eq!(
        only("21:05 < alice> hi").nick,
        only("21:05 <@alice> hi").nick
    );
}

/// Split on the first `> `, so a message about IRC syntax is not re-parsed.
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

/// `HH:MM  * nick text`; the two spaces mark an action.
#[test]
fn an_action_is_the_two_space_star_form() {
    let e = only("21:05  * alice waves");
    assert_eq!(e.kind, Kind::Action);
    assert_eq!(e.nick.as_deref(), Some("alice"));
    assert_eq!(e.text, "waves");
}

/// `HH:MM -!- …`: joins, parts, quits, nick changes, modes. Kept whole.
#[test]
fn an_event_has_no_nick_and_keeps_its_whole_text() {
    let e = only("21:05 -!- alice [alice@example.invalid] has joined #chan");
    assert_eq!(e.kind, Kind::Event);
    assert_eq!(e.nick, None);
    assert_eq!(e.text, "alice [alice@example.invalid] has joined #chan");
}

/// `HH:MM !server * text`, the largest class in real logs.
#[test]
fn a_server_notice_is_recognised_and_attributed_to_the_server() {
    let e = only("21:05 !irc.example.invalid *** You are now logged in");
    assert_eq!(e.kind, Kind::Notice);
    assert_eq!(e.nick.as_deref(), Some("irc.example.invalid"));
    assert_eq!(e.text, "You are now logged in");
}

/// The `*` is decoration; some notices have none.
#[test]
fn a_server_notice_without_the_stars_is_still_a_notice() {
    let e = only("21:05 !irc.example.invalid Closing link");
    assert_eq!(e.kind, Kind::Notice);
    assert_eq!(e.nick.as_deref(), Some("irc.example.invalid"));
    assert_eq!(e.text, "Closing link");
}

/// A notice from a person, `-nick(user@host)- text`; the hostmask is dropped.
#[test]
fn a_user_notice_keeps_the_nick_and_drops_the_hostmask() {
    let e = only("21:05 -alice(alice@example.invalid)- ping");
    assert_eq!(e.kind, Kind::Notice);
    assert_eq!(e.nick.as_deref(), Some("alice"));
    assert_eq!(e.text, "ping");
}

/// An older irssi theme's person notice, `[notice(nick)] text`.
#[test]
fn the_older_bracketed_notice_form_is_also_a_notice() {
    let e = only("21:05 [notice(alice)] ping");
    assert_eq!(e.kind, Kind::Notice);
    assert_eq!(e.nick.as_deref(), Some("alice"));
    assert_eq!(e.text, "ping");
}

/// OTR plugin status lines are events.
#[test]
fn otr_plugin_status_is_an_event_and_keeps_its_prefix() {
    let e = only("21:05 OTR: Private conversation started");
    assert_eq!(e.kind, Kind::Event);
    assert_eq!(e.nick, None);
    assert_eq!(e.text, "OTR: Private conversation started");
}

/// The OTR prefix is literal, so another plugin's `word: text` is reported,
/// not absorbed.
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

/// irssi keeps a file open across midnight, so lines after the marker belong to
/// the next day although the path names the old one.
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

/// An unreadable marker is reported rather than leaving the date unchanged.
#[test]
fn an_unreadable_day_changed_marker_is_reported_not_ignored() {
    let parsed = parse_log(DAY, "--- Day changed Sometime In Marchtember\n");
    assert_eq!(parsed.unparsed, vec![1]);
    assert!(parsed.entries.is_empty());
}

// ---------------------------------------------------------------- the shape

/// Line numbers are part of the dedupe key, so they count physical lines,
/// skipped ones included.
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

/// Unknown shapes are reported by line number, not dropped.
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

/// An impossible time would make MariaDB reject the row mid-import.
#[test]
fn an_impossible_clock_time_is_refused() {
    for line in ["24:00 <alice> hi", "21:60 <alice> hi", "99:99 <alice> hi"] {
        let parsed = parse_log(DAY, line);
        assert!(parsed.entries.is_empty(), "accepted {line}");
        assert_eq!(parsed.unparsed, vec![1], "did not report {line}");
    }
}

/// The MariaDB `DATETIME` literal; seconds are always zero.
#[test]
fn a_timestamp_renders_as_a_mariadb_datetime() {
    let e = only("09:05 <alice> hi");
    assert_eq!(e.at.to_string(), "2026-08-14 09:05:00");
}

// ------------------------------------------------- one line, two readers

/// A line parsed alone reads as it does in its file, apart from `line_no`.
/// `irc_tail` parses single lines and must write the row `import_irclogs` would.
#[test]
fn a_line_parsed_alone_matches_the_same_line_parsed_in_its_file() {
    // The shapes where the two readings could differ.
    let file = "--- Log opened Fri Aug 14 00:00:00 2026\n\
                10:00 < alice> a plain line\n\
                10:01 <@bob> an op speaks\n\
                10:02  * alice waves\n\
                10:03 !server [*** ] a server notice\n\
                10:04 -!- carol [carol@host] has joined #chan\n";

    let whole = parse_log(DAY, file);
    assert!(
        whole.unparsed.is_empty(),
        "fixture must parse: {:?}",
        whole.unparsed
    );
    assert_eq!(whole.entries.len(), 5, "five entries in the fixture");

    let lines: Vec<&str> = file.lines().collect();
    for entry in &whole.entries {
        let raw = lines[entry.line_no as usize - 1];
        let alone = parse_log(DAY, &format!("{raw}\n"));
        assert_eq!(
            alone.entries.len(),
            1,
            "line {} parsed alone yielded {} entries: {raw:?}",
            entry.line_no,
            alone.entries.len()
        );
        let solo = &alone.entries[0];
        assert_eq!(solo.at, entry.at, "timestamp differs for {raw:?}");
        assert_eq!(solo.kind, entry.kind, "kind differs for {raw:?}");
        assert_eq!(solo.nick, entry.nick, "nick differs for {raw:?}");
        assert_eq!(solo.text, entry.text, "text differs for {raw:?}");
        // Alone, every line is line 1; the plugin reports the real one.
        assert_eq!(solo.line_no, 1, "a line read alone is always line 1");
    }
}
