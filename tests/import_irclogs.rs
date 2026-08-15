//! End-to-end tests for the importer's INCREMENTAL behaviour.
//!
//! These drive the real `import_irclogs` binary against a real MariaDB, because
//! what is being tested is not a function — it is the contract between what the
//! binary decides to read, what it writes, and what it records having read. A
//! unit test of the comparison would pass while the thing that matters (a line
//! arriving and never being imported) went wrong.
//!
//! ⚠ **The failure mode this guards is SILENT DATA LOSS.** Every other bug in
//! this importer is loud: a parse failure is reported, a duplicate is refused by
//! the unique key, a connection error stops the run. A file wrongly marked as
//! already-read produces no error, no warning and no row — the message simply is
//! not in the archive, and nothing ever looks at that file again.
//!
//! They run when `SIGNAL_TEST_DATABASE_URL` points at a *throwaway* database and
//! are skipped otherwise. Each test uses a network tag of its own, unique to the
//! test AND to the run (see [`tag`]), so the rows and the `irc_import_state` keys
//! of one collide with neither another test's nor an earlier run's. They are safe
//! to run in parallel and safe to run twice. NEVER point this at the real signal
//! database.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// `mysql://user:pass@host:port/name` split into the five variables the binary
/// reads from the environment. None when the variable is unset, which is what
/// makes these tests skip rather than fail on a machine with no database.
fn db_env() -> Option<Vec<(&'static str, String)>> {
    let url = std::env::var("SIGNAL_TEST_DATABASE_URL").ok()?;
    let rest = url.strip_prefix("mysql://")?;
    let (creds, hostpath) = rest.split_once('@')?;
    let (user, pass) = creds.split_once(':')?;
    let (hostport, name) = hostpath.split_once('/')?;
    let (host, port) = hostport.split_once(':').unwrap_or((hostport, "3306"));
    Some(vec![
        ("DB_HOST", host.to_string()),
        ("DB_PORT", port.to_string()),
        ("DB_NAME", name.to_string()),
        ("DB_USER", user.to_string()),
        ("DB_PASSWORD", pass.to_string()),
    ])
}

/// A network tag unique to `base` *and* to this run.
///
/// ⚠ **UNIQUE PER TEST IS NOT ENOUGH — IT HAS TO BE UNIQUE PER RUN.** The tags
/// used to be constants, which kept the five tests out of each other's way and
/// did nothing about the run before. A second `cargo test` against the same
/// database then failed four of five: the rows are still there, so the dedupe key
/// refuses them, and `irc_import_state` still holds the `rel_path` — which embeds
/// the tag — so the importer reports `0 log files read` where the test wants 2
/// rows written. Green on a fresh database and red on a used one is worse than
/// either, because it reads as a bug in whatever was being changed at the time.
///
/// Dropping the rows instead is not on offer: `trg_irc_stats_bd` refuses a DELETE
/// on `irc_messages` by design, so nothing here can clean up after itself and
/// every run has to land somewhere new.
///
/// One nonce for the whole process, so every row a single run wrote shares a
/// suffix and can be read back as one batch by hand.
fn tag(base: &str) -> String {
    static NONCE: OnceLock<String> = OnceLock::new();
    let nonce = NONCE.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before the epoch")
            .as_nanos();
        // The pid alone repeats across boots and the clock alone can repeat under
        // a coarse timer; together they are unique in practice. `network` is
        // VARCHAR(64) and this is well under it.
        format!("{}-{nanos}", std::process::id())
    });
    format!("{base}-{nonce}")
}

/// A log tree holding one day of one conversation, under a tag of its own.
fn tree(tag: &str, lines: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("irclogs-test-{tag}"));
    let dir = root.join(tag).join("2020").join("01").join("01");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("#chan.log"), lines).unwrap();
    root
}

fn log_path(root: &Path, tag: &str) -> PathBuf {
    root.join(tag)
        .join("2020")
        .join("01")
        .join("01")
        .join("#chan.log")
}

/// One import run. Returns stdout; panics with stderr if the binary failed.
fn import(root: &Path, extra: &[&str]) -> String {
    let env = db_env().expect("checked by the caller");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_import_irclogs"));
    cmd.arg("--root")
        .arg(root)
        .args(["--self-nick", "me"])
        .args(extra);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "importer failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// irssi's own format: `HH:MM < nick> text`, the padded form a channel writes.
const DAY_ONE: &str = "10:00 < alice> first\n10:01 < alice> second\n";

#[test]
fn a_second_run_reads_nothing_and_writes_nothing() {
    if db_env().is_none() {
        eprintln!("skipping: SIGNAL_TEST_DATABASE_URL not set");
        return;
    }
    let tag = &tag("tnetsecond");
    let root = tree(tag, DAY_ONE);

    let first = import(&root, &["--apply"]);
    assert!(first.contains("1 log files read"), "{first}");
    assert!(first.contains("wrote 2 rows"), "{first}");

    let second = import(&root, &["--apply"]);
    assert!(second.contains("0 log files read"), "{second}");
    assert!(
        second.contains("1 unchanged since the last import"),
        "the skip has to be REPORTED, or a run that read nothing looks like a \
         run that found nothing: {second}"
    );
    assert!(second.contains("wrote 0 rows"), "{second}");
}

#[test]
fn an_appended_line_is_imported_and_only_that_file_is_read() {
    if db_env().is_none() {
        return;
    }
    let tag = &tag("tnetappend");
    let root = tree(tag, DAY_ONE);
    import(&root, &["--apply"]);

    // A second day, so the tree holds a file that must NOT be re-read alongside
    // the one that must.
    let other = root.join(tag).join("2020").join("01").join("02");
    fs::create_dir_all(&other).unwrap();
    fs::write(other.join("#chan.log"), DAY_ONE).unwrap();
    let second = import(&root, &["--apply"]);
    assert!(second.contains("1 log files read"), "{second}");
    assert!(second.contains("wrote 2 rows"), "{second}");

    // Now append to the FIRST day, the case that actually happens: irssi adds a
    // line to today's file, which the archive has already read once.
    let mut text = DAY_ONE.to_string();
    text.push_str("10:02 < alice> third\n");
    fs::write(log_path(&root, tag), &text).unwrap();

    let third = import(&root, &["--apply"]);
    assert!(third.contains("1 log files read"), "{third}");
    assert!(
        third.contains("wrote 1 rows"),
        "only the new line is new — the two already imported are refused by the \
         dedupe key, not by the skip: {third}"
    );
}

#[test]
fn all_re_reads_files_that_have_not_changed() {
    if db_env().is_none() {
        return;
    }
    let tag = &tag("tnetall");
    let root = tree(tag, DAY_ONE);
    import(&root, &["--apply"]);

    let forced = import(&root, &["--apply", "--all"]);
    assert!(forced.contains("1 log files read"), "{forced}");
    assert!(
        forced.contains("wrote 0 rows"),
        "re-reading is not re-writing: the lines are already there and the \
         unique key says so: {forced}"
    );
    assert!(
        !forced.contains("unchanged since the last import"),
        "--all read everything, so there is nothing to declare skipped: {forced}"
    );
}

/// The text of every line stored for one conversation, oldest first.
async fn stored_text(tag: &str) -> Vec<String> {
    let url = std::env::var("SIGNAL_TEST_DATABASE_URL").unwrap();
    let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
    // ⚠ The `query_as` TUPLE form, not `query_scalar`. `irc_messages.text` is
    // NULLable (an event line — a join, a mode change — has no text), and
    // dev-lint's DL-SQLX-ROW-TYPES peels one `Option` off a `query_scalar`
    // target assuming it is the collection wrapper, so
    // `query_scalar::<_, Option<String>>` reads to it as a bare `String` and no
    // turbofish can satisfy it. The tuple form is judged correctly.
    let rows: Vec<(Option<String>,)> = sqlx::query_as(
        "SELECT m.text FROM irc_messages m
           JOIN irc_conversations c ON c.id = m.conversation_id
          WHERE c.network = ? ORDER BY m.file_date, m.line_no",
    )
    .bind(tag)
    .fetch_all(&pool)
    .await
    .unwrap();
    rows.into_iter().map(|(t,)| t.unwrap_or_default()).collect()
}

/// ⚠ **THE CORRUPTION THIS PREVENTS IS PERMANENT.** `rsync` copies a log file
/// whatever irssi is doing to it, so a snapshot can end halfway through a line.
/// Imported, that fragment takes the `line_no` the finished line will have — and
/// the dedupe key then refuses the real one forever. No error, no warning, and
/// no run that could ever correct it; the archive would simply hold half a
/// sentence.
///
/// It is also why the cadence matters: at 24 runs a day this is rare, and the
/// whole point of #880 is to run it far more often.
#[tokio::test]
async fn a_half_written_last_line_is_left_until_it_is_finished() {
    if db_env().is_none() {
        eprintln!("skipping: SIGNAL_TEST_DATABASE_URL not set");
        return;
    }
    let tag = &tag("tnetpartial");
    // No trailing newline: irssi is still writing this line.
    let root = tree(tag, "10:00 < alice> first\n10:01 < alice> half of a");
    let first = import(&root, &["--apply"]);
    assert!(
        first.contains("wrote 1 rows"),
        "only the finished line: {first}"
    );
    assert_eq!(stored_text(tag).await, ["first"]);

    // irssi finishes the line.
    fs::write(
        log_path(&root, tag),
        "10:00 < alice> first\n10:01 < alice> half of a sentence\n",
    )
    .unwrap();
    let second = import(&root, &["--apply"]);
    assert!(second.contains("wrote 1 rows"), "{second}");
    assert_eq!(
        stored_text(tag).await,
        ["first", "half of a sentence"],
        "the whole line, not the fragment that was there when rsync looked"
    );
}

#[test]
fn a_dry_run_records_nothing_so_the_next_real_run_still_reads_the_file() {
    if db_env().is_none() {
        return;
    }
    let tag = &tag("tnetdry");
    let root = tree(tag, DAY_ONE);

    let dry = import(&root, &[]);
    assert!(dry.contains("1 log files read"), "{dry}");
    assert!(dry.contains("DRY RUN"), "{dry}");

    // ⚠ THE DATA-LOSS CASE. If the dry run had recorded progress, this run would
    // skip the file and its lines would never be imported by anything — the
    // archive would be missing them with no error anywhere.
    let real = import(&root, &["--apply"]);
    assert!(real.contains("1 log files read"), "{real}");
    assert!(real.contains("wrote 2 rows"), "{real}");
}
