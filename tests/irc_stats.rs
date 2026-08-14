//! `irc_conversation_stats` is maintained by triggers, so the thing under test
//! is the DATABASE, not a Rust function — nothing here would catch a mistake if
//! it asserted against application code.
//!
//! ⚠ NOTHING IS DROPPED AND NOTHING IS CLEANED UP. Every run invents its own
//! network name, so its conversation is new, its stats row starts absent, and
//! the absolute numbers below are exact no matter what else is in the database.
//! That is the same isolation `tests/import_irclogs.rs` uses ("each test uses
//! its own network tag"), and it is what lets this share one database with that
//! suite while cargo runs both binaries in parallel.
//!
//! Two earlier attempts are worth not repeating: dropping the archive tables
//! pulled them out from under a concurrent import test, and dropping
//! `schema_version` to force a migration replay left the shared database
//! permanently broken (the replayed `ALTER TABLE`s hit columns that were still
//! there). Creating a second database instead is refused by the gate's
//! least-privilege `signal` user, which is confined to one.
//!
//! Skips when `SIGNAL_TEST_DATABASE_URL` is unset, the same convention as the
//! rest of the suite.

use std::time::{SystemTime, UNIX_EPOCH};

use signal_archiver::db::{Db, IrcLine};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlPool, Row};

async fn stats(pool: &MySqlPool, conversation_id: u64) -> Option<(i64, String)> {
    let row = sqlx::query(
        "SELECT cnt, COALESCE(DATE_FORMAT(last_sent_at, '%Y-%m-%d %H:%i:%s'), '') AS last
           FROM irc_conversation_stats WHERE conversation_id = ?",
    )
    .bind(conversation_id)
    .fetch_optional(pool)
    .await
    .expect("read stats")?;
    Some((row.get("cnt"), row.get("last")))
}

fn line(line_no: u32, sent_at: &str, kind: &'static str) -> IrcLine {
    IrcLine {
        line_no,
        sent_at: sent_at.to_string(),
        nick: Some("someone".to_string()),
        is_self: false,
        kind,
        text: format!("line {line_no}"),
    }
}

#[tokio::test]
async fn triggers_maintain_the_conversation_stats() {
    let Ok(url) = std::env::var("SIGNAL_TEST_DATABASE_URL") else {
        // ⚠ Skipping locally is a convenience; skipping in CI would be a lie.
        // A green run that tested nothing is exactly how the triggers would
        // ship unverified — the failure mode is a PASS, so it has to be made
        // impossible rather than watched for.
        assert!(
            std::env::var("CI").is_err(),
            "SIGNAL_TEST_DATABASE_URL is unset in CI: the trigger tests would \
             skip and irc_conversation_stats would ship unverified"
        );
        eprintln!("SIGNAL_TEST_DATABASE_URL unset — skipping");
        return;
    };

    // `connect` applies whatever migrations are outstanding, which is what puts
    // the table and its triggers there. It does NOT need a replay: if they are
    // already applied the triggers already exist, and either way what follows
    // exercises the live ones.
    let db = Db::connect(&url).await.expect("migrations apply");
    let pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to SIGNAL_TEST_DATABASE_URL");

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let network = format!("test-{}-{nonce}", std::process::id());

    let chan = db
        .upsert_irc_conversation(&network, "#chan", true, false)
        .await
        .expect("conversation");
    assert_eq!(
        stats(&pool, chan).await,
        None,
        "a conversation with no lines has no stats row at all, which is why \
         the reader has to COALESCE rather than assume one"
    );

    // --- a counted kind counts, an uncounted kind does not -------------------
    let wrote = db
        .insert_irc_lines(
            chan,
            &network,
            "2026-01-01",
            &[
                line(1, "2026-01-01 10:00:00", "message"),
                line(2, "2026-01-01 11:00:00", "action"),
                line(3, "2026-01-01 12:00:00", "event"),
                line(4, "2026-01-01 13:00:00", "notice"),
            ],
        )
        .await
        .expect("insert");
    assert_eq!(wrote, 4, "all four lines are archived");
    assert_eq!(
        stats(&pool, chan).await,
        Some((2, "2026-01-01 11:00:00".to_string())),
        "only message+action count, and the newest of THOSE is the last time — \
         the 13:00 notice must not become the conversation's last message"
    );

    // --- THE HINGE: replay must be free --------------------------------------
    // `irc_tail` re-offers the plugin's whole ring after every restart and the
    // importer re-reads any file whose mtime moved. If an ignored INSERT IGNORE
    // fired the trigger, every restart would inflate every count.
    let wrote = db
        .insert_irc_lines(
            chan,
            &network,
            "2026-01-01",
            &[
                line(1, "2026-01-01 10:00:00", "message"),
                line(2, "2026-01-01 11:00:00", "action"),
            ],
        )
        .await
        .expect("replay");
    assert_eq!(wrote, 0, "the dedupe key refuses both");
    assert_eq!(
        stats(&pool, chan).await,
        Some((2, "2026-01-01 11:00:00".to_string())),
        "a refused insert must not count"
    );

    // --- lines do not arrive in timestamp order ------------------------------
    // The importer walks files by path, so yesterday's log can land after
    // today's. A plain assignment would move the conversation backwards in the
    // list; GREATEST is what stops it.
    db.insert_irc_lines(
        chan,
        &network,
        "2025-12-31",
        &[line(1, "2025-12-31 09:00:00", "message")],
    )
    .await
    .expect("older file");
    assert_eq!(
        stats(&pool, chan).await,
        Some((3, "2026-01-01 11:00:00".to_string())),
        "an older line counts but does not move the last-message time backwards"
    );

    // --- a second conversation is independent --------------------------------
    let dm = db
        .upsert_irc_conversation(&network, "someone", false, false)
        .await
        .expect("dm");
    db.insert_irc_lines(
        dm,
        &network,
        "2026-01-02",
        &[line(1, "2026-01-02 08:00:00", "message")],
    )
    .await
    .expect("dm line");
    assert_eq!(
        stats(&pool, dm).await,
        Some((1, "2026-01-02 08:00:00".to_string()))
    );
    assert_eq!(
        stats(&pool, chan).await,
        Some((3, "2026-01-01 11:00:00".to_string())),
        "writing to one conversation must not touch another"
    );

    // --- the guards refuse what cannot be maintained -------------------------
    // Scoped to this run's own conversation, so a failure here cannot damage
    // anything else sharing the database.
    let deleted = sqlx::query("DELETE FROM irc_messages WHERE conversation_id = ?")
        .bind(chan)
        .execute(&pool)
        .await;
    assert!(
        deleted.is_err(),
        "a DELETE must be refused: MAX(sent_at) cannot be recovered incrementally"
    );

    let recast = sqlx::query("UPDATE irc_messages SET kind = 'event' WHERE conversation_id = ?")
        .bind(chan)
        .execute(&pool)
        .await;
    assert!(recast.is_err(), "changing kind must be refused");

    let moved = sqlx::query(
        "UPDATE irc_messages SET sent_at = '2030-01-01 00:00:00' WHERE conversation_id = ?",
    )
    .bind(chan)
    .execute(&pool)
    .await;
    assert!(moved.is_err(), "changing sent_at must be refused");

    // --- but a repair that cannot drift the stats is ALLOWED -----------------
    // `is_self` has needed correcting in production; refusing it would have made
    // the guard worse than the drift it prevents.
    sqlx::query("UPDATE irc_messages SET is_self = 1, text = 'fixed' WHERE conversation_id = ?")
        .bind(chan)
        .execute(&pool)
        .await
        .expect("correcting is_self/text is allowed");
    assert_eq!(
        stats(&pool, chan).await,
        Some((3, "2026-01-01 11:00:00".to_string())),
        "and it changed nothing"
    );
}
