//! What somebody is called, and what they were called before — against a real
//! MariaDB, because everything that could be wrong here is the SQL.
//!
//! ⚠ THESE EXIST BECAUSE A RENAME IS COMING IN BULK. signal-cli resolves
//! `envelope.sourceName` with Signal's own precedence, and 0.14.7 adds a branch
//! above the other two — the first/last name you type in the app. Upgrading past
//! it renames every contact who has one, on the first message after the pod
//! restarts. `contacts` held one name per person, so that event would have
//! silently destroyed what people were called before it.
//!
//! ⚠ THEY ALSO EXIST BECAUSE A WRONG DIAGNOSIS GOT THIS FAR. The claim was
//! that `COALESCE(VALUES(x), x)` made the name write-once; it does the opposite,
//! and the archive had been tracking signal-cli correctly all along. The first
//! run of `the_old_name_is_kept_and_dated` said so within a minute, which is the
//! only reason the wrong story stopped there.
//!
//! ⚠ NOTHING IS DROPPED AND NOTHING IS CLEANED UP — each test invents its own
//! uuid, so its rows start absent and the counts are exact whatever else is in the
//! database. Same isolation as `tests/telegram_store.rs`.
//!
//! Skips when `SIGNAL_TEST_DATABASE_URL` is unset, and refuses to skip in CI.

use signal_archiver::db::Db;
use sqlx::AssertSqlSafe;
use sqlx::Row as _;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

async fn connect() -> Option<(Db, MySqlPool)> {
    let Ok(url) = std::env::var("SIGNAL_TEST_DATABASE_URL") else {
        // ⚠ Skipping locally is a convenience; skipping in CI would be a lie. The
        // failure mode of every test in this file is a PASS, so it has to be made
        // impossible rather than watched for.
        assert!(
            std::env::var("CI").is_err(),
            "SIGNAL_TEST_DATABASE_URL is unset in CI: the contact writes would \
             skip and would ship unverified"
        );
        eprintln!("SIGNAL_TEST_DATABASE_URL unset — skipping");
        return None;
    };
    let db = Db::connect(&url).await.expect("migrations apply");
    let pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to SIGNAL_TEST_DATABASE_URL");
    Some((db, pool))
}

/// A uuid nothing else in the database or the suite can be using.
fn uuid(slot: u32) -> String {
    format!("test-{}-{slot}", std::process::id())
}

async fn display_name(pool: &MySqlPool, uuid: &str) -> Option<String> {
    sqlx::query_scalar("SELECT display_name FROM contacts WHERE uuid = ?")
        .bind(uuid)
        .fetch_one(pool)
        .await
        .expect("the contact row exists")
}

/// Every name this contact has worn, oldest first, with whether it is current.
async fn history(pool: &MySqlPool, uuid: &str) -> Vec<(String, bool)> {
    sqlx::query(
        "SELECT name, seen_until IS NULL AS current FROM contact_names WHERE uuid = ? ORDER BY id",
    )
    .bind(uuid)
    .fetch_all(pool)
    .await
    .expect("history reads")
    .into_iter()
    .map(|r| {
        let current: i8 = r.try_get("current").unwrap();
        (r.try_get::<String, _>("name").unwrap(), current != 0)
    })
    .collect()
}

/// Signal's display name is whatever `getContactOrProfileName` resolves to, and
/// it changes the moment the branch above the current one starts matching — which
/// is what a signal-cli upgrade does to everybody at once. The archive follows it.
#[tokio::test]
async fn a_renamed_contact_is_renamed_here_too() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = uuid(1);

    db.upsert_contact(&id, None, Some("Tata")).await.unwrap();
    assert_eq!(display_name(&pool, &id).await.as_deref(), Some("Tata"));

    // The same name again, which is what arrives with every one of her messages.
    db.upsert_contact(&id, None, Some("Tata")).await.unwrap();
    assert_eq!(
        history(&pool, &id).await,
        [("Tata".to_string(), true)],
        "a name seen twice is one name, not two"
    );

    // She is given a first and last name in the app.
    db.upsert_contact(&id, None, Some("Tania Boiko"))
        .await
        .unwrap();
    assert_eq!(
        display_name(&pool, &id).await.as_deref(),
        Some("Tania Boiko"),
        "the name follows Signal, rather than freezing at the first one seen"
    );
}

/// ⚠ A RENAME MUST NOT ERASE WHAT SHE WAS CALLED WHEN SHE SAID SOMETHING. An
/// archive that only overwrites answers "what is she called" and loses "what was
/// she called then", and the second is the one a reader of an old thread has.
#[tokio::test]
async fn the_old_name_is_kept_and_dated() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = uuid(2);

    db.upsert_contact(&id, None, Some("Tata")).await.unwrap();
    db.upsert_contact(&id, None, Some("Tania Boiko"))
        .await
        .unwrap();

    assert_eq!(
        history(&pool, &id).await,
        [
            ("Tata".to_string(), false),
            ("Tania Boiko".to_string(), true)
        ],
        "both names, oldest first, and exactly one of them current"
    );

    // ⚠ The end is DATED, not merely flagged. A boolean would say a name stopped
    // being used and never when, which is the fact a thread from that week needs.
    let ended: Option<i64> = sqlx::query_scalar(
        "SELECT UNIX_TIMESTAMP(seen_until) FROM contact_names WHERE uuid = ? AND name = 'Tata'",
    )
    .bind(&id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(ended.is_some_and(|t| t > 0), "the old name carries its end");
}

/// ⚠ LEARNING NOTHING IS NOT LEARNING THAT SHE HAS NO NAME. A sighting with no
/// name at all — a receipt, a typing frame, a group member we have no profile for
/// — must not blank a name we hold, and must not open a chapter in the history
/// either. Splitting the write into two statements is what put this case at risk,
/// so it is pinned here.
#[tokio::test]
async fn a_nameless_sighting_does_not_wipe_a_name() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = uuid(3);

    db.upsert_contact(&id, None, Some("Tata")).await.unwrap();
    db.upsert_contact(&id, Some("+447700900000"), None)
        .await
        .unwrap();

    assert_eq!(
        display_name(&pool, &id).await.as_deref(),
        Some("Tata"),
        "learning nothing is not learning that she has no name"
    );
    assert_eq!(
        history(&pool, &id).await,
        [("Tata".to_string(), true)],
        "and it opens no second chapter"
    );

    let phone: Option<String> = sqlx::query_scalar("SELECT phone FROM contacts WHERE uuid = ?")
        .bind(&id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(phone.as_deref(), Some("+447700900000"));
}

/// Asserts the end state rather than the sequence, because that is what a fresh
/// database gets once every migration has run.
#[tokio::test]
async fn the_superseded_column_is_gone() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = uuid(4);

    db.upsert_contact(&id, None, Some("Tata")).await.unwrap();
    db.upsert_contact(&id, None, Some("Tania Boiko"))
        .await
        .unwrap();

    let name: Option<String> =
        sqlx::query_scalar("SELECT display_name FROM contacts WHERE uuid = ?")
            .bind(&id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(name.as_deref(), Some("Tania Boiko"));

    // ⚠ Asked of the SCHEMA, not by selecting the column — a query naming a
    // dropped column is an error, which a test could mistake for any other
    // failure. This asks what columns exist and expects one to be absent.
    // `information_schema` is the SERVER's catalogue, not this repo's schema, so
    // no migration creates it and none should — asking it is the point.
    // ⚠ The marker is the LAST comment line deliberately: it must sit within two
    // lines of the FLAGGED line, which is the SQL string rather than the call,
    // and prose after it pushes it out of range.
    // dev-lint: allow-sqlx — the server's own catalogue, by design.
    let still_there: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.columns
          WHERE table_schema = DATABASE() AND table_name = 'contacts'
            AND column_name = 'profile_name'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(still_there, 0, "v46 dropped it");
}

// ---- the frame itself -------------------------------------------------------

/// ⚠ SIGNAL SAYS EVERYTHING EXACTLY ONCE. There is no server-side history to
/// re-walk — Telegram has one, which is why a gap there costs an afternoon and a
/// gap here costs the message. `JsonDataMessage` carries 23 fields at the
/// deployed 0.14.5 and `parse_frame` reads four, so keeping the bytes is what
/// makes the other nineteen recoverable at all: a column can be added and
/// backfilled later, a field never captured cannot.
#[tokio::test]
async fn the_frame_is_kept_whole_and_a_replay_is_free() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    // A ts nothing else uses, and a field this archive has NO column for — which
    // is the whole point: it has to survive anyway.
    let ts = 1_600_000_000_000i64 + std::process::id() as i64;
    let frame = serde_json::json!({"envelope": {
        "sourceUuid": "frame-test", "timestamp": ts,
        "serverReceivedTimestamp": ts + 1,
        "dataMessage": {
            "message": "hello",
            "expiresInSeconds": 604800,
            "viewOnce": true,
            "mentions": [{"uuid": "someone", "start": 0, "length": 5}],
            "textStyles": [{"style": "BOLD", "start": 0, "length": 5}]
        }
    }});

    assert!(
        db.record_signal_frame(&frame).await.unwrap(),
        "first sighting"
    );
    // ⚠ signal-cli re-delivers on reconnect, and this archive reconnects on every
    // deploy. A replay must cost nothing and must not double the row.
    assert!(
        !db.record_signal_frame(&frame).await.unwrap(),
        "a replayed frame is recognised, not stored twice"
    );

    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM signal_frames WHERE envelope_ts = ?")
        .bind(ts)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);

    // ⚠ The assertion that matters: a field with no column survives, and is
    // QUERYABLE. If this ever fails, the table has become a write-only hole and
    // the backfill it exists to enable is not possible.
    // ⚠ `JSON_VALUE`, NOT `->>`. The `->>` operator is MySQL's; MariaDB
    // rejects it outright (error 1064). Pinned here rather than discovered
    // halfway through a backfill over the whole table.
    let at = |path: &'static str| {
        let pool = pool.clone();
        async move {
            // `path` is a `&'static str` written at each call site below; nothing
            // from the database or the frame reaches this string. Safe to assert.
            sqlx::query_scalar::<_, Option<String>>(AssertSqlSafe(format!(
                "SELECT JSON_VALUE(frame, '{path}') FROM signal_frames WHERE envelope_ts = ?"
            )))
            .bind(ts)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };

    // Every one of these is a field the archive has no column for, read back out
    // of the frame. This is the backfill this table exists to make possible.
    //
    // ⚠ `1`, NOT `"true"`. MariaDB's JSON_VALUE renders a JSON boolean as
    // 1/0. Pinned because a backfill comparing against 'true' would silently
    // classify every view-once message as ordinary — a wrong answer, not an error.
    assert_eq!(
        at("$.envelope.dataMessage.viewOnce").await.as_deref(),
        Some("1"),
        "viewOnce survives"
    );
    assert_eq!(
        at("$.envelope.dataMessage.textStyles[0].style")
            .await
            .as_deref(),
        Some("BOLD"),
        "and a text style"
    );
    assert_eq!(
        at("$.envelope.dataMessage.mentions[0].uuid")
            .await
            .as_deref(),
        Some("someone"),
        "and who was mentioned"
    );
    assert_eq!(
        at("$.envelope.dataMessage.expiresInSeconds")
            .await
            .as_deref(),
        Some("604800"),
        "and the disappearing timer"
    );
    assert_eq!(
        at("$.envelope.serverReceivedTimestamp").await.as_deref(),
        Some((ts + 1).to_string().as_str()),
        "and the envelope's own server timestamp"
    );
}

/// ⚠ TWO FRAMES CAN SHARE A TIMESTAMP AND BE DIFFERENT THINGS — a message and
/// the receipt that acknowledges it, a sync and the original. The key is the
/// frame's own bytes for that reason: keying on (timestamp, source) would file
/// the second as a replay of the first and lose it.
#[tokio::test]
async fn two_frames_sharing_a_timestamp_both_survive() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let ts = 1_610_000_000_000i64 + std::process::id() as i64;
    let msg = serde_json::json!({"envelope": {
        "sourceUuid": "frame-test-2", "timestamp": ts, "dataMessage": {"message": "hi"}
    }});
    let receipt = serde_json::json!({"envelope": {
        "sourceUuid": "frame-test-2", "timestamp": ts,
        "receiptMessage": {"when": ts, "isDelivery": true, "timestamps": [ts]}
    }});

    assert!(db.record_signal_frame(&msg).await.unwrap());
    assert!(db.record_signal_frame(&receipt).await.unwrap());

    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM signal_frames WHERE envelope_ts = ?")
        .bind(ts)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 2, "same timestamp, different frames, both kept");
}

// ---- the backfill the frames exist for --------------------------------------

/// ⚠ THIS IS WHAT `signal_frames` WAS FOR, EXERCISED. The columns did not
/// exist when these envelopes arrived; the values come out anyway because the
/// frame was stored whole before anything read it. Without that table, v47 could
/// only ever have been an `ALTER` and a shrug.
///
/// The test writes a message the OLD way — no server times, no timer, exactly
/// what a row from before v47 looks like — then runs the backfill statement and
/// asserts the row learned from its frame.
#[tokio::test]
async fn a_message_learns_its_server_times_from_its_kept_frame() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let ts = 1_620_000_000_000i64 + std::process::id() as i64;
    let thread = format!("dm:backfill-{}", std::process::id());

    // The frame, as it arrived. Distinct numbers throughout so a reader that
    // returns the wrong one cannot pass.
    let frame = serde_json::json!({"envelope": {
        "sourceUuid": "backfill-test", "timestamp": ts,
        "serverReceivedTimestamp": ts - 3,
        "serverDeliveredTimestamp": ts - 1,
        "dataMessage": {"message": "hello", "timestamp": ts, "expiresInSeconds": 86400}
    }});
    assert!(db.record_signal_frame(&frame).await.unwrap());

    // The row as it would have been written before v47 existed.
    sqlx::query(
        "INSERT INTO messages (thread_id, sender_uuid, server_ts, body, is_outgoing)
         VALUES (?, 'backfill-test', ?, 'hello', 0)",
    )
    .bind(&thread)
    .bind(ts)
    .execute(&pool)
    .await
    .unwrap();

    // ⚠ The v48 statement verbatim. Copied rather than invoked because a
    // migration only ever runs once per database — re-running it here is the only
    // way to exercise it against a row created after it had already passed.
    // dev-lint: allow-sqlx — the v48 migration's own statement, under test.
    sqlx::query(
        "UPDATE messages m
          JOIN signal_frames f ON f.envelope_ts = m.server_ts
           SET m.server_received_ts = COALESCE(
                   m.server_received_ts,
                   JSON_VALUE(f.frame, '$.envelope.serverReceivedTimestamp')),
               m.server_delivered_ts = COALESCE(
                   m.server_delivered_ts,
                   JSON_VALUE(f.frame, '$.envelope.serverDeliveredTimestamp')),
               m.expires_in_seconds = COALESCE(
                   m.expires_in_seconds,
                   JSON_VALUE(f.frame, '$.envelope.dataMessage.expiresInSeconds'),
                   JSON_VALUE(f.frame, '$.envelope.syncMessage.sentMessage.expiresInSeconds'))",
    )
    .execute(&pool)
    .await
    .unwrap();

    let got: (Option<i64>, Option<i64>, Option<i32>) = sqlx::query_as(
        "SELECT server_received_ts, server_delivered_ts, expires_in_seconds
           FROM messages WHERE thread_id = ? AND server_ts = ?",
    )
    .bind(&thread)
    .bind(ts)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(got.0, Some(ts - 3), "Signal's receive time, from the frame");
    assert_eq!(got.1, Some(ts - 1), "and its delivery time");
    assert_eq!(
        got.2,
        Some(86400),
        "and the day-long timer it was sent under"
    );
}

/// ⚠ A MESSAGE WITH NO FRAME LEARNS NOTHING, AND THAT IS THE EXPECTED SHAPE OF
/// THIS BACKFILL. Frame capture began 2026-09-21; everything before it has no
/// envelope and never will, because Signal keeps no server-side history. So the
/// backfill touching almost nothing is correct rather than broken — and the
/// columns must stay NULL rather than acquiring the sender's clock, which would
/// make an unmeasured value indistinguishable from a measured one.
#[tokio::test]
async fn a_message_with_no_frame_keeps_null_rather_than_guessing() {
    let Some((_db, pool)) = connect().await else {
        return;
    };
    let ts = 1_630_000_000_000i64 + std::process::id() as i64;
    let thread = format!("dm:noframe-{}", std::process::id());

    sqlx::query(
        "INSERT INTO messages (thread_id, sender_uuid, server_ts, body, is_outgoing)
         VALUES (?, 'noframe-test', ?, 'older than the frames', 0)",
    )
    .bind(&thread)
    .bind(ts)
    .execute(&pool)
    .await
    .unwrap();

    // dev-lint: allow-sqlx — the v48 migration's own statement, under test.
    sqlx::query(
        "UPDATE messages m
          JOIN signal_frames f ON f.envelope_ts = m.server_ts
           SET m.server_received_ts = COALESCE(
                   m.server_received_ts,
                   JSON_VALUE(f.frame, '$.envelope.serverReceivedTimestamp'))",
    )
    .execute(&pool)
    .await
    .unwrap();

    let got: (Option<i64>, Option<i64>, Option<i32>) = sqlx::query_as(
        "SELECT server_received_ts, server_delivered_ts, expires_in_seconds
           FROM messages WHERE thread_id = ? AND server_ts = ?",
    )
    .bind(&thread)
    .bind(ts)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        got,
        (None, None, None),
        "no frame, no values — not the sender's clock standing in"
    );
}
