//! What somebody is called, and what they were called before — against a real
//! MariaDB, because everything that could be wrong here is the SQL.
//! Nothing is cleaned up: each test uses its own uuid, so its rows start absent.
//!
//! Skips when `SIGNAL_TEST_DATABASE_URL` is unset, and refuses to skip in CI.

use signal_archiver::db::Db;
use sqlx::AssertSqlSafe;
use sqlx::Row as _;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

async fn connect() -> Option<(Db, MySqlPool)> {
    let Ok(url) = std::env::var("SIGNAL_TEST_DATABASE_URL") else {
        // A skip passes, so CI must not skip.
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

/// The display name follows whatever `getContactOrProfileName` resolves to.
#[tokio::test]
async fn a_renamed_contact_is_renamed_here_too() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = uuid(1);

    db.upsert_contact(&id, None, Some("Tata")).await.unwrap();
    assert_eq!(display_name(&pool, &id).await.as_deref(), Some("Tata"));

    // The same name arrives with every message.
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

/// A rename keeps what she was called before, for old threads.
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

    // Dated, not just flagged.
    let ended: Option<i64> = sqlx::query_scalar(
        "SELECT UNIX_TIMESTAMP(seen_until) FROM contact_names WHERE uuid = ? AND name = 'Tata'",
    )
    .bind(&id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(ended.is_some_and(|t| t > 0), "the old name carries its end");
}

/// A sighting with no name (a receipt, a typing frame, an unknown group
/// member) neither blanks the name nor opens a new one.
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

/// Asserts the end state a fresh database reaches.
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

    // Asks the server's catalogue: selecting a dropped column would error like
    // any other failure.
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

#[tokio::test]
async fn the_frame_is_kept_whole_and_a_replay_is_free() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    // A timestamp nothing else uses, and a field with no column.
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
    // signal-cli re-delivers on every reconnect.
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

    // A field with no column must be queryable. `JSON_VALUE`, because MariaDB
    // rejects `->>` (error 1064).
    let at = |path: &'static str| {
        let pool = pool.clone();
        async move {
            // `path` is a literal at each call site.
            sqlx::query_scalar::<_, Option<String>>(AssertSqlSafe(format!(
                "SELECT JSON_VALUE(frame, '{path}') FROM signal_frames WHERE envelope_ts = ?"
            )))
            .bind(ts)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };

    // Fields with no column, read back out of the frame. JSON_VALUE renders a
    // JSON boolean as 1/0, not 'true'.
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

/// Two frames can share a timestamp and be different things, such as a message
/// and its receipt.
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

/// A row written without the v47 columns learns them from its frame.
#[tokio::test]
async fn a_message_learns_its_server_times_from_its_kept_frame() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let ts = 1_620_000_000_000i64 + std::process::id() as i64;
    let thread = format!("dm:backfill-{}", std::process::id());

    // Distinct numbers throughout, so returning the wrong one fails.
    let frame = serde_json::json!({"envelope": {
        "sourceUuid": "backfill-test", "timestamp": ts,
        "serverReceivedTimestamp": ts - 3,
        "serverDeliveredTimestamp": ts - 1,
        "dataMessage": {"message": "hello", "timestamp": ts, "expiresInSeconds": 86400}
    }});
    assert!(db.record_signal_frame(&frame).await.unwrap());

    sqlx::query(
        "INSERT INTO messages (thread_id, sender_uuid, server_ts, body, is_outgoing)
         VALUES (?, 'backfill-test', ?, 'hello', 0)",
    )
    .bind(&thread)
    .bind(ts)
    .execute(&pool)
    .await
    .unwrap();

    // The v48 statement verbatim: a migration runs once per database.
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

/// A message with no frame keeps NULLs rather than the sender's clock.
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
