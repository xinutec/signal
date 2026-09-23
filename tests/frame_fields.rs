//! Text styles, link previews and quote details, against a real MariaDB: written
//! live by the ingester, and backfilled from `signal_frames`. The frames are
//! real ones with ids replaced.
//!
//! Skips when `SIGNAL_TEST_DATABASE_URL` is unset, and refuses to skip in CI.

use serde_json::{Value, json};
use signal_archiver::db::{BACKFILL_LINK_PREVIEWS, BACKFILL_QUOTES, BACKFILL_TEXT_STYLES, Db};
use signal_archiver::parse::{Action, parse_frame};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

async fn connect() -> Option<(Db, MySqlPool)> {
    let Ok(url) = std::env::var("SIGNAL_TEST_DATABASE_URL") else {
        // A skip passes, so CI must not skip.
        assert!(
            std::env::var("CI").is_err(),
            "SIGNAL_TEST_DATABASE_URL is unset in CI: the frame-field writes would \
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

/// A sender and timestamps nothing else in the database can be using.
fn ids(slot: i64) -> (String, i64) {
    let pid = i64::from(std::process::id());
    (
        format!("frame-fields-{pid}-{slot}"),
        1_700_000_000_000 + pid * 1_000 + slot * 10,
    )
}

fn sent(me: &str, ts: i64, extra: Value) -> Value {
    let mut sent = json!({"destinationUuid": me, "timestamp": ts, "expiresInSeconds": 0});
    for (k, v) in extra.as_object().unwrap() {
        sent[k] = v.clone();
    }
    json!({"envelope": {"sourceUuid": me, "timestamp": ts,
                        "syncMessage": {"sentMessage": sent}}})
}

fn styled(me: &str, ts: i64) -> Value {
    sent(
        me,
        ts,
        json!({"message": "Hello bold italic strike-through mono-space, spoiler, mono strike",
               "textStyles": [
                   {"length": 4, "start": 6, "style": "BOLD"},
                   {"length": 11, "start": 54, "style": "MONOSPACE"},
                   {"length": 11, "start": 54, "style": "STRIKETHROUGH"}]}),
    )
}

fn linked(me: &str, ts: i64) -> Value {
    sent(
        me,
        ts,
        json!({"message": "https://xinutec.org",
               "previews": [{"description": "", "image": null,
                             "title": "Welcome to nginx!", "url": "https://xinutec.org"}]}),
    )
}

fn reply(me: &str, ts: i64, target: i64) -> Value {
    sent(
        me,
        ts,
        json!({"message": "This is many styles.",
               "quote": {"attachments": [], "author": "+440000000000", "authorUuid": me,
                         "id": target, "text": "Hello bold italic"}}),
    )
}

async fn styles_of(pool: &MySqlPool, me: &str, ts: i64) -> Vec<(String, i32, i32)> {
    sqlx::query_as(
        "SELECT s.style, s.start_utf16, s.length_utf16
           FROM signal_text_styles s JOIN messages m ON m.id = s.message_id
          WHERE m.sender_uuid = ? AND m.server_ts = ?
          ORDER BY s.start_utf16, s.style",
    )
    .bind(me)
    .bind(ts)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn previews_of(
    pool: &MySqlPool,
    me: &str,
    ts: i64,
) -> Vec<(i32, String, Option<String>, Option<String>)> {
    sqlx::query_as(
        "SELECT p.position, p.url, p.title, p.description
           FROM signal_link_previews p JOIN messages m ON m.id = p.message_id
          WHERE m.sender_uuid = ? AND m.server_ts = ?
          ORDER BY p.position",
    )
    .bind(me)
    .bind(ts)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn quote_of(pool: &MySqlPool, me: &str, ts: i64) -> (Option<String>, Option<String>) {
    sqlx::query_as(
        "SELECT quote_author_uuid, quote_text FROM messages
          WHERE sender_uuid = ? AND server_ts = ?",
    )
    .bind(me)
    .bind(ts)
    .fetch_one(pool)
    .await
    .unwrap()
}

fn expected_styles() -> Vec<(String, i32, i32)> {
    vec![
        ("BOLD".into(), 6, 4),
        ("MONOSPACE".into(), 54, 11),
        ("STRIKETHROUGH".into(), 54, 11),
    ]
}

fn expected_previews() -> Vec<(i32, String, Option<String>, Option<String>)> {
    vec![(
        0,
        "https://xinutec.org".into(),
        Some("Welcome to nginx!".into()),
        None,
    )]
}

async fn ingest(db: &Db, frame: &Value) {
    let Action::Message(m) = parse_frame(frame).action else {
        panic!("not a message");
    };
    db.upsert_conversation(&m.thread_id).await.unwrap();
    let id = db.insert_message(&m).await.unwrap().expect("a new row");
    db.insert_text_styles(id, &m.styles).await.unwrap();
    db.insert_link_previews(id, &m.previews).await.unwrap();
}

#[tokio::test]
async fn the_ingester_keeps_styles_previews_and_the_quote() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let (me, ts) = ids(1);
    ingest(&db, &styled(&me, ts)).await;
    ingest(&db, &linked(&me, ts + 1)).await;
    ingest(&db, &reply(&me, ts + 2, ts)).await;

    assert_eq!(styles_of(&pool, &me, ts).await, expected_styles());
    assert_eq!(previews_of(&pool, &me, ts + 1).await, expected_previews());
    assert_eq!(
        quote_of(&pool, &me, ts + 2).await,
        (Some(me.clone()), Some("Hello bold italic".into()))
    );
}

/// Rows written before these columns existed learn them from their kept
/// frames, and running the backfill again adds nothing.
#[tokio::test]
async fn the_backfill_reads_them_from_the_kept_frames() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let (me, ts) = ids(2);
    let thread = format!("dm:{me}");
    for (frame, t, body) in [
        (
            styled(&me, ts),
            ts,
            "Hello bold italic strike-through mono-space, spoiler, mono strike",
        ),
        (linked(&me, ts + 1), ts + 1, "https://xinutec.org"),
        (reply(&me, ts + 2, ts), ts + 2, "This is many styles."),
    ] {
        assert!(db.record_signal_frame(&frame).await.unwrap());
        sqlx::query(
            "INSERT INTO messages (thread_id, sender_uuid, server_ts, body, is_outgoing)
             VALUES (?, ?, ?, ?, 1)",
        )
        .bind(&thread)
        .bind(&me)
        .bind(t)
        .bind(body)
        .execute(&pool)
        .await
        .unwrap();
    }

    for _ in 0..2 {
        for stmt in [
            BACKFILL_QUOTES,
            BACKFILL_TEXT_STYLES,
            BACKFILL_LINK_PREVIEWS,
        ] {
            // dev-lint: allow-sqlx — the migrations' own statements, under test.
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }
    }

    assert_eq!(styles_of(&pool, &me, ts).await, expected_styles());
    assert_eq!(previews_of(&pool, &me, ts + 1).await, expected_previews());
    assert_eq!(
        quote_of(&pool, &me, ts + 2).await,
        (Some(me.clone()), Some("Hello bold italic".into()))
    );
    // The other rows gained nothing they did not carry.
    assert!(styles_of(&pool, &me, ts + 1).await.is_empty());
    assert!(previews_of(&pool, &me, ts).await.is_empty());
    assert_eq!(quote_of(&pool, &me, ts).await, (None, None));
}
