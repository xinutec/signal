//! Telegram writes, against a real MariaDB.
//!
//! The mapping is unit-tested in `src/telegram/map/` and needs nothing. What is
//! here is everything the DATABASE decides: whether an edit keeps the words it
//! replaces, whether a replayed update writes twice, which conversations a
//! peer-less deletion reaches, and whether a session survives being written down.
//! None of those are answerable against a mock, because in every case the thing
//! that could be wrong is the SQL.
//!
//! ⚠ NOTHING IS DROPPED AND NOTHING IS CLEANED UP, the same isolation
//! `tests/irc_stats.rs` uses: each test invents its own conversation ids and its
//! own message-id range, so its rows start absent and the counts below are exact
//! whatever else is in the database. This matters more here than there —
//! `mark_telegram_deleted` deliberately reaches ACROSS conversations, so a shared
//! message-id range would let one test retract another's rows.
//!
//! Skips when `SIGNAL_TEST_DATABASE_URL` is unset, and refuses to skip in CI.

use signal_archiver::db::{Db, TelegramBackfill, TelegramDeleteScope, TelegramStored};
use signal_archiver::telegram::ConvKind;
use signal_archiver::telegram::map::{MsgKind, PeerSpace, Reaction, Row};
use signal_archiver::telegram::session::DbSession;
use sqlx::Row as _;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

/// Distinct ids per test, so tests that write across conversations cannot collide.
///
/// The process id separates concurrent test BINARIES (cargo runs them in
/// parallel, each its own process) and `slot` separates tests within one binary,
/// which share a pid across threads. `msg_id` is an INT, so the arithmetic has to
/// stay inside about 2.1 billion — hence the modulo.
struct Ids {
    conversation: i64,
    channel: i64,
    msg_base: i32,
}

fn ids(slot: i32) -> Ids {
    let pid = (std::process::id() % 20_000) as i32;
    Ids {
        conversation: 700_000_000_000 + i64::from(pid) * 1_000 + i64::from(slot),
        channel: -1_000_000_000_000 - i64::from(pid) * 1_000 - i64::from(slot),
        msg_base: pid * 100_000 + slot * 1_000,
    }
}

async fn connect() -> Option<(Db, MySqlPool)> {
    let Ok(url) = std::env::var("SIGNAL_TEST_DATABASE_URL") else {
        // ⚠ Skipping locally is a convenience; skipping in CI would be a lie. The
        // failure mode of every test in this file is a PASS, so it has to be made
        // impossible rather than watched for.
        assert!(
            std::env::var("CI").is_err(),
            "SIGNAL_TEST_DATABASE_URL is unset in CI: the Telegram writes would \
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

fn row(conversation: i64, space: PeerSpace, msg_id: i32, text: &str) -> Row {
    Row {
        conversation_id: conversation,
        peer_space: space,
        msg_id,
        sent_at: 1_700_000_000,
        sender_id: Some(4242),
        is_outgoing: false,
        kind: MsgKind::Message,
        text: Some(text.to_owned()),
        media_kind: None,
        edited_at: None,
        reply_to_msg_id: None,
        fwd_from_name: None,
        reactions: Vec::new(),
    }
}

async fn text_of(pool: &MySqlPool, conversation: i64, msg_id: i32) -> Option<String> {
    sqlx::query_scalar(
        "SELECT text FROM telegram_messages WHERE conversation_id = ? AND msg_id = ?",
    )
    .bind(conversation)
    .bind(msg_id)
    .fetch_optional(pool)
    .await
    .expect("read the message")
    .flatten()
}

async fn history(pool: &MySqlPool, conversation: i64, msg_id: i32) -> Vec<(Option<i64>, String)> {
    sqlx::query(
        "SELECT was_edited_at, COALESCE(text, '') AS text FROM telegram_message_edits
          WHERE conversation_id = ? AND msg_id = ? ORDER BY id",
    )
    .bind(conversation)
    .bind(msg_id)
    .fetch_all(pool)
    .await
    .expect("read the edit history")
    .iter()
    .map(|r| (r.get("was_edited_at"), r.get("text")))
    .collect()
}

/// ⚠ **The point of the whole edit table.** Telegram's edit keeps the message id,
/// so storing it is an UPDATE — and the words being replaced exist nowhere else at
/// that moment. This is the test that says they are not lost, and that a second
/// delivery of the same edit does not append a duplicate.
#[tokio::test]
async fn an_edit_keeps_the_text_it_replaces_and_a_replay_adds_nothing() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(1);
    let msg = id.msg_base + 1;

    assert_eq!(
        db.store_telegram_message(&row(id.conversation, PeerSpace::User, msg, "first"), None)
            .await
            .expect("insert"),
        TelegramStored::Inserted
    );
    assert!(
        history(&pool, id.conversation, msg).await.is_empty(),
        "a message nobody has edited has no history"
    );

    let edited = Row {
        edited_at: Some(1_700_000_500),
        text: Some("second".to_owned()),
        ..row(id.conversation, PeerSpace::User, msg, "second")
    };
    assert_eq!(
        db.store_telegram_message(&edited, None)
            .await
            .expect("edit"),
        TelegramStored::Edited
    );
    assert_eq!(
        text_of(&pool, id.conversation, msg).await.as_deref(),
        Some("second")
    );
    assert_eq!(
        history(&pool, id.conversation, msg).await,
        vec![(None, "first".to_owned())],
        "the pre-edit text is filed with no edit date, because it had none"
    );

    // The same update again — which `catch_up` will genuinely deliver.
    assert_eq!(
        db.store_telegram_message(&edited, None)
            .await
            .expect("replay"),
        TelegramStored::Unchanged
    );
    assert_eq!(
        history(&pool, id.conversation, msg).await.len(),
        1,
        "a replayed edit must not append a second copy of the same text"
    );

    // A SECOND, genuinely different edit keeps the first edit's text too, filed
    // under the date that version carried.
    let again = Row {
        edited_at: Some(1_700_000_900),
        text: Some("third".to_owned()),
        ..row(id.conversation, PeerSpace::User, msg, "third")
    };
    assert_eq!(
        db.store_telegram_message(&again, None)
            .await
            .expect("second edit"),
        TelegramStored::Edited
    );
    assert_eq!(
        history(&pool, id.conversation, msg).await,
        vec![
            (None, "first".to_owned()),
            (Some(1_700_000_500), "second".to_owned())
        ]
    );
    assert_eq!(
        text_of(&pool, id.conversation, msg).await.as_deref(),
        Some("third")
    );
}

/// ⚠ **The reason `mark_telegram_deleted` takes a scope instead of a conversation.**
/// `updateDeleteMessages` carries no peer, because private chats and basic groups
/// share one message-id sequence — but a CHANNEL has its own, so the same number
/// means a different message there. Applying a peer-less deletion everywhere would
/// retract an unrelated channel post, and this is the test that would catch it.
#[tokio::test]
async fn a_peerless_deletion_does_not_reach_a_channel() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(2);
    let msg = id.msg_base + 1;

    db.upsert_telegram_conversation(id.conversation, ConvKind::Dm, Some("a person"), None)
        .await
        .expect("dm conversation");
    db.upsert_telegram_conversation(id.channel, ConvKind::Channel, Some("a channel"), None)
        .await
        .expect("channel conversation");
    db.store_telegram_message(
        &row(id.conversation, PeerSpace::User, msg, "in the dm"),
        None,
    )
    .await
    .expect("dm message");
    db.store_telegram_message(
        &row(id.channel, PeerSpace::Channel, msg, "in the channel"),
        None,
    )
    .await
    .expect("channel message");

    let hit = db
        .mark_telegram_deleted(&[msg], TelegramDeleteScope::SharedSequence)
        .await
        .expect("delete");
    assert_eq!(hit, 1, "exactly the non-channel row, and nothing else");
    assert!(deleted(&pool, id.conversation, msg).await);
    assert!(
        !deleted(&pool, id.channel, msg).await,
        "a channel post sharing the number must survive"
    );

    // And a channel deletion, which DOES name its peer, reaches only that channel.
    let hit = db
        .mark_telegram_deleted(&[msg], TelegramDeleteScope::Channel(id.channel))
        .await
        .expect("channel delete");
    assert_eq!(hit, 1);
    assert!(deleted(&pool, id.channel, msg).await);
}

async fn deleted(pool: &MySqlPool, conversation: i64, msg_id: i32) -> bool {
    let flag: i8 = sqlx::query_scalar(
        "SELECT deleted FROM telegram_messages WHERE conversation_id = ? AND msg_id = ?",
    )
    .bind(conversation)
    .bind(msg_id)
    .fetch_one(pool)
    .await
    .expect("read the flag");
    flag != 0
}

/// The text of a retracted message is KEPT, as it is for Signal. The viewer
/// decides what to show; the archive does not decide what to hold.
#[tokio::test]
async fn a_deletion_keeps_the_words() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(3);
    let msg = id.msg_base + 1;
    db.upsert_telegram_conversation(id.conversation, ConvKind::Dm, None, None)
        .await
        .expect("conversation");
    db.store_telegram_message(
        &row(id.conversation, PeerSpace::User, msg, "said and unsaid"),
        None,
    )
    .await
    .expect("insert");
    db.mark_telegram_deleted(&[msg], TelegramDeleteScope::SharedSequence)
        .await
        .expect("delete");
    assert_eq!(
        text_of(&pool, id.conversation, msg).await.as_deref(),
        Some("said and unsaid")
    );
}

/// ⚠ A reaction that was taken away is ABSENT from the new list rather than
/// present with a count of zero, so the write has to clear what it is replacing.
/// An upsert alone leaves every reaction a message ever had, at its high-water
/// mark, forever.
#[tokio::test]
async fn a_withdrawn_reaction_stops_being_counted() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(4);
    let msg = id.msg_base + 1;

    let thumb = Reaction {
        emoji: Some("👍".to_owned()),
        custom_emoji_id: None,
        cnt: 2,
        chosen: false,
    };
    let heart = Reaction {
        emoji: Some("❤".to_owned()),
        custom_emoji_id: None,
        cnt: 1,
        chosen: true,
    };
    db.replace_telegram_reactions(id.conversation, msg, &[thumb.clone(), heart])
        .await
        .expect("two reactions");
    assert_eq!(reactions(&pool, id.conversation, msg).await.len(), 2);

    db.replace_telegram_reactions(id.conversation, msg, &[thumb])
        .await
        .expect("one reaction");
    assert_eq!(
        reactions(&pool, id.conversation, msg).await,
        vec![("👍".to_owned(), 2)],
        "the heart is gone, not kept at its last count"
    );

    // ⚠ The documented limit, pinned so it is a decision rather than a surprise:
    // Telegram OMITS the field for a message with no reactions, so an empty list
    // cannot be told from "no news" and does not clear anything. Removing the last
    // reaction is invisible to this archive.
    db.replace_telegram_reactions(id.conversation, msg, &[])
        .await
        .expect("no news");
    assert_eq!(
        reactions(&pool, id.conversation, msg).await.len(),
        1,
        "an empty list is no news, not a clearance"
    );
}

async fn reactions(pool: &MySqlPool, conversation: i64, msg_id: i32) -> Vec<(String, i32)> {
    sqlx::query(
        "SELECT COALESCE(emoji, '') AS emoji, cnt FROM telegram_reactions
          WHERE conversation_id = ? AND msg_id = ? ORDER BY emoji",
    )
    .bind(conversation)
    .bind(msg_id)
    .fetch_all(pool)
    .await
    .expect("read reactions")
    .iter()
    .map(|r| (r.get("emoji"), r.get("cnt")))
    .collect()
}

/// ⚠ The backfill frontier must never move backwards. The live stream stores NEW
/// messages through the same path, and a plain assignment would let one of those
/// reset `oldest_seen` to the top and walk a decade of history again on every
/// restart.
#[tokio::test]
async fn the_backfill_frontier_only_moves_older() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let _ = &pool;
    let id = ids(5);

    db.record_telegram_backfill(id.conversation, Some(500), false, 10)
        .await
        .expect("first page");
    db.record_telegram_backfill(id.conversation, Some(200), false, 10)
        .await
        .expect("older page");
    db.record_telegram_backfill(id.conversation, Some(9_000), false, 1)
        .await
        .expect("a live message with a high id");

    assert_eq!(
        db.telegram_backfill_state(id.conversation)
            .await
            .expect("read state"),
        Some(TelegramBackfill {
            oldest_seen: Some(200),
            complete: false,
            messages_stored: 21,
        })
    );

    // And `complete` latches: an empty page settles it, and a later page that
    // stored something must not unsettle it into walking the history again.
    db.record_telegram_backfill(id.conversation, None, true, 0)
        .await
        .expect("empty page");
    db.record_telegram_backfill(id.conversation, Some(150), false, 1)
        .await
        .expect("a stray older message");
    assert!(
        db.telegram_backfill_state(id.conversation)
            .await
            .expect("read state")
            .expect("a state")
            .complete
    );
}

/// ⚠ The session has to survive being written down, and the AUTH KEY is the part
/// that cannot be regenerated cheaply — a fresh login costs a flood wait measured
/// in hours. A store that round-trips everything except the key would look
/// perfectly healthy until the first restart.
#[tokio::test]
async fn a_session_survives_a_round_trip_with_its_auth_key() {
    let Some((db, _pool)) = connect().await else {
        return;
    };
    use grammers_session::Session;
    use grammers_session::types::{DcOption, PeerId, PeerInfo, UpdateState};

    let session = DbSession::load(db.pool()).await.expect("load");
    let key = [7u8; 256];
    let dc = DcOption {
        id: 2,
        ipv4: "149.154.167.50:443".parse().expect("ipv4"),
        ipv6: "[2001:67c:4e8:f002::a]:443".parse().expect("ipv6"),
        auth_key: Some(key),
    };
    session.set_dc_option(&dc).await.expect("dc");
    session.set_home_dc_id(2).await.expect("home");
    session
        .cache_peer(&PeerInfo::User {
            id: 777,
            auth: None,
            bot: Some(false),
            is_self: Some(true),
        })
        .await
        .expect("self");
    session
        .set_update_state(UpdateState::Primary {
            pts: 4242,
            date: 1_700_000_000,
            seq: 9,
        })
        .await
        .expect("state");
    assert!(session.flush(db.pool()).await.expect("flush"));
    assert!(
        !session.flush(db.pool()).await.expect("second flush"),
        "an unchanged session writes nothing"
    );

    let reloaded = DbSession::load(db.pool()).await.expect("reload");
    assert_eq!(reloaded.home_dc_id().expect("home dc"), 2);
    assert_eq!(
        reloaded
            .dc_option(2)
            .expect("dc option")
            .and_then(|o| o.auth_key),
        Some(key),
        "the auth key is the one thing a fresh login cannot cheaply replace"
    );
    assert_eq!(reloaded.self_id().expect("self id"), Some(777));
    assert!(reloaded.is_authorized().expect("authorized"));
    assert!(
        reloaded
            .peer(PeerId::self_user())
            .await
            .expect("self peer")
            .is_some(),
        "the sentinel still resolves after a round trip"
    );
    assert_eq!(reloaded.updates_state().await.expect("updates").pts, 4242);
}
