//! Telegram writes, against a real MariaDB.
//!
//! Nothing is cleaned up: each test uses its own conversation ids and message-id
//! range, so its rows start absent. The ranges must not overlap, because
//! `mark_telegram_deleted` reaches across conversations.
//!
//! Skips when `SIGNAL_TEST_DATABASE_URL` is unset, and refuses to skip in CI.

use signal_archiver::db::{
    Db, TelegramBackfill, TelegramDeleteScope, TelegramReadDirection, TelegramStored,
};
use signal_archiver::parse::{CallEvent, CallEventKind, Receipt, ReceiptKind};
use signal_archiver::telegram::ConvKind;
use signal_archiver::telegram::map::{
    Call, Entity, MsgKind, PeerSpace, Reaction, ReactionAuthor, Reactions, Row,
};
use signal_archiver::telegram::session::DbSession;
use sqlx::Row as _;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

/// Distinct ids per test: the pid separates test binaries, `slot` tests within
/// one. The modulo keeps `msg_id` inside an INT.
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
        // A skip passes, so CI must not skip.
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
        media_size: None,
        media_mime: None,
        edited_at: None,
        edit_hidden: false,
        reply_to_msg_id: None,
        fwd_from_id: None,
        fwd_from_name: None,
        fwd_date: None,
        fwd_channel_post: None,
        grouped_id: None,
        via_bot_id: None,
        ttl_period: None,
        reply_quote: None,
        reply_to_peer_id: None,
        service_action: None,
        call: None,
        entities: Vec::new(),
        reactions: Reactions::default(),
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

    // `catch_up` replays updates.
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

    // A second edit files the first edit's text under that version's date.
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

    // A channel deletion names its channel.
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

#[tokio::test]
async fn a_withdrawn_reaction_stops_being_current_without_being_forgotten() {
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
    db.replace_telegram_reactions(id.conversation, msg, &[thumb.clone(), heart.clone()])
        .await
        .expect("two reactions");
    assert_eq!(reactions(&pool, id.conversation, msg).await.len(), 2);

    db.replace_telegram_reactions(id.conversation, msg, std::slice::from_ref(&thumb))
        .await
        .expect("one reaction");
    assert_eq!(
        reactions(&pool, id.conversation, msg).await,
        vec![("👍".to_owned(), 2)],
        "the heart is no longer on the message"
    );

    // Dated, not deleted.
    let held = all_reactions(&pool, id.conversation, msg).await;
    assert_eq!(held.len(), 2, "both rows are still here");
    assert_eq!(
        held.iter()
            .find(|(e, _)| e == "❤")
            .map(|(_, removed)| *removed),
        Some(true),
        "the heart is dated, not gone"
    );

    db.replace_telegram_reactions(id.conversation, msg, &[thumb.clone(), heart.clone()])
        .await
        .expect("the heart returns");
    assert_eq!(reactions(&pool, id.conversation, msg).await.len(), 2);
    assert_eq!(
        all_reactions(&pool, id.conversation, msg).await.len(),
        2,
        "restored in place — a reaction that comes back is the same reaction"
    );

    db.replace_telegram_reactions(id.conversation, msg, &[thumb])
        .await
        .expect("withdrawn again");

    // An empty list is indistinguishable from an absent field; see
    // `replace_telegram_reactions`.
    db.replace_telegram_reactions(id.conversation, msg, &[])
        .await
        .expect("no news");
    assert_eq!(
        reactions(&pool, id.conversation, msg).await.len(),
        1,
        "an empty list is no news, not a clearance"
    );
}

/// Every reaction row, current or not, with whether it was withdrawn.
async fn all_reactions(pool: &MySqlPool, conversation: i64, msg_id: i32) -> Vec<(String, bool)> {
    sqlx::query(
        "SELECT COALESCE(emoji, '') AS emoji, removed_at IS NOT NULL AS removed
           FROM telegram_reactions
          WHERE conversation_id = ? AND msg_id = ? ORDER BY emoji",
    )
    .bind(conversation)
    .bind(msg_id)
    .fetch_all(pool)
    .await
    .expect("read reactions")
    .iter()
    .map(|r| (r.get("emoji"), r.get::<i8, _>("removed") != 0))
    .collect()
}

/// The current reactions.
async fn reactions(pool: &MySqlPool, conversation: i64, msg_id: i32) -> Vec<(String, i32)> {
    sqlx::query(
        "SELECT COALESCE(emoji, '') AS emoji, cnt FROM telegram_reactions
          WHERE conversation_id = ? AND msg_id = ? AND removed_at IS NULL
          ORDER BY emoji",
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

    // `complete` latches.
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

#[tokio::test]
async fn a_stored_message_is_enriched_with_facts_it_did_not_have() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(6);
    let msg = id.msg_base + 1;

    let bare = Row {
        media_kind: Some(signal_archiver::telegram::map::MediaKind::Photo),
        ..row(id.conversation, PeerSpace::User, msg, "look at this")
    };
    assert_eq!(
        db.store_telegram_message(&bare, None)
            .await
            .expect("insert"),
        TelegramStored::Inserted
    );
    assert_eq!(media_facts(&pool, id.conversation, msg).await, (None, None));

    let known = Row {
        media_size: Some(204_800),
        media_mime: Some("image/jpeg".to_owned()),
        ..bare.clone()
    };
    assert_eq!(
        db.store_telegram_message(&known, None)
            .await
            .expect("enrich"),
        TelegramStored::Enriched
    );
    assert_eq!(
        media_facts(&pool, id.conversation, msg).await,
        (Some(204_800), Some("image/jpeg".to_owned()))
    );

    assert_eq!(
        db.store_telegram_message(&known, None)
            .await
            .expect("replay"),
        TelegramStored::Unchanged
    );

    let disagrees = Row {
        media_size: Some(999_999),
        ..known.clone()
    };
    assert_eq!(
        db.store_telegram_message(&disagrees, None)
            .await
            .expect("disagreement"),
        TelegramStored::Unchanged
    );
    assert_eq!(
        media_facts(&pool, id.conversation, msg).await.0,
        Some(204_800),
        "the first reading stands"
    );
}

async fn media_facts(
    pool: &MySqlPool,
    conversation: i64,
    msg_id: i32,
) -> (Option<i64>, Option<String>) {
    let row: (Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT media_size, media_mime FROM telegram_messages
          WHERE conversation_id = ? AND msg_id = ?",
    )
    .bind(conversation)
    .bind(msg_id)
    .fetch_one(pool)
    .await
    .expect("read the media facts");
    row
}

async fn sender_name_of(pool: &MySqlPool, conversation: i64, msg_id: i32) -> Option<String> {
    sqlx::query_scalar(
        "SELECT sender_name FROM telegram_messages WHERE conversation_id = ? AND msg_id = ?",
    )
    .bind(conversation)
    .bind(msg_id)
    .fetch_one(pool)
    .await
    .expect("the row is there")
}

/// `sender_name` comes from the caller's peer lookup, which can fail for reasons
/// unrelated to the message.
#[tokio::test]
async fn a_name_the_first_delivery_could_not_resolve_is_filled_by_a_later_one() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(11);
    let msg = id.msg_base + 1;
    let r = row(id.conversation, PeerSpace::User, msg, "who said this?");

    assert_eq!(
        db.store_telegram_message(&r, None).await.expect("insert"),
        TelegramStored::Inserted
    );
    assert_eq!(sender_name_of(&pool, id.conversation, msg).await, None);

    assert_eq!(
        db.store_telegram_message(&r, Some("Tessa"))
            .await
            .expect("enrich"),
        TelegramStored::Enriched
    );
    assert_eq!(
        sender_name_of(&pool, id.conversation, msg).await.as_deref(),
        Some("Tessa")
    );

    assert_eq!(
        db.store_telegram_message(&r, None).await.expect("replay"),
        TelegramStored::Unchanged
    );
    assert_eq!(
        sender_name_of(&pool, id.conversation, msg).await.as_deref(),
        Some("Tessa")
    );
}

#[tokio::test]
async fn a_read_mark_is_kept_per_advance_and_never_re_dated() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(12);

    assert_eq!(
        db.telegram_read_mark(id.conversation, TelegramReadDirection::Outbox)
            .await
            .expect("read"),
        None
    );

    assert!(
        !db.record_telegram_read_mark(id.conversation, TelegramReadDirection::Outbox, 0)
            .await
            .expect("zero"),
        "0 is not a mark"
    );

    assert!(
        db.record_telegram_read_mark(id.conversation, TelegramReadDirection::Outbox, 100)
            .await
            .expect("first")
    );
    let (max_id, first_seen) = db
        .telegram_read_mark(id.conversation, TelegramReadDirection::Outbox)
        .await
        .expect("read")
        .expect("a mark");
    assert_eq!(max_id, 100);

    // The hourly sweep restates marks.
    assert!(
        !db.record_telegram_read_mark(id.conversation, TelegramReadDirection::Outbox, 100)
            .await
            .expect("again"),
        "a mark already held writes nothing"
    );
    assert_eq!(
        db.telegram_read_mark(id.conversation, TelegramReadDirection::Outbox)
            .await
            .expect("read"),
        Some((100, first_seen)),
        "the first sighting keeps its time"
    );

    assert!(
        db.record_telegram_read_mark(id.conversation, TelegramReadDirection::Outbox, 140)
            .await
            .expect("advance")
    );
    assert_eq!(
        db.telegram_read_mark(id.conversation, TelegramReadDirection::Outbox)
            .await
            .expect("read")
            .map(|(m, _)| m),
        Some(140)
    );
    let rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM telegram_read_marks WHERE conversation_id = ? AND direction = 'outbox'",
    )
    .bind(id.conversation)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(rows, 2, "both advances are kept, not one row moved");

    assert!(
        db.record_telegram_read_mark(id.conversation, TelegramReadDirection::Inbox, 7)
            .await
            .expect("inbox")
    );
    assert_eq!(
        db.telegram_read_mark(id.conversation, TelegramReadDirection::Outbox)
            .await
            .expect("read")
            .map(|(m, _)| m),
        Some(140),
        "the outbox mark is untouched by an inbox one"
    );
}

async fn reaction_authors(
    pool: &MySqlPool,
    conversation: i64,
    msg_id: i32,
) -> Vec<(i64, String, bool)> {
    sqlx::query(
        "SELECT peer_id, COALESCE(emoji, '') AS emoji, removed_at IS NOT NULL AS removed
           FROM telegram_reaction_authors
          WHERE conversation_id = ? AND msg_id = ? ORDER BY peer_id, emoji",
    )
    .bind(conversation)
    .bind(msg_id)
    .fetch_all(pool)
    .await
    .expect("read reaction authors")
    .iter()
    .map(|r| {
        (
            r.get("peer_id"),
            r.get("emoji"),
            r.get::<i8, _>("removed") != 0,
        )
    })
    .collect()
}

fn author(peer_id: i64, emoji: &str, reacted_at: i64) -> ReactionAuthor {
    ReactionAuthor {
        peer_id,
        emoji: Some(emoji.to_owned()),
        custom_emoji_id: None,
        reacted_at,
    }
}

/// The complete case is asserted too, or a writer that never retracts would pass.
#[tokio::test]
async fn a_sampled_list_of_reactors_retracts_nobody() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(11);
    let msg = id.msg_base + 1;

    db.record_telegram_reaction_authors(
        id.conversation,
        msg,
        &Reactions {
            counts: Vec::new(),
            authors: vec![
                author(101, "👍", 1_700_000_100),
                author(102, "❤", 1_700_000_200),
            ],
            complete: true,
        },
    )
    .await
    .expect("two reactors");
    assert_eq!(reaction_authors(&pool, id.conversation, msg).await.len(), 2);

    // Only 101 named, but truncated.
    db.record_telegram_reaction_authors(
        id.conversation,
        msg,
        &Reactions {
            counts: Vec::new(),
            authors: vec![author(101, "👍", 1_700_000_100)],
            complete: false,
        },
    )
    .await
    .expect("a sample");
    assert_eq!(
        reaction_authors(&pool, id.conversation, msg).await,
        vec![(101, "👍".to_owned(), false), (102, "❤".to_owned(), false),],
        "a truncated list may not date anybody"
    );

    db.record_telegram_reaction_authors(
        id.conversation,
        msg,
        &Reactions {
            counts: Vec::new(),
            authors: vec![author(101, "👍", 1_700_000_100)],
            complete: true,
        },
    )
    .await
    .expect("a complete list");
    assert_eq!(
        reaction_authors(&pool, id.conversation, msg).await,
        vec![(101, "👍".to_owned(), false), (102, "❤".to_owned(), true)],
        "a complete list dates who is missing — and keeps the row"
    );
}

#[tokio::test]
async fn re_reading_a_reaction_keeps_the_moment_it_happened() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(12);
    let msg = id.msg_base + 1;

    let first = Reactions {
        counts: Vec::new(),
        authors: vec![author(303, "👍", 1_700_000_100)],
        complete: true,
    };
    db.record_telegram_reaction_authors(id.conversation, msg, &first)
        .await
        .expect("first read");

    let later = Reactions {
        counts: Vec::new(),
        authors: vec![author(303, "👍", 1_888_888_888)],
        complete: true,
    };
    db.record_telegram_reaction_authors(id.conversation, msg, &later)
        .await
        .expect("second read");

    let when: i64 = sqlx::query_scalar(
        "SELECT reacted_at FROM telegram_reaction_authors
          WHERE conversation_id = ? AND msg_id = ? AND peer_id = 303",
    )
    .bind(id.conversation)
    .bind(msg)
    .fetch_one(&pool)
    .await
    .expect("read the date");
    assert_eq!(when, 1_700_000_100, "the first observation stands");
}

#[tokio::test]
async fn an_entity_that_only_moved_is_not_an_entity_that_went_away() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(13);
    let msg = id.msg_base + 1;

    let link = Entity {
        kind: "textUrl",
        offset_utf16: 4,
        length_utf16: 4,
        url: Some("https://example.org/somewhere".to_owned()),
        user_id: None,
        language: None,
        document_id: None,
    };
    db.replace_telegram_entities(id.conversation, msg, std::slice::from_ref(&link))
        .await
        .expect("one link");

    // A bold run in front makes the link second in the list, same span.
    let bold = Entity {
        kind: "bold",
        offset_utf16: 0,
        length_utf16: 3,
        url: None,
        user_id: None,
        language: None,
        document_id: None,
    };
    db.replace_telegram_entities(id.conversation, msg, &[bold, link.clone()])
        .await
        .expect("two entities");

    let live: Vec<(String, i32)> = sqlx::query(
        "SELECT kind, offset_utf16 FROM telegram_message_entities
          WHERE conversation_id = ? AND msg_id = ? AND removed_at IS NULL
          ORDER BY offset_utf16",
    )
    .bind(id.conversation)
    .bind(msg)
    .fetch_all(&pool)
    .await
    .expect("read entities")
    .iter()
    .map(|r| (r.get("kind"), r.get("offset_utf16")))
    .collect();
    assert_eq!(
        live,
        vec![("bold".to_owned(), 0), ("textUrl".to_owned(), 4)],
        "both are current; the link did not move and was not retracted"
    );
}

#[tokio::test]
async fn a_call_learns_its_duration_without_losing_it_again() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(14);
    let msg = id.msg_base + 1;

    db.record_telegram_call(
        id.conversation,
        msg,
        &Call {
            call_id: 5150,
            duration_s: Some(2_820),
            reason: Some("hangup"),
            video: true,
        },
    )
    .await
    .expect("a finished call");

    db.record_telegram_call(
        id.conversation,
        msg,
        &Call {
            call_id: 5150,
            duration_s: None,
            reason: None,
            video: true,
        },
    )
    .await
    .expect("a thinner report");

    let (duration, reason): (Option<i32>, Option<String>) = sqlx::query_as(
        "SELECT duration_s, reason FROM telegram_calls
          WHERE conversation_id = ? AND msg_id = ?",
    )
    .bind(id.conversation)
    .bind(msg)
    .fetch_one(&pool)
    .await
    .expect("read the call");
    assert_eq!(duration, Some(2_820), "the duration is kept");
    assert_eq!(reason.as_deref(), Some("hangup"));
}

#[tokio::test]
async fn the_recapture_frontier_only_moves_forward() {
    let Some((db, _pool)) = connect().await else {
        return;
    };
    let id = ids(15);

    for offset in 0..3 {
        db.store_telegram_message(
            &row(
                id.conversation,
                PeerSpace::User,
                id.msg_base + offset,
                "held",
            ),
            None,
        )
        .await
        .expect("store");
    }

    let first = db
        .telegram_recapture_batch(id.conversation, 2)
        .await
        .expect("a batch");
    assert_eq!(first, vec![id.msg_base, id.msg_base + 1]);

    db.record_telegram_recapture(id.conversation, id.msg_base + 1)
        .await
        .expect("advance");
    assert_eq!(
        db.telegram_recapture_batch(id.conversation, 2)
            .await
            .expect("a batch"),
        vec![id.msg_base + 2],
        "the batch resumes past the frontier rather than repeating it"
    );

    db.record_telegram_recapture(id.conversation, id.msg_base)
        .await
        .expect("a late, lower report");
    assert_eq!(
        db.telegram_recapture_batch(id.conversation, 2)
            .await
            .expect("a batch"),
        vec![id.msg_base + 2],
        "the frontier did not walk backwards"
    );

    db.record_telegram_recapture(id.conversation, id.msg_base + 2)
        .await
        .expect("finish");
    assert!(
        db.telegram_recapture_batch(id.conversation, 2)
            .await
            .expect("a batch")
            .is_empty(),
        "an exhausted conversation stops the loop"
    );
}

#[tokio::test]
async fn a_signal_receipt_is_kept_once_and_never_re_dated() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    // `signal_receipts` is keyed on send time, which is global.
    let base = 1_900_000_000_000 + i64::from(std::process::id() % 20_000) * 100;

    let first = Receipt {
        author: "alice".to_owned(),
        kind: ReceiptKind::Read,
        when_ts: base + 10,
        targets: vec![base + 1, base + 2],
    };
    assert_eq!(
        db.record_signal_receipt(&first).await.expect("first"),
        2,
        "one receipt, two messages, two rows"
    );

    let replay = Receipt {
        when_ts: base + 999,
        ..first.clone()
    };
    assert_eq!(
        db.record_signal_receipt(&replay).await.expect("replay"),
        0,
        "a replayed receipt teaches nothing"
    );

    let when: i64 = sqlx::query_scalar(
        "SELECT when_ts FROM signal_receipts
          WHERE target_ts = ? AND author_uuid = 'alice' AND kind = 'read'",
    )
    .bind(base + 1)
    .fetch_one(&pool)
    .await
    .expect("read it back");
    assert_eq!(when, base + 10, "the first observation stands");

    let delivered = Receipt {
        author: "alice".to_owned(),
        kind: ReceiptKind::Delivery,
        when_ts: base + 5,
        targets: vec![base + 1],
    };
    assert_eq!(
        db.record_signal_receipt(&delivered)
            .await
            .expect("delivery"),
        1,
        "delivered and read coexist on one message"
    );
}

#[tokio::test]
async fn a_call_keeps_every_frame_that_arrived() {
    let Some((db, _pool)) = connect().await else {
        return;
    };
    let call_id = 8_000_000 + i64::from(std::process::id() % 20_000);

    let offer = CallEvent {
        call_id,
        peer: "bob".to_owned(),
        event: CallEventKind::Offer,
        detail: Some("audio_call".to_owned()),
        device_id: None,
        event_ts: 1_900_000_000_000,
    };
    assert_eq!(db.record_signal_call_event(&offer).await.expect("offer"), 1);
    assert_eq!(
        db.record_signal_call_event(&offer).await.expect("replay"),
        0,
        "the same frame twice is one frame"
    );

    let hangup_a = CallEvent {
        event: CallEventKind::Hangup,
        detail: Some("normal".to_owned()),
        device_id: Some(1),
        event_ts: 1_900_000_060_000,
        ..offer.clone()
    };
    let hangup_b = CallEvent {
        device_id: Some(2),
        event_ts: 1_900_000_060_500,
        ..hangup_a.clone()
    };
    assert_eq!(db.record_signal_call_event(&hangup_a).await.expect("a"), 1);
    assert_eq!(
        db.record_signal_call_event(&hangup_b).await.expect("b"),
        1,
        "a second device hanging up is a second frame, not a duplicate"
    );
}
