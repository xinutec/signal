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

use signal_archiver::db::{
    Db, TelegramBackfill, TelegramDeleteScope, TelegramReadDirection, TelegramStored,
};
use signal_archiver::telegram::ConvKind;
use signal_archiver::telegram::map::{
    Call, Entity, MsgKind, PeerSpace, Reaction, ReactionAuthor, Reactions, Row,
};
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

    // ⚠ **BUT THE ARCHIVE STILL HOLDS IT.** It used to be DELETEd, and that made a
    // re-walk destructive: Telegram returns only the reactions a message has NOW,
    // so re-reading a message whose ❤️ had been taken back erased the archive's
    // record that it ever existed. A deleted message here keeps its words and an
    // edited one keeps every version; a reaction is no different.
    let held = all_reactions(&pool, id.conversation, msg).await;
    assert_eq!(held.len(), 2, "both rows are still here");
    assert_eq!(
        held.iter()
            .find(|(e, _)| e == "❤")
            .map(|(_, removed)| *removed),
        Some(true),
        "the heart is dated, not gone"
    );

    // And putting it back makes it current again rather than adding a second row.
    db.replace_telegram_reactions(id.conversation, msg, &[thumb.clone(), heart.clone()])
        .await
        .expect("the heart returns");
    assert_eq!(reactions(&pool, id.conversation, msg).await.len(), 2);
    assert_eq!(
        all_reactions(&pool, id.conversation, msg).await.len(),
        2,
        "restored in place — a reaction that comes back is the same reaction"
    );

    // Back to one, for the empty-set case below.
    db.replace_telegram_reactions(id.conversation, msg, &[thumb])
        .await
        .expect("withdrawn again");

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

/// Every reaction row the archive holds, current or not, with whether it has been
/// withdrawn.
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

/// What is on the message NOW — the set the viewer draws.
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

/// ⚠ **A message the archive already holds gains facts a later build can see.**
/// This is what makes adding a column possible at all: the backfill marks a
/// conversation `complete` and never returns, and a forced re-walk stores nothing
/// because the insert is IGNOREd and the edit path only fires when `edit_date`
/// moves. Without enrichment, `media_size` would have been NULL forever on every
/// row ingested before it existed — a column the archive could not populate.
///
/// Also pinned: enrichment is IDEMPOTENT, and it does NOT overwrite a value it
/// already has. Media facts come from the message and do not change, so a
/// disagreement means one of the two readings is wrong; taking the newer one
/// silently would hide that.
#[tokio::test]
async fn a_stored_message_is_enriched_with_facts_it_did_not_have() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(6);
    let msg = id.msg_base + 1;

    // As an older build stored it: a photo, with nothing known about its bytes.
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

    // The same message, delivered again by a build that reads sizes.
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

    // Again, unchanged: nothing left to learn, so nothing is written and the
    // outcome says so. This is what keeps a re-walk cheap.
    assert_eq!(
        db.store_telegram_message(&known, None)
            .await
            .expect("replay"),
        TelegramStored::Unchanged
    );

    // ⚠ And a DISAGREEING value is refused rather than taken. If two readings of
    // one message differ about its size, the archive keeps the first and the
    // difference stays visible instead of being quietly resolved.
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

/// ⚠ **THE ENRICHMENT'S ONE GAP, WHICH IS WHY A RE-WALK WOULD NOT HAVE BEEN
/// COMPLETE.**
///
/// `sender_name` is unlike every other column here: it is not derived from the
/// message, it comes from the caller's peer lookup — and that returns nothing
/// when the peer is not in the session cache. So a message can be stored with a
/// `sender_id` and no name through no fault of the message, and the viewer draws
/// it with a BLANK sender.
///
/// It was left out of the enrichment because it predates it, and the gap was
/// silent in the way a missing enrichment always is: nothing fails, the column
/// just stays NULL forever. 652 stored rows were in that state when this was
/// found — 527 of them one conversation whose peer never resolved.
///
/// This pins the repair, and pins that a name once known is not replaced by a
/// later delivery that happens not to know it.
#[tokio::test]
async fn a_name_the_first_delivery_could_not_resolve_is_filled_by_a_later_one() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(11);
    let msg = id.msg_base + 1;
    let r = row(id.conversation, PeerSpace::User, msg, "who said this?");

    // Stored with no name, which is what an unresolved peer looks like.
    assert_eq!(
        db.store_telegram_message(&r, None).await.expect("insert"),
        TelegramStored::Inserted
    );
    assert_eq!(sender_name_of(&pool, id.conversation, msg).await, None);

    // The same message again, this time with the peer resolved.
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

    // ⚠ And a later delivery that does NOT know the name leaves the one we have.
    // A peer drops out of the cache for reasons that have nothing to do with the
    // message, so "I could not resolve it this time" is not evidence the stored
    // name is wrong — and blanking it would make the repair undo itself on the
    // next pass.
    assert_eq!(
        db.store_telegram_message(&r, None).await.expect("replay"),
        TelegramStored::Unchanged
    );
    assert_eq!(
        sender_name_of(&pool, id.conversation, msg).await.as_deref(),
        Some("Tessa")
    );
}

/// ⚠ **THE ONE FACT IN THIS ARCHIVE THAT CANNOT BE RE-FETCHED.** Telegram keeps
/// messages, so anything about them can be recovered by reading again. It keeps
/// no log of READING — a dialog carries only the current high-water marks — so a
/// read not recorded as it happens is gone for good.
///
/// That is why this table is append-only rather than a column holding the latest
/// value, and these are the properties that make it a record rather than a cache
/// of Telegram's current state.
#[tokio::test]
async fn a_read_mark_is_kept_per_advance_and_never_re_dated() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(12);

    // Nothing read yet.
    assert_eq!(
        db.telegram_read_mark(id.conversation, TelegramReadDirection::Outbox)
            .await
            .expect("read"),
        None
    );

    // ⚠ Zero is Telegram's "nothing has been read", not a mark. Storing it would
    // put a row at the bottom of every conversation claiming a read that never
    // happened.
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

    // ⚠ **RE-SEEING A MARK MUST NOT RE-DATE IT.** The sweep re-states every
    // conversation's marks once an hour, so an upsert here would push the
    // observation time forward on every pass — and the answer to "when was this
    // read?" would always be "in the last hour", for every message, forever.
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

    // An ADVANCE is its own row, which is what makes the table a history: the
    // pair (100, then) and (140, later) says when each stretch was read.
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

    // ⚠ The two directions are separate facts about separate people. Sharing a
    // row would make "they read mine" and "I read theirs" overwrite each other,
    // and the outbox one — the only one that says anything about them — would be
    // the loser every time the archive owner opened the chat.
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

/// ⚠ **A SAMPLE MUST NOT RETRACT THE PEOPLE IT COULD NOT SEE.**
///
/// This is v28's lesson one layer down and easier to get wrong, because a short
/// list looks like data rather than like absence. Telegram truncates
/// `recent_reactions` for a message with many reactors, so naming two out of
/// twenty is not a statement that eighteen people changed their minds.
///
/// The complete case is asserted too. Without it this test would pass just as
/// well if the writer never retracted anybody at all, which is a different bug
/// with the same green tick.
#[tokio::test]
async fn a_sampled_list_of_reactors_retracts_nobody() {
    let Some((db, pool)) = connect().await else {
        return;
    };
    let id = ids(11);
    let msg = id.msg_base + 1;

    // Two people, and Telegram says two reacted: a complete statement.
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

    // Now only 101 is named, but the list is a SAMPLE. 102 is still reacting.
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

    // The same short list, now a COMPLETE statement: 102 really has gone.
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

/// ⚠ Re-reading a reaction must not restamp WHEN it happened. The first
/// observation is the one that answers the question; an upsert that wrote
/// `reacted_at` again would drift the answer forward every re-capture, exactly
/// as `telegram_read_marks` documents for its own `observed_at`.
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

    // A later delivery reports the same reaction with a different date.
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

/// ⚠ An entity's identity is its SPAN. Keying on a position in the list would
/// mean an edit that inserts one bold run at the start renumbers everything
/// after it, and the writer would date spans that merely moved.
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

    // An edit puts a bold run in front. The link is now SECOND in the list but
    // is the same span — had it been keyed by index, it would read as removed.
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

/// ⚠ A call's service message exists from the moment the call STARTS, so an early
/// delivery has no duration and a later one does. Enriching rather than
/// overwriting is what stops a re-capture from erasing how long a call took.
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

    // A re-capture that happens to carry no duration must not blank it.
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
    assert_eq!(duration, Some(2_820), "47 minutes is not forgotten");
    assert_eq!(reason.as_deref(), Some("hangup"));
}
