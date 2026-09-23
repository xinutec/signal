//! `telegram` — the archive's Telegram feed: one login, both history and live.
//!
//! Telegram keeps history server-side, so one session pages backwards through
//! everything while holding the update stream; both write under the same dedupe
//! key.
//!
//! ```text
//!                       ┌── iter_dialogs ──▶ conversations + the peer cache
//!  Telegram ──MTProto──▶├── iter_messages ─▶ backfill, oldest-ward, resumable
//!  (as Pippijn)         └── stream_updates ▶ new, edited and deleted, live
//!                                    │
//!                                    ▼
//!                             signal MariaDB  (telegram_*)
//! ```
//!
//! A user client, not a bot: a bot is a separate account and cannot read its
//! owner's chats. The stored session is therefore a credential; see
//! `telegram::session`. Secret chats are device-local and unreachable.
//!
//! Four modes:
//!
//! * `telegram login <phone>` — interactive, once. Reads the code from stdin and
//!   stores the session. Run with `kubectl run -it` or locally against the same
//!   database. The number is an argument because the Deployment's env holds only
//!   what the archiver reads.
//! * `telegram` — the archiver. Refuses to start without a stored session.
//! * `telegram recapture` — re-reads every stored message so later columns get
//!   filled; see [`recapture`].
//! * `telegram probe` — read-only; counts how often each optional field is
//!   actually set; see [`probe`].
//!
//! Config via env: `DB_HOST`, `DB_PORT` (3306), `DB_NAME`, `DB_USER`,
//! `DB_PASSWORD`, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use grammers_client::Client;
use grammers_client::client::UpdatesConfiguration;
use grammers_client::session::Session;
use grammers_client::session::types::{PeerId, PeerRef};
use grammers_client::session::updates::UpdatesLike;
use grammers_client::update::Update;
use grammers_mtsender::SenderPool;
use signal_archiver::db::{
    Db, TelegramDeleteScope, TelegramMediaState, TelegramReadDirection, TelegramStored,
};
use signal_archiver::telegram::map::{self, Row};
use signal_archiver::telegram::session::DbSession;
use signal_archiver::telegram::{ConvKind, peer_name};
use sqlx::{AssertSqlSafe, Row as _};

/// How often a changed session is written back. Losing up to this much costs a
/// few replayed updates.
const SESSION_FLUSH: Duration = Duration::from_secs(10);

/// Messages per history page; Telegram's cap.
const PAGE: usize = 100;

/// A pause between history pages, so a backfill leaves room on the connection
/// for the live stream.
const PAGE_PAUSE: Duration = Duration::from_millis(400);

/// A pause between backfill sweeps once every conversation is complete.
const IDLE_SWEEP: Duration = Duration::from_secs(3600);

/// How often the feed looks for media a reader asked for. A poll, because the
/// pod holding the Telegram session accepts no connections.
const REQUEST_POLL: Duration = Duration::from_secs(3);

/// Asked-for files per poll. Requested media is the large kind, so one at a time
/// keeps the live stream's connection usable.
const REQUEST_BATCH: i64 = 1;

/// The largest media fetched without being asked. Every photo fits; most video
/// does not. A size rather than a `media_kind` test because the bytes are the
/// cost.
const EAGER_MAX_BYTES: i64 = 4 * 1024 * 1024;

#[derive(Clone)]
struct Cfg {
    api_id: i32,
    api_hash: String,
    /// Where fetched media is written; the viewer mounts it read-only.
    media_dir: String,
}

fn cfg() -> Result<Cfg> {
    Ok(Cfg {
        api_id: std::env::var("TELEGRAM_API_ID")
            .context("TELEGRAM_API_ID not set")?
            .parse()
            .context("TELEGRAM_API_ID is not a number")?,
        api_hash: std::env::var("TELEGRAM_API_HASH").context("TELEGRAM_API_HASH not set")?,
        media_dir: std::env::var("TELEGRAM_MEDIA_DIR")
            .unwrap_or_else(|_| "/telegram-media".to_owned()),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,signal_archiver=debug".into()),
        )
        .init();

    let cfg = cfg()?;
    let db = Db::connect(&signal_archiver::db::url_from_env()?)
        .await
        .context("connecting to MariaDB")?;
    let session = Arc::new(DbSession::load(db.pool()).await?);

    let pool = SenderPool::new(Arc::clone(&session), cfg.api_id);
    let SenderPool {
        runner,
        handle,
        updates,
    } = pool;
    tokio::spawn(runner.run());
    let client = Client::new(handle);

    // The flusher outlives both modes: `login` generates the auth key that must
    // survive, and the archiver mutates the update state continuously.
    tokio::spawn(flush_session(
        Arc::clone(&session),
        db.pool().clone(),
        SESSION_FLUSH,
    ));

    let mut args = std::env::args().skip(1);
    let mode = args.next();
    match mode.as_deref() {
        Some("login") => {
            let phone = args
                .next()
                .context("usage: telegram login <phone in E.164, e.g. +31…>")?;
            login(&client, &cfg, &phone).await?;
            // Now, not on the timer: the new auth key is expensive to recreate.
            session.flush(db.pool()).await?;
            tracing::info!("logged in; the session is stored");
            Ok(())
        }
        Some("probe") => probe(&client, &db).await,
        Some("recapture") => {
            let self_id = session.self_id()?.context("not logged in")?;
            recapture(&client, &db, self_id).await
        }
        None => archive(&client, &db, &cfg, &session, updates).await,
        Some(other) => {
            bail!(
                "unknown mode {other:?}; expected `login <phone>`, `probe`, `recapture`, \
             or no argument"
            )
        }
    }
}

/// Write the session back whenever it has changed.
async fn flush_session(
    session: Arc<DbSession>,
    pool: sqlx::mysql::MySqlPool,
    every: Duration,
) -> ! {
    loop {
        tokio::time::sleep(every).await;
        match session.flush(&pool).await {
            Ok(true) => tracing::debug!("session flushed"),
            Ok(false) => {}
            // Not fatal: the in-memory session is correct and the next tick retries.
            Err(e) => tracing::error!("could not flush the Telegram session: {e:#}"),
        }
    }
}

/// The one-time interactive login.
async fn login(client: &Client, cfg: &Cfg, phone: &str) -> Result<()> {
    if client.is_authorized().await? {
        tracing::info!("already logged in; nothing to do");
        return Ok(());
    }
    let token = client
        .request_login_code(phone, &cfg.api_hash)
        .await
        .context("requesting a login code")?;
    let code = prompt("the code Telegram just sent: ")?;

    use grammers_client::client::SignInError;
    match client.sign_in(&token, &code).await {
        Ok(user) => {
            tracing::info!("signed in as {}", user.full_name());
            Ok(())
        }
        Err(SignInError::PasswordRequired(token)) => {
            // 2FA.
            if let Some(hint) = token.hint() {
                println!("two-factor password (hint: {hint})");
            }
            let password = prompt("password: ")?;
            let user = client
                .check_password(token, password.trim())
                .await
                .context("checking the two-factor password")?;
            tracing::info!("signed in as {}", user.full_name());
            Ok(())
        }
        Err(SignInError::InvalidCode) => bail!("that code was not valid; run `login` again"),
        Err(SignInError::SignUpRequired) => {
            bail!("this number has no Telegram account, and a third-party client cannot create one")
        }
        Err(e) => Err(e).context("signing in"),
    }
}

fn prompt(what: &str) -> Result<String> {
    use std::io::{BufRead, Write};
    print!("{what}");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    Ok(line.trim().to_owned())
}

/// The archiver: dialogs, then live updates, then history.
async fn archive(
    client: &Client,
    db: &Db,
    cfg: &Cfg,
    session: &Arc<DbSession>,
    updates: tokio::sync::mpsc::UnboundedReceiver<UpdatesLike>,
) -> Result<()> {
    // A feed that is not logged in would look like a quiet account, so refuse.
    if !session.is_authorized()? {
        bail!(
            "no Telegram session is stored: run `telegram login` once against this database \
             (see the module docs). Refusing to run as a feed that cannot read anything."
        );
    }
    let self_id = session.self_id()?.context(
        "the stored session has no self user, which should be impossible once authorised",
    )?;

    // Before the update stream: catch-up can close a gap only for peers in the
    // session cache, and listing the dialogs is what fills it.
    let pending = sweep(client, db).await?;
    tracing::info!("{} conversation(s) with history to walk", pending.len());

    let requests = {
        let client = client.clone();
        let db = db.clone();
        let cfg = cfg.clone();
        let session = Arc::clone(session);
        tokio::spawn(async move { serve_requests(&client, &db, &cfg, &session).await })
    };

    let live = {
        let client = client.clone();
        let db = db.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move { follow(&client, &db, &cfg, self_id, updates).await })
    };
    let history = {
        let client = client.clone();
        let db = db.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move { backfill(&client, &db, &cfg, self_id).await })
    };

    // Any task ending is fatal: a half-running feed looks healthy from outside.
    tokio::select! {
        r = live => r.context("the update stream task panicked")?.context("following updates"),
        r = history => r.context("the backfill task panicked")?.context("backfilling history"),
        r = requests => r.context("the request task panicked")?.context("serving requests"),
    }
}

/// A bare `channel_id` normalised like every other peer, so a supergroup's read
/// marks share its messages' id.
fn channel_peer(channel_id: i64) -> i64 {
    map::normalise_peer(&grammers_tl_types::enums::Peer::Channel(
        grammers_tl_types::types::PeerChannel { channel_id },
    ))
    .0
}

/// Both read marks a dialog carries, stored if either has moved.
async fn read_marks(db: &Db, id: i64, dialog: &grammers_client::peer::Dialog) -> Result<()> {
    let (inbox, outbox) = match &dialog.raw {
        grammers_tl_types::enums::Dialog::Dialog(d) => (d.read_inbox_max_id, d.read_outbox_max_id),
        grammers_tl_types::enums::Dialog::Folder(_) => return Ok(()),
    };
    for (direction, max_id) in [
        (TelegramReadDirection::Inbox, inbox),
        (TelegramReadDirection::Outbox, outbox),
    ] {
        if db.record_telegram_read_mark(id, direction, max_id).await? {
            tracing::info!(
                "conversation {id}: {} read up to {max_id}",
                direction.as_str()
            );
        }
    }
    Ok(())
}

/// List every conversation, record its kind and name, and return those whose
/// history is unfinished. One `getDialogs` listing per call; listing per page
/// draws `FLOOD_WAIT`. The peer is in hand here, so `ConvKind::from_peer` can
/// tell a supergroup from a broadcast.
async fn sweep(client: &Client, db: &Db) -> Result<Vec<(i64, PeerRef)>> {
    let mut dialogs = client.iter_dialogs();
    let mut pending = Vec::new();
    while let Some(dialog) = dialogs.next().await.context("listing dialogs")? {
        let peer = dialog.peer();
        let Some(id) = peer.id().bot_api_dialog_id() else {
            // The self-user sentinel.
            continue;
        };
        db.upsert_telegram_conversation(
            id,
            ConvKind::from_peer(peer),
            peer_name(peer).as_deref(),
            peer.username(),
        )
        .await?;
        // Before the `complete` skip. Live updates can be missed; the sweep restates
        // the current marks, so a miss costs lateness rather than the mark.
        read_marks(db, id, &dialog).await?;
        if db
            .telegram_backfill_state(id)
            .await?
            .is_some_and(|state| state.complete)
        {
            continue;
        }
        match peer
            .to_ref()
            .await
            .map_err(|e| anyhow::anyhow!("resolving {id}: {e}"))?
        {
            Some(peer_ref) => pending.push((id, peer_ref)),
            None => tracing::warn!("no reference for conversation {id}; skipping this sweep"),
        }
    }
    Ok(pending)
}

/// Sender names for messages that do not carry their users. A live DM
/// (`updateShortMessage`) names its sender by id only, so the name is resolved
/// through the session's peer cache.
///
/// Memoised for the process lifetime: catch-up can replay hundreds of updates,
/// and a lookup each would draw a flood wait. Failures are not cached.
#[derive(Default)]
struct Names(tokio::sync::Mutex<HashMap<PeerId, String>>);

impl Names {
    /// Learn our own name via `get_me`, seeded under both the self sentinel
    /// (outgoing DMs) and the real id (outgoing group messages).
    ///
    /// `resolve_peer` on the sentinel fails with `Dropped`: the request succeeds,
    /// but the result is looked up under the sentinel rather than the real id.
    async fn learn_self(&self, client: &Client, self_id: i64) {
        let name = match client.get_me().await {
            Ok(me) => me.full_name(),
            // Not fatal; enrichment fills the name on a later delivery.
            Err(e) => {
                tracing::warn!(
                    "could not ask Telegram who this session is: {e}; \
                     outgoing messages will be stored without a sender name"
                );
                return;
            }
        };
        if name.is_empty() {
            return;
        }
        let mut cache = self.0.lock().await;
        cache.insert(PeerId::self_user(), name.clone());
        if let Some(id) = PeerId::user(self_id) {
            cache.insert(id, name);
        }
    }

    async fn of(
        &self,
        client: &Client,
        message: &grammers_client::message::Message,
    ) -> Option<String> {
        if let Some(name) = message.sender().and_then(peer_name) {
            return Some(name);
        }
        let id = message.sender_id()?;
        if let Some(name) = self.0.lock().await.get(&id) {
            return Some(name.clone());
        }
        let peer_ref = match message.sender_ref().await {
            Ok(peer_ref) => peer_ref?,
            Err(e) => {
                tracing::warn!("no reference for sender {id:?}: {e}");
                return None;
            }
        };
        let name = match client.resolve_peer(peer_ref).await {
            Ok(peer) => peer_name(&peer)?,
            Err(e) => {
                tracing::warn!("could not resolve sender {id:?}: {e}");
                return None;
            }
        };
        self.0.lock().await.insert(id, name.clone());
        Some(name)
    }
}

/// Messages per re-capture batch. `messages.getMessages` takes at most 100 ids.
const RECAPTURE_PAGE: u32 = 100;

/// Re-read every stored message so columns added after it landed get filled by
/// `store_telegram_message`'s enrichment. The backfill never revisits a
/// completed conversation, so this is the only way to fill them.
///
/// A message Telegram no longer has comes back as a hole and is skipped, not
/// marked deleted. Expect `FLOOD_WAIT`; grammers sleeps through it, and progress
/// is saved per batch.
///
/// Run with the feed scaled to zero: two clients on one session would both write
/// the update state.
async fn recapture(client: &Client, db: &Db, self_id: i64) -> Result<()> {
    let names = Names::default();
    names.learn_self(client, self_id).await;

    let mut conversations = Vec::new();
    let mut dialogs = client.iter_dialogs();
    while let Some(dialog) = dialogs.next().await.context("listing dialogs")? {
        let peer = dialog.peer();
        let Some(id) = peer.id().bot_api_dialog_id() else {
            continue;
        };
        match peer.to_ref().await {
            Ok(Some(peer_ref)) => conversations.push((id, peer_ref)),
            _ => tracing::warn!("no reference for conversation {id}; it cannot be re-captured"),
        }
    }
    tracing::info!("re-capturing {} conversation(s)", conversations.len());

    let mut total = 0u64;
    for (conversation_id, peer_ref) in conversations {
        let held = db.telegram_message_count(conversation_id).await?;
        let mut done = 0u64;
        loop {
            let ids = db
                .telegram_recapture_batch(conversation_id, RECAPTURE_PAGE)
                .await?;
            let Some(&last) = ids.last() else {
                break;
            };
            let fetched = client
                .get_messages_by_id(peer_ref, &ids)
                .await
                .with_context(|| format!("re-reading {} ids from {conversation_id}", ids.len()))?;
            let mut present = 0u64;
            for message in fetched.into_iter().flatten() {
                present += 1;
                let sender = names.of(client, &message).await;
                store(db, self_id, &message.raw, sender).await?;
            }
            // After the writes; see migration v39.
            db.record_telegram_recapture(conversation_id, last).await?;
            done += ids.len() as u64;
            total += present;
            tracing::info!(
                "conversation {conversation_id}: {done}/{held} re-read \
                 ({present} of {} still on Telegram)",
                ids.len()
            );
        }
    }
    tracing::info!("re-capture finished; {total} message(s) re-read");
    Ok(())
}

/// Count how often each optional field is actually set, over a sample of stored
/// messages. The TL schema says what can arrive, not what does.
///
/// Run with the feed scaled to zero, as for [`recapture`]. Prints counts only,
/// never message text.
async fn probe(client: &Client, db: &Db) -> Result<()> {
    let mut refs: HashMap<i64, PeerRef> = HashMap::new();
    let mut dialogs = client.iter_dialogs();
    while let Some(dialog) = dialogs.next().await.context("listing dialogs")? {
        let peer = dialog.peer();
        if let Some(id) = peer.id().bot_api_dialog_id()
            && let Ok(Some(peer_ref)) = peer.to_ref().await
        {
            refs.insert(id, peer_ref);
        }
    }

    // Sampled by shape; a uniform sample would be almost all plain text.
    let sql = "(SELECT DISTINCT conversation_id, msg_id FROM telegram_reactions \
                 WHERE removed_at IS NULL ORDER BY RAND() LIMIT 200) \
               UNION (SELECT conversation_id, msg_id FROM telegram_messages WHERE kind = 'service') \
               UNION (SELECT conversation_id, msg_id FROM telegram_messages \
                 WHERE reply_to_msg_id IS NOT NULL ORDER BY RAND() LIMIT 150) \
               UNION (SELECT conversation_id, msg_id FROM telegram_messages \
                 WHERE fwd_from_id IS NOT NULL OR fwd_from_name IS NOT NULL) \
               UNION (SELECT conversation_id, msg_id FROM telegram_messages \
                 WHERE text LIKE '%http%' ORDER BY RAND() LIMIT 150) \
               UNION (SELECT conversation_id, msg_id FROM telegram_messages \
                 WHERE media_kind IS NOT NULL ORDER BY RAND() LIMIT 150) \
               UNION (SELECT conversation_id, msg_id FROM telegram_messages \
                 WHERE kind = 'message' ORDER BY RAND() LIMIT 150)";
    let rows = sqlx::query(AssertSqlSafe(sql)).fetch_all(db.pool()).await?;

    let mut by_conversation: HashMap<i64, Vec<i32>> = HashMap::new();
    for row in &rows {
        let conversation_id: i64 = row.try_get("conversation_id")?;
        let msg_id: i32 = row.try_get("msg_id")?;
        by_conversation
            .entry(conversation_id)
            .or_default()
            .push(msg_id);
    }

    let mut seen = 0usize;
    let mut tally: BTreeMap<String, usize> = BTreeMap::new();
    let mut note = |tally: &mut BTreeMap<String, usize>, k: &str, on: bool| {
        if on {
            *tally.entry(k.to_owned()).or_default() += 1;
        }
    };

    for (conversation_id, ids) in &by_conversation {
        let Some(peer_ref) = refs.get(conversation_id) else {
            tracing::warn!("conversation {conversation_id} is not in the dialog list; skipped");
            continue;
        };
        // `messages.getMessages` takes at most 100 ids per call.
        for chunk in ids.chunks(100) {
            let fetched = client
                .get_messages_by_id(*peer_ref, chunk)
                .await
                .with_context(|| format!("fetching {} ids from {conversation_id}", chunk.len()))?;
            for message in fetched.into_iter().flatten() {
                seen += 1;
                probe_one(&message.raw, &mut tally, &mut note);
            }
        }
    }

    tracing::info!("probed {seen} message(s); fields present:");
    for (field, n) in &tally {
        let pct = (*n as f64) * 100.0 / (seen.max(1) as f64);
        tracing::info!("  {field:<34} {n:>6}  ({pct:.1}%)");
    }
    Ok(())
}

fn probe_one(
    raw: &grammers_tl_types::enums::Message,
    tally: &mut BTreeMap<String, usize>,
    note: &mut impl FnMut(&mut BTreeMap<String, usize>, &str, bool),
) {
    use grammers_tl_types::enums::Message as M;
    match raw {
        M::Empty(_) => note(tally, "messageEmpty", true),
        M::Message(m) => {
            note(tally, "message", true);
            note(tally, "  entities", m.entities.is_some());
            note(tally, "  grouped_id (album)", m.grouped_id.is_some());
            note(tally, "  ttl_period", m.ttl_period.is_some());
            note(tally, "  via_bot_id", m.via_bot_id.is_some());
            note(tally, "  pinned", m.pinned);
            note(tally, "  noforwards", m.noforwards);
            note(tally, "  silent", m.silent);
            note(tally, "  media", m.media.is_some());
            note(tally, "  edit_date", m.edit_date.is_some());
            note(tally, "  effect", m.effect.is_some());
            note(tally, "  factcheck", m.factcheck.is_some());
            if let Some(n) = m.entities.as_ref().map(Vec::len) {
                *tally
                    .entry("  entities (total count)".to_owned())
                    .or_default() += n;
            }
            if let Some(r) = &m.reply_to {
                note(tally, "  reply_to", true);
                let grammers_tl_types::enums::MessageReplyHeader::Header(r) = r else {
                    return;
                };
                note(tally, "    quote_text", r.quote_text.is_some());
                note(tally, "    quote_entities", r.quote_entities.is_some());
                note(
                    tally,
                    "    reply_to_peer_id (cross-chat)",
                    r.reply_to_peer_id.is_some(),
                );
                note(tally, "    reply_media", r.reply_media.is_some());
                note(tally, "    reply_to_top_id", r.reply_to_top_id.is_some());
            }
            if let Some(grammers_tl_types::enums::MessageFwdHeader::Header(f)) = &m.fwd_from {
                note(tally, "  fwd_from", true);
                note(tally, "    fwd date (always sent)", true);
                note(tally, "    fwd from_id", f.from_id.is_some());
                note(tally, "    fwd from_name", f.from_name.is_some());
                note(tally, "    fwd channel_post", f.channel_post.is_some());
                note(
                    tally,
                    "    fwd saved_from_peer",
                    f.saved_from_peer.is_some(),
                );
                note(tally, "    fwd imported", f.imported);
            }
            probe_reactions(m.reactions.as_ref(), tally, note);
        }
        M::Service(m) => {
            note(tally, "messageService", true);
            note(tally, &format!("  action {}", action_name(&m.action)), true);
            if let grammers_tl_types::enums::MessageAction::PhoneCall(c) = &m.action {
                note(tally, "    call duration", c.duration.is_some());
                note(tally, "    call reason", c.reason.is_some());
                note(tally, "    call video", c.video);
            }
            probe_reactions(m.reactions.as_ref(), tally, note);
        }
    }
}

fn probe_reactions(
    reactions: Option<&grammers_tl_types::enums::MessageReactions>,
    tally: &mut BTreeMap<String, usize>,
    note: &mut impl FnMut(&mut BTreeMap<String, usize>, &str, bool),
) {
    let Some(grammers_tl_types::enums::MessageReactions::Reactions(r)) = reactions else {
        return;
    };
    note(tally, "  reactions", true);
    note(tally, "    reactions.min (partial)", r.min);
    note(tally, "    reactions.can_see_list", r.can_see_list);
    note(
        tally,
        "    recent_reactions (who)",
        r.recent_reactions.is_some(),
    );
    if let Some(recent) = &r.recent_reactions {
        *tally
            .entry("    recent_reactions (named, total)".to_owned())
            .or_default() += recent.len();
        let counted: i32 = r
            .results
            .iter()
            .map(|c| {
                let grammers_tl_types::enums::ReactionCount::Count(c) = c;
                c.count
            })
            .sum();
        note(
            tally,
            "    recent_reactions names every reactor",
            i32::try_from(recent.len()).is_ok_and(|n| n >= counted),
        );
    }
}

fn action_name(action: &grammers_tl_types::enums::MessageAction) -> &'static str {
    use grammers_tl_types::enums::MessageAction as A;
    match action {
        A::ChatCreate(_) => "chatCreate",
        A::ChatEditTitle(_) => "chatEditTitle",
        A::ChatEditPhoto(_) => "chatEditPhoto",
        A::ChatAddUser(_) => "chatAddUser",
        A::ChatDeleteUser(_) => "chatDeleteUser",
        A::PhoneCall(_) => "phoneCall",
        A::ContactSignUp => "contactSignUp",
        A::SetMessagesTtl(_) => "setMessagesTtl",
        A::GroupCall(_) => "groupCall",
        A::PinMessage => "pinMessage",
        _ => "other",
    }
}

/// Hold the update stream, storing what arrives.
async fn follow(
    client: &Client,
    db: &Db,
    cfg: &Cfg,
    self_id: i64,
    updates: tokio::sync::mpsc::UnboundedReceiver<UpdatesLike>,
) -> Result<()> {
    // `catch_up` replays what arrived while the pod was down. grammers drops
    // updates past `update_queue_limit` (default 100), which loses read marks: the
    // sweep restates only the latest. The queue is pre-allocated, so it is bounded
    // well inside the pod's memory limit rather than `None`.
    let mut stream = client
        .stream_updates(
            updates,
            UpdatesConfiguration {
                catch_up: true,
                update_queue_limit: Some(10_000),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("opening the update stream: {e}"))?;

    let names = Names::default();
    names.learn_self(client, self_id).await;
    loop {
        let update = stream.next().await.context("reading an update")?;
        if let Err(e) = apply(client, db, cfg, &names, self_id, &update).await {
            // One bad update must not end the feed.
            tracing::error!("could not store an update: {e:#}");
        }
    }
}

async fn apply(
    client: &Client,
    db: &Db,
    cfg: &Cfg,
    names: &Names,
    self_id: i64,
    update: &Update,
) -> Result<()> {
    match update {
        Update::NewMessage(m) | Update::MessageEdited(m) => {
            // `m.raw` is the whole Update, shadowing the message's `raw`.
            let inner: &grammers_client::message::Message = m;
            store(db, self_id, &inner.raw, names.of(client, inner).await).await?;
            if let Some(row) = map::map_message(&inner.raw, self_id) {
                fetch_media(db, cfg, inner, row.conversation_id).await?;
            }
            Ok(())
        }
        Update::MessageDeleted(d) => {
            // See `Db::mark_telegram_deleted` for the scope.
            let scope = match d.channel_id() {
                Some(channel_id) => TelegramDeleteScope::Channel(
                    map::normalise_peer(&grammers_tl_types::enums::Peer::Channel(
                        grammers_tl_types::types::PeerChannel { channel_id },
                    ))
                    .0,
                ),
                None => TelegramDeleteScope::SharedSequence,
            };
            let n = db.mark_telegram_deleted(d.messages(), scope).await?;
            tracing::info!("{n} message(s) marked deleted ({scope:?})");
            Ok(())
        }
        // grammers has no wrapped variant for read updates, so they arrive `Raw`.
        // `ReadHistory*` covers DMs and basic groups, `ReadChannel*` channels and
        // supergroups.
        Update::Raw(raw) => {
            use grammers_tl_types::enums::Update as Tl;
            let (peer, direction, max_id) = match &raw.raw {
                Tl::ReadHistoryInbox(u) => (
                    map::normalise_peer(&u.peer).0,
                    TelegramReadDirection::Inbox,
                    u.max_id,
                ),
                Tl::ReadHistoryOutbox(u) => (
                    map::normalise_peer(&u.peer).0,
                    TelegramReadDirection::Outbox,
                    u.max_id,
                ),
                Tl::ReadChannelInbox(u) => (
                    channel_peer(u.channel_id),
                    TelegramReadDirection::Inbox,
                    u.max_id,
                ),
                Tl::ReadChannelOutbox(u) => (
                    channel_peer(u.channel_id),
                    TelegramReadDirection::Outbox,
                    u.max_id,
                ),
                _ => return Ok(()),
            };
            if db
                .record_telegram_read_mark(peer, direction, max_id)
                .await?
            {
                tracing::info!(
                    "conversation {peer}: {} read up to {max_id}",
                    direction.as_str()
                );
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Store one wire message and whatever came with it.
async fn store(
    db: &Db,
    self_id: i64,
    raw: &grammers_tl_types::enums::Message,
    sender_name: Option<String>,
) -> Result<TelegramStored> {
    let Some(row) = map::map_message(raw, self_id) else {
        return Ok(TelegramStored::Unchanged);
    };
    // A message can arrive from a conversation the sweep has not listed yet; the
    // viewer's join needs its row. The next sweep corrects kind and name.
    db.upsert_telegram_conversation(row.conversation_id, kind_from_space(&row), None, None)
        .await?;
    let outcome = db
        .store_telegram_message(&row, sender_name.as_deref())
        .await?;
    db.replace_telegram_reactions(row.conversation_id, row.msg_id, &row.reactions.counts)
        .await?;
    db.record_telegram_reaction_authors(row.conversation_id, row.msg_id, &row.reactions)
        .await?;
    db.replace_telegram_entities(row.conversation_id, row.msg_id, &row.entities)
        .await?;
    if let Some(call) = &row.call {
        db.record_telegram_call(row.conversation_id, row.msg_id, call)
            .await?;
    }
    if outcome == TelegramStored::Edited {
        tracing::info!(
            "message {}/{} was edited; the previous text is kept",
            row.conversation_id,
            row.msg_id
        );
    }
    Ok(outcome)
}

/// Fetch this message's media if it is under [`EAGER_MAX_BYTES`], else offer it.
/// A failed download is recorded as `failed`, never returned as an error, so one
/// file cannot stop the walk.
async fn fetch_media(
    db: &Db,
    cfg: &Cfg,
    message: &grammers_client::message::Message,
    conversation_id: i64,
) -> Result<()> {
    let Some(media) = message.media() else {
        return Ok(());
    };
    let msg_id = message.id();
    if db
        .telegram_media_state(conversation_id, msg_id)
        .await?
        .is_some()
    {
        return Ok(());
    }

    let size = media.size().and_then(|s| i64::try_from(s).ok());
    let Some(size) = size else {
        // Not a file: a poll, a location, a link preview.
        return Ok(());
    };
    if size > EAGER_MAX_BYTES {
        db.record_telegram_media_state(conversation_id, msg_id, TelegramMediaState::Offered, None)
            .await?;
        return Ok(());
    }

    // `msg_id` is unique only within a conversation.
    let stored_name = format!("{conversation_id}_{msg_id}");
    let path = std::path::Path::new(&cfg.media_dir).join(&stored_name);
    // `Ok(false)` means there was nothing to download.
    match message.download_media(&path).await {
        Ok(true) => {
            // No size from `stat`: it races the write. See migration v26.
            db.record_telegram_media_stored(
                conversation_id,
                msg_id,
                &stored_name,
                media_content_type(&media).as_deref(),
            )
            .await?;
        }
        Ok(false) => {
            // A thumbnail-only or expired reference; a reader may yet get it.
            db.record_telegram_media_state(
                conversation_id,
                msg_id,
                TelegramMediaState::Offered,
                Some("telegram returned no file for this message"),
            )
            .await?;
        }
        Err(e) => {
            // Remove the partial file.
            let _ = tokio::fs::remove_file(&path).await;
            let note = format!("{e}");
            tracing::warn!("media {conversation_id}/{msg_id} could not be fetched: {note}");
            db.record_telegram_media_state(
                conversation_id,
                msg_id,
                TelegramMediaState::Failed,
                Some(note.chars().take(200).collect::<String>().as_str()),
            )
            .await?;
        }
    }
    Ok(())
}

/// What to serve the bytes as. Telegram photos are always JPEG; documents carry
/// their own mime.
fn media_content_type(media: &grammers_client::media::Media) -> Option<String> {
    use grammers_client::media::Media as M;
    match media {
        M::Photo(_) => Some("image/jpeg".to_owned()),
        M::Sticker(s) => s.document.mime_type().map(str::to_owned),
        M::Document(d) => d.mime_type().map(str::to_owned),
        _ => None,
    }
}

/// The best kind available without the peer: a supergroup reads as `channel`
/// until the sweep corrects it.
fn kind_from_space(row: &Row) -> ConvKind {
    match row.peer_space {
        map::PeerSpace::User => ConvKind::Dm,
        map::PeerSpace::Chat => ConvKind::Group,
        map::PeerSpace::Channel => ConvKind::Channel,
    }
}

/// Fetch the media readers have asked for, of any size.
async fn serve_requests(
    client: &Client,
    db: &Db,
    cfg: &Cfg,
    session: &Arc<DbSession>,
) -> Result<()> {
    loop {
        let wanted = db.wanted_telegram_media(REQUEST_BATCH).await?;
        if wanted.is_empty() {
            tokio::time::sleep(REQUEST_POLL).await;
            continue;
        }
        for (conversation_id, msg_id) in wanted {
            if let Err(e) = serve_one(client, db, cfg, session, conversation_id, msg_id).await {
                // Marked failed so it leaves the queue; a reader can ask again.
                tracing::warn!("requested media {conversation_id}/{msg_id} failed: {e:#}");
                db.record_telegram_media_state(
                    conversation_id,
                    msg_id,
                    TelegramMediaState::Failed,
                    Some(
                        format!("{e:#}")
                            .chars()
                            .take(200)
                            .collect::<String>()
                            .as_str(),
                    ),
                )
                .await?;
            }
        }
    }
}

/// Re-fetch one message by id and download whatever it carries.
async fn serve_one(
    client: &Client,
    db: &Db,
    cfg: &Cfg,
    session: &Arc<DbSession>,
    conversation_id: i64,
    msg_id: i32,
) -> Result<()> {
    // `PeerId` uses the Bot-API dialog format, which is what the archive stores.
    let peer_id = grammers_client::session::types::PeerId::from_bot_api_dialog_id(conversation_id)
        .with_context(|| format!("{conversation_id} is not a dialog id"))?;
    let peer_ref = session
        .peer_ref(peer_id)
        .await
        .map_err(|e| anyhow::anyhow!("resolving {conversation_id}: {e}"))?
        .with_context(|| format!("no peer reference for {conversation_id}"))?;

    let messages = client
        .get_messages_by_id(peer_ref, &[msg_id])
        .await
        .context("re-fetching the message")?;
    // A message deleted since it was offered comes back as a hole.
    let Some(message) = messages.into_iter().next().flatten() else {
        anyhow::bail!("telegram no longer has message {msg_id}");
    };
    let Some(media) = message.media() else {
        anyhow::bail!("message {msg_id} no longer carries media");
    };

    let stored_name = format!("{conversation_id}_{msg_id}");
    let path = std::path::Path::new(&cfg.media_dir).join(&stored_name);
    match message.download_media(&path).await {
        Ok(true) => {
            db.record_telegram_media_stored(
                conversation_id,
                msg_id,
                &stored_name,
                media_content_type(&media).as_deref(),
            )
            .await?;
            tracing::info!("fetched requested media {conversation_id}/{msg_id}");
            Ok(())
        }
        Ok(false) => anyhow::bail!("telegram returned no file"),
        Err(e) => {
            let _ = tokio::fs::remove_file(&path).await;
            Err(anyhow::anyhow!("{e}"))
        }
    }
}

/// Walk every conversation's history backwards, resuming where it left off.
async fn backfill(client: &Client, db: &Db, cfg: &Cfg, self_id: i64) -> Result<()> {
    loop {
        let pending = sweep(client, db).await?;
        if pending.is_empty() {
            // Joining a group can add history older than anything seen.
            tracing::info!("every conversation is archived; sweeping again later");
            tokio::time::sleep(IDLE_SWEEP).await;
            continue;
        }
        for (id, peer_ref) in pending {
            let from = db
                .telegram_backfill_state(id)
                .await?
                .and_then(|state| state.oldest_seen);
            walk(client, db, cfg, self_id, id, peer_ref, from).await?;
        }
    }
}

/// Walk one conversation to the end of its history. `MessageIter` pages itself;
/// progress is checkpointed every [`PAGE`] messages so a restart resumes.
async fn walk(
    client: &Client,
    db: &Db,
    cfg: &Cfg,
    self_id: i64,
    id: i64,
    peer_ref: PeerRef,
    from: Option<i32>,
) -> Result<()> {
    let mut iter = client.iter_messages(peer_ref);
    // Unset means from the newest; set means older than this.
    if let Some(offset) = from {
        iter = iter.offset_id(offset);
    }

    // `messages_stored` counts new rows only, so a re-walk does not inflate it.
    let mut walked = 0i64;
    let mut stored = 0i64;
    let mut enriched = 0i64;
    let mut total_walked = 0i64;
    let mut oldest = None;
    while let Some(message) = iter.next().await.context("reading a history page")? {
        let sender = message.sender().and_then(peer_name);
        // Row before bytes: a row without its file is recoverable, the reverse is not.
        match store(db, self_id, &message.raw, sender).await? {
            TelegramStored::Inserted | TelegramStored::Edited => stored += 1,
            TelegramStored::Enriched => enriched += 1,
            TelegramStored::Unchanged => {}
        }
        fetch_media(db, cfg, &message, id).await?;
        oldest = Some(match oldest {
            None => message.id(),
            Some(prev) => i32::min(prev, message.id()),
        });
        walked += 1;
        total_walked += 1;
        if (walked as usize).is_multiple_of(PAGE) {
            // The delta since the last checkpoint.
            db.record_telegram_backfill(id, oldest, false, stored)
                .await?;
            tracing::info!(
                "conversation {id}: {total_walked} walked so far, {stored} stored, \
                 {enriched} enriched in this page"
            );
            walked = 0;
            stored = 0;
            enriched = 0;
            tokio::time::sleep(PAGE_PAUSE).await;
        }
    }

    // The iterator running out is Telegram's only end-of-history signal.
    db.record_telegram_backfill(id, oldest, true, stored)
        .await?;
    tracing::info!("conversation {id} is fully archived ({total_walked} walked)");
    tokio::time::sleep(PAGE_PAUSE).await;
    Ok(())
}
