//! `telegram` — the archive's Telegram feed: one login, both history and live.
//!
//! Every other origin in this archive needed two feeds. Signal took an Android
//! plaintext export for the past and a linked device for the present; Google Chat
//! took a browser capture that can never be repeated; IRC takes files off a disk
//! in another cluster. Telegram keeps history server-side, so ONE authorised
//! session can page backwards through everything and hold the update stream at
//! the same time — which is why this binary does both and why the two share a
//! dedupe key rather than being reconciled afterwards.
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
//! ⚠ **A bot could not do this.** A Telegram bot is a separate account and cannot
//! read the chats of the person who owns it, so an archive of Pippijn's own
//! conversations has to be a USER client with his own `api_id`. That is also why
//! the session in `telegram_session` is a credential and not a cache: see
//! `telegram::session`.
//!
//! ⚠ **Secret chats are not here and cannot be.** They are device-local by
//! construction — the server never holds them — so no login reaches them. Nothing
//! in this archive will say so; this line is the only record.
//!
//! Two modes:
//!
//! * `telegram login <phone>` — interactive, once per account lifetime. Asks
//!   Telegram for a code, reads it from stdin, and stores the resulting session.
//!   Run it with `kubectl run -it` or locally against the same database.
//!
//!   ⚠ The number is an ARGUMENT rather than an environment variable, and that is
//!   not a style choice. Env is the service's configuration, and every variable in
//!   it is one the Deployment is expected to carry — dev-lint checks exactly that
//!   and was right to report a `TELEGRAM_PHONE` the archiver never reads. A phone
//!   number is an input to a one-off command, so it is passed like one.
//! * `telegram` — the archiver. Refuses to start without a stored session, because
//!   a feed that is quietly not logged in looks exactly like a quiet week.
//!
//! Config via env: `DB_HOST`, `DB_PORT` (3306), `DB_NAME`, `DB_USER`,
//! `DB_PASSWORD`, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`.

use std::collections::HashMap;
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

/// How often the session is written back when anything in it changed.
///
/// The session mutates constantly — every peer of every response — so this is a
/// coalescing interval rather than a durability promise. Losing up to this much
/// costs re-reading a few updates, because the dedupe key makes a replay free;
/// losing the AUTH KEY would cost a flood wait, and that is written the moment
/// it is created because a new datacentre option is a change like any other.
const SESSION_FLUSH: Duration = Duration::from_secs(10);

/// Messages per history page.
///
/// Telegram's own cap is 100. Asking for fewer would multiply round trips over a
/// decade of conversation for nothing.
const PAGE: usize = 100;

/// A pause between history pages.
///
/// ⚠ Politeness with teeth: the retry policy already sleeps through a flood wait
/// up to a minute, so this is not what keeps the account safe. What it buys is
/// that a backfill of years of history does not saturate the connection the LIVE
/// stream shares, which would make a new message arrive minutes late while the
/// archive catches up on 2019.
const PAGE_PAUSE: Duration = Duration::from_millis(400);

/// A pause between backfill sweeps once every conversation is complete.
const IDLE_SWEEP: Duration = Duration::from_secs(3600);

/// How often the feed looks for media a reader has asked for.
///
/// ⚠ This is the latency of a tap, so it is short — but it is a POLL against an
/// indexed lookup on a table of a few thousand rows, not a scan, and an idle archive
/// costs one such lookup every three seconds. The alternative was giving this pod an
/// inbound endpoint, and the property that nothing in the cluster can dial the
/// process holding a logged-in Telegram account is worth more than three seconds.
const REQUEST_POLL: Duration = Duration::from_secs(3);

/// How many asked-for files to fetch before looking for more.
///
/// One at a time, deliberately. The requests that reach here are the LARGE media —
/// that is what being asked for means — so a batch would mean several multi-hundred-
/// megabyte downloads sharing the connection the live stream also uses.
const REQUEST_BATCH: i64 = 1;

/// The largest media this fetches without being asked.
///
/// ⚠ **MEASURED BEFORE IT WAS CHOSEN.** The archive records every file's size from
/// the message itself, at no network cost, so the split is arithmetic rather than
/// instinct: 4,906 photos come to about 0.9GB and the largest is 0.8MB, while 832
/// videos come to 3.8GB and ONE of them is 1.5GB. A ceiling here rather than a test
/// on `media_kind` because the kind is a label and the bytes are the cost — a
/// 40MB "photo" nobody anticipated should wait to be asked for, and a 200KB video
/// may as well come along.
const EAGER_MAX_BYTES: i64 = 4 * 1024 * 1024;

#[derive(Clone)]
struct Cfg {
    api_id: i32,
    api_hash: String,
    /// Where fetched media is written. A mount, read-only in the viewer that serves
    /// it.
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
            // Explicitly, not on a timer: the key generated above is the one thing
            // here that cannot be recreated cheaply.
            session.flush(db.pool()).await?;
            tracing::info!("logged in; the session is stored");
            Ok(())
        }
        None => archive(&client, &db, &cfg, &session, updates).await,
        Some(other) => bail!("unknown mode {other:?}; expected `login <phone>` or no argument"),
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
            // Not fatal, and deliberately not a reason to stop archiving: the
            // in-memory session is still correct, and the next tick retries. A
            // process that died here would take the feed down over a write it
            // will get another chance at in ten seconds.
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
            // 2FA. The hint is Telegram's, and printing it is the only help
            // available at this point.
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
        // ⚠ Named rather than folded into a generic failure, because the remedy
        // differs: a wrong code is worth retrying immediately, and sign-up is not
        // something this program can ever do.
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
    // ⚠ Asked of the STORED session, before dialling anybody. A feed that starts,
    // fails to authorise and retries forever is indistinguishable from a quiet
    // account; this says which it is, once, and stops.
    if !session.is_authorized()? {
        bail!(
            "no Telegram session is stored: run `telegram login` once against this database \
             (see the module docs). Refusing to run as a feed that cannot read anything."
        );
    }
    let self_id = session.self_id()?.context(
        "the stored session has no self user, which should be impossible once authorised",
    )?;

    // ⚠ DIALOGS FIRST, AND NOT ONLY FOR THE CONVERSATION NAMES. `stream_updates`
    // can only close a gap in the update sequence for peers already in the session
    // cache, so a sweep of the dialog list is what makes catch-up work at all.
    // `auto_cache_peers` does the caching as a side effect of this call.
    // ⚠ BEFORE the update stream, not merely early. `stream_updates` can only
    // close a gap in the sequence for peers already in the session cache, and this
    // is what puts them there. The backfill sweeps again on its own; two listings
    // at startup is the price of that ordering being explicit.
    let pending = sweep(client, db).await?;
    tracing::info!("{} conversation(s) with history to walk", pending.len());

    // A third task: what readers have asked for. Concurrent with both, because a tap
    // must not wait for a decade of history to finish walking.
    let requests = {
        let client = client.clone();
        let db = db.clone();
        let cfg = cfg.clone();
        let session = Arc::clone(session);
        tokio::spawn(async move { serve_requests(&client, &db, &cfg, &session).await })
    };

    // Live updates run CONCURRENTLY with the backfill. A decade of history takes
    // hours; a message that arrives during it must not wait for them.
    // ⚠ Both tasks need the media directory, so `Cfg` is cloned into each rather
    // than borrowed: a spawned task cannot hold a reference to a local.
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

    // Either ending is fatal: a feed with only half of itself running is the
    // failure this archive cannot see from the outside.
    tokio::select! {
        r = live => r.context("the update stream task panicked")?.context("following updates"),
        r = history => r.context("the backfill task panicked")?.context("backfilling history"),
        r = requests => r.context("the request task panicked")?.context("serving requests"),
    }
}

/// List every conversation the account has, record what each one IS, and return
/// the ones whose history is not finished.
///
/// ⚠ **ONE `messages.getDialogs` PER CALL, and that is the whole point of the
/// shape.** The first version listed the dialogs inside the per-conversation loop
/// and took ONE page of 100 messages per pass — so a conversation with five
/// thousand messages needed fifty passes and fifty full dialog listings. Telegram
/// answered with `FLOOD_WAIT` on `getDialogs`, sleeping 17-18 seconds at a time:
/// self-healing, nothing lost, and the backfill crawling for a reason that was
/// entirely ours.
///
/// It also seeds the session's peer cache, which is what `stream_updates` needs
/// before it can close a gap — so this runs once at startup for that reason alone,
/// and then once per sweep for this one.
///
/// The conversation upsert happens HERE rather than in the walk because this is
/// where the peer is in hand: `ConvKind::from_peer` can tell a supergroup from a
/// broadcast, which an id cannot.
/// A channel's id in the archive's one id space.
///
/// ⚠ The channel read updates name a BARE `channel_id`, not a `Peer`, so the
/// normalisation every other id here goes through has to be reached deliberately.
/// Skipping it would file a supergroup's read marks under a different id from its
/// own messages — see `PeerSpace`.
fn channel_peer(channel_id: i64) -> i64 {
    map::normalise_peer(&grammers_tl_types::enums::Peer::Channel(
        grammers_tl_types::types::PeerChannel { channel_id },
    ))
    .0
}

/// Both read marks a dialog carries, stored if either has moved.
///
/// ⚠ **`getDialogs` HAS ALWAYS CARRIED THESE and the sweep threw them away.** The
/// hourly pass read a dialog's peer, kind, name and username and dropped the rest,
/// so the one fact in this archive that cannot be re-fetched later was being
/// discarded on the floor every hour. No extra API call was ever needed for it.
async fn read_marks(db: &Db, id: i64, dialog: &grammers_client::peer::Dialog) -> Result<()> {
    let (inbox, outbox) = match &dialog.raw {
        grammers_tl_types::enums::Dialog::Dialog(d) => (d.read_inbox_max_id, d.read_outbox_max_id),
        // A folder is a grouping, not a conversation, and has no marks of its own.
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

async fn sweep(client: &Client, db: &Db) -> Result<Vec<(i64, PeerRef)>> {
    let mut dialogs = client.iter_dialogs();
    let mut pending = Vec::new();
    while let Some(dialog) = dialogs.next().await.context("listing dialogs")? {
        let peer = dialog.peer();
        let Some(id) = peer.id().bot_api_dialog_id() else {
            // The self-user sentinel, which is not a conversation.
            continue;
        };
        db.upsert_telegram_conversation(
            id,
            ConvKind::from_peer(peer),
            peer_name(peer).as_deref(),
            peer.username(),
        )
        .await?;
        // ⚠ **BEFORE the `complete` check below, and that is not a detail.** Every
        // conversation in this archive is already backfilled, so anything recorded
        // after that `continue` would be recorded for nothing — the sweep's read
        // marks would silently never be written at all.
        //
        // The sweep is the FLOOR for read marks. The live updates below catch a read
        // within seconds, but Telegram guarantees delivery only for message updates,
        // and this feed has already been seen dropping 71 queued updates on a
        // restart. `getDialogs` re-states the current marks every hour regardless,
        // so a missed update costs lateness rather than the fact.
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
            // Without a reference nothing can be requested for it. Reported rather
            // than skipped silently, because a conversation that never gets one
            // would otherwise be permanently and invisibly unarchived.
            None => tracing::warn!("no reference for conversation {id}; skipping this sweep"),
        }
    }
    Ok(pending)
}

/// Who sent it, for the updates that do not say.
///
/// ⚠ **A LIVE DM UPDATE CARRIES NO USERS AT ALL.** Telegram's compact
/// `updateShortMessage` names the sender by id and nothing else, and
/// `Message::sender` is an in-packet lookup rather than a fetch — so it answers
/// `None` for essentially every ordinary line typed in a one-to-one chat. That
/// is why every live DM row had a NULL `sender_name` while the backfill's rows
/// were fine: `iter_messages` answers with the users attached, so a re-walk
/// silently papered over the hole and the archive only looked complete.
///
/// So the name is asked for: `sender_ref` consults the session's peer cache —
/// the dialog sweep put every dialog peer there — and `resolve_peer` turns that
/// reference into a peer that has a name.
///
/// ⚠ **Memoised for the life of the process, deliberately.** `catch_up: true`
/// can replay hundreds of updates at once after a restart, and one
/// `users.getUsers` apiece would be a flood wait rather than an archive. The
/// price is that a rename mid-process is not seen until the pod restarts, which
/// `sender_name` can afford: it is a denormalised snapshot of who spoke, and the
/// name anything reads as CURRENT comes from `telegram_conversations`, which the
/// hourly sweep refreshes.
///
/// ⚠ **A failed lookup is not remembered.** It is a round trip that can fail for
/// a minute at a time, and caching that minute would cost a process lifetime of
/// nameless rows.
#[derive(Default)]
struct Names(tokio::sync::Mutex<HashMap<PeerId, String>>);

impl Names {
    /// Learn our own name, once, because `resolve_peer` cannot tell us.
    ///
    /// ⚠ **THE SELF SENTINEL DOES NOT RESOLVE, AND THE ERROR READS BACKWARDS.**
    /// `Message::sender_id` answers `PeerId::self_user()` — `2^40`, not a user id —
    /// for an outgoing message in a one-to-one chat. Handing that to `resolve_peer`
    /// fails with `Dropped`, which sounds like a lost request and is not one: the
    /// call reaches Telegram, gets the right user back, and then looks the result up
    /// in its own map under the SENTINEL key it was handed rather than the real id.
    /// The miss is the last line of `resolve_peer`, not the wire. Retrying, backing
    /// off or blaming the connection would all be chasing a network fault that never
    /// happened.
    ///
    /// `get_me` is the call that does answer, so it is made once and both keys are
    /// seeded: an outgoing DM is attributed to the sentinel, an outgoing group
    /// message to the real id.
    async fn learn_self(&self, client: &Client, self_id: i64) {
        let name = match client.get_me().await {
            Ok(me) => me.full_name(),
            // Not fatal. A feed that will not archive because it could not read its
            // own name would be a worse failure than outgoing rows missing one, and
            // the enrichment fills those in on any later delivery.
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

/// Hold the update stream, storing what arrives.
async fn follow(
    client: &Client,
    db: &Db,
    cfg: &Cfg,
    self_id: i64,
    updates: tokio::sync::mpsc::UnboundedReceiver<UpdatesLike>,
) -> Result<()> {
    // ⚠ `catch_up: true` is the whole reason the update state is persisted. Without
    // it a restart silently begins at "now" and everything said while the pod was
    // down is missing — and missing in a way no count reveals, because the
    // conversation simply has no rows for those minutes. It is safe to ask for
    // because a replayed update is free: `(conversation_id, msg_id)` already holds
    // it.
    let mut stream = client
        .stream_updates(
            updates,
            UpdatesConfiguration {
                catch_up: true,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("opening the update stream: {e}"))?;

    let names = Names::default();
    names.learn_self(client, self_id).await;
    loop {
        let update = stream.next().await.context("reading an update")?;
        if let Err(e) = apply(client, db, cfg, &names, self_id, &update).await {
            // One bad update must not end the feed. Logged with the update's shape
            // so the next one of its kind can be handled deliberately.
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
            // ⚠ **`m.raw` IS NOT THE MESSAGE.** `update::Message` has its own
            // `raw` field holding the whole `tl::enums::Update`, and it shadows
            // the `raw` of the `message::Message` it derefs to — so the obvious
            // spelling compiles into passing an Update where a Message belongs
            // (or, worse, would not compile only by luck of the types). Bound
            // through the deref target explicitly, once, with its type written
            // down.
            let inner: &grammers_client::message::Message = m;
            store(db, self_id, &inner.raw, names.of(client, inner).await).await?;
            if let Some(row) = map::map_message(&inner.raw, self_id) {
                fetch_media(db, cfg, inner, row.conversation_id).await?;
            }
            Ok(())
        }
        Update::MessageDeleted(d) => {
            // ⚠ Which conversations this reaches depends on whether Telegram named
            // one — see `Db::mark_telegram_deleted`, where the reason is a property
            // of Telegram's id sequences rather than a choice.
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
        // ⚠ **The read updates arrive as `Raw`**, because grammers gives friendly
        // variants only for the events it wraps and these are not among them. That
        // is the documented way to reach one, not a workaround being smuggled in —
        // and a minor version that promotes them to their own variant will make
        // this arm stop matching rather than misbehave.
        //
        // Four constructors for two facts: Telegram splits DMs and small groups
        // (`ReadHistory*`) from channels and supergroups (`ReadChannel*`), and the
        // channel pair names a bare `channel_id` where the other names a `Peer`.
        // Both end up normalised into the archive's one id space.
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
                // Somebody typing, a bot callback, a raw update this archive has no
                // column for.
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
        // Everything else is somebody typing or a bot callback.
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
        // Nothing to store, and nothing learned.
        return Ok(TelegramStored::Unchanged);
    };
    // ⚠ The conversation row FIRST, and with the kind the id implies rather than
    // the one the peer would give. A message can arrive from a conversation the
    // dialog sweep never listed (an old group, a channel just joined), and
    // `telegram_messages` would otherwise reference a conversation that is not
    // there — which the viewer's join reads as no conversation at all. The dialog
    // sweep corrects the kind and the name when it next runs; a supergroup is
    // filed as `group` there and only ever wrong in between.
    db.upsert_telegram_conversation(row.conversation_id, kind_from_space(&row), None, None)
        .await?;
    let outcome = db
        .store_telegram_message(&row, sender_name.as_deref())
        .await?;
    db.replace_telegram_reactions(row.conversation_id, row.msg_id, &row.reactions)
        .await?;
    if outcome == TelegramStored::Edited {
        tracing::info!(
            "message {}/{} was edited; the previous text is kept",
            row.conversation_id,
            row.msg_id
        );
    }
    Ok(outcome)
}

/// Fetch this message's media if it is small enough to take without being asked,
/// and otherwise record that it is there to be asked for.
///
/// ⚠ **NEVER FATAL.** A download that fails must not end the feed or stop the walk:
/// the archive's job is the messages, and a picture that did not arrive is recorded
/// as `failed` with its reason so it can be retried deliberately. An error here
/// returning `Err` would let one unfetchable file stop a decade of history.
///
/// ⚠ **The row is written AFTER the file is closed.** `download_media` streams chunk
/// by chunk, so a path published before the last chunk is a path to a short file,
/// and nothing downstream can tell a short file from a small one.
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
    // Already accounted for: stored, offered and waiting, or failed with a reason.
    // Re-offering would be harmless and re-downloading would not, and a re-walk
    // passes every message again.
    if db
        .telegram_media_state(conversation_id, msg_id)
        .await?
        .is_some()
    {
        return Ok(());
    }

    let size = media.size().and_then(|s| i64::try_from(s).ok());
    let Some(size) = size else {
        // Not a file — a poll, a location, a link preview. Nothing to hold, and no
        // offer to make either.
        return Ok(());
    };
    if size > EAGER_MAX_BYTES {
        db.record_telegram_media_state(conversation_id, msg_id, TelegramMediaState::Offered, None)
            .await?;
        return Ok(());
    }

    // ⚠ The name carries the conversation AND the message, because `msg_id` alone
    // is unique only within a conversation — two files from different chats would
    // otherwise overwrite each other on the volume, and the loser would be a photo
    // showing somebody else's picture.
    let stored_name = format!("{conversation_id}_{msg_id}");
    let path = std::path::Path::new(&cfg.media_dir).join(&stored_name);
    // ⚠ `download_media` returns a BOOL, and `false` means "there was nothing to
    // download" rather than a failure. Treating it as success would record a stored
    // file that is not there — the reader would then serve a 404 for a message the
    // archive claims to hold.
    match message.download_media(&path).await {
        Ok(true) => {
            // ⚠ **NO `stat` HERE, AND THAT IS A FIX RATHER THAN AN OMISSION.** This
            // recorded `metadata(path).len()` at exactly this point and got 0 for
            // files of 158KB: `tokio::fs::File` does its work on a blocking pool and
            // makes no promise that the inode reflects the write when the call
            // returns. The size is already known from the message — see the v25
            // migration for the measurement.
            db.record_telegram_media_stored(
                conversation_id,
                msg_id,
                &stored_name,
                media_content_type(&media).as_deref(),
            )
            .await?;
        }
        Ok(false) => {
            // Nothing fetchable behind it: a thumbnail-only or expired reference.
            // Recorded as offered rather than failed, because there is no error to
            // report and a reader asking may yet get it.
            db.record_telegram_media_state(
                conversation_id,
                msg_id,
                TelegramMediaState::Offered,
                Some("telegram returned no file for this message"),
            )
            .await?;
        }
        Err(e) => {
            // The partial file is removed: a valid-looking file of the wrong length
            // is worse than none, because nothing later comes back to notice it.
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

/// What to serve the bytes as.
///
/// Telegram photos are compressed JPEG — that is what the variant means — and a
/// document carries its own mime. Anything else gets none, and the reader falls
/// back to `application/octet-stream` rather than guessing from the extension there
/// is not.
fn media_content_type(media: &grammers_client::media::Media) -> Option<String> {
    use grammers_client::media::Media as M;
    match media {
        M::Photo(_) => Some("image/jpeg".to_owned()),
        M::Sticker(s) => s.document.mime_type().map(str::to_owned),
        M::Document(d) => d.mime_type().map(str::to_owned),
        _ => None,
    }
}

/// The best kind available without the peer.
///
/// ⚠ A channel id may be a broadcast OR a supergroup and this cannot tell which,
/// so it says `channel` and the dialog sweep fixes it. Recorded as the known
/// approximation it is rather than hidden behind a plausible guess.
fn kind_from_space(row: &Row) -> ConvKind {
    match row.peer_space {
        map::PeerSpace::User => ConvKind::Dm,
        map::PeerSpace::Chat => ConvKind::Group,
        map::PeerSpace::Channel => ConvKind::Channel,
    }
}

/// Fetch the media readers have asked for.
///
/// ⚠ **NO SIZE CEILING HERE, and that is the entire point of the queue.** The eager
/// pass skips anything over `EAGER_MAX_BYTES` precisely so that a 1.5GB video is not
/// pulled speculatively; being asked for is the signal that somebody wants this one.
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
                // ⚠ Recorded as failed rather than left `wanted`, or the queue would
                // hand this same row back on the next poll forever and nothing behind
                // it would ever be fetched. A reader can ask again; a stuck queue is
                // not something anybody can act on.
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
    // ⚠ The folded id goes back to a peer the same way it came from one. `PeerId`
    // uses the Bot-API dialog format internally, which is the format this archive
    // stores, so the round trip is exact rather than a re-derivation that could
    // disagree with `normalise_peer`.
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
    // ⚠ `get_messages_by_id` answers positionally and a deleted message comes back
    // as a HOLE rather than an error, so the `Option` is the real case: a reader can
    // ask for a picture whose message was retracted since it was offered.
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
            // Sweeping again on a long timer rather than exiting, because a
            // conversation can gain history OLDER than anything seen: joining a
            // group hands over everything said before you arrived.
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

/// Walk one conversation to the end of its history.
///
/// ⚠ **ONE ITERATOR FOR THE WHOLE CONVERSATION.** `MessageIter` pages internally —
/// it advances its own offset and knows when it has had the last chunk — so
/// letting it run is one `getHistory` per hundred messages and NO dialog listing
/// in between. Setting `.limit(PAGE)` instead, as this did first, turned the outer
/// loop into the pager and made every page cost a full dialog sweep.
///
/// ⚠ **Progress is checkpointed every `PAGE` messages, which is what keeps it
/// resumable.** The iterator's own position lives in memory; a pod that dies
/// mid-walk resumes from the last checkpoint rather than from the top. Recording
/// only at the end would mean a conversation of ten thousand messages either
/// finished or started over.
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
    // `offset_id` unset means "from the newest"; set means "older than this", which
    // is what resuming is.
    if let Some(offset) = from {
        iter = iter.offset_id(offset);
    }

    // ⚠ `walked` and `stored` are different numbers and the column is named for the
    // second. Counting every message the walk SAW would inflate `messages_stored`
    // on every re-walk — and a re-walk is exactly what an enrichment pass is, so the
    // counter would drift each time a column was added. `enriched` is reported but
    // not counted: it is the same message, better described.
    let mut walked = 0i64;
    let mut stored = 0i64;
    let mut enriched = 0i64;
    let mut total_walked = 0i64;
    let mut oldest = None;
    while let Some(message) = iter.next().await.context("reading a history page")? {
        let sender = message.sender().and_then(peer_name);
        // ⚠ The MESSAGE first, the bytes second. A picture whose row is missing is
        // a file nothing references; a row whose picture is missing is a message
        // that reads correctly and offers to fetch one. Only the second is
        // recoverable, so the order is not arbitrary.
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
            // A checkpoint, and the delta since the last one — never a running
            // total, or the stored count would be added again at every checkpoint.
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

    // ⚠ The iterator running out IS the end of the history — the only signal
    // Telegram gives — so `complete` is set here and nowhere else. An interrupted
    // walk never reaches this line, which is exactly what makes it resume rather
    // than declare itself finished.
    db.record_telegram_backfill(id, oldest, true, stored)
        .await?;
    tracing::info!("conversation {id} is fully archived ({total_walked} walked)");
    tokio::time::sleep(PAGE_PAUSE).await;
    Ok(())
}
