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

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use grammers_client::Client;
use grammers_client::client::UpdatesConfiguration;
use grammers_client::session::updates::UpdatesLike;
use grammers_client::update::Update;
use grammers_mtsender::SenderPool;
use signal_archiver::db::{Db, TelegramDeleteScope, TelegramStored};
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

struct Cfg {
    api_id: i32,
    api_hash: String,
}

fn cfg() -> Result<Cfg> {
    Ok(Cfg {
        api_id: std::env::var("TELEGRAM_API_ID")
            .context("TELEGRAM_API_ID not set")?
            .parse()
            .context("TELEGRAM_API_ID is not a number")?,
        api_hash: std::env::var("TELEGRAM_API_HASH").context("TELEGRAM_API_HASH not set")?,
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
        None => archive(&client, &db, &session, updates).await,
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
    let conversations = sweep_dialogs(client, db).await?;
    tracing::info!("{conversations} conversations known");

    // Live updates run CONCURRENTLY with the backfill. A decade of history takes
    // hours; a message that arrives during it must not wait for them.
    let live = {
        let client = client.clone();
        let db = db.clone();
        tokio::spawn(async move { follow(&client, &db, self_id, updates).await })
    };
    let history = {
        let client = client.clone();
        let db = db.clone();
        tokio::spawn(async move { backfill(&client, &db, self_id).await })
    };

    // Either ending is fatal: a feed with only half of itself running is the
    // failure this archive cannot see from the outside.
    tokio::select! {
        r = live => r.context("the update stream task panicked")?.context("following updates"),
        r = history => r.context("the backfill task panicked")?.context("backfilling history"),
    }
}

/// Record every conversation the account has, and seed the peer cache.
async fn sweep_dialogs(client: &Client, db: &Db) -> Result<usize> {
    let mut dialogs = client.iter_dialogs();
    let mut seen = 0;
    while let Some(dialog) = dialogs.next().await.context("listing dialogs")? {
        let peer = dialog.peer();
        let id = peer
            .id()
            .bot_api_dialog_id()
            .context("a dialog whose peer is the self-user sentinel")?;
        db.upsert_telegram_conversation(
            id,
            ConvKind::from_peer(peer),
            peer_name(peer).as_deref(),
            peer.username(),
        )
        .await?;
        seen += 1;
    }
    Ok(seen)
}

/// Hold the update stream, storing what arrives.
async fn follow(
    client: &Client,
    db: &Db,
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

    loop {
        let update = stream.next().await.context("reading an update")?;
        if let Err(e) = apply(db, self_id, &update).await {
            // One bad update must not end the feed. Logged with the update's shape
            // so the next one of its kind can be handled deliberately.
            tracing::error!("could not store an update: {e:#}");
        }
    }
}

async fn apply(db: &Db, self_id: i64, update: &Update) -> Result<()> {
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
            store(db, self_id, &inner.raw, m.sender().and_then(peer_name))
                .await
                .map(|_| ())
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
        // Everything else is somebody typing, a bot callback, or a raw update this
        // archive has no column for.
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

/// Walk every conversation's history backwards, resuming where it left off.
async fn backfill(client: &Client, db: &Db, self_id: i64) -> Result<()> {
    loop {
        let mut worked = false;
        let mut dialogs = client.iter_dialogs();
        while let Some(dialog) = dialogs.next().await.context("listing dialogs")? {
            let peer = dialog.peer();
            let Some(id) = peer.id().bot_api_dialog_id() else {
                continue;
            };
            let state = db.telegram_backfill_state(id).await?;
            if state.is_some_and(|s| s.complete) {
                continue;
            }
            worked = true;
            let Some(peer_ref) = peer
                .to_ref()
                .await
                .map_err(|e| anyhow::anyhow!("resolving {id}: {e}"))?
            else {
                tracing::warn!("no reference for conversation {id}; skipping this sweep");
                continue;
            };

            let offset = state.and_then(|s| s.oldest_seen).unwrap_or(0);
            let mut iter = client.iter_messages(peer_ref).limit(PAGE);
            // `offset_id(0)` means "from the newest"; anything else means "older
            // than this", which is what resuming is.
            if offset != 0 {
                iter = iter.offset_id(offset);
            }

            // ⚠ `walked` and `stored` are different numbers and the column is named
            // for the second. Counting every message the walk SAW would inflate
            // `messages_stored` on every re-walk — and a re-walk is exactly what an
            // enrichment pass is, so the counter would drift each time a column was
            // added. `enriched` is reported but not counted: it is the same message,
            // better described.
            let mut walked = 0i64;
            let mut stored = 0i64;
            let mut enriched = 0i64;
            let mut oldest = None;
            while let Some(message) = iter.next().await.context("reading a history page")? {
                let sender = message.sender().and_then(peer_name);
                match store(db, self_id, &message.raw, sender).await? {
                    TelegramStored::Inserted | TelegramStored::Edited => stored += 1,
                    TelegramStored::Enriched => enriched += 1,
                    TelegramStored::Unchanged => {}
                }
                oldest = Some(match oldest {
                    None => message.id(),
                    Some(prev) => i32::min(prev, message.id()),
                });
                walked += 1;
                if (walked as usize).is_multiple_of(PAGE) {
                    tokio::time::sleep(PAGE_PAUSE).await;
                }
            }

            // ⚠ `complete` only when the page was EMPTY — which is what the WALK
            // returned, not what was stored. Keying it on `stored` would declare a
            // conversation finished the moment a page held nothing new, which on a
            // re-walk is the first page.
            let complete = walked == 0;
            db.record_telegram_backfill(id, oldest, complete, stored)
                .await?;
            if complete {
                tracing::info!("conversation {id} is fully archived");
            } else {
                tracing::info!(
                    "conversation {id}: walked {walked}, stored {stored}, enriched {enriched}"
                );
            }
            tokio::time::sleep(PAGE_PAUSE).await;
        }
        if !worked {
            // Nothing left to walk. Sweeping again on a long timer rather than
            // exiting, because a conversation can gain history that is older than
            // anything seen: joining a group hands over everything said before.
            tracing::info!("every conversation is archived; sweeping again later");
            tokio::time::sleep(IDLE_SWEEP).await;
        }
    }
}
