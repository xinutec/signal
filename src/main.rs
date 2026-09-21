//! signal-archiver — ingests Signal messages from `signal-cli-rest-api`'s
//! receive websocket into MariaDB, with enrichment (contact/group names,
//! stickers, attachment bytes) and delete-tracking.
//!
//! Frame PARSING lives in the `parse` module (pure, unit-tested); this binary
//! connects to the per-account receive websocket and EXECUTES the parsed
//! actions against the DB. Linking + the Signal protocol are handled by
//! signal-cli-rest-api (MODE=json-rpc).
//!
//! Config via env: DB_HOST, DB_PORT (3306), DB_NAME, DB_USER, DB_PASSWORD,
//! SIGNAL_NUMBER (E.164), SIGNAL_API_WS (ws://signal-cli-rest-api:8080),
//! SIGNAL_API_HTTP (http://signal-cli-rest-api:8080), ATTACHMENTS_DIR (/attachments).

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// If no frame at all (not even a server ping) arrives within this window, the
/// connection is probably a silently-dead socket (NAT/idle drop with no close).
/// We send a keepalive ping to probe; if `MAX_IDLE_PROBES` consecutive windows
/// pass with no traffic, we give up and force a reconnect.
const READ_TIMEOUT: Duration = Duration::from_secs(90);
const MAX_IDLE_PROBES: u32 = 3;

use signal_archiver::attach;
use signal_archiver::db::Db;
use signal_archiver::parse::{Action, display_name_of, parse_frame};

/// Shared state for the per-frame dispatcher.
#[derive(Clone)]
struct Ctx {
    db: Db,
    http: reqwest::Client,
    http_base: String,
    number: String,
    attach_dir: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,signal_archiver=debug".into()),
        )
        .init();

    let number = std::env::var("SIGNAL_NUMBER").context("SIGNAL_NUMBER not set")?;
    let api_ws = env_or("SIGNAL_API_WS", "ws://signal-cli-rest-api:8080");
    let http_base = env_or("SIGNAL_API_HTTP", "http://signal-cli-rest-api:8080")
        .trim_end_matches('/')
        .to_string();
    let attach_dir = env_or("ATTACHMENTS_DIR", "/attachments");
    let ws_url = format!("{}/v1/receive/{}", api_ws.trim_end_matches('/'), number);

    tokio::fs::create_dir_all(&attach_dir)
        .await
        .with_context(|| format!("creating attachments dir {attach_dir}"))?;

    let db = Db::connect(&signal_archiver::db::url_from_env()?)
        .await
        .context("connecting to MariaDB")?;
    let ctx = Ctx {
        db,
        http: reqwest::Client::new(),
        http_base,
        number,
        attach_dir,
    };
    tracing::info!("DB connected + migrated; ingesting from {ws_url}");

    tokio::spawn(refresh_group_names(ctx.clone()));
    tokio::spawn(refresh_contact_names(ctx.clone()));

    loop {
        match run_ws(&ws_url, &ctx).await {
            Ok(()) => tracing::warn!("receive stream ended; reconnecting in 7s"),
            Err(e) => tracing::error!("websocket error: {e:#}; reconnecting in 10s"),
        }
        tokio::time::sleep(Duration::from_secs(7)).await;
    }
}

async fn run_ws(ws_url: &str, ctx: &Ctx) -> Result<()> {
    let (mut ws, _) = connect_async(ws_url).await.context("ws connect")?;
    tracing::info!("websocket connected");
    // Count consecutive idle windows; any received frame (incl. pings/pongs)
    // proves the link is alive and resets it.
    let mut idle_probes = 0u32;
    loop {
        let next = match timeout(READ_TIMEOUT, ws.next()).await {
            Ok(next) => next,
            Err(_elapsed) => {
                idle_probes += 1;
                if idle_probes > MAX_IDLE_PROBES {
                    anyhow::bail!(
                        "no ws traffic for ~{}s ({idle_probes} idle windows); connection is dead",
                        READ_TIMEOUT.as_secs() * idle_probes as u64
                    );
                }
                tracing::warn!(
                    "no ws traffic for {}s; sending keepalive ping (probe {idle_probes}/{MAX_IDLE_PROBES})",
                    READ_TIMEOUT.as_secs()
                );
                // A broken pipe surfaces here on write even before a read would.
                // tungstenite 0.29 payloads are `Bytes`; empty ping body.
                ws.send(WsMessage::Ping(Default::default()))
                    .await
                    .context("keepalive ping")?;
                continue;
            }
        };
        let Some(msg) = next else { break }; // stream ended cleanly
        idle_probes = 0;
        let msg = msg.context("ws read")?;
        if msg.is_close() {
            tracing::warn!("server closed the websocket");
            break;
        }
        if msg.is_ping() {
            // A failed pong means the socket is gone; surface it so the caller
            // reconnects instead of looping blind on a dead connection.
            ws.send(WsMessage::Pong(msg.into_data()))
                .await
                .context("sending websocket pong")?;
            continue;
        }
        let text = match msg.to_text() {
            Ok(t) if !t.trim().is_empty() => t,
            _ => continue,
        };
        let frame: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("skipping non-JSON frame: {e}");
                continue;
            }
        };
        if let Err(e) = dispatch(ctx, &frame).await {
            tracing::warn!("failed to archive a frame: {e:#}");
        }
    }
    Ok(())
}

/// Execute the parsed action for one frame against the DB.
async fn dispatch(ctx: &Ctx, frame: &Value) -> Result<()> {
    // ⚠ THE FRAME IS KEPT BEFORE IT IS UNDERSTOOD. Signal says everything
    // exactly once — there is no server-side history to re-walk, unlike Telegram
    // — so a field this archive has no column for is lost the moment the socket
    // moves on. `JsonDataMessage` carries 23 at 0.14.5 and `parse_frame` reads
    // four. Storing the bytes first means the other nineteen can be given columns
    // whenever there is a reason, and BACKFILLED, rather than being gone.
    //
    // ⚠ ITS FAILURE IS LOGGED, NOT PROPAGATED. A frame we cannot file is still
    // a frame we can act on, and the message matters more than the copy of it.
    // Returning the error here would drop a message because its archive copy
    // failed, which inverts the point.
    match ctx.db.record_signal_frame(frame).await {
        Ok(true) => tracing::debug!("frame kept"),
        Ok(false) => {}
        Err(e) => tracing::error!("could not keep the raw frame: {e:#}"),
    }

    let parsed = parse_frame(frame);

    if let Some(c) = &parsed.contact {
        ctx.db
            .upsert_contact(&c.uuid, c.phone.as_deref(), c.name.as_deref())
            .await?;
    }
    if let Some((thread, name)) = &parsed.dm_name {
        ctx.db.set_conversation_name(thread, name).await?;
    }

    match parsed.action {
        Action::Skip => {}
        Action::Receipt(r) => {
            let n = ctx.db.record_signal_receipt(&r).await?;
            // Quiet when it taught us nothing: receipts are re-delivered often
            // and a line per replay would drown the log that matters.
            if n > 0 {
                tracing::info!(
                    "{} {n} message(s) for {} at {}",
                    r.kind.as_str(),
                    r.author,
                    r.when_ts
                );
            }
        }
        Action::Call(c) => {
            if ctx.db.record_signal_call_event(&c).await? > 0 {
                tracing::info!(
                    "call {} {} with {}{}",
                    c.call_id,
                    c.event.as_str(),
                    c.peer,
                    c.detail
                        .as_deref()
                        .map(|d| format!(" ({d})"))
                        .unwrap_or_default()
                );
            }
        }
        Action::Delete { sender, target_ts } => {
            let n = ctx.db.mark_deleted(&sender, target_ts).await?;
            tracing::info!(
                "remote-delete flagged {n} message(s) (sender={sender}, ts={target_ts})"
            );
        }
        Action::Edit(e) => {
            ctx.db.upsert_conversation(&e.thread_id).await?;
            let n = ctx.db.mark_edited(&e.sender, e.target_ts).await?;
            ctx.db
                .insert_edit(
                    &e.thread_id,
                    &e.sender,
                    e.edit_ts,
                    e.body.as_deref(),
                    e.target_ts,
                    e.is_outgoing,
                )
                .await?;
            tracing::info!(
                "edit flagged {n} original(s) + stored new version (sender={}, target={})",
                e.sender,
                e.target_ts
            );
        }
        Action::Reaction(r) => {
            ctx.db.upsert_conversation(&r.thread_id).await?;
            ctx.db
                .insert_reaction(
                    &r.thread_id,
                    r.target_ts,
                    &r.author,
                    r.emoji.as_deref(),
                    r.reaction_ts,
                    r.removed,
                )
                .await?;
        }
        Action::Message(m) => {
            ctx.db.upsert_conversation(&m.thread_id).await?;
            // `None` = a duplicate INSERT IGNORE dropped; skip its children.
            if let Some(msg_id) = ctx.db.insert_message(&m).await? {
                for att in &m.attachments {
                    let stored = match &att.id {
                        Some(id) => download_attachment(ctx, id).await,
                        None => None,
                    };
                    ctx.db
                        .insert_attachment(
                            msg_id,
                            att.content_type.as_deref(),
                            att.file_name.as_deref(),
                            att.size,
                            stored.as_deref(),
                        )
                        .await?;
                }
            }
        }
    }
    Ok(())
}

/// Best-effort: fetch the attachment blob from the rest-api and store it.
///
/// ⚠ The body is STREAMED, not buffered — see `attach::write_stream` for why
/// that is the difference between a memory limit that is a ceiling and one that
/// is a bet on how big somebody else's video is.
async fn download_attachment(ctx: &Ctx, id: &str) -> Option<String> {
    let url = format!("{}/v1/attachments/{}", ctx.http_base, id);
    let resp = ctx
        .http
        .get(&url)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        tracing::warn!("attachment {id} fetch returned {}", resp.status());
        return None;
    }
    let safe: String = id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let path = format!("{}/{}", ctx.attach_dir, safe);
    match attach::write_stream(Path::new(&path), resp.bytes_stream()).await {
        Ok(()) => Some(path),
        Err(e) => {
            tracing::warn!("storing attachment {id} failed: {e:#}");
            None
        }
    }
}

/// Periodically pull group titles (the receive payload only carries the id).
/// Keep contact names in step with what Signal shows.
///
/// ⚠ `envelope.sourceName` CANNOT DO THIS ON 0.14.5, which is the whole
/// reason for a second source of the same fact — see `display_name_of`. A name
/// that changes flows through `upsert_contact`, so the one it replaces is dated
/// rather than overwritten.
async fn refresh_contact_names(ctx: Ctx) {
    let url = format!("{}/v1/contacts/{}", ctx.http_base, ctx.number);
    loop {
        // ⚠ Generous timeout on purpose: this endpoint resolves profiles and is
        // measurably slower than /v1/groups, and a timeout here reads as "no
        // contacts" — which would be a silent no-op rather than an error.
        if let Ok(resp) = ctx
            .http
            .get(&url)
            .timeout(Duration::from_secs(120))
            .send()
            .await
            && let Ok(bytes) = resp.bytes().await
            && let Ok(Value::Array(contacts)) = serde_json::from_slice::<Value>(&bytes)
        {
            let (mut named, mut skipped) = (0usize, 0usize);
            for c in &contacts {
                let Some(uuid) = c
                    .get("uuid")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                else {
                    skipped += 1;
                    continue;
                };
                let Some(name) = display_name_of(c) else {
                    // No name from any of the three sources. Deliberately NOT an
                    // upsert with None: `upsert_contact` treats that as "learned
                    // nothing", which is right, but counting it is how a silent
                    // regression here becomes visible in the log.
                    skipped += 1;
                    continue;
                };
                let phone = c
                    .get("number")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty());
                if let Err(e) = ctx.db.upsert_contact(uuid, phone, Some(&name)).await {
                    tracing::warn!("failed to store contact name for {uuid}: {e}");
                } else {
                    named += 1;
                }
            }
            tracing::debug!("refreshed {named} contact name(s), {skipped} with no name to take");
        }
        // Hourly. A rename is a rare, human-paced event and this endpoint is the
        // expensive one; the live path still learns a name from every message.
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

async fn refresh_group_names(ctx: Ctx) {
    let url = format!("{}/v1/groups/{}", ctx.http_base, ctx.number);
    loop {
        if let Ok(resp) = ctx
            .http
            .get(&url)
            .timeout(Duration::from_secs(20))
            .send()
            .await
            && let Ok(bytes) = resp.bytes().await
            && let Ok(Value::Array(groups)) = serde_json::from_slice::<Value>(&bytes)
        {
            for g in &groups {
                let (Some(iid), Some(name)) = (
                    g.get("internal_id").and_then(Value::as_str),
                    g.get("name").and_then(Value::as_str),
                ) else {
                    continue;
                };
                if let Err(e) = ctx
                    .db
                    .set_conversation_name(&format!("group:{iid}"), name)
                    .await
                {
                    tracing::warn!("failed to store group name for {iid}: {e}");
                }
            }
            tracing::debug!("refreshed {} group name(s)", groups.len());
        }
        tokio::time::sleep(Duration::from_secs(600)).await;
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
