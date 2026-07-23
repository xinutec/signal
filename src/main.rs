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

use signal_archiver::db::Db;
use signal_archiver::parse::{Action, parse_frame};

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

    let db = Db::connect(&database_url()?)
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
            if let Some(msg_id) = ctx
                .db
                .insert_message(
                    &m.thread_id,
                    &m.sender,
                    m.server_ts,
                    m.body.as_deref(),
                    m.quote_target_ts,
                    m.is_outgoing,
                )
                .await?
            {
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
    let bytes = resp.bytes().await.ok()?;
    let safe: String = id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let path = format!("{}/{}", ctx.attach_dir, safe);
    match tokio::fs::write(&path, &bytes).await {
        Ok(()) => Some(path),
        Err(e) => {
            tracing::warn!("writing attachment {id} failed: {e}");
            None
        }
    }
}

/// Periodically pull group titles (the receive payload only carries the id).
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

fn database_url() -> Result<String> {
    let host = std::env::var("DB_HOST").context("DB_HOST not set")?;
    let port = env_or("DB_PORT", "3306");
    let name = std::env::var("DB_NAME").context("DB_NAME not set")?;
    let user = std::env::var("DB_USER").context("DB_USER not set")?;
    let pass = std::env::var("DB_PASSWORD").context("DB_PASSWORD not set")?;
    Ok(format!("mysql://{user}:{pass}@{host}:{port}/{name}"))
}
