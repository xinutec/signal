//! The live tier: an IRC line in the archive within a second, rather than at
//! the next import.
//!
//! A long poll to the irssi plugin returns each new line with its place in the
//! log, `(file_date, line_no)`. Parsed with `irclog::parse_log`, every row is the
//! one `import_irclogs` would write on the same dedupe key, so the import, which
//! still runs, finds it present and fills anything this missed.
//!
//! A wedged poll must not pass for a quiet channel: the plugin answers an empty
//! list on its own deadline, and every cycle touches `--heartbeat` for the
//! liveness probe.
//!
//! ```text
//! irc_tail --host 10.100.0.1 --port 2230 --key /ssh/id_ed25519 \
//!     --known-hosts /ssh/known_hosts --map mynet2=mynet \
//!     --heartbeat /run/irc-tail/alive
//! ```
//!
//! Config via env, as the ingester: `DB_HOST`, `DB_PORT` (3306), `DB_NAME`,
//! `DB_USER`, `DB_PASSWORD`, and the required `IRC_SELF_NICK` (+
//! `IRC_SELF_NICK_ALT`), which decide whose lines are Pippijn's own.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use signal_archiver::db::{Db, IrcLine};
use signal_archiver::irclog::{Date, parse_log};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// How long the plugin may hold a request before answering "nothing yet". Under
/// the forced command's alarm, so a quiet channel and a wedged irssi differ.
const WAIT_MS: u64 = 120_000;

/// Ceiling on one round trip, above `WAIT_MS` plus the ssh handshake.
const ROUND_TRIP: Duration = Duration::from_secs(170);

/// How long to wait before reconnecting after a failed cycle.
const RECONNECT: Duration = Duration::from_secs(5);

struct Args {
    host: String,
    port: u16,
    key: PathBuf,
    known_hosts: PathBuf,
    self_nicks: Vec<String>,
    map: Vec<(String, String)>,
    heartbeat: Option<PathBuf>,
}

#[derive(Deserialize)]
struct Reply {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    seq: u64,
    #[serde(default)]
    events: Vec<Event>,
    /// The plugin's ring overran this client's cursor: some lines will never
    /// arrive here and only the reconciler will place them.
    #[serde(default)]
    gap: bool,
}

#[derive(Deserialize)]
struct Event {
    tag: String,
    target: String,
    /// Whether the plugin found the line's place in irssi's log; if not, the
    /// import places it.
    #[serde(default)]
    logged: bool,
    #[serde(default)]
    file_date: Option<String>,
    #[serde(default)]
    line_no: Option<u32>,
    #[serde(default)]
    line: Option<String>,
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        host: String::new(),
        port: 2230,
        key: PathBuf::new(),
        known_hosts: PathBuf::new(),
        self_nicks: vec![],
        map: vec![],
        heartbeat: None,
    };
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        let mut value = || argv.next().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--host" => args.host = value()?,
            "--port" => args.port = value()?.parse().context("--port")?,
            "--key" => args.key = PathBuf::from(value()?),
            "--known-hosts" => args.known_hosts = PathBuf::from(value()?),
            "--heartbeat" => args.heartbeat = Some(PathBuf::from(value()?)),
            "--map" => {
                let pair = value()?;
                let (from, to) = pair
                    .split_once('=')
                    .with_context(|| format!("--map wants from=to, got {pair}"))?;
                args.map.push((from.to_string(), to.to_string()));
            }
            other => bail!("unknown argument {other}"),
        }
    }
    if args.host.is_empty() {
        bail!("--host is required");
    }

    // Required: without it every line is filed as somebody else's, and a daemon's
    // warning goes unread. From the environment, so the pod needs no shell.
    args.self_nicks = std::env::var("IRC_SELF_NICK")
        .ok()
        .into_iter()
        .chain(std::env::var("IRC_SELF_NICK_ALT").ok())
        .filter(|n| !n.is_empty())
        .collect();
    if args.self_nicks.is_empty() {
        bail!(
            "IRC_SELF_NICK is not set: every line would be filed as somebody \
             else's, including Pippijn's own"
        );
    }
    Ok(args)
}

/// One long poll: ask what has happened since `after`, and wait for the answer.
///
/// The key is a copy made at startup: `ssh` refuses the permissions of a
/// root-owned, read-only secret volume.
async fn poll(args: &Args, key: &Path, after: u64) -> Result<Reply> {
    let request = serde_json::json!({ "after": after, "timeout_ms": WAIT_MS }).to_string();

    let mut child = Command::new("ssh")
        .arg("-T")
        .arg("-q")
        .args(["-F", "/dev/null"])
        .args(["-o", "BatchMode=yes"])
        .args(["-o", "IdentitiesOnly=yes"])
        .args(["-o", "StrictHostKeyChecking=yes"])
        .arg("-o")
        .arg(format!("UserKnownHostsFile={}", args.known_hosts.display()))
        .args(["-o", "ConnectTimeout=10"])
        // The poll idles for minutes; keepalives stop a NAT forgetting it.
        .args(["-o", "ServerAliveInterval=30"])
        .args(["-o", "ServerAliveCountMax=3"])
        .arg("-i")
        .arg(key)
        .arg("-p")
        .arg(args.port.to_string())
        .arg(format!("irssi@{}", args.host))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning ssh")?;

    {
        let mut stdin = child.stdin.take().context("ssh stdin")?;
        stdin.write_all(request.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.shutdown().await?;
    }

    let out = tokio::time::timeout(ROUND_TRIP, child.wait_with_output())
        .await
        .context("the tail round trip exceeded its ceiling")??;
    if !out.status.success() {
        bail!(
            "irc-tail exited {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }

    let body = String::from_utf8_lossy(&out.stdout);
    let reply: Reply = serde_json::from_str(body.trim()).context("decoding the tail reply")?;
    if !reply.ok {
        bail!("the plugin refused: {}", reply.error.unwrap_or_default());
    }
    Ok(reply)
}

/// Write one event as the row the importer would have written.
///
/// Returns whether a row was inserted; false when the import or the send echo
/// got there first.
async fn store(
    db: &Db,
    args: &Args,
    conversations: &mut BTreeMap<(String, String), u64>,
    ev: &Event,
) -> Result<bool> {
    // Without a place in the log there is no dedupe key.
    if !ev.logged {
        return Ok(false);
    }
    let (Some(file_date), Some(line_no), Some(line)) =
        (ev.file_date.as_deref(), ev.line_no, ev.line.as_deref())
    else {
        return Ok(false);
    };

    let date = parse_file_date(file_date)?;
    // The importer's parser, so this row is the one the import would write.
    let parsed = parse_log(date, &format!("{line}\n"));
    let Some(entry) = parsed.entries.into_iter().next() else {
        return Ok(false);
    };

    let stored_network = args
        .map
        .iter()
        .find(|(from, _)| *from == ev.tag)
        .map_or(ev.tag.as_str(), |(_, to)| to.as_str());
    let is_status = args.self_nicks.contains(&ev.target);

    let key = (stored_network.to_string(), ev.target.clone());
    let conversation_id = match conversations.get(&key) {
        Some(id) => *id,
        None => {
            let id = db
                .upsert_irc_conversation(
                    stored_network,
                    &ev.target,
                    ev.target.starts_with(['#', '&']),
                    is_status,
                )
                .await?;
            conversations.insert(key, id);
            id
        }
    };

    let irc_line = IrcLine {
        // The plugin's line number; `parse_log` saw only this one line.
        line_no,
        sent_at: entry.at.to_string(),
        nick: entry.nick.clone(),
        is_self: entry
            .nick
            .as_ref()
            .is_some_and(|n| args.self_nicks.contains(n)),
        kind: entry.kind.as_str(),
        text: entry.text.clone(),
    };

    // The raw tag, before `--map`, as in the dedupe key (migration v8).
    let written = db
        .insert_irc_lines(conversation_id, &ev.tag, file_date, &[irc_line])
        .await?;
    Ok(written > 0)
}

fn parse_file_date(s: &str) -> Result<Date> {
    let mut parts = s.split('-');
    let mut next = |what: &str| -> Result<u32> {
        parts
            .next()
            .with_context(|| format!("file_date has no {what}: {s}"))?
            .parse()
            .with_context(|| format!("file_date {what} is not a number: {s}"))
    };
    let year = next("year")? as i32;
    let month = next("month")?;
    let day = next("day")?;
    Ok(Date { year, month, day })
}

/// Touch the file the liveness probe reads.
///
/// Every successful cycle, including empty ones.
fn beat(args: &Args) {
    let Some(path) = &args.heartbeat else { return };
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    if let Err(e) = std::fs::write(path, format!("{:?}\n", std::time::SystemTime::now())) {
        eprintln!(
            "irc_tail: could not write the heartbeat to {}: {e}",
            path.display()
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let db = Db::connect(&signal_archiver::db::url_from_env()?).await?;

    // See `poll`: the mounted secret is not readable by this process's user.
    let key = PathBuf::from("/tmp/irc-tail-key");
    std::fs::copy(&args.key, &key).with_context(|| format!("copying {}", args.key.display()))?;
    let mut perms = std::fs::metadata(&key)?.permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o400);
    std::fs::set_permissions(&key, perms)?;

    let mut conversations: BTreeMap<(String, String), u64> = BTreeMap::new();
    // From 0: the first poll replays the plugin's whole ring, which the dedupe
    // key makes harmless.
    let mut after: u64 = 0;

    println!(
        "irc_tail: polling {}:{} for logged lines",
        args.host, args.port
    );
    beat(&args);

    loop {
        match poll(&args, &key, after).await {
            Ok(reply) => {
                if reply.gap {
                    eprintln!(
                        "irc_tail: the plugin's ring overran our cursor — \
                         some lines will arrive with the next import, not here"
                    );
                }
                let mut wrote = 0;
                let mut lost = 0;
                for ev in &reply.events {
                    match store(&db, &args, &mut conversations, ev).await {
                        Ok(true) => wrote += 1,
                        Ok(false) => {}
                        // The import will place it.
                        Err(e) => {
                            lost += 1;
                            eprintln!("irc_tail: could not store a line: {e:#}");
                        }
                    }
                }
                if !reply.events.is_empty() {
                    // "Already archived" is the normal outcome for a line sent
                    // from the app, whose echo is written first; only "lost" is
                    // a fault.
                    let held = reply.events.len() - wrote - lost;
                    let alarm = if lost > 0 {
                        format!(", {lost} lost to errors above")
                    } else {
                        String::new()
                    };
                    println!(
                        "irc_tail: {offered} line(s) offered, {wrote} new, \
                         {held} already archived{alarm}, seq now {seq}",
                        offered = reply.events.len(),
                        seq = reply.seq,
                    );
                }
                after = reply.seq;
                beat(&args);
            }
            Err(e) => {
                // No heartbeat: this is what the liveness probe is for.
                eprintln!("irc_tail: poll failed, retrying in {RECONNECT:?}: {e:#}");
                tokio::time::sleep(RECONNECT).await;
            }
        }
    }
}
