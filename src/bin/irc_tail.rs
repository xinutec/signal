//! The live tier: learn about an IRC line in under a second instead of waiting
//! for the next import.
//!
//! ⚠ **THIS IS NOT A SECOND IMPORTER, AND THE DISTINCTION IS THE DESIGN.** It
//! holds one long poll open to the irssi plugin, which answers with the lines
//! irssi has just logged AND WHERE THEY ARE — the `(file_date, line_no)` that,
//! with the conversation and the source tag, is the archive's dedupe key. Every
//! row it writes is therefore the row `import_irclogs` would have written, on
//! the same key, so the periodic import later finds it already present. The two
//! tiers cannot disagree about a line's identity because they compute it the
//! same way.
//!
//! It is also why the *line* is parsed rather than the plugin's convenience
//! fields: `irclog::parse_log` is the one thing that decides what a logged line
//! means — that `< nick>` carries a channel mode in a fixed column, that an
//! action is not a message — and a second interpretation here would drift from
//! the importer's within a week.
//!
//! **What happens when this fails is the point.** A missed line is not lost; it
//! is late, because the reconciler is still running. That is what allows this
//! tier to be the simple one. ⚠ The failure it must NOT have is the quiet kind:
//! a wedged poll looks exactly like a quiet channel, so the plugin answers an
//! empty list on its own deadline and every cycle touches `--heartbeat`, which
//! the pod's liveness probe reads. Silence restarts the pod rather than passing
//! for calm.
//!
//! ```text
//! irc_tail --host 10.100.0.1 --port 2230 --key /ssh/id_ed25519 \
//!     --known-hosts /ssh/known_hosts --map mynet2=mynet \
//!     --heartbeat /run/irc-tail/alive
//! ```
//!
//! Config via env, as the ingester: `DB_HOST`, `DB_PORT` (3306), `DB_NAME`,
//! `DB_USER`, `DB_PASSWORD` — and `IRC_SELF_NICK` (+ `IRC_SELF_NICK_ALT`),
//! which decide whose lines are Pippijn's own and are REQUIRED for the reason
//! `parse_args` gives.

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

/// How long the plugin may hold a request before answering "nothing yet".
///
/// Bounded well under the forced command's own alarm so that the PLUGIN is what
/// ends a quiet exchange. If ssh timed out first, a quiet channel and a wedged
/// irssi would arrive here as the same event.
const WAIT_MS: u64 = 120_000;

/// Ceiling on one round trip, above `WAIT_MS` plus the ssh handshake.
const ROUND_TRIP: Duration = Duration::from_secs(170);

/// How long to wait before reconnecting after a failed cycle. Long enough not to
/// hammer a restarting irssi, short enough that a blip costs one message's
/// latency rather than a conversation's.
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
    /// Whether the plugin found the line in irssi's log. False means the line
    /// exists but its place is not yet known — the reconciler's job, not ours.
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

    // ⚠ FROM THE ENVIRONMENT, AND REQUIRED, and both halves of that are
    // corrections to how this shipped.
    //
    // From the environment because the alternative is a `/bin/sh -c` wrapper in
    // the pod spec purely to expand a variable — the importer needs a shell
    // anyway (rsync then import), this does not, and adding one to pass an
    // argument is machinery for its own sake.
    //
    // REQUIRED because without it every line is attributed to somebody else,
    // and that is what happened: the first message pushed after this went live
    // was Pippijn's own and the app drew it as another person's, because the
    // Deployment passed no nicks at all. The importer only PRINTS a warning in
    // that case, which is defensible for a command somebody is watching and
    // useless for a daemon nobody is. Refusing to start turns a silent
    // mislabelling into a CrashLoopBackOff, which is the loudest thing a pod
    // can do.
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
/// ⚠ The key is copied to a writable path at startup rather than used in place:
/// a Kubernetes secret volume is root-owned and mounted read-only, so `ssh`
/// refuses its permissions — see the note in `messages`' `irc_send.rs`, which
/// learned this the same way.
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
        // The plugin parks for two minutes, so the connection is idle for two
        // minutes; without keepalives a NAT or a firewall in between is free to
        // forget it and the poll hangs until the round-trip ceiling.
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
/// Returns whether a row was actually inserted, which is usually false for a
/// line the reconciler happened to reach first — the dedupe key doing its job,
/// not an error.
async fn store(
    db: &Db,
    args: &Args,
    conversations: &mut BTreeMap<(String, String), u64>,
    ev: &Event,
) -> Result<bool> {
    // Without a place in the log there is no dedupe key, and a row invented
    // without one would be the duplicate this design exists to avoid.
    if !ev.logged {
        return Ok(false);
    }
    let (Some(file_date), Some(line_no), Some(line)) =
        (ev.file_date.as_deref(), ev.line_no, ev.line.as_deref())
    else {
        return Ok(false);
    };

    let date = parse_file_date(file_date)?;
    // ⚠ THE IMPORTER'S PARSER, on the importer's input. Everything about what a
    // logged line means — the channel mode in `<@nick>`, an action against a
    // message — is decided in one place, so this row and the one the next import
    // would write are the same row.
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
        // ⚠ THE PLUGIN'S NUMBER, not the parser's. `parse_log` numbered this
        // line 1 because it was handed one line; its real position in the file
        // is what irssi's log says, and that is half the dedupe key.
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

    // The RAW tag, before `--map`: two connections to one network write the same
    // path under different tags, and this is what keeps their lines apart.
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
/// ⚠ Every cycle, including the ones that found nothing. A heartbeat that only
/// beat when something happened would call a quiet evening a failure — and,
/// worse, would let a poll that has stopped asking pass as quiet.
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
    let db = Db::connect(&db_url()?).await?;

    // See `poll`: the mounted secret is not readable by this process's user.
    let key = PathBuf::from("/tmp/irc-tail-key");
    std::fs::copy(&args.key, &key).with_context(|| format!("copying {}", args.key.display()))?;
    let mut perms = std::fs::metadata(&key)?.permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o400);
    std::fs::set_permissions(&key, perms)?;

    let mut conversations: BTreeMap<(String, String), u64> = BTreeMap::new();
    // ⚠ Starts at 0, so the first poll is handed the plugin's whole ring. That is
    // deliberate and safe: every one of those lines is written on the dedupe key,
    // so a line the reconciler already has is refused rather than duplicated.
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
                    // Stated rather than inferred: the fast path is knowingly
                    // incomplete here and the import is what will close it.
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
                        // One bad event must not end the loop: the reconciler
                        // will place that line, and the next one may be fine.
                        Err(e) => {
                            lost += 1;
                            eprintln!("irc_tail: could not store a line: {e:#}");
                        }
                    }
                }
                if !reply.events.is_empty() {
                    // ⚠ Three outcomes, and only the third is a fault. Saying
                    // just "N offered, W written" reports the healthy race as a
                    // shortfall: sending from the app archives the echo first,
                    // so the tail is SUPPOSED to lose and write nothing. An
                    // alert that fires on correct behaviour is one you learn to
                    // ignore, which is how the real case below gets missed.
                    let held = reply.events.len() - wrote - lost;
                    let alarm = if lost > 0 {
                        format!(", {lost} LOST to errors above")
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
                // ⚠ NOT a heartbeat. A poll that cannot reach irssi is exactly
                // the state the liveness probe exists to notice; beating here
                // would report health on the strength of having tried.
                eprintln!("irc_tail: poll failed, retrying in {RECONNECT:?}: {e:#}");
                tokio::time::sleep(RECONNECT).await;
            }
        }
    }
}

fn db_url() -> Result<String> {
    let host = std::env::var("DB_HOST").context("DB_HOST not set")?;
    let port = std::env::var("DB_PORT").unwrap_or_else(|_| "3306".to_string());
    let name = std::env::var("DB_NAME").context("DB_NAME not set")?;
    let user = std::env::var("DB_USER").context("DB_USER not set")?;
    let pass = std::env::var("DB_PASSWORD").context("DB_PASSWORD not set")?;
    Ok(format!("mysql://{user}:{pass}@{host}:{port}/{name}"))
}
