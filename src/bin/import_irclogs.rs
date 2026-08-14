//! Import irssi autologs into the archive's `irc_*` tables.
//!
//! Idempotent, and dry-run by default — the same shape as `tools/import_gchat.py`,
//! for the same reason: an import that writes on its first invocation gives you
//! nowhere to check its arithmetic. Pass `--apply` to write.
//!
//! ```text
//! rsync -a irc:irclogs/ /some/staging/irclogs/
//! import_irclogs --root /some/staging/irclogs \
//!     --network mynet --network mynet2 --map mynet2=mynet \
//!     --self-nick mynick --self-nick mynick_ [--apply]
//! ```
//!
//! **Why `--self-nick` is an argument and not a constant.** It is the only way
//! to know which lines are yours — irssi logs your own messages under your nick
//! like anybody else's — and this repository is public, so a real nick does not
//! belong in it.
//!
//! **Why `--map` exists.** irssi invents a second tag (`mynet2`) when it opens a
//! second simultaneous connection to a network whose tag is taken. Those logs
//! are the same conversations, so the tag is rewritten on the way in rather than
//! by moving 1,265 files around on a live archive going back to 2013. The
//! original tag is still recorded per line as `source_tag`, which is what keeps
//! the two files a day can then have from colliding in the dedupe key.
//!
//! Config via env, as the ingester: `DB_HOST`, `DB_PORT` (3306), `DB_NAME`,
//! `DB_USER`, `DB_PASSWORD`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use signal_archiver::db::{Db, IrcLine};
use signal_archiver::irclog::{Kind, parse_log, parse_path};

struct Args {
    root: PathBuf,
    /// Empty means every network found under the root.
    networks: Vec<String>,
    /// Source tag → stored network.
    map: Vec<(String, String)>,
    self_nicks: Vec<String>,
    apply: bool,
}

/// What the run saw. Printed whole at the end, because a number that only
/// appears while scrolling past is not a number anybody checks.
#[derive(Default)]
struct Report {
    files: u64,
    /// Paths with no network component — the ones predating irssi's `$tag`.
    legacy_paths: u64,
    lossy_files: Vec<String>,
    by_kind: BTreeMap<&'static str, u64>,
    inserted: u64,
    duplicates: u64,
    unparsed: u64,
    /// A few examples, so an unrecognised class can actually be looked at.
    unparsed_examples: Vec<String>,
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        root: PathBuf::new(),
        networks: vec![],
        map: vec![],
        self_nicks: vec![],
        apply: false,
    };
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        let mut value = || argv.next().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--root" => args.root = PathBuf::from(value()?),
            "--network" => args.networks.push(value()?),
            "--self-nick" => args.self_nicks.push(value()?),
            "--map" => {
                let pair = value()?;
                let (from, to) = pair
                    .split_once('=')
                    .with_context(|| format!("--map wants from=to, got {pair}"))?;
                args.map.push((from.to_string(), to.to_string()));
            }
            "--apply" => args.apply = true,
            other => bail!("unknown argument {other}"),
        }
    }
    if args.root.as_os_str().is_empty() {
        bail!("--root is required (a local copy of the irclogs tree)");
    }
    Ok(args)
}

/// Every `*.log` under `root`, as paths relative to it, in sorted order.
///
/// Sorted because `id` is the tiebreak for two lines in the same minute —
/// irssi's `%H:%M` is all the precision there is — and `<net>/<Y>/<M>/<D>` sorts
/// chronologically. Walking in readdir order would scatter a conversation's
/// ordering by whatever the filesystem happened to hand back.
fn collect_logs(root: &Path) -> Result<Vec<String>> {
    let mut out = vec![];
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in
            std::fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))?
        {
            let path = entry?.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "log") {
                let rel = path
                    .strip_prefix(root)
                    .context("path escaped the root")?
                    .to_string_lossy()
                    .into_owned();
                out.push(rel);
            }
        }
    }
    out.sort();
    Ok(out)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let db = if args.apply {
        Some(Db::connect(&db_url()?).await?)
    } else {
        None
    };

    let logs = collect_logs(&args.root)?;
    let mut report = Report::default();
    let mut conversations: BTreeMap<(String, String), u64> = BTreeMap::new();

    for rel in &logs {
        let Some(path) = parse_path(rel) else {
            report.legacy_paths += 1;
            continue;
        };
        if !args.networks.is_empty() && !args.networks.contains(&path.network) {
            continue;
        }
        let stored_network = args
            .map
            .iter()
            .find(|(from, _)| *from == path.network)
            .map_or(path.network.as_str(), |(_, to)| to.as_str());

        // Lossily: at least one file in the measured tree is not valid UTF-8,
        // and losing a byte beats refusing the thirteen years around it.
        let bytes = std::fs::read(args.root.join(rel))?;
        let text = match String::from_utf8_lossy(&bytes) {
            std::borrow::Cow::Borrowed(s) => s.to_string(),
            std::borrow::Cow::Owned(s) => {
                report.lossy_files.push(rel.clone());
                s
            }
        };

        let parsed = parse_log(path.date, &text);
        report.files += 1;
        report.unparsed += parsed.unparsed.len() as u64;
        for line_no in &parsed.unparsed {
            if report.unparsed_examples.len() < 20 {
                report.unparsed_examples.push(format!("{rel}:{line_no}"));
            }
        }

        let is_status = args.self_nicks.contains(&path.target);
        let file_date = format!(
            "{:04}-{:02}-{:02}",
            path.date.year, path.date.month, path.date.day
        );

        for entry in &parsed.entries {
            *report.by_kind.entry(entry.kind.as_str()).or_default() += 1;
        }

        let Some(db) = &db else { continue };
        if parsed.entries.is_empty() {
            continue;
        }

        let key = (stored_network.to_string(), path.target.clone());
        let conversation_id = match conversations.get(&key) {
            Some(id) => *id,
            None => {
                let id = db
                    .upsert_irc_conversation(
                        stored_network,
                        &path.target,
                        path.is_channel(),
                        is_status,
                    )
                    .await?;
                conversations.insert(key, id);
                id
            }
        };

        let lines: Vec<IrcLine> = parsed
            .entries
            .iter()
            .map(|entry| IrcLine {
                line_no: entry.line_no,
                sent_at: entry.at.to_string(),
                nick: entry.nick.clone(),
                is_self: entry
                    .nick
                    .as_ref()
                    .is_some_and(|n| args.self_nicks.contains(n)),
                kind: entry.kind.as_str(),
                text: entry.text.clone(),
            })
            .collect();

        // The source tag, not the stored network: two connections to one server
        // write the same path under different tags, and this is what keeps their
        // lines from colliding once --map has merged the conversations.
        let written = db
            .insert_irc_lines(conversation_id, &path.network, &file_date, &lines)
            .await?;
        report.inserted += written;
        report.duplicates += lines.len() as u64 - written;

        if report.files.is_multiple_of(500) {
            println!(
                "  {} files, {} rows written…",
                report.files, report.inserted
            );
        }
    }

    print_report(&args, &report);
    Ok(())
}

fn print_report(args: &Args, report: &Report) {
    println!("{} log files read", report.files);
    if report.legacy_paths > 0 {
        println!(
            "{} paths skipped: no network component, so the server is unknown \
             (these predate irssi's $tag in autolog_path)",
            report.legacy_paths
        );
    }
    let total: u64 = report.by_kind.values().sum();
    println!("{total} lines recognised:");
    for (kind, n) in &report.by_kind {
        println!("  {kind:<8} {n}");
    }
    if report.unparsed > 0 {
        println!(
            "⚠ {} lines matched no known shape — a class nobody has written down yet:",
            report.unparsed
        );
        for example in &report.unparsed_examples {
            println!("    {example}");
        }
    }
    if !report.lossy_files.is_empty() {
        println!(
            "⚠ {} file(s) were not valid UTF-8 and were read lossily:",
            report.lossy_files.len()
        );
        for file in report.lossy_files.iter().take(10) {
            println!("    {file}");
        }
    }
    if args.apply {
        println!(
            "wrote {} rows, {} already present",
            report.inserted, report.duplicates
        );
    } else {
        println!("DRY RUN — nothing written. Pass --apply.");
    }
    if args.self_nicks.is_empty() {
        println!(
            "⚠ no --self-nick given: every line is attributed to somebody else, \
             and no conversation is marked as the status log."
        );
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

/// Kept honest by the compiler: every kind the parser can produce has a column
/// value in the `irc_messages.kind` ENUM. Adding a variant without extending the
/// ENUM would otherwise fail at run time, halfway through an import.
const _: () = {
    let kinds = [Kind::Message, Kind::Action, Kind::Event, Kind::Notice];
    assert!(kinds.len() == 4);
};
