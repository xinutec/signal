//! Import irssi autologs into the archive's `irc_*` tables.
//!
//! Idempotent, and dry-run by default; pass `--apply` to write.
//!
//! ```text
//! rsync -a irc:irclogs/ /some/staging/irclogs/
//! import_irclogs --root /some/staging/irclogs \
//!     --network mynet --network mynet2 --map mynet2=mynet \
//!     --self-nick mynick --self-nick mynick_ [--apply] [--all]
//! ```
//!
//! Under `--apply`, each file's `(mtime, size)` goes into `irc_import_state` and
//! unchanged files are skipped, so a run costs what arrived. `--all` reads
//! everything; use it after changing the parser.
//!
//! `--self-nick` marks your own lines. It is an argument because this
//! repository is public.
//!
//! `--map` merges the second tag irssi invents for a second simultaneous
//! connection (`mynet2`) into the first. The original tag stays in `source_tag`;
//! see migration v8.
//!
//! Config via env, as the ingester: `DB_HOST`, `DB_PORT` (3306), `DB_NAME`,
//! `DB_USER`, `DB_PASSWORD`.

use std::collections::{BTreeMap, HashMap};
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
    /// Read every file, whatever `irc_import_state` says. The end-of-run report
    /// describes only the files read, so this is the whole-corpus audit.
    all: bool,
}

/// What the run saw, printed at the end.
#[derive(Default)]
struct Report {
    files: u64,
    /// Unchanged since the last `--apply` run, so not opened at all.
    skipped: u64,
    /// Paths with no network component.
    legacy_paths: u64,
    lossy_files: Vec<String>,
    by_kind: BTreeMap<&'static str, u64>,
    inserted: u64,
    duplicates: u64,
    unparsed: u64,
    /// A few examples of unrecognised lines.
    unparsed_examples: Vec<String>,
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        root: PathBuf::new(),
        networks: vec![],
        map: vec![],
        self_nicks: vec![],
        apply: false,
        all: false,
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
            "--all" => args.all = true,
            other => bail!("unknown argument {other}"),
        }
    }
    if args.root.as_os_str().is_empty() {
        bail!("--root is required (a local copy of the irclogs tree)");
    }
    Ok(args)
}

/// Whether a log file has changed: mtime in nanoseconds, and size in bytes.
/// The size catches what a coarse `rsync` mtime might not.
fn file_state(path: &Path) -> Result<(i64, i64)> {
    let meta = std::fs::metadata(path)?;
    let mtime = meta
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64);
    Ok((mtime, meta.len() as i64))
}

/// The part of a snapshot that is safe to parse: up to and including the last
/// newline.
///
/// A line without its newline may still be being written. Imported, it would
/// keep its truncated text forever, since the complete line has the same
/// `line_no`. Left out, it is read whole once the file grows.
fn complete_lines(text: &str) -> &str {
    match text.rfind('\n') {
        Some(i) => &text[..=i],
        None => "",
    }
}

/// Every `*.log` under `root`, as paths relative to it, in sorted order.
///
/// Sorted, because `id` orders lines within a minute and `<net>/<Y>/<M>/<D>`
/// sorts chronologically.
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
        Some(Db::connect(&signal_archiver::db::url_from_env()?).await?)
    } else {
        None
    };

    let logs = collect_logs(&args.root)?;
    let mut report = Report::default();
    let mut conversations: BTreeMap<(String, String), u64> = BTreeMap::new();

    // Empty under `--all`, and on a dry run, which has no database connection.
    let already_read = match (&db, args.all) {
        (Some(db), false) => db.irc_import_state().await?,
        _ => HashMap::new(),
    };
    // Files read but not yet recorded as read, awaiting the next flush.
    let mut pending_state: Vec<(String, i64, i64)> = vec![];

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

        let full = args.root.join(rel);
        let state = file_state(&full)
            .with_context(|| format!("reading the state of {}", full.display()))?;
        if already_read.get(rel) == Some(&state) {
            report.skipped += 1;
            continue;
        }

        // Lossily: some old logs are not valid UTF-8.
        let bytes = std::fs::read(&full)?;
        let text = match String::from_utf8_lossy(&bytes) {
            std::borrow::Cow::Borrowed(s) => s.to_string(),
            std::borrow::Cow::Owned(s) => {
                report.lossy_files.push(rel.clone());
                s
            }
        };

        let parsed = parse_log(path.date, complete_lines(&text));
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

        if !parsed.entries.is_empty() {
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

            // The source tag, not the mapped network; see migration v8.
            let written = db
                .insert_irc_lines(conversation_id, &path.network, &file_date, &lines)
                .await?;
            report.inserted += written;
            report.duplicates += lines.len() as u64 - written;
        }

        // After its lines are in, empty files included. A file that failed never
        // gets here, so the next run retries it. Flushed with the progress line.
        pending_state.push((rel.clone(), state.0, state.1));

        if report.files.is_multiple_of(500) {
            db.record_irc_imports(&pending_state).await?;
            pending_state.clear();
            println!(
                "  {} files, {} rows written…",
                report.files, report.inserted
            );
        }
    }

    // The final partial batch.
    if let Some(db) = &db
        && !pending_state.is_empty()
    {
        db.record_irc_imports(&pending_state).await?;
    }

    print_report(&args, &report);
    Ok(())
}

fn print_report(args: &Args, report: &Report) {
    println!("{} log files read", report.files);
    // Everything below describes only the files this run read.
    if report.skipped > 0 {
        println!(
            "{} unchanged since the last import and not opened — the counts below \
             describe the {} file(s) actually read, not the archive. `--all` \
             re-reads everything.",
            report.skipped, report.files
        );
    }
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

/// Fails to compile when a `Kind` is added: extend the `irc_messages.kind` ENUM
/// with it, or the import fails at run time.
const _: fn(Kind) = |k| match k {
    Kind::Message | Kind::Action | Kind::Event | Kind::Notice => {}
};
