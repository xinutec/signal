//! Writing an attachment blob to disk, one chunk at a time.
//!
//! ⚠ THE POINT IS THE MEMORY CEILING, not the file. The ingester used to fetch
//! an attachment with `resp.bytes().await` — the whole blob resident before the
//! write — so its peak memory was the largest thing anybody sent it, a number
//! this process does not choose and cannot see coming. That is why the
//! container could not carry a memory limit: any cap would be a cap on somebody
//! else's video, and the OOM-kill would arrive looking like an unexplained
//! crash-loop rather than like the attachment it was. Measured on isis
//! 2026-08-17: 5Mi steady state against a 64Mi request, which says nothing at
//! all about the peak.
//!
//! Streamed, resident size is one chunk, so a limit is a real ceiling again.
//!
//! In the lib rather than beside its one caller because the caller is a binary,
//! and a `main.rs` function cannot be reached from `tests/` — the same reason
//! `parse` lives here.

use std::path::Path;

use anyhow::{Context, Result};
use futures::{Stream, StreamExt};

/// Write every chunk of `body` to `path`, in order.
///
/// On any failure the partial file is REMOVED before returning. A half-written
/// blob is worse than none: the caller does not record the row, so nothing ever
/// comes back to finish it or to notice that it is short — it would simply sit
/// on the volume being a valid-looking file of the wrong length.
pub async fn write_stream<S, B, E>(path: &Path, body: S) -> Result<()>
where
    S: Stream<Item = std::result::Result<B, E>>,
    B: AsRef<[u8]>,
    E: std::error::Error + Send + Sync + 'static,
{
    match write_chunks(path, body).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = tokio::fs::remove_file(path).await;
            Err(e)
        }
    }
}

async fn write_chunks<S, B, E>(path: &Path, body: S) -> Result<()>
where
    S: Stream<Item = std::result::Result<B, E>>,
    B: AsRef<[u8]>,
    E: std::error::Error + Send + Sync + 'static,
{
    use tokio::io::AsyncWriteExt;

    let mut file = tokio::fs::File::create(path)
        .await
        .with_context(|| format!("creating {}", path.display()))?;
    let mut body = std::pin::pin!(body);
    while let Some(chunk) = body.next().await {
        let chunk = chunk.context("reading the attachment body")?;
        file.write_all(chunk.as_ref())
            .await
            .with_context(|| format!("writing {}", path.display()))?;
    }
    // Explicit rather than trusting the drop: `tokio::fs::File`'s drop cannot
    // report an error, and a flush that fails on a full volume is exactly the
    // case where a short file must not pass as a whole one.
    file.flush()
        .await
        .with_context(|| format!("flushing {}", path.display()))?;
    Ok(())
}
