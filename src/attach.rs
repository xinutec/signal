//! Writing an attachment blob to disk, one chunk at a time.
//!
//! Streaming keeps resident memory at one chunk, so the container's memory
//! limit does not depend on the largest attachment anyone sends.

use std::path::Path;

use anyhow::{Context, Result};
use futures::{Stream, StreamExt};

/// Write every chunk of `body` to `path`, in order.
///
/// On failure the partial file is removed; nothing would come back to finish it.
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
