//! `attach::write_stream` — the chunks land in order, and a body that fails
//! part-way leaves NO file behind.
//!
//! The property that motivated the function (bounded resident memory) is not
//! one a test can observe; what a test can hold is that streaming did not cost
//! correctness, which is the way this could have gone wrong quietly.

use std::io::{Error, ErrorKind};
use std::path::PathBuf;

use futures::stream;
use signal_archiver::attach::write_stream;

fn tmp(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "signal-archiver-test-{name}-{}",
        std::process::id()
    ));
    p
}

#[tokio::test]
async fn chunks_are_concatenated_in_order() {
    let path = tmp("ordered");
    let body = stream::iter(vec![
        Ok::<_, Error>(b"the ".to_vec()),
        Ok(b"whole".to_vec()),
        Ok(b" file".to_vec()),
    ]);

    write_stream(&path, body).await.expect("write");

    assert_eq!(std::fs::read(&path).unwrap(), b"the whole file");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn an_empty_body_still_writes_an_empty_file() {
    // A zero-byte attachment is a real answer from the bridge, and it must not
    // be indistinguishable from a failed download.
    let path = tmp("empty");
    let body = stream::iter(Vec::<Result<Vec<u8>, Error>>::new());

    write_stream(&path, body).await.expect("write");

    assert!(path.exists());
    assert_eq!(std::fs::read(&path).unwrap(), b"");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn a_failure_part_way_leaves_no_file() {
    // The caller does not record the row when this errors, so a short file
    // would sit on the volume for ever looking like a whole one.
    let path = tmp("truncated");
    let body = stream::iter(vec![
        Ok(b"first half".to_vec()),
        Err(Error::new(ErrorKind::UnexpectedEof, "connection reset")),
    ]);

    let err = write_stream(&path, body).await.expect_err("must fail");

    assert!(
        !path.exists(),
        "a partial blob was left at {}",
        path.display()
    );
    assert!(
        format!("{err:#}").contains("connection reset"),
        "the underlying cause is lost: {err:#}"
    );
}
