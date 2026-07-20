// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::borrow::Cow;

use indicatif::{ProgressBar, ProgressFinish, ProgressStyle};
use tempfile::TempPath;
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc,
    task::{self, JoinHandle},
};

/// Spawn a task writing every received line to a temporary JSON file.
///
/// The returned handle resolves once every sender is dropped.
///
/// Write and flush failures are propagated, never logged and swallowed. A
/// truncated file is indistinguishable from a short one downstream, and under
/// [`crate::fetcher::Semantics::Authoritative`] that reads as "these rows no
/// longer exist" — a dropped write would silently delete stored rows.
pub fn temp_file_writer() -> (mpsc::Sender<String>, JoinHandle<anyhow::Result<TempPath>>) {
    let (tx, rx) = mpsc::channel::<String>(1024);

    let file_handle = task::spawn(async move {
        let (file, path) = task::spawn_blocking(|| tempfile::NamedTempFile::with_suffix(".json"))
            .await??
            .into_parts();

        write_lines(rx, tokio::fs::File::from_std(file))
            .await
            .map_err(|error| anyhow::anyhow!("failed to write {}: {error}", path.display()))?;

        Ok(path)
    });

    (tx, file_handle)
}

/// Drain `rx` into `sink`, returning the first write or flush error.
///
/// Split out from [`temp_file_writer`] so the failure path can be driven with a
/// sink that fails on demand — a real disk error can't be provoked in-process,
/// and this is the path where swallowing one silently deletes rows.
async fn write_lines<W>(mut rx: mpsc::Receiver<String>, sink: W) -> std::io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut writer = tokio::io::BufWriter::with_capacity(128 * 1024, sink);

    // Drain the channel even after a failure, so senders never block on a
    // full channel waiting for a writer that has given up.
    let mut result = Ok(());
    while let Some(x) = rx.recv().await {
        if result.is_ok() {
            result = writer.write_all(x.as_bytes()).await;
        }
    }

    // Flush both the buffered writer and the underlying sink
    result = result.and(writer.flush().await);
    result.and(writer.into_inner().flush().await)
}

pub fn resource_table_name(resource_type: impl AsRef<str>) -> String {
    resource_type
        .as_ref()
        .trim_start_matches("AWS::")
        .replace("::", "_")
        .to_lowercase()
}

pub fn progress_bar(message: impl Into<Cow<'static, str>>, total: u64) -> ProgressBar {
    let style = ProgressStyle::default_bar()
        .template("{msg:30} [{human_pos:>6}/{human_len:<6}] [{bar:40.cyan/blue}] ({per_sec})")
        .expect("valid template")
        .progress_chars("=> ");

    ProgressBar::new(total)
        .with_finish(ProgressFinish::AndLeave)
        .with_style(style)
        .with_message(message)
}

#[cfg(test)]
mod spool_tests {
    use super::*;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A sink that fails where it is told to. Stands in for a full disk, which
    /// can't be provoked in-process.
    struct FailingSink {
        fail_write: bool,
        fail_flush: bool,
    }

    impl tokio::io::AsyncWrite for FailingSink {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.fail_write {
                return Poll::Ready(Err(io::Error::other("disk full")));
            }
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.fail_flush {
                return Poll::Ready(Err(io::Error::other("flush failed")));
            }
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Sends `lines` and returns what the spool made of them.
    async fn spool(lines: Vec<String>, sink: FailingSink) -> std::io::Result<()> {
        let (tx, rx) = mpsc::channel::<String>(1024);
        let handle = task::spawn(async move { write_lines(rx, sink).await });
        for line in lines {
            // A send error means the writer gave up early, which is itself the
            // bug this test guards against — surface it rather than ignoring it.
            tx.send(line).await.expect("spool stopped receiving early");
        }
        drop(tx);
        handle.await.unwrap()
    }

    /// Enough to overflow the 128KiB buffer, so `write_all` reaches the sink
    /// rather than being absorbed and only surfacing at flush.
    fn big_lines() -> Vec<String> {
        vec!["x".repeat(4096) + "\n"; 64]
    }

    /// The core guarantee: a failed write is an error, not a short file. Under
    /// `Authoritative` semantics a short file is read as "these rows are gone".
    #[tokio::test]
    async fn write_failure_is_reported() {
        let result = spool(
            big_lines(),
            FailingSink {
                fail_write: true,
                fail_flush: false,
            },
        )
        .await;

        assert!(
            result.is_err(),
            "a failed write must not report success — the truncated file would be landed"
        );
    }

    /// Small writes sit in the buffer and only reach the sink at flush, so the
    /// flush path needs its own check or a short file still slips through.
    #[tokio::test]
    async fn flush_failure_is_reported() {
        let result = spool(
            vec!["one line\n".to_owned()],
            FailingSink {
                fail_write: false,
                fail_flush: true,
            },
        )
        .await;

        assert!(
            result.is_err(),
            "a failed flush must not report success — the buffered rows never reached disk"
        );
    }

    /// The writer must keep draining after a failure. Returning early drops the
    /// receiver, and a fetcher still sending would then block on a full channel
    /// or get a confusing send error instead of the real disk error.
    #[tokio::test]
    async fn senders_are_not_stranded_after_a_write_failure() {
        // More lines than the channel holds, so a stalled reader would deadlock.
        let many = vec!["y".repeat(4096) + "\n"; 2048];
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            spool(
                many,
                FailingSink {
                    fail_write: true,
                    fail_flush: false,
                },
            ),
        )
        .await
        .expect("spool deadlocked after a write failure");

        assert!(
            result.is_err(),
            "the write failure should still be reported"
        );
    }

    #[tokio::test]
    async fn a_clean_run_reports_success() {
        let result = spool(
            big_lines(),
            FailingSink {
                fail_write: false,
                fail_flush: false,
            },
        )
        .await;

        assert!(result.is_ok());
    }

    /// The real writer, end to end: every line reaches the file.
    #[tokio::test]
    async fn temp_file_writer_records_every_line() {
        let (tx, handle) = temp_file_writer();
        for i in 0..1000 {
            tx.send(format!("line-{i}\n")).await.unwrap();
        }
        drop(tx);

        let path = handle.await.unwrap().unwrap();
        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(contents.lines().count(), 1000);
        assert!(contents.starts_with("line-0\n"));
        assert!(contents.ends_with("line-999\n"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ec2_instance() {
        assert_eq!(resource_table_name("AWS::EC2::Instance"), "ec2_instance");
    }

    #[test]
    fn lambda_function() {
        assert_eq!(
            resource_table_name("AWS::Lambda::Function"),
            "lambda_function"
        );
    }

    #[test]
    fn no_aws_prefix() {
        assert_eq!(resource_table_name("EC2::Instance"), "ec2_instance");
    }

    #[test]
    fn multi_segment() {
        assert_eq!(
            resource_table_name("AWS::Route53Resolver::ResolverRule"),
            "route53resolver_resolverrule"
        );
    }
}
