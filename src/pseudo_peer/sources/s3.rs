use super::{BlockSource, utils};
use crate::node::types::BlockAndReceipts;
use aws_sdk_s3::types::RequestPayer;
use futures::{FutureExt, future::BoxFuture};
use reth_metrics::{Metrics, metrics, metrics::Counter};
use std::{sync::Arc, time::Duration};
use tracing::{info, warn};

const DEFAULT_CONCURRENCY: u64 = 50;
const MAX_CONCURRENCY: u64 = 1000;

/// Block source that reads blocks from S3 (--s3)
#[derive(Debug, Clone)]
pub struct S3BlockSource {
    client: Arc<aws_sdk_s3::Client>,
    bucket: String,
    polling_interval: Duration,
    metrics: S3BlockSourceMetrics,
}

#[derive(Metrics, Clone)]
#[metrics(scope = "block_source.s3")]
pub struct S3BlockSourceMetrics {
    /// How many times the S3 block source is polling for a block
    pub polling_attempt: Counter,
    /// How many times the S3 block source has polled a block
    pub fetched: Counter,
}

impl S3BlockSource {
    pub fn new(client: aws_sdk_s3::Client, bucket: String, polling_interval: Duration) -> Self {
        Self {
            client: client.into(),
            bucket,
            polling_interval,
            metrics: S3BlockSourceMetrics::default(),
        }
    }

    async fn pick_path_with_highest_number(
        client: &aws_sdk_s3::Client,
        bucket: &str,
        dir: &str,
        is_dir: bool,
    ) -> Option<(u64, String)> {
        let request = client
            .list_objects()
            .bucket(bucket)
            .prefix(dir)
            .delimiter("/")
            .request_payer(RequestPayer::Requester);
        let response = request.send().await.ok()?;
        let files: Vec<String> = if is_dir {
            response
                .common_prefixes?
                .iter()
                .map(|object| object.prefix.as_ref().unwrap().to_string())
                .collect()
        } else {
            response
                .contents?
                .iter()
                .map(|object| object.key.as_ref().unwrap().to_string())
                .collect()
        };
        utils::name_with_largest_number(&files, is_dir)
    }
}

impl BlockSource for S3BlockSource {
    fn collect_block(&self, height: u64) -> BoxFuture<'static, eyre::Result<BlockAndReceipts>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let metrics = self.metrics.clone();
        async move {
            let path = utils::rmp_path(height);

            let mut last_error = None;
            for attempt in 1..=20 {
                metrics.polling_attempt.increment(1);

                let request = client
                    .get_object()
                    .request_payer(RequestPayer::Requester)
                    .bucket(&bucket)
                    .key(&path);

                let response = match request.send().await {
                    Ok(response) => response,
                    Err(error) => {
                        last_error = Some(eyre::eyre!(error));
                        warn!(height, attempt, error = ?last_error, "S3 get_object failed");
                        tokio::time::sleep(Duration::from_millis((250 * attempt).min(2_000))).await;
                        continue;
                    }
                };

                let bytes = match response.body.collect().await {
                    Ok(bytes) => bytes.into_bytes(),
                    Err(error) => {
                        last_error = Some(eyre::eyre!(error));
                        warn!(height, attempt, error = ?last_error, "S3 body stream failed");
                        tokio::time::sleep(Duration::from_millis((250 * attempt).min(2_000))).await;
                        continue;
                    }
                };

                let mut decoder = lz4_flex::frame::FrameDecoder::new(&bytes[..]);
                let blocks: Vec<BlockAndReceipts> = match rmp_serde::from_read(&mut decoder) {
                    Ok(blocks) => blocks,
                    Err(error) => {
                        last_error = Some(eyre::eyre!(error));
                        warn!(height, attempt, error = ?last_error, "S3 block decode failed");
                        tokio::time::sleep(Duration::from_millis((250 * attempt).min(2_000))).await;
                        continue;
                    }
                };

                if let Some(block) = blocks.first() {
                    metrics.fetched.increment(1);
                    return Ok(block.clone());
                }

                last_error = Some(eyre::eyre!("S3 object decoded with no blocks"));
                warn!(height, attempt, error = ?last_error, "S3 object was empty");
                tokio::time::sleep(Duration::from_millis((250 * attempt).min(2_000))).await;
            }

            Err(last_error.unwrap_or_else(|| eyre::eyre!("S3 block fetch failed")))
        }
        .boxed()
    }

    fn find_latest_block_number(&self) -> BoxFuture<'static, Option<u64>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        async move {
            let (_, first_level) =
                Self::pick_path_with_highest_number(&client, &bucket, "", true).await?;
            let (_, second_level) =
                Self::pick_path_with_highest_number(&client, &bucket, &first_level, true).await?;
            let (block_number, third_level) =
                Self::pick_path_with_highest_number(&client, &bucket, &second_level, false).await?;

            info!("Latest block number: {} with path {}", block_number, third_level);
            Some(block_number)
        }
        .boxed()
    }

    /// Uses `tokio::spawn` batches instead of the default `buffered()` stream.
    /// This distributes response processing across the runtime's thread pool,
    /// avoiding the single-task polling bottleneck (~10x faster on fast networks).
    fn collect_blocks(
        &self,
        heights: Vec<u64>,
    ) -> BoxFuture<'static, eyre::Result<Vec<BlockAndReceipts>>> {
        let concurrency = self.recommended_chunk_size() as usize;
        let futs: Vec<_> = heights
            .into_iter()
            .map(|h| self.collect_block(h))
            .collect();
        async move {
            let mut results = Vec::with_capacity(futs.len());
            let mut futs = futs.into_iter();
            loop {
                let batch: Vec<_> = (&mut futs)
                    .take(concurrency)
                    .map(|fut| tokio::spawn(fut))
                    .collect();
                if batch.is_empty() {
                    break;
                }
                for handle in batch {
                    results.push(handle.await.map_err(|e| eyre::eyre!(e))??);
                }
            }
            Ok(results)
        }
        .boxed()
    }

    fn recommended_chunk_size(&self) -> u64 {
        std::env::var("S3_BLOCK_SOURCE_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| (1..=MAX_CONCURRENCY).contains(value))
            .unwrap_or(DEFAULT_CONCURRENCY)
    }

    fn polling_interval(&self) -> Duration {
        self.polling_interval
    }
}
