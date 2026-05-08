use super::{BlockSource, BlockSourceBoxed};
use crate::node::types::BlockAndReceipts;
use futures::{FutureExt, future::BoxFuture};
use tracing::debug;

/// Block source that tries a local/primary source first, then falls back to another source.
#[derive(Debug, Clone)]
pub struct FallbackBlockSource {
    primary: BlockSourceBoxed,
    fallback: BlockSourceBoxed,
}

impl FallbackBlockSource {
    pub fn new(primary: BlockSourceBoxed, fallback: BlockSourceBoxed) -> Self {
        Self { primary, fallback }
    }
}

impl BlockSource for FallbackBlockSource {
    fn collect_block(&self, height: u64) -> BoxFuture<'static, eyre::Result<BlockAndReceipts>> {
        let primary = self.primary.clone();
        let fallback = self.fallback.clone();
        async move {
            match primary.collect_block(height).await {
                Ok(block) => Ok(block),
                Err(err) => {
                    debug!(height, error = ?err, "Primary block source missed block; trying fallback");
                    fallback.collect_block(height).await
                }
            }
        }
        .boxed()
    }

    fn find_latest_block_number(&self) -> BoxFuture<'static, Option<u64>> {
        let fallback = self.fallback.clone();
        let primary = self.primary.clone();
        async move { fallback.find_latest_block_number().await.or(primary.find_latest_block_number().await) }
            .boxed()
    }

    fn recommended_chunk_size(&self) -> u64 {
        self.fallback.recommended_chunk_size()
    }

    fn polling_interval(&self) -> std::time::Duration {
        self.fallback.polling_interval()
    }
}
