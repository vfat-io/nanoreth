#![allow(clippy::owned_cow)]
use crate::{
    HlBlock,
    consensus::HlConsensus,
    node::{
        HlNode,
        network::block_import::{HlBlockImport, handle::ImportHandle, service::ImportService},
        primitives::HlPrimitives,
        rpc::engine_api::payload::HlPayloadTypes,
        types::ReadPrecompileCalls,
    },
    pseudo_peer::{BlockSourceConfig, BlockSourceHead, DbBlockNumberFn, start_pseudo_peer},
};
use alloy_rlp::{Decodable, Encodable};
use alloy_rpc_types::engine::ForkchoiceState;
use reth::{
    api::{FullNodeTypes, TxTy},
    builder::{BuilderContext, components::NetworkBuilder},
    transaction_pool::{PoolTransaction, TransactionPool},
};
use reth_discv4::NodeRecord;
use reth_engine_primitives::ConsensusEngineHandle;
use reth_eth_wire::{BasicNetworkPrimitives, NewBlock, NewBlockPayload};
use reth_ethereum_primitives::PooledTransactionVariant;
use reth_network::{NetworkConfig, NetworkHandle, NetworkManager};
use reth_network_api::PeersInfo;
use reth_payload_primitives::EngineApiMessageVersion;
use reth_provider::StageCheckpointReader;
use reth_stages_types::StageId;
use reth_storage_api::BlockNumReader;
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tracing::{info, warn};

pub mod block_import;

/// Received blocks can advance forkchoice without waiting for remote head discovery.
async fn forward_received_heads(
    mut heads: watch::Receiver<Option<BlockSourceHead>>,
    targets: mpsc::UnboundedSender<BlockSourceHead>,
) {
    let mut cadence = tokio::time::interval(Duration::from_millis(100));
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_forwarded = None;
    loop {
        cadence.tick().await;
        let head = *heads.borrow_and_update();
        if let Some(head) = head
            && last_forwarded.is_none_or(|last| head.supersedes(last))
        {
            if targets.send(head).is_err() {
                return;
            }
            last_forwarded = Some(head);
        }
        if heads.changed().await.is_err() {
            return;
        }
    }
}

/// A delayed discovery result must not rewind a head already supplied by received blocks.
async fn next_source_head(
    targets: &mut mpsc::UnboundedReceiver<BlockSourceHead>,
    previous: &mut Option<BlockSourceHead>,
) -> Option<BlockSourceHead> {
    while let Some(head) = targets.recv().await {
        if previous.is_none_or(|previous| head.supersedes(previous)) {
            *previous = Some(head);
            return Some(head);
        }
    }
    None
}

#[cfg(test)]
mod source_head_notification_tests {
    use super::*;
    use crate::{
        HlBlockBody, HlHeader,
        node::types::BlockAndReceipts,
        pseudo_peer::{BlockStore, LocalBlockSource},
    };
    use alloy_primitives::B256;

    fn block(number: u64) -> BlockAndReceipts {
        let mut header = HlHeader::default();
        header.inner.number = number;
        BlockAndReceipts::from_db(
            HlBlock {
                header,
                body: HlBlockBody {
                    inner: Default::default(),
                    sidecars: None,
                    read_precompile_calls: None,
                    highest_precompile_address: None,
                },
            },
            Vec::new(),
        )
    }

    fn store() -> BlockStore {
        // No files exist here: received blocks must not depend on source head discovery.
        BlockStore::new(
            Arc::new(Box::new(LocalBlockSource::new(std::path::PathBuf::from(
                "/unused-source-head-notification-test",
            )))),
            None,
            999,
        )
    }

    #[tokio::test]
    async fn source_head_notifications_do_not_wait_for_discovery() {
        let store = store();
        let (targets, mut receiver) = mpsc::unbounded_channel();
        let forwarding =
            tokio::spawn(forward_received_heads(store.received_head_updates(), targets));
        let received = block(42);
        store.index_block(&received);
        let target = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("received block should advance head without discovery")
            .unwrap();
        assert_eq!(target, BlockSourceHead { number: 42, hash: received.hash() });
        forwarding.abort();
    }

    #[tokio::test]
    async fn source_head_notifications_ignore_late_discovery() {
        let store = store();
        store.index_block(&block(40));
        let newest = block(42);
        store.index_block(&newest);
        store.index_block(&block(41));
        let (targets, mut receiver) = mpsc::unbounded_channel();
        let forwarding =
            tokio::spawn(forward_received_heads(store.received_head_updates(), targets.clone()));
        let mut previous = None;
        let head = tokio::time::timeout(
            Duration::from_secs(1),
            next_source_head(&mut receiver, &mut previous),
        )
        .await
        .expect("already received head should be delivered")
        .unwrap();
        assert_eq!(head, BlockSourceHead { number: 42, hash: newest.hash() });

        targets.send(BlockSourceHead { number: 41, hash: B256::ZERO }).unwrap();
        targets.send(head).unwrap();
        targets.send(BlockSourceHead { number: 42, hash: B256::repeat_byte(255) }).unwrap();
        let next = block(43);
        store.index_block(&next);
        let head = tokio::time::timeout(
            Duration::from_secs(1),
            next_source_head(&mut receiver, &mut previous),
        )
        .await
        .expect("late discovery must not block the next received head")
        .unwrap();
        assert_eq!(head, BlockSourceHead { number: 43, hash: next.hash() });
        forwarding.abort();
    }
}

/// HL `NewBlock` message value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HlNewBlock(pub NewBlock<HlBlock>);

mod rlp {
    use super::*;
    use crate::{
        HlBlockBody, HlHeader,
        node::primitives::{BlockBody, TransactionSigned},
    };
    use alloy_consensus::BlobTransactionSidecar;
    use alloy_primitives::{Address, U128};
    use alloy_rlp::{RlpDecodable, RlpEncodable};
    use alloy_rpc_types::Withdrawals;
    use std::borrow::Cow;

    #[derive(RlpEncodable, RlpDecodable)]
    #[rlp(trailing)]
    struct BlockHelper<'a> {
        header: Cow<'a, HlHeader>,
        transactions: Cow<'a, Vec<TransactionSigned>>,
        ommers: Cow<'a, Vec<HlHeader>>,
        withdrawals: Option<Cow<'a, Withdrawals>>,
    }

    #[derive(RlpEncodable, RlpDecodable)]
    #[rlp(trailing)]
    struct HlNewBlockHelper<'a> {
        block: BlockHelper<'a>,
        td: U128,
        sidecars: Option<Cow<'a, Vec<BlobTransactionSidecar>>>,
        read_precompile_calls: Option<Cow<'a, ReadPrecompileCalls>>,
        highest_precompile_address: Option<Cow<'a, Address>>,
    }

    impl<'a> From<&'a HlNewBlock> for HlNewBlockHelper<'a> {
        fn from(value: &'a HlNewBlock) -> Self {
            let b = &value.0.block;
            Self {
                block: BlockHelper {
                    header: Cow::Borrowed(&b.header),
                    transactions: Cow::Borrowed(&b.body.inner.transactions),
                    ommers: Cow::Borrowed(&b.body.inner.ommers),
                    withdrawals: b.body.inner.withdrawals.as_ref().map(Cow::Borrowed),
                },
                td: value.0.td,
                sidecars: b.body.sidecars.as_ref().map(Cow::Borrowed),
                read_precompile_calls: b.body.read_precompile_calls.as_ref().map(Cow::Borrowed),
                highest_precompile_address: b
                    .body
                    .highest_precompile_address
                    .as_ref()
                    .map(Cow::Borrowed),
            }
        }
    }

    impl Encodable for HlNewBlock {
        fn encode(&self, out: &mut dyn bytes::BufMut) {
            HlNewBlockHelper::from(self).encode(out);
        }

        fn length(&self) -> usize {
            HlNewBlockHelper::from(self).length()
        }
    }

    impl Decodable for HlNewBlock {
        fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
            let h = HlNewBlockHelper::decode(buf)?;
            Ok(HlNewBlock(NewBlock {
                block: HlBlock {
                    header: h.block.header.into_owned(),
                    body: HlBlockBody {
                        inner: BlockBody {
                            transactions: h.block.transactions.into_owned(),
                            ommers: h.block.ommers.into_owned(),
                            withdrawals: h.block.withdrawals.map(|w| w.into_owned()),
                        },
                        sidecars: h.sidecars.map(|s| s.into_owned()),
                        read_precompile_calls: h.read_precompile_calls.map(|s| s.into_owned()),
                        highest_precompile_address: h
                            .highest_precompile_address
                            .map(|s| s.into_owned()),
                    },
                },
                td: h.td,
            }))
        }
    }
}

impl NewBlockPayload for HlNewBlock {
    type Block = HlBlock;

    fn block(&self) -> &Self::Block {
        &self.0.block
    }
}

/// Network primitives for HL.
pub type HlNetworkPrimitives =
    BasicNetworkPrimitives<HlPrimitives, PooledTransactionVariant, HlNewBlock>;

/// A basic hl network builder.
#[derive(Debug)]
pub struct HlNetworkBuilder {
    pub(crate) engine_handle_rx:
        Arc<Mutex<Option<oneshot::Receiver<ConsensusEngineHandle<HlPayloadTypes>>>>>,

    // optional because we might sync from network
    pub(crate) block_source_config: Option<BlockSourceConfig>,

    pub(crate) debug_cutoff_height: Option<u64>,

    pub(crate) allow_network_overrides: bool,
}

impl HlNetworkBuilder {
    /// Returns the [`NetworkConfig`] that contains the settings to launch the p2p network.
    ///
    /// This applies the configured [`HlNetworkBuilder`] settings.
    pub fn network_config<Node>(
        self,
        ctx: &BuilderContext<Node>,
        mut fcu_trigger_rx: mpsc::UnboundedReceiver<BlockSourceHead>,
    ) -> eyre::Result<NetworkConfig<Node::Provider, HlNetworkPrimitives>>
    where
        Node: FullNodeTypes<Types = HlNode>,
    {
        let (to_import, from_network) = mpsc::unbounded_channel();
        let (to_network, import_outcome) = mpsc::unbounded_channel();
        let handle = ImportHandle::new(to_import, import_outcome);
        let consensus = Arc::new(HlConsensus { provider: ctx.provider().clone() });

        ctx.task_executor().spawn_critical("block import", async move {
            let engine = self
                .engine_handle_rx
                .lock()
                .await
                .take()
                .expect("node should only be launched once")
                .await
                .unwrap();

            let fcu_engine = engine.clone();
            tokio::spawn(async move {
                // Use the target hash as finalized so reth's backfill_sync_target()
                // sees an unknown finalized hash and triggers the pipeline.
                // If finalized is set to the current best block (which is already
                // known), reth concludes "fully synced" and never starts backfill.
                let mut previous = None;
                while let Some(target) = next_source_head(&mut fcu_trigger_rx, &mut previous).await
                {
                    let target_hash = target.hash;
                    let state = ForkchoiceState {
                        head_block_hash: target_hash,
                        safe_block_hash: target_hash,
                        finalized_block_hash: target_hash,
                    };
                    info!(
                        target: "reth::cli",
                        head = %target_hash,
                        "Sending forkchoice update from block source"
                    );
                    if let Err(e) = fcu_engine
                        .fork_choice_updated(state, None, EngineApiMessageVersion::default())
                        .await
                    {
                        warn!(
                            target: "reth::cli",
                            head = %target_hash,
                            %e,
                            "Failed to send forkchoice update from block source"
                        );
                    }
                }
            });

            ImportService::new(consensus, engine, from_network, to_network).await.unwrap();
        });

        let mut config_builder = ctx.network_config_builder()?;

        // Only apply localhost-only network settings if network overrides are NOT allowed
        if !self.allow_network_overrides {
            config_builder = config_builder
                .discovery_addr(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0))
                .listener_addr(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0))
                .disable_dns_discovery()
                .disable_nat();
        }

        config_builder = config_builder
            .boot_nodes(boot_nodes())
            .set_head(ctx.head())
            .with_pow()
            .block_import(Box::new(HlBlockImport::new(handle)));

        Ok(ctx.build_network_config(config_builder))
    }
}

impl<Node, Pool> NetworkBuilder<Node, Pool> for HlNetworkBuilder
where
    Node: FullNodeTypes<Types = HlNode>,
    Pool: TransactionPool<
            Transaction: PoolTransaction<
                Consensus = TxTy<Node::Types>,
                Pooled = PooledTransactionVariant,
            >,
        > + Unpin
        + 'static,
{
    type Network = NetworkHandle<HlNetworkPrimitives>;

    async fn build_network(
        self,
        ctx: &BuilderContext<Node>,
        pool: Pool,
    ) -> eyre::Result<Self::Network> {
        let block_source_config = self.block_source_config.clone();
        let debug_cutoff_height = self.debug_cutoff_height;
        let (fcu_trigger_tx, fcu_trigger_rx) = mpsc::unbounded_channel();
        let handle = ctx.start_network(
            NetworkManager::builder(self.network_config(ctx, fcu_trigger_rx)?).await?,
            pool,
        );
        let local_node_record = handle.local_node_record();
        let destination_network = handle.clone();
        info!(target: "reth::cli", enode=%local_node_record, "P2P networking initialized");

        if let Some(block_source_config) = block_source_config {
            let finish_block = ctx
                .provider()
                .get_stage_checkpoint(StageId::Finish)?
                .unwrap_or_default()
                .block_number;
            let bodies_block = ctx
                .provider()
                .get_stage_checkpoint(StageId::Bodies)?
                .unwrap_or_default()
                .block_number;
            let next_block_number = std::cmp::max(finish_block, bodies_block) + 1;

            // Give the block store a handle to the node's database so it can
            // resolve hash→number directly instead of scanning the block source.
            let provider = ctx.provider().clone();
            let db_block_number: DbBlockNumberFn =
                Arc::new(move |hash| provider.block_number(hash).ok().flatten());

            let chain_spec = ctx.chain_spec();
            ctx.task_executor().spawn_critical("pseudo peer", async move {
                let block_store = block_source_config
                    .create_block_store(
                        (*chain_spec).clone(),
                        next_block_number,
                        Some(db_block_number),
                    )
                    .await;

                let fcu_block_store = block_store.clone();
                tokio::spawn(forward_received_heads(
                    block_store.received_head_updates(),
                    fcu_trigger_tx.clone(),
                ));
                let fcu_trigger_tx = fcu_trigger_tx.clone();
                tokio::spawn(async move {
                    let mut last_sent = None;
                    let polling_interval =
                        fcu_block_store.polling_interval().max(std::time::Duration::from_secs(1));
                    loop {
                        if let Some(latest) = fcu_block_store.find_latest_block_number().await
                            && last_sent != Some(latest)
                        {
                            match fcu_block_store.get_by_number(latest).await {
                                Ok(block) => {
                                    let hash = block.hash();
                                    info!(
                                        target: "reth::cli",
                                        number = %latest,
                                        hash = %hash,
                                        "Queueing forkchoice trigger from block source"
                                    );
                                    if fcu_trigger_tx
                                        .send(BlockSourceHead { number: latest, hash })
                                        .is_err()
                                    {
                                        break;
                                    }
                                    last_sent = Some(latest);
                                }
                                Err(e) => {
                                    info!(
                                        target: "reth::cli",
                                        number = %latest,
                                        %e,
                                        "Failed to read latest block for forkchoice trigger"
                                    );
                                }
                            }
                        }
                        tokio::time::sleep(polling_interval).await;
                    }
                });

                start_pseudo_peer(
                    chain_spec.clone(),
                    destination_network,
                    block_store,
                    debug_cutoff_height,
                    Some(next_block_number),
                )
                .await
                .unwrap();
            });
        } else {
            info!(target: "reth::cli", "No block source configured - syncing from P2P peers only");
        }

        Ok(handle)
    }
}

/// HL mainnet bootnodes <https://github.com/bnb-chain/hl/blob/master/params/bootnodes.go#L23>
static BOOTNODES: [&str; 0] = [];

pub fn boot_nodes() -> Vec<NodeRecord> {
    BOOTNODES[..].iter().map(|s| s.parse().unwrap()).collect()
}
