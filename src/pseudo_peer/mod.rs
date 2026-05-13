//! A pseudo peer library that ingests multiple block sources to reth
//!
//! This library exposes `start_pseudo_peer` to support reth-side NetworkState/StateFetcher
//! to fetch blocks and feed it to its stages

pub mod block_store;
pub mod cli;
pub mod config;
pub mod network;
mod patch;
pub mod service;
pub mod sources;
pub mod utils;

use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

pub use block_store::*;
pub use cli::*;
pub use config::*;
pub use network::*;
pub use service::*;
pub use sources::*;

/// Re-export commonly used types
pub mod prelude {
    pub use super::{
        block_store::BlockStore,
        config::BlockSourceConfig,
        service::{BlockPoller, PseudoPeer},
        sources::{BlockSource, LocalBlockSource, RpcBlockSource, S3BlockSource},
    };
}

use crate::chainspec::HlChainSpec;
use reth_discv4::NodeRecord;
use reth_network::{NetworkEvent, NetworkEventListenerProvider};
use reth_network_api::Peers;
use std::str::FromStr;

fn block_poller_enabled() -> bool {
    std::env::var("NANORETH_DISABLE_BLOCK_POLLER")
        .map(|value| !matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(true)
}

/// Main function that starts the network manager and processes eth requests
pub async fn start_pseudo_peer(
    chain_spec: Arc<HlChainSpec>,
    destination_peer: String,
    block_store: Arc<BlockStore>,
    debug_cutoff_height: Option<u64>,
    start_block_number: Option<u64>,
) -> eyre::Result<()> {

    // Parse the destination peer enode string
    let node_record = NodeRecord::from_str(&destination_peer)
        .map_err(|e| eyre::eyre!("Failed to parse destination peer: {e}"))?;

    // Create network manager (no boot_nodes — we add the peer directly)
    let (mut network, start_tx) = create_network_manager(
        (*chain_spec).clone(),
        block_store.clone(),
        debug_cutoff_height,
        start_block_number,
    )
    .await?;

    // Create the channels for receiving eth messages
    let (eth_tx, mut eth_rx) = mpsc::channel(32);
    let (transaction_tx, mut transaction_rx) = mpsc::unbounded_channel();

    network.set_eth_request_handler(eth_tx);
    network.set_transactions(transaction_tx);

    let network_handle = network.handle().clone();
    let mut network_events = network_handle.event_listener();
    info!("Starting network manager...");

    let mut service = PseudoPeer::new(chain_spec, block_store);
    tokio::spawn(network);
    let start_block_poller = block_poller_enabled();
    if !start_block_poller {
        info!("Block poller disabled by NANORETH_DISABLE_BLOCK_POLLER");
    }

    // Directly add the main node as a peer (bypasses discovery)
    info!(
        peer_id = %node_record.id,
        addr = %node_record.tcp_addr(),
        "Adding main node as direct peer"
    );
    network_handle.add_trusted_peer(node_record.id, node_record.tcp_addr());

    let mut first = true;

    // Main event loop
    loop {
        tokio::select! {
            Some(event) = tokio_stream::StreamExt::next(&mut network_events) => {
                info!("Network event: {event:?}");
                if start_block_poller && matches!(event, NetworkEvent::ActivePeerSession { .. }) && first {
                    start_tx.send(()).await?;
                    first = false;
                }
            }

            _ = transaction_rx.recv() => {}

            Some(eth_req) = eth_rx.recv() => {
                if let Err(e) = service.process_eth_request(eth_req).await {
                    error!("Error processing eth request: {e:?}");
                } else {
                    info!("Processed eth request");
                }
            }
        }
    }
}
