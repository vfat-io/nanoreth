use clap::Parser;
use reth_hl::{
    chainspec::parser::chain_value_parser,
    pseudo_peer::{PseudoPeerCommand, start_pseudo_peer},
};
use tracing::info;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    reth_cli_util::sigsegv_handler::install();
    reth_hl::version::init_reth_hl_version();

    let command = PseudoPeerCommand::parse();
    let _guard = command.logs.init_tracing()?;

    let chain_spec = chain_value_parser("mainnet")?;
    let source_config = command
        .source
        .parse()
        .await?
        .ok_or_else(|| eyre::eyre!("no block source configured"))?;
    let block_store = source_config
        .create_block_store((*chain_spec).clone(), 0, None)
        .await;

    info!(destination_peer = %command.destination_peer, "Starting standalone pseudo peer");
    start_pseudo_peer(chain_spec, command.destination_peer, block_store, None, None).await
}
