//! Exercise the real call and estimate paths against one fixed small-block environment.

use super::{HlEthApi, HlEthApiInner};
use crate::{
    HlBlock, HlHeader, HlPrimitives,
    chainspec::{HlChainSpec, hl::hl_mainnet},
    node::{
        evm::config::HlEvmConfig,
        pool::HlPooledTransaction,
        types::{ReadPrecompileCalls, ReadPrecompileInput, ReadPrecompileResult},
    },
};
use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, Sealable, U256, address, b256, hex};
use alloy_rpc_types_eth::{
    BlockOverrides, TransactionRequest,
    state::{AccountOverride, EvmOverrides, StateOverride},
};
use reth_evm::ConfigureEvm;
use reth_network_api::noop::NoopNetwork;
use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
use reth_revm::{database::StateProviderDatabase, db::CacheDB};
use reth_rpc::EthApi;
use reth_rpc_eth_api::{
    RpcConverter, RpcNodeCore,
    helpers::{Call, EthCall, estimate::EstimateCall},
    node::RpcNodeCoreAdapter,
};
use reth_rpc_eth_types::{EthApiError, GasCap, receipt::EthReceiptConverter};
use reth_transaction_pool::noop::NoopTransactionPool;
use revm::context_interface::Transaction;
use std::sync::Arc;

type Provider = MockEthProvider<HlPrimitives, HlChainSpec>;
type TestNode = RpcNodeCoreAdapter<
    Provider,
    NoopTransactionPool<HlPooledTransaction>,
    NoopNetwork,
    HlEvmConfig,
>;
type TestApi =
    HlEthApi<TestNode, RpcConverter<Ethereum, HlEvmConfig, EthReceiptConverter<HlChainSpec>>>;

const SENDER: Address = address!("000000000000000000000000000000000000dead");
const TARGET: Address = address!("00000000000000000000000000000000b16b10c0");
const SMALL_BLOCK_GAS: u64 = 3_000_000;
const BIG_BLOCK_GAS: u64 = 30_000_000;
const BLOCK_NUMBER: u64 = 48_903_326;
// Return GASLIMIT and storage slot zero after memory expansion that costs more than 3M gas.
// No external calls, refunds, or HyperCore precompile inputs are needed for this regression.
const EXPENSIVE_CODE: &[u8] = &hex!("456000526000546020526000621400005260406000f3");

struct Fixture {
    api: TestApi,
    provider: Provider,
    header: HlHeader,
}

impl Fixture {
    fn new(rpc_gas_cap: u64) -> Self {
        let header = HlHeader {
            inner: alloy_consensus::Header {
                number: BLOCK_NUMBER,
                timestamp: 1_792_357_200,
                gas_limit: SMALL_BLOCK_GAS,
                base_fee_per_gas: Some(100_000_000),
                excess_blob_gas: Some(0),
                ..Default::default()
            },
            ..Default::default()
        };
        Self::with_header(rpc_gas_cap, header)
    }

    fn with_header(rpc_gas_cap: u64, header: HlHeader) -> Self {
        let chain_spec = HlChainSpec { inner: hl_mainnet(), ..Default::default() };
        let provider = MockEthProvider::<HlPrimitives>::new().with_chain_spec(chain_spec);
        provider.add_block(
            header.hash_slow(),
            HlBlock { header: header.clone(), ..Default::default() },
        );
        provider.add_account(SENDER, ExtendedAccount::new(0, U256::from(10u64.pow(18))));
        provider.add_account(
            TARGET,
            ExtendedAccount::new(0, U256::ZERO)
                .with_bytecode(Bytes::from_static(EXPENSIVE_CODE))
                .extend_storage([(B256::ZERO, U256::from(42))]),
        );
        let evm = HlEvmConfig::new(provider.chain_spec.clone());
        let eth_api = EthApi::builder(
            provider.clone(),
            NoopTransactionPool::<HlPooledTransaction>::new(),
            NoopNetwork::default(),
            evm,
        )
        .gas_cap(GasCap(rpc_gas_cap))
        .build_inner();
        Self { api: HlEthApi { inner: Arc::new(HlEthApiInner { eth_api }) }, provider, header }
    }

    fn request(&self, gas: Option<u64>) -> TransactionRequest {
        let mut request: TransactionRequest = serde_json::from_value(serde_json::json!({
            "from": SENDER, "to": TARGET, "gasPrice": "0x0"
        }))
        .unwrap();
        request.gas = gas;
        request
    }

    async fn estimate(&self, request: TransactionRequest) -> Result<U256, EthApiError> {
        EstimateCall::estimate_gas_at(&self.api, request, BLOCK_NUMBER.into(), None).await
    }

    async fn call(&self, request: TransactionRequest) -> Result<Bytes, EthApiError> {
        EthCall::call(&self.api, request, Some(BLOCK_NUMBER.into()), EvmOverrides::default()).await
    }

    fn expected_output(&self) -> Bytes {
        let mut output = U256::from(SMALL_BLOCK_GAS).to_be_bytes::<32>().to_vec();
        output.extend(U256::from(42).to_be_bytes::<32>());
        output.into()
    }
}

#[tokio::test]
async fn estimates_above_small_block_limit_with_and_without_explicit_gas() {
    let fixture = Fixture::new(50_000_000);
    assert!(fixture.call(fixture.request(Some(SMALL_BLOCK_GAS))).await.is_err());
    assert_eq!(
        fixture.call(fixture.request(Some(BIG_BLOCK_GAS))).await.unwrap(),
        fixture.expected_output()
    );

    for gas in [None, Some(BIG_BLOCK_GAS)] {
        let estimate = fixture.estimate(fixture.request(gas)).await.unwrap().to::<u64>();
        assert!(estimate > SMALL_BLOCK_GAS && estimate <= BIG_BLOCK_GAS);
        assert_eq!(
            fixture.call(fixture.request(Some(estimate))).await.unwrap(),
            fixture.expected_output()
        );
    }
}

#[tokio::test]
async fn latest_and_pending_estimates_use_big_block_ceiling() {
    let fixture = Fixture::new(50_000_000);
    for tag in [BlockNumberOrTag::Latest, BlockNumberOrTag::Pending] {
        let estimate = EstimateCall::estimate_gas_at(
            &fixture.api,
            fixture.request(None),
            BlockId::Number(tag),
            None,
        )
        .await
        .unwrap()
        .to::<u64>();
        assert!(estimate > SMALL_BLOCK_GAS && estimate <= BIG_BLOCK_GAS);
    }
}

#[tokio::test]
async fn priced_estimates_and_calls_keep_the_small_header_environment() {
    let fixture = Fixture::new(50_000_000);
    for eip1559 in [false, true] {
        let mut request = fixture.request(None);
        if eip1559 {
            request.gas_price = None;
            request.max_fee_per_gas = Some(1_000_000_000);
            request.max_priority_fee_per_gas = Some(0);
        } else {
            request.gas_price = Some(1_000_000_000);
        }
        let estimate = fixture.estimate(request.clone()).await.unwrap().to::<u64>();
        assert!(estimate > SMALL_BLOCK_GAS && estimate <= BIG_BLOCK_GAS);
        assert_eq!(fixture.call(request).await.unwrap(), fixture.expected_output());
    }
}

#[tokio::test]
async fn explicit_gas_and_caller_affordability_remain_limits() {
    let fixture = Fixture::new(50_000_000);
    assert!(fixture.estimate(fixture.request(Some(2_000_000))).await.is_err());

    fixture.provider.add_account(SENDER, ExtendedAccount::new(0, U256::from(2_000_000)));
    let mut request = fixture.request(Some(BIG_BLOCK_GAS));
    request.gas_price = Some(1);
    assert!(fixture.estimate(request).await.is_err());

    let mut request = fixture.request(None);
    request.value = Some(U256::from(2_000_001));
    assert!(fixture.estimate(request).await.is_err());
}

#[tokio::test]
async fn rpc_and_configured_transaction_caps_remain_limits() {
    let fixture = Fixture::new(2_000_000);
    assert!(fixture.estimate(fixture.request(None)).await.is_err());
    assert!(fixture.estimate(fixture.request(Some(BIG_BLOCK_GAS))).await.is_err());

    let fixture = Fixture::new(50_000_000);
    let mut env = fixture.api.evm_config().evm_env(&fixture.header).unwrap();
    env.cfg_env.tx_gas_limit_cap = Some(2_000_000);
    assert!(
        EstimateCall::estimate_gas_with(
            &fixture.api,
            env,
            fixture.request(Some(BIG_BLOCK_GAS)),
            fixture.provider.clone(),
            None
        )
        .is_err()
    );

    let fixture = Fixture::new(0);
    assert!(fixture.estimate(fixture.request(None)).await.unwrap() > U256::from(SMALL_BLOCK_GAS));

    let fixture = Fixture::new(50_000_000);
    fixture.provider.add_account(
        TARGET,
        ExtendedAccount::new(0, U256::ZERO)
            .with_bytecode(Bytes::from_static(&hex!("6000628000005200"))),
    );
    assert!(fixture.estimate(fixture.request(Some(60_000_000))).await.is_err());
}

#[tokio::test]
async fn priced_block_override_cannot_exceed_rpc_gas_cap() {
    let fixture = Fixture::new(5_000_000);
    let mut request = fixture.request(None);
    request.gas_price = Some(1);
    for (override_limit, expected_tx_limit) in [(2_000_000, 2_000_000), (10_000_000, 5_000_000)] {
        let mut db = CacheDB::new(StateProviderDatabase::new(fixture.provider.clone()));
        let env = fixture.api.evm_config().evm_env(&fixture.header).unwrap();
        let (env, tx) = fixture
            .api
            .prepare_call_env(
                env,
                request.clone(),
                &mut db,
                EvmOverrides::new(
                    None,
                    Some(Box::new(BlockOverrides {
                        gas_limit: Some(override_limit),
                        ..Default::default()
                    })),
                ),
            )
            .unwrap();
        assert_eq!(env.block_env.gas_limit, override_limit);
        assert_eq!(tx.gas_limit(), expected_tx_limit);
    }
}

#[tokio::test]
async fn genuine_reverts_and_basic_transfers_keep_their_results() {
    let fixture = Fixture::new(50_000_000);
    fixture.provider.add_account(
        TARGET,
        ExtendedAccount::new(0, U256::ZERO)
            .with_bytecode(Bytes::from_static(&hex!("63deadbeef6000526004601cfd"))),
    );
    let error = fixture.estimate(fixture.request(Some(BIG_BLOCK_GAS))).await.unwrap_err();
    let error: jsonrpsee::types::ErrorObjectOwned = error.into();
    assert_eq!(error.code(), 3);
    assert_eq!(error.data().unwrap().get(), "\"0xdeadbeef\"");

    fixture.provider.add_account(TARGET, ExtendedAccount::new(0, U256::ZERO));
    assert_eq!(fixture.estimate(fixture.request(None)).await.unwrap(), U256::from(21_000));
}

#[tokio::test]
async fn state_overrides_are_applied_before_estimation() {
    let fixture = Fixture::new(50_000_000);
    let overrides: StateOverride = [(
        TARGET,
        AccountOverride { code: Some(Bytes::from_static(&[0x00])), ..Default::default() },
    )]
    .into_iter()
    .collect();
    let estimate = EstimateCall::estimate_gas_at(
        &fixture.api,
        fixture.request(None),
        BLOCK_NUMBER.into(),
        Some(overrides),
    )
    .await
    .unwrap();
    assert!((U256::from(21_000)..U256::from(22_000)).contains(&estimate));
    // The override is call-local: the original expensive account still needs a big block.
    assert!(fixture.estimate(fixture.request(None)).await.unwrap() > U256::from(SMALL_BLOCK_GAS));
}

#[tokio::test]
async fn historical_precompile_inputs_stay_pinned_and_missing_inputs_still_fail() {
    let fixture = Fixture::new(50_000_000);
    let precompile = address!("000000000000000000000000000000000000080e");
    // STATICCALL with a fixed 10k gas argument, returning its bytes or reverting on failure.
    fixture.provider.add_account(
        TARGET,
        ExtendedAccount::new(0, U256::ZERO).with_bytecode(Bytes::from_static(&hex!(
            "602060006000600061080e612710fa1560185760206000f35b60006000fd"
        ))),
    );
    let mut historical = HlBlock { header: fixture.header.clone(), ..Default::default() };
    historical.body.highest_precompile_address = Some(precompile);
    historical.body.read_precompile_calls = Some(ReadPrecompileCalls(vec![(
        precompile,
        vec![(
            ReadPrecompileInput { input: Bytes::new(), gas_limit: 10_000 },
            ReadPrecompileResult::Ok { gas_used: 1_000, bytes: Bytes::from_static(&[0xfe, 0xed]) },
        )],
    )]));
    fixture.provider.add_block(historical.header.hash_slow(), historical.clone());
    let mut head = historical.clone();
    head.header.inner.number += 1;
    head.header.inner.timestamp += 1;
    head.body.read_precompile_calls = None;
    fixture.provider.add_block(head.header.hash_slow(), head);

    let estimate = fixture.estimate(fixture.request(None)).await.unwrap().to::<u64>();
    let output = fixture.call(fixture.request(Some(estimate))).await.unwrap();
    assert_eq!(&output[..2], &[0xfe, 0xed]);
    assert_eq!(output.len(), 32);

    historical.body.read_precompile_calls = None;
    fixture.provider.add_block(historical.header.hash_slow(), historical);
    assert!(fixture.estimate(fixture.request(None)).await.is_err());
}

#[tokio::test]
async fn full_increase_routes_estimate_on_the_original_small_block_state() {
    #[derive(serde::Deserialize)]
    struct Account {
        balance: U256,
        nonce: u64,
        code: Option<Bytes>,
        #[serde(default)]
        storage: std::collections::BTreeMap<B256, U256>,
    }
    #[derive(serde::Deserialize)]
    struct Route {
        provider: String,
        transaction: TransactionRequest,
    }
    #[derive(serde::Deserialize)]
    struct Replay {
        header: alloy_consensus::Header,
        accounts: std::collections::BTreeMap<Address, Account>,
        transactions: Vec<Route>,
    }

    // Public prestateTracer snapshots for both calls at block 48903326, without state or
    // block overrides. Their common accounts/slots were checked for identical prestate.
    // The fixture includes provenance and is a size-prefixed LZ4-compressed JSON object.
    let json =
        lz4_flex::decompress_size_prepended(include_bytes!("fixtures/hyperevm-increase.json.lz4"))
            .unwrap();
    let replay: Replay = serde_json::from_slice(&json).unwrap();
    assert_eq!(replay.header.number, BLOCK_NUMBER);
    assert_eq!(replay.header.gas_limit, SMALL_BLOCK_GAS);
    assert_eq!(
        replay.header.hash_slow(),
        b256!("7766b777e6f7e0058d034ac5e1934092a570aa27e8c89ac0597a35a22586945b")
    );
    let fixture =
        Fixture::with_header(50_000_000, HlHeader { inner: replay.header, ..Default::default() });
    for (address, account) in replay.accounts {
        let mut state = ExtendedAccount::new(account.nonce, account.balance);
        if let Some(code) = account.code {
            state = state.with_bytecode(code);
        }
        fixture.provider.add_account(address, state.extend_storage(account.storage));
    }

    for route in replay.transactions {
        let mut request = route.transaction;
        request.gas = Some(SMALL_BLOCK_GAS);
        assert!(fixture.call(request.clone()).await.is_err(), "{}", route.provider);
        request.gas = Some(BIG_BLOCK_GAS);
        assert_eq!(
            fixture.call(request.clone()).await.unwrap(),
            Bytes::new(),
            "{}",
            route.provider
        );
        for gas in [None, Some(BIG_BLOCK_GAS)] {
            request.gas = gas;
            let estimate = fixture.estimate(request.clone()).await.unwrap().to::<u64>();
            assert!(estimate > SMALL_BLOCK_GAS && estimate <= BIG_BLOCK_GAS, "{}", route.provider);
            request.gas = Some(estimate);
            assert_eq!(
                fixture.call(request.clone()).await.unwrap(),
                Bytes::new(),
                "{}",
                route.provider
            );
        }
    }
}
