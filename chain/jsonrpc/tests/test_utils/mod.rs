#[cfg(feature = "test_features")]
use actix::Actor;
use actix::Addr;
use futures::{future, FutureExt, TryFutureExt};
use serde_json::json;

use near_chain_configs::GenesisConfig;
use near_client::test_utils::setup_no_network_with_validity_period_and_no_epoch_sync;
use near_client::ViewClientActor;
use near_jsonrpc::{start_http, RpcConfig};
use near_jsonrpc_primitives::message::{from_slice, Message};
use near_network::test_utils::open_port;
#[cfg(feature = "test_features")]
use near_network::test_utils::{make_ibf_routing_pool, make_peer_manager};
use near_primitives::types::NumBlocks;

lazy_static::lazy_static! {
    pub static ref TEST_GENESIS_CONFIG: GenesisConfig =
        GenesisConfig::from_json(include_str!("../../../../nearcore/res/genesis_config.json"));
}

pub enum NodeType {
    Validator,
    NonValidator,
}

pub fn start_all(node_type: NodeType) -> (Addr<ViewClientActor>, String) {
    start_all_with_validity_period_and_no_epoch_sync(node_type, 100, false)
}

pub fn start_all_with_validity_period_and_no_epoch_sync(
    node_type: NodeType,
    transaction_validity_period: NumBlocks,
    enable_doomslug: bool,
) -> (Addr<ViewClientActor>, String) {
    let (client_addr, view_client_addr) = setup_no_network_with_validity_period_and_no_epoch_sync(
        vec!["test1".parse().unwrap(), "test2".parse().unwrap()],
        if let NodeType::Validator = node_type {
            "test1".parse().unwrap()
        } else {
            "other".parse().unwrap()
        },
        true,
        transaction_validity_period,
        enable_doomslug,
    );

    let addr = format!("127.0.0.1:{}", open_port());

    #[cfg(feature = "test_features")]
    let ibf_routing_pool = make_ibf_routing_pool();
    #[cfg(feature = "test_features")]
    let peer_manager_addr = make_peer_manager(
        "test2",
        open_port(),
        vec![("test1", open_port())],
        10,
        ibf_routing_pool.clone(),
    )
    .0
    .start();

    start_http(
        RpcConfig::new(&addr),
        TEST_GENESIS_CONFIG.clone(),
        client_addr.clone(),
        view_client_addr.clone(),
        #[cfg(feature = "test_features")]
        peer_manager_addr,
        #[cfg(feature = "test_features")]
        ibf_routing_pool,
    );
    (view_client_addr, addr)
}

#[macro_use]
pub mod macros {
    #![allow(unused_macros)] // Suppress Rustc warnings even though this macro is used.

    macro_rules! actix_test_env {
        ($scope:expr) => {
            init_test_logger();

            run_actix(async { $scope });
        };
    }

    macro_rules! test_with_client {
        ($node_type:expr, $client:ident, $block:expr) => {
            actix_test_env!({
                let (_, addr) = test_utils::start_all($node_type);

                let $client = JsonRpcClient::connect(&format!("http://{}", addr));

                actix::spawn(async move {
                    $block.await;
                    System::current().stop();
                });
            });
        };
    }
}

/// Prepare a `RPCRequest` with a given client, server address, method and parameters.
pub async fn call_method<T>(
    client: &awc::Client,
    server_addr: &str,
    method: &serde_json::Value,
    params: serde_json::Value,
) -> Result<T, near_jsonrpc_primitives::errors::RpcError>
where
    T: serde::de::DeserializeOwned,
{
    let request = json!({
        "jsonrpc": "2.0",
        "method": method,
        "id": "dontcare",
        "params": params,
    });
    // TODO: simplify this.
    client
        .post(server_addr)
        .insert_header(("Content-Type", "application/json"))
        .send_json(&request)
        .map_err(|err| {
            near_jsonrpc_primitives::errors::RpcError::new_internal_error(
                None,
                format!("{:?}", err),
            )
        })
        .and_then(|mut response| {
            response.body().map(|body| match body {
                Ok(bytes) => from_slice(&bytes).map_err(|err| {
                    near_jsonrpc_primitives::errors::RpcError::parse_error(format!(
                        "Error {:?} in {:?}",
                        err, bytes
                    ))
                }),
                Err(err) => Err(near_jsonrpc_primitives::errors::RpcError::parse_error(format!(
                    "Failed to retrieve payload: {:?}",
                    err
                ))),
            })
        })
        .and_then(|message| {
            future::ready(match message {
                Message::Response(resp) => resp.result.and_then(|x| {
                    serde_json::from_value(x).map_err(|err| {
                        near_jsonrpc_primitives::errors::RpcError::parse_error(format!(
                            "Failed to parse: {:?}",
                            err
                        ))
                    })
                }),
                _ => Err(near_jsonrpc_primitives::errors::RpcError::parse_error(format!(
                    "Failed to parse JSON RPC response"
                ))),
            })
        })
        .await
}
