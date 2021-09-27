use std::str::FromStr;

use actix::{Actor, System};
use futures::{future, FutureExt};
use serde_json::json;

use near_actix_test_utils::run_actix;
use near_crypto::{KeyType, PublicKey, Signature};
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_logger_utils::init_test_logger;
use near_network::test_utils::WaitOrTimeout;
use near_primitives::account::{AccessKey, AccessKeyPermission};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockId, BlockReference, ShardId, SyncCheckpoint};
use near_primitives::views::QueryRequest;

#[macro_use]
pub mod test_utils;

/// Retrieve blocks via json rpc
#[test]
fn test_block_by_id_height() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let block = client
            .call(methods::any::<methods::block::RpcBlockRequest>(
                "block",
                json!([BlockId::Height(0)]),
            ))
            .await
            .unwrap();
        assert_eq!(block.author, "test1".parse().unwrap());
        assert_eq!(block.header.height, 0);
        assert_eq!(block.header.epoch_id.0.as_ref(), &[0; 32]);
        assert_eq!(block.header.hash.0.as_ref().len(), 32);
        assert_eq!(block.header.prev_hash.0.as_ref(), &[0; 32]);
        assert_eq!(
            block.header.prev_state_root,
            CryptoHash::from_str("7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t").unwrap()
        );
        assert!(block.header.timestamp > 0);
        assert_eq!(block.header.validator_proposals.len(), 0);
    });
}

/// Retrieve blocks via json rpc
#[test]
fn test_block_by_id_hash() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let block = client
            .clone()
            .call(methods::any::<methods::block::RpcBlockRequest>(
                "block",
                json!([BlockId::Height(0)]),
            ))
            .await
            .unwrap();
        let same_block = client
            .call(methods::any::<methods::block::RpcBlockRequest>(
                "block",
                json!([BlockId::Hash(block.header.hash)]),
            ))
            .await
            .unwrap();
        assert_eq!(block.header.height, 0);
        assert_eq!(same_block.header.height, 0);
    });
}

/// Retrieve blocks via json rpc
#[test]
fn test_block_query() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let block_response1 = client
            .clone()
            .call(methods::block::RpcBlockRequest {
                block_reference: BlockReference::BlockId(BlockId::Height(0)),
            })
            .await
            .unwrap();
        let block_response2 = client
            .clone()
            .call(methods::block::RpcBlockRequest {
                block_reference: BlockReference::BlockId(BlockId::Hash(
                    block_response1.header.hash,
                )),
            })
            .await
            .unwrap();
        let block_response3 = client
            .clone()
            .call(methods::block::RpcBlockRequest { block_reference: BlockReference::latest() })
            .await
            .unwrap();
        let block_response4 = client
            .clone()
            .call(methods::block::RpcBlockRequest {
                block_reference: BlockReference::SyncCheckpoint(SyncCheckpoint::Genesis),
            })
            .await
            .unwrap();
        let block_response5 = client
            .call(methods::block::RpcBlockRequest {
                block_reference: BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable),
            })
            .await
            .unwrap();
        for block in
            &[block_response1, block_response2, block_response3, block_response4, block_response5]
        {
            assert_eq!(block.author, "test1".parse().unwrap());
            assert_eq!(block.header.height, 0);
            assert_eq!(block.header.epoch_id.as_ref(), &[0; 32]);
            assert_eq!(block.header.hash.as_ref().len(), 32);
            assert_eq!(block.header.prev_hash.as_ref(), &[0; 32]);
            assert_eq!(
                block.header.prev_state_root,
                CryptoHash::from_str("7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t").unwrap()
            );
            assert!(block.header.timestamp > 0);
            assert_eq!(block.header.validator_proposals.len(), 0);
        }
    });
}

/// Retrieve chunk via json rpc
#[test]
fn test_chunk_by_hash() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let chunk = client
            .clone()
            .call(methods::any::<methods::chunk::RpcChunkRequest>(
                "chunk",
                json!([(BlockId::Height(0), 0 as ShardId)]),
            ))
            .await
            .unwrap();
        assert_eq!(chunk.author, "test2".parse().unwrap());
        assert_eq!(chunk.header.balance_burnt, 0);
        assert_eq!(chunk.header.chunk_hash.as_ref().len(), 32);
        assert_eq!(chunk.header.encoded_length, 8);
        assert_eq!(chunk.header.encoded_merkle_root.as_ref().len(), 32);
        assert_eq!(chunk.header.gas_limit, 1000000);
        assert_eq!(chunk.header.gas_used, 0);
        assert_eq!(chunk.header.height_created, 0);
        assert_eq!(chunk.header.height_included, 0);
        assert_eq!(chunk.header.outgoing_receipts_root.as_ref().len(), 32);
        assert_eq!(chunk.header.prev_block_hash.as_ref().len(), 32);
        assert_eq!(chunk.header.prev_state_root.as_ref().len(), 32);
        assert_eq!(chunk.header.rent_paid, 0);
        assert_eq!(chunk.header.shard_id, 0);
        assert!(matches!(chunk.header.signature, Signature::ED25519(_)));
        assert_eq!(chunk.header.tx_root.as_ref(), &[0; 32]);
        assert_eq!(chunk.header.validator_proposals, vec![]);
        assert_eq!(chunk.header.validator_reward, 0);
        let same_chunk = client
            .call(methods::any::<methods::chunk::RpcChunkRequest>(
                "chunk",
                json!([chunk.header.chunk_hash]),
            ))
            .await
            .unwrap();
        assert_eq!(chunk.header.chunk_hash, same_chunk.header.chunk_hash);
    });
}

/// Retrieve chunk via json rpc
#[test]
fn test_chunk_invalid_shard_id() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let handler_error = client
            .call(methods::any::<methods::chunk::RpcChunkRequest>(
                "chunk",
                json!([(BlockId::Height(0), 100 as ShardId)]),
            ))
            .await
            .expect_err("request must not succeed")
            .handler_error();

        assert!(matches!(
            handler_error,
            Ok(near_jsonrpc_primitives::types::chunks::RpcChunkError::InvalidShardId { .. })
        ));
    });
}

/// Connect to json rpc and query account info with soft-deprecated query API.
#[test]
fn test_query_by_path_account() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let status = client.clone().call(methods::status::RpcStatusRequest).await.unwrap();
        let block_hash = status.sync_info.latest_block_hash;

        let query_response = client
            .call(methods::any::<methods::query::RpcQueryRequest>(
                "query",
                json!(["account/test".to_string(), "".to_string()]),
            ))
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        assert_eq!(query_response.block_hash, block_hash);
        let account_info = if let QueryResponseKind::ViewAccount(account) = query_response.kind {
            account
        } else {
            panic!("queried account, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(account_info.amount, 0);
        assert_eq!(account_info.code_hash.as_ref(), &[0; 32]);
        assert_eq!(account_info.locked, 0);
        assert_eq!(account_info.storage_paid_at, 0);
        assert_eq!(account_info.storage_usage, 0);
    });
}

/// Connect to json rpc and query account info.
#[test]
fn test_query_account() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let status = client.clone().call(methods::status::RpcStatusRequest).await.unwrap();
        let block_hash = status.sync_info.latest_block_hash;
        let query_response_1 = client
            .clone()
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewAccount { account_id: "test".parse().unwrap() },
            })
            .await
            .unwrap();
        let query_response_2 = client
            .clone()
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::BlockId(BlockId::Height(0)),
                request: QueryRequest::ViewAccount { account_id: "test".parse().unwrap() },
            })
            .await
            .unwrap();
        let query_response_3 = client
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::BlockId(BlockId::Hash(block_hash)),
                request: QueryRequest::ViewAccount { account_id: "test".parse().unwrap() },
            })
            .await
            .unwrap();
        for query_response in [query_response_1, query_response_2, query_response_3].iter() {
            assert_eq!(query_response.block_height, 0);
            assert_eq!(query_response.block_hash, block_hash);
            let account_info = if let QueryResponseKind::ViewAccount(ref account) =
                query_response.kind
            {
                account
            } else {
                panic!("queried account, but received something else: {:?}", query_response.kind);
            };
            assert_eq!(account_info.amount, 0);
            assert_eq!(account_info.code_hash.as_ref(), &[0; 32]);
            assert_eq!(account_info.locked, 0);
            assert_eq!(account_info.storage_paid_at, 0);
            assert_eq!(account_info.storage_usage, 0);
        }
    });
}

/// Connect to json rpc and query account info with soft-deprecated query API.
#[test]
fn test_query_by_path_access_keys() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .call(methods::any::<methods::query::RpcQueryRequest>(
                "query",
                json!(["access_key/test".to_string(), "".to_string()]),
            ))
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let access_keys = if let QueryResponseKind::AccessKeyList(access_keys) = query_response.kind
        {
            access_keys
        } else {
            panic!("queried access keys, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(access_keys.keys.len(), 1);
        assert_eq!(access_keys.keys[0].access_key, AccessKey::full_access().into());
        assert_eq!(access_keys.keys[0].public_key, PublicKey::empty(KeyType::ED25519));
    });
}

/// Connect to json rpc and query account info.
#[test]
fn test_query_access_keys() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewAccessKeyList { account_id: "test".parse().unwrap() },
            })
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let access_keys = if let QueryResponseKind::AccessKeyList(access_keys) = query_response.kind
        {
            access_keys
        } else {
            panic!("queried access keys, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(access_keys.keys.len(), 1);
        assert_eq!(access_keys.keys[0].access_key, AccessKey::full_access().into());
        assert_eq!(access_keys.keys[0].public_key, PublicKey::empty(KeyType::ED25519));
    });
}

/// Connect to json rpc and query account info with soft-deprecated query API.
#[test]
fn test_query_by_path_access_key() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .call(methods::any::<methods::query::RpcQueryRequest>(
                "query",
                json!([
                    "access_key/test/ed25519:23vYngy8iL7q94jby3gszBnZ9JptpMf5Hgf7KVVa2yQ2"
                        .to_string(),
                    "".to_string()
                ]),
            ))
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let access_key = if let QueryResponseKind::AccessKey(access_keys) = query_response.kind {
            access_keys
        } else {
            panic!("queried access keys, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(access_key.nonce, 0);
        assert_eq!(access_key.permission, AccessKeyPermission::FullAccess.into());
    });
}

/// Connect to json rpc and query account info.
#[test]
fn test_query_access_key() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewAccessKey {
                    account_id: "test".parse().unwrap(),
                    public_key: "ed25519:23vYngy8iL7q94jby3gszBnZ9JptpMf5Hgf7KVVa2yQ2"
                        .parse()
                        .unwrap(),
                },
            })
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let access_key = if let QueryResponseKind::AccessKey(access_keys) = query_response.kind {
            access_keys
        } else {
            panic!("queried access keys, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(access_key.nonce, 0);
        assert_eq!(access_key.permission, AccessKeyPermission::FullAccess.into());
    });
}

/// Connect to json rpc and query state.
#[test]
fn test_query_state() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewState {
                    account_id: "test".parse().unwrap(),
                    prefix: vec![].into(),
                },
            })
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let state = if let QueryResponseKind::ViewState(state) = query_response.kind {
            state
        } else {
            panic!("queried state, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(state.values.len(), 0);
    });
}

/// Connect to json rpc and call function
#[test]
fn test_query_call_function() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::CallFunction {
                    account_id: "test".parse().unwrap(),
                    method_name: "method".to_string(),
                    args: vec![].into(),
                },
            })
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let call_result = if let QueryResponseKind::CallResult(call_result) = query_response.kind {
            call_result
        } else {
            panic!(
                "expected a call function result, but received something else: {:?}",
                query_response.kind
            );
        };
        assert_eq!(call_result.result.len(), 0);
        assert_eq!(call_result.logs.len(), 0);
    });
}

/// query contract code
#[test]
fn test_query_contract_code() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewCode { account_id: "test".parse().unwrap() },
            })
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let code = if let QueryResponseKind::ViewCode(code) = query_response.kind {
            code
        } else {
            panic!("queried code, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(code.code, Vec::<u8>::new());
        assert_eq!(code.hash.to_string(), "11111111111111111111111111111111");
    });
}

/// Retrieve client status via JSON RPC.
#[test]
fn test_status() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let status = client.call(methods::status::RpcStatusRequest).await.unwrap();
        assert_eq!(status.chain_id, "unittest");
        assert_eq!(status.sync_info.latest_block_height, 0);
        assert_eq!(status.sync_info.syncing, false);
    });
}

/// Retrieve client status failed.
#[test]
fn test_status_fail() {
    actix_test_env!({
        let (_, addr) = test_utils::start_all(test_utils::NodeType::NonValidator);

        let client = JsonRpcClient::connect(&format!("http://{}", addr));
        WaitOrTimeout::new(
            Box::new(move |_| {
                actix::spawn(client.clone().call(methods::health::RpcHealthRequest).then(|res| {
                    if res.is_err() {
                        System::current().stop();
                    }
                    future::ready(())
                }));
            }),
            100,
            10000,
        )
        .start();
    });
}

/// Check health fails when node is absent.
#[test]
fn test_health_fail() {
    init_test_logger();

    run_actix(async {
        let client = JsonRpcClient::connect("http://127.0.0.1:12322");

        actix::spawn(client.call(methods::health::RpcHealthRequest).then(|res| {
            assert!(res.is_err());
            System::current().stop();
            future::ready(())
        }));
    });
}

/// Health fails when node doesn't produce block for period of time.
#[test]
fn test_health_fail_no_blocks() {
    actix_test_env!({
        let (_, addr) = test_utils::start_all(test_utils::NodeType::NonValidator);

        let client = JsonRpcClient::connect(&format!("http://{}", addr));
        WaitOrTimeout::new(
            Box::new(move |_| {
                actix::spawn(client.clone().call(methods::health::RpcHealthRequest).then(|res| {
                    if res.is_err() {
                        System::current().stop();
                    }
                    future::ready(())
                }));
            }),
            300,
            10000,
        )
        .start();
    });
}

/// Retrieve client health.
#[test]
fn test_health_ok() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let health = client.call(methods::health::RpcHealthRequest).await;
        assert!(health.is_ok());
    });
}

#[test]
fn test_validators_ordered() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let validators = client
            .call(methods::EXPERIMENTAL_validators_ordered::RpcValidatorsOrderedRequest {
                block_id: None,
            })
            .await
            .unwrap();
        assert_eq!(
            validators.into_iter().map(|v| v.take_account_id()).collect::<Vec<_>>(),
            vec!["test1".parse().unwrap(), "test2".parse().unwrap()]
        )
    });
}

/// Retrieve genesis config via JSON RPC.
/// WARNING: Be mindful about changing genesis structure as it is part of the public protocol!
#[test]
fn test_genesis_config() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let genesis_config = client
            .call(methods::EXPERIMENTAL_genesis_config::RpcGenesisConfigRequest)
            .await
            .unwrap();
        #[cfg(not(feature = "nightly_protocol"))]
        {
            assert_eq!(genesis_config.protocol_version, near_primitives::version::PROTOCOL_VERSION);
        }
        assert!(!genesis_config.chain_id.is_empty());
    });
}

/// Retrieve gas price
#[test]
fn test_gas_price_by_height() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let gas_price = client
            .call(methods::gas_price::RpcGasPriceRequest { block_id: Some(BlockId::Height(0)) })
            .await
            .unwrap();
        assert!(gas_price.gas_price > 0);
    });
}

/// Retrieve gas price
#[test]
fn test_gas_price_by_hash() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let block = client
            .clone()
            .call(methods::block::RpcBlockRequest {
                block_reference: BlockReference::BlockId(BlockId::Height(0)),
            })
            .await
            .unwrap();
        let gas_price = client
            .call(methods::gas_price::RpcGasPriceRequest {
                block_id: Some(BlockId::Hash(block.header.hash)),
            })
            .await
            .unwrap();
        assert!(gas_price.gas_price > 0);
    });
}

/// Retrieve gas price
#[test]
fn test_gas_price() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let gas_price =
            client.call(methods::gas_price::RpcGasPriceRequest { block_id: None }).await.unwrap();
        assert!(gas_price.gas_price > 0);
    });
}

#[test]
fn test_invalid_methods() {
    actix_test_env!({
        let (_, addr) = test_utils::start_all(test_utils::NodeType::NonValidator);

        let client = awc::Client::new();
        actix::spawn(async move {
            let method_names = vec![
                json!("\u{0}\u{0}\u{0}k\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}SRP"),
                json!(null),
                json!(true),
                json!(false),
                json!(0),
                json!(""),
            ];

            for method_name in method_names {
                if let Err(err) = test_utils::call_method::<()>(
                    &client,
                    &format!("http://{}", addr),
                    &method_name,
                    json!([]),
                )
                .await
                {
                    assert!(
                        matches!(
                            err.into(),
                            near_jsonrpc_client::errors::JsonRpcError::<()>::ServerError(
                                near_jsonrpc_client::errors::JsonRpcServerError::RequestValidationError(
                                    ..
                                ),
                            )
                        ),
                        "Invalid method {:?} must return error",
                        method_name
                    );
                }
            }
            System::current().stop();
        });
    });
}

#[test]
#[ignore] // https://github.com/nearprotocol/nearcore/issues/2789
fn test_query_view_account_non_existing_account_must_return_error() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewAccount {
                    account_id: "invalidaccount".parse().unwrap(),
                },
            })
            .await
            .unwrap();

        assert!(
            !matches!(query_response.kind, QueryResponseKind::ViewAccount(_)),
            "queried view account for not exsiting account, but received success instead of error"
        );
    });
}

#[test]
#[ignore] // https://github.com/nearprotocol/nearcore/issues/2790
fn test_view_access_key_non_existing_account_id_and_public_key_must_return_error() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewAccessKey {
                    account_id: "\u{0}\u{0}\u{0}\u{0}\u{0}9".parse().unwrap(),
                    public_key: "99999999999999999999999999999999999999999999".parse().unwrap(),
                },
            })
            .await
            .unwrap();

        assert!(
            !matches!(query_response.kind, QueryResponseKind::AccessKey(_)),
            "queried access key with not existing account and public key, received success instead of error"
        );
    });
}

#[test]
#[ignore] // https://github.com/nearprotocol/nearcore/issues/2791
fn test_call_function_non_existing_account_method_name() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::CallFunction {
                    method_name:
                        "\u{0}\u{0}\u{0}k\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}SRP"
                            .to_string(),
                    args: vec![].into(),
                    account_id: "\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}"
                        .parse()
                        .unwrap(),
                },
            })
            .await
            .unwrap();

        assert!(
            !matches!(query_response.kind, QueryResponseKind::CallResult(_)),
            "queried call function with not existing account and method name, received success instead of error"
        );
    });
}

#[test]
#[ignore] // https://github.com/nearprotocol/nearcore/issues/2792
fn test_view_access_key_list_non_existing_account() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewAccessKeyList {
                    account_id: "\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{c}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0},".parse().unwrap(),
                },
            })
            .await
            .unwrap();

        assert!(
            !matches!(query_response.kind, QueryResponseKind::AccessKeyList(_)),
            "queried access key list with not existing account, received success instead of error"
        );
    });
}

#[test]
#[ignore] // https://github.com/nearprotocol/nearcore/issues/2793
fn test_view_state_non_existing_account_invalid_prefix() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .call(methods::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewState {
                    account_id: "\u{0}\u{0}\u{0}\u{0}\u{0}\u{4}\u{0}\u{0}\u{0}\u{8}\u{0}\u{0}\u{0}\u{0}\u{0}eeeeeeeeeeeeeeeeeeeeeeeeeeeee".parse().unwrap(),
                    prefix: "eeeeeeeeeeee".as_bytes().to_vec().into(),
                },
            })
            .await
            .unwrap();

        assert!(
            !matches!(query_response.kind, QueryResponseKind::ViewState(_)),
            "queried view account for not exsiting account, but received success instead of error"
        );
    });
}

#[test]
#[ignore] // https://github.com/nearprotocol/nearcore/issues/2800
fn test_validators_non_existing_block_hash() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let validators_response = client
            .call(methods::any::<methods::validators::RpcValidatorRequest>(
                "validators",
                json!(near_primitives::types::BlockId::Hash(
                    near_primitives::hash::CryptoHash::from_str(
                        "123PXBoQKnTnARA49ctEzAiradrAAAEtLRCJGpjH24qC",
                    )
                    .unwrap(),
                )),
            ))
            .await;

        assert!(
            validators_response.is_err(),
            "validators for non exsiting block hash, but received success instead of error"
        );
    });
}

#[test]
fn test_get_chunk_with_object_in_params() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let chunk = client
            .clone()
            .call(methods::any::<methods::chunk::RpcChunkRequest>(
                "chunk",
                json!({
                    "block_id": 0u64,
                    "shard_id": 0u64,
                }),
            ))
            .await
            .unwrap();
        assert_eq!(chunk.author, "test2".parse().unwrap());
        assert_eq!(chunk.header.balance_burnt, 0);
        assert_eq!(chunk.header.chunk_hash.as_ref().len(), 32);
        assert_eq!(chunk.header.encoded_length, 8);
        assert_eq!(chunk.header.encoded_merkle_root.as_ref().len(), 32);
        assert_eq!(chunk.header.gas_limit, 1000000);
        assert_eq!(chunk.header.gas_used, 0);
        assert_eq!(chunk.header.height_created, 0);
        assert_eq!(chunk.header.height_included, 0);
        assert_eq!(chunk.header.outgoing_receipts_root.as_ref().len(), 32);
        assert_eq!(chunk.header.prev_block_hash.as_ref().len(), 32);
        assert_eq!(chunk.header.prev_state_root.as_ref().len(), 32);
        assert_eq!(chunk.header.rent_paid, 0);
        assert_eq!(chunk.header.shard_id, 0);
        assert!(matches!(chunk.header.signature, Signature::ED25519(_)));
        assert_eq!(chunk.header.tx_root.as_ref(), &[0; 32]);
        assert_eq!(chunk.header.validator_proposals, vec![]);
        assert_eq!(chunk.header.validator_reward, 0);
        let same_chunk = client
            .call(methods::any::<methods::chunk::RpcChunkRequest>(
                "chunk",
                json!([chunk.header.chunk_hash]),
            ))
            .await
            .unwrap();
        assert_eq!(chunk.header.chunk_hash, same_chunk.header.chunk_hash);
    });
}
