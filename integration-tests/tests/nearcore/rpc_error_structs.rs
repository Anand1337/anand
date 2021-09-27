use actix::{Actor, System};
use futures::{future, FutureExt, TryFutureExt};
use serde_json::json;

use crate::node_cluster::NodeCluster;
use integration_tests::genesis_helpers::genesis_block;
use near_actix_test_utils::spawn_interruptible;
use near_client::GetBlock;
use near_crypto::{InMemorySigner, KeyType};
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_logger_utils::init_integration_logger;
use near_network::test_utils::WaitOrTimeout;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::BlockId;

// Queries json-rpc block that doesn't exists
// Checks if the struct is expected and contains the proper data
#[test]
fn test_block_unknown_block_error() {
    init_integration_logger();

    let cluster = NodeCluster::new(4, |index| format!("block_unknown{}", index))
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(2)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|_genesis, rpc_addrs, clients| async move {
        let view_client = clients[0].1.clone();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();

                // We are sending this tx unstop, just to get over the warm up period.
                // Probably make sense to stop after 1 time though.
                spawn_interruptible(view_client.send(GetBlock::latest()).then(move |res| {
                    if let Ok(Ok(block)) = res {
                        if block.header.height > 1 {
                            let client =
                                JsonRpcClient::connect(&format!("http://{}", rpc_addrs_copy[2]));
                            spawn_interruptible(
                                client
                                    .call(
                                        methods::any::<methods::block::RpcBlockRequest>(
                                            "block",
                                            json!([BlockId::Height(block.header.height + 100)])
                                        )
                                    )
                                    .map_err(|err| {
                                        let handler_error = err.handler_error();
                                        assert!(
                                            matches!(
                                                handler_error,
                                                Ok(methods::block::RpcBlockError::UnknownBlock { .. })
                                            ),
                                            "expected an UnknownBlock handler error response, got [{:?}]",
                                            handler_error
                                        );
                                        System::current().stop();
                                    })
                                    .map_ok(|_| panic!("The block mustn't be found"))
                                    .map(drop),
                            );
                        }
                    }
                    future::ready(())
                }));
            }),
            100,
            40000,
        )
        .start();
    });
}

// Queries json-rpc chunk that doesn't exists
// (randomish chunk hash, we hope it won't happen in test case)
// Checks if the struct is expected and contains the proper data
#[test]
fn test_chunk_unknown_chunk_error() {
    init_integration_logger();

    let cluster = NodeCluster::new(4, |index| format!("chunk_unknown{}", index))
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(2)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|_genesis, rpc_addrs, clients| async move {
        let view_client = clients[0].1.clone();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();

                // We are sending this tx unstop, just to get over the warm up period.
                // Probably make sense to stop after 1 time though.
                spawn_interruptible(view_client.send(GetBlock::latest()).then(move |res| {
                    if let Ok(Ok(block)) = res {
                        if block.header.height > 1 {
                            let client =
                                JsonRpcClient::connect(&format!("http://{}", rpc_addrs_copy[2]));
                            spawn_interruptible(
                                client
                                    .call(methods::chunk::RpcChunkRequest {
                                        chunk_reference: near_jsonrpc_primitives::types::chunks::ChunkReference::ChunkHash {
                                            chunk_id:
                                                "3tMcx4KU2KvkwJPMWPXqK2MUU1FDVbigPFNiAeuVa7Tu"
                                                    .parse()
                                                    .unwrap(),
                                        },
                                    })
                                    .map_err(|err| {
                                        let handler_error = err.handler_error();
                                        assert!(
                                            matches!(
                                                handler_error,
                                                Ok(methods::chunk::RpcChunkError::UnknownChunk {
                                                    chunk_hash: near_primitives::sharding::ChunkHash(crypto_hash)
                                                })
                                                if crypto_hash
                                                == "3tMcx4KU2KvkwJPMWPXqK2MUU1FDVbigPFNiAeuVa7Tu"
                                                    .parse()
                                                    .unwrap()
                                            ),
                                            "expected an UnknownChunk handler error response, got [{:?}]",
                                            handler_error
                                        );
                                        System::current().stop();
                                    })
                                    .map_ok(|_| panic!("The chunk mustn't be found"))
                                    .map(drop),
                            );
                        }
                    }
                    future::ready(())
                }));
            }),
            100,
            40000,
        )
        .start();
    });
}

// Queries json-rpc EXPERIMENTAL_protocol_config that doesn't exists
// Checks if the struct is expected and contains the proper data
#[test]
fn test_protocol_config_unknown_block_error() {
    init_integration_logger();

    let cluster = NodeCluster::new(4, |index| format!("protocol_config_block_unknown{}", index))
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(2)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|_genesis, rpc_addrs, clients| async move {
        let view_client = clients[0].1.clone();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();

                // We are sending this tx unstop, just to get over the warm up period.
                // Probably make sense to stop after 1 time though.
                spawn_interruptible(view_client.send(GetBlock::latest()).then(move |res| {
                    if let Ok(Ok(block)) = res {
                        if block.header.height > 1 {
                            let client =
                                JsonRpcClient::connect(&format!("http://{}", rpc_addrs_copy[2]));
                            spawn_interruptible(
                                client
                                    .call(methods::EXPERIMENTAL_protocol_config::RpcProtocolConfigRequest {
                                        block_reference:
                                            near_primitives::types::BlockReference::BlockId(
                                                BlockId::Height(block.header.height + 100),
                                            ),
                                    })
                                    .map_err(|err| {
                                        let handler_error = err.handler_error();
                                        assert!(
                                            matches!(
                                                handler_error,
                                                Ok(methods::EXPERIMENTAL_protocol_config::RpcProtocolConfigError::UnknownBlock { .. }),
                                            ),
                                            "expected an UnknownBlock handler error response, got [{:?}]",
                                            handler_error
                                        );
                                        System::current().stop();
                                    })
                                    .map_ok(|_| panic!("The block mustn't be found"))
                                    .map(drop),
                            );
                        }
                    }
                    future::ready(())
                }));
            }),
            100,
            40000,
        )
        .start();
    });
}

// Queries json-rpc gas_price that doesn't exists
// Checks if the struct is expected and contains the proper data
#[test]
fn test_gas_price_unknown_block_error() {
    init_integration_logger();

    let cluster = NodeCluster::new(4, |index| format!("gas_price_block_unknown{}", index))
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(2)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|_genesis, rpc_addrs, clients| async move {
        let view_client = clients[0].1.clone();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();

                // We are sending this tx unstop, just to get over the warm up period.
                // Probably make sense to stop after 1 time though.
                spawn_interruptible(view_client.send(GetBlock::latest()).then(move |res| {
                    if let Ok(Ok(block)) = res {
                        if block.header.height > 1 {
                            let client =
                                JsonRpcClient::connect(&format!("http://{}", rpc_addrs_copy[2]));
                            spawn_interruptible(
                                client
                                    .call(methods::gas_price::RpcGasPriceRequest {
                                        block_id: Some(BlockId::Height(block.header.height + 100)),
                                    })
                                    .map_err(|err| {
                                        let handler_error = err.handler_error();
                                        assert!(
                                            matches!(
                                                handler_error,
                                                Ok(methods::gas_price::RpcGasPriceError::UnknownBlock { .. }),
                                            ),
                                            "expected an UnknownBlock handler error response, got [{:?}]",
                                            handler_error
                                        );
                                        System::current().stop();
                                    })
                                    .map_ok(|_| panic!("The block mustn't be found"))
                                    .map(drop),
                            );
                        }
                    }
                    future::ready(())
                }));
            }),
            100,
            40000,
        )
        .start();
    });
}

// Queries json-rpc EXPERIMENTAL_receipt that doesn't exists
// Checks if the struct is expected and contains the proper data
#[test]
fn test_receipt_id_unknown_receipt_error() {
    init_integration_logger();

    let cluster = NodeCluster::new(4, |index| format!("receipt_unknown{}", index))
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(2)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|_genesis, rpc_addrs, clients| async move {
        let view_client = clients[0].1.clone();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();

                // We are sending this tx unstop, just to get over the warm up period.
                // Probably make sense to stop after 1 time though.
                spawn_interruptible(view_client.send(GetBlock::latest()).then(move |res| {
                    if let Ok(Ok(block)) = res {
                        if block.header.height > 1 {
                            let client =
                                JsonRpcClient::connect(&format!("http://{}", rpc_addrs_copy[2]));
                            spawn_interruptible(
                                client
                                    .call(
                                        methods::EXPERIMENTAL_receipt::RpcReceiptRequest {
                                            receipt_reference: near_jsonrpc_primitives::types::receipts::ReceiptReference {
                                                receipt_id: "3tMcx4KU2KvkwJPMWPXqK2MUU1FDVbigPFNiAeuVa7Tu".parse().unwrap()
                                            }
                                        }
                                    )
                                    .map_err(|err| {
                                        let handler_error = err.handler_error();
                                        assert!(
                                            matches!(
                                                handler_error,
                                                Ok(methods::EXPERIMENTAL_receipt::RpcReceiptError::UnknownReceipt { receipt_id })
                                                if receipt_id
                                                == "3tMcx4KU2KvkwJPMWPXqK2MUU1FDVbigPFNiAeuVa7Tu"
                                                    .parse()
                                                    .unwrap()
                                            ),
                                            "expected an UnknownReceipt handler error response, got [{:?}]",
                                            handler_error
                                        );
                                        System::current().stop();
                                    })
                                    .map_ok(|_| panic!("The block mustn't be found"))
                                    .map(drop),
                            );
                        }
                    }
                    future::ready(())
                }));
            }),
            100,
            40000,
        )
        .start();
    });
}

/// Starts 2 validators and 2 light clients (not tracking anything).
/// Sends tx to first light client through `broadcast_tx_commit` and checks that the transaction has failed.
/// Checks if the struct is expected and contains the proper data
#[test]
fn test_tx_invalid_tx_error() {
    init_integration_logger();

    let cluster = NodeCluster::new(4, |index| format!("tx_invalid{}", index))
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(2)
        .set_epoch_length(1000)
        .set_genesis_height(0);

    cluster.exec_until_stop(|genesis, rpc_addrs, clients| async move {
        let view_client = clients[0].1.clone();

        let genesis_hash = *genesis_block(&genesis).hash();
        let signer =
            InMemorySigner::from_seed("near.5".parse().unwrap(), KeyType::ED25519, "near.5");
        let transaction = SignedTransaction::send_money(
            1,
            "near.5".parse().unwrap(),
            "near.2".parse().unwrap(),
            &signer,
            10000,
            genesis_hash,
        );

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();
                let transaction_copy = transaction.clone();
                let tx_hash = transaction_copy.get_hash();

                spawn_interruptible(view_client.send(GetBlock::latest()).then(move |res| {
                    if let Ok(Ok(block)) = res {
                        if block.header.height > 10 {
                            let client =
                                JsonRpcClient::connect(&format!("http://{}", rpc_addrs_copy[2]));
                            spawn_interruptible(
                                client
                                    .call(
                                        methods::EXPERIMENTAL_broadcast_tx_sync::RpcBroadcastTxSyncRequest {
                                            signed_transaction: transaction_copy
                                        }
                                    )
                                    .map_err(move |err| {
                                        let handler_error = err.handler_error();
                                        assert!(
                                            matches!(
                                                handler_error,
                                                Ok(methods::EXPERIMENTAL_broadcast_tx_sync::RpcTransactionError::RequestRouted {
                                                    transaction_hash
                                                })
                                                if transaction_hash == tx_hash
                                            ),
                                            "expected a RequestRouted handler error response, got [{:?}]",
                                            handler_error
                                        );
                                        System::current().stop();
                                    })
                                    .map_ok(|_| panic!("The transaction mustn't succeed"))
                                    .map(drop),
                            );
                        }
                    }
                    future::ready(())
                }));
            }),
            100,
            40000,
        )
        .start();
    });
}

#[test]
fn test_query_rpc_account_view_unknown_block_must_return_error() {
    init_integration_logger();

    let cluster = NodeCluster::new(1, |index| format!("invalid_account{}", index))
        .set_num_shards(1)
        .set_num_validator_seats(1)
        .set_num_lightclients(0)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|_, rpc_addrs, _| async move {
        let client = JsonRpcClient::connect(&format!("http://{}", rpc_addrs[0]));
        let query_response = client
            .call(methods::query::RpcQueryRequest {
                block_reference: near_primitives::types::BlockReference::BlockId(BlockId::Height(
                    1,
                )),
                request: near_primitives::views::QueryRequest::ViewAccount {
                    account_id: "near.0".parse().unwrap(),
                },
            })
            .await;

        let handler_error = query_response
            .map(|result| result.kind)
            .expect_err("request must not succeed")
            .handler_error();

        assert!(
            matches!(handler_error, Ok(methods::query::RpcQueryError::UnknownBlock { .. })),
            "expected an UnknownBlock handler error response, got [{:?}]",
            handler_error
        );

        System::current().stop();
    });
}
