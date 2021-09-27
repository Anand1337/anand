use std::sync::{Arc, Mutex};

use actix::{Actor, System};
use futures::{FutureExt, TryFutureExt};

use near_actix_test_utils::run_actix;
use near_crypto::{InMemorySigner, KeyType};
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_logger_utils::init_test_logger;
use near_network::test_utils::WaitOrTimeout;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::serialize::to_base64;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::BlockReference;
use near_primitives::views::FinalExecutionStatus;

#[macro_use]
pub mod test_utils;

/// Test sending transaction via json rpc without waiting.
#[test]
fn test_send_tx_async() {
    actix_test_env!({
        let (_, addr) = test_utils::start_all(test_utils::NodeType::Validator);

        let client = JsonRpcClient::connect(&format!("http://{}", addr));

        let tx_hash2 = Arc::new(Mutex::new(None));
        let tx_hash2_1 = tx_hash2.clone();
        let tx_hash2_2 = tx_hash2.clone();

        let client_1 = client.clone();
        actix::spawn(async move {
            let res = client_1
                .clone()
                .call(methods::block::RpcBlockRequest { block_reference: BlockReference::latest() })
                .await;
            let block_hash = res.unwrap().header.hash;
            let signer =
                InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
            let tx = SignedTransaction::send_money(
                1,
                "test1".parse().unwrap(),
                "test2".parse().unwrap(),
                &signer,
                100,
                block_hash,
            );
            *tx_hash2_1.lock().unwrap() = Some(tx.get_hash());
            let request =
                methods::broadcast_tx_async::RpcBroadcastTxAsyncRequest { signed_transaction: tx };
            let result = client_1.call(&request).await.unwrap();
            assert_eq!(request.signed_transaction.get_hash(), result);
        });

        WaitOrTimeout::new(
            Box::new(move |_| {
                if let Some(tx_hash) = *tx_hash2_2.lock().unwrap() {
                    actix::spawn(
                        client
                            .clone()
                            .call(methods::tx::RpcTransactionStatusRequest {
                                transaction_info: methods::tx::TransactionInfo::TransactionId {
                                    hash: tx_hash,
                                    account_id: "test1".parse().unwrap(),
                                },
                            })
                            .map_err(|err| println!("Error: {:?}", err))
                            .map_ok(|result| {
                                if let FinalExecutionStatus::SuccessValue(_) = result.status {
                                    System::current().stop();
                                }
                            })
                            .map(drop),
                    );
                }
            }),
            100,
            2000,
        )
        .start();
    });
}

/// Test sending transaction and waiting for it to be committed to a block.
#[test]
fn test_send_tx_commit() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let block_hash = client
            .clone()
            .call(methods::block::RpcBlockRequest { block_reference: BlockReference::latest() })
            .await
            .unwrap()
            .header
            .hash;
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let result = client
            .call(methods::broadcast_tx_commit::RpcBroadcastTxCommitRequest {
                signed_transaction: SignedTransaction::send_money(
                    1,
                    "test1".parse().unwrap(),
                    "test2".parse().unwrap(),
                    &signer,
                    100,
                    block_hash,
                ),
            })
            .await
            .unwrap();
        assert_eq!(result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    });
}

/// Test that expired transaction should be rejected
#[test]
fn test_expired_tx() {
    actix_test_env!({
        let (_, addr) = test_utils::start_all_with_validity_period_and_no_epoch_sync(
            test_utils::NodeType::Validator,
            1,
            false,
        );

        let block_hash = Arc::new(Mutex::new(None));
        let block_height = Arc::new(Mutex::new(None));

        WaitOrTimeout::new(
            Box::new(move |_| {
                let block_hash = block_hash.clone();
                let block_height = block_height.clone();
                let client = JsonRpcClient::connect(&format!("http://{}", addr));
                actix::spawn(async move {
                    let res = client
                        .clone()
                        .call(methods::block::RpcBlockRequest {
                            block_reference: BlockReference::latest(),
                        })
                        .await;
                    let header = res.unwrap().header;
                    let hash = block_hash.lock().unwrap().clone();
                    let height = block_height.lock().unwrap().clone();
                    if let Some(block_hash) = hash {
                        if let Some(height) = height {
                            if header.height - height >= 2 {
                                let signer = InMemorySigner::from_seed(
                                    "test1".parse().unwrap(),
                                    KeyType::ED25519,
                                    "test1",
                                );
                                let tx = SignedTransaction::send_money(
                                    1,
                                    "test1".parse().unwrap(),
                                    "test2".parse().unwrap(),
                                    &signer,
                                    100,
                                    block_hash,
                                );
                                actix::spawn(async move {
                                    let res = client
                                        .clone()
                                        .call(methods::broadcast_tx_commit::RpcBroadcastTxCommitRequest {
                                            signed_transaction: tx,
                                        })
                                        .await;
                                    if let Err(err) = res {
                                        let handler_error = err.handler_error();
                                        assert!(
                                            matches!(
                                                handler_error,
                                                Ok(methods::broadcast_tx_commit::RpcTransactionError::InvalidTransaction {
                                                    context: near_primitives::errors::InvalidTxError::Expired
                                                })
                                            ),
                                            "expected an InvalidTransaction handler error response, got [{:?}]",
                                            handler_error
                                        );
                                        System::current().stop();
                                    }
                                });
                            }
                        }
                    } else {
                        *block_hash.lock().unwrap() = Some(header.hash);
                        *block_height.lock().unwrap() = Some(header.height);
                    };
                });
            }),
            100,
            1000,
        )
        .start();
    });
}

/// Test sending transaction based on a different fork should be rejected
#[test]
fn test_replay_protection() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        client
            .call(methods::broadcast_tx_commit::RpcBroadcastTxCommitRequest {
                signed_transaction: SignedTransaction::send_money(
                    1,
                    "test1".parse().unwrap(),
                    "test2".parse().unwrap(),
                    &signer,
                    100,
                    hash(&[1]),
                ),
            })
            .await
            .expect_err("transaction should not succeed");
    });
}

#[test]
fn test_tx_status_missing_tx() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let tx_hash = CryptoHash::default();
        let handler_error = client
            .call(methods::tx::RpcTransactionStatusRequest {
                transaction_info: methods::tx::TransactionInfo::TransactionId {
                    hash: tx_hash.clone(),
                    account_id: "test1".parse().unwrap(),
                },
            })
            .await
            .expect_err("request must not succeed")
            .handler_error();
        assert!(
            matches!(
                handler_error,
                Ok(methods::tx::RpcTransactionError::UnknownTransaction {
                    requested_transaction_hash
                })
                if requested_transaction_hash == tx_hash
            ),
            "expected an UnknownTransaction handler error response, got [{:?}]",
            handler_error
        );
    });
}

#[test]
fn test_check_invalid_tx() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        // invalid base hash
        let tx = SignedTransaction::send_money(
            1,
            "test1".parse().unwrap(),
            "test2".parse().unwrap(),
            &signer,
            100,
            hash(&[1]),
        );
        actix::spawn(async {
            let handler_err = client
                .call(methods::EXPERIMENTAL_check_tx::RpcCheckTxRequest { signed_transaction: tx })
                .await
                .expect_err("request should not succeed")
                .handler_error();
            assert!(
                matches!(
                    handler_err,
                    Ok(methods::EXPERIMENTAL_check_tx::RpcTransactionError::InvalidTransaction {
                        context: near_primitives::errors::InvalidTxError::Expired,
                    })
                ),
                "expected an InvalidTransaction handler error response, got [{:?}]",
                handler_err
            );
            System::current().stop();
        });
    });
}
