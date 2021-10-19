use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures::Future;
use serde_json::json;

use near_client::StatusResponse;
use near_crypto::{PublicKey, Signer};
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::transactions::RpcTransactionError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::serialize::to_base;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, MaybeBlockId, ShardId,
};
use near_primitives::views::{
    AccessKeyView, AccountView, BlockView, CallResult, ChunkView, ContractCodeView,
    ExecutionOutcomeView, FinalExecutionOutcomeView, ViewStateResult,
};

use crate::user::User;

pub struct RpcUser {
    account_id: AccountId,
    signer: Arc<dyn Signer>,
    addr: String,
}

impl RpcUser {
    fn actix<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        Fut: Future<Output = Result<T, E>> + 'static,
        F: FnOnce(JsonRpcClient) -> Fut + 'static,
    {
        let addr = self.addr.clone();
        actix::System::new()
            .block_on(async move { f(JsonRpcClient::connect(&format!("http://{}", addr))).await })
    }

    pub fn new(addr: &str, account_id: AccountId, signer: Arc<dyn Signer>) -> RpcUser {
        RpcUser { account_id, addr: addr.to_owned(), signer }
    }

    pub fn get_status(&self) -> Option<StatusResponse> {
        self.actix(|client| client.call(methods::status::RpcStatusRequest)).ok()
    }

    pub fn query(
        &self,
        path: String,
        data: &[u8],
    ) -> Result<methods::query::RpcQueryResponse, String> {
        let data = to_base(data);
        self.actix(move |client| {
            client
                .call(methods::any::<methods::query::RpcQueryRequest>("query", json!([path, data])))
        })
        .map_err(|err| err.to_string())
    }

    pub fn validators(
        &self,
        block_id: MaybeBlockId,
    ) -> Result<methods::validators::RpcValidatorResponse, String> {
        self.actix(move |client| {
            client.call(methods::any::<methods::validators::RpcValidatorRequest>(
                "validators",
                json!([block_id]),
            ))
        })
        .map_err(|err| err.to_string())
    }
}

impl User for RpcUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountView, String> {
        let query_response = self.query(format!("account/{}", account_id), &[])?;
        match query_response.kind {
            near_jsonrpc_primitives::types::query::QueryResponseKind::ViewAccount(account_view) => {
                Ok(account_view)
            }
            _ => Err("Invalid type of response".into()),
        }
    }

    fn view_state(&self, account_id: &AccountId, prefix: &[u8]) -> Result<ViewStateResult, String> {
        let query_response = self.query(format!("contract/{}", account_id), prefix)?;
        match query_response.kind {
            near_jsonrpc_primitives::types::query::QueryResponseKind::ViewState(
                view_state_result,
            ) => Ok(view_state_result),
            _ => Err("Invalid type of response".into()),
        }
    }

    fn view_contract_code(&self, account_id: &AccountId) -> Result<ContractCodeView, String> {
        let query_response = self.query(format!("code/{}", account_id), &[])?;
        match query_response.kind {
            near_jsonrpc_primitives::types::query::QueryResponseKind::ViewCode(
                contract_code_view,
            ) => Ok(contract_code_view),
            _ => Err("Invalid type of response".into()),
        }
    }

    fn view_call(
        &self,
        account_id: &AccountId,
        method_name: &str,
        args: &[u8],
    ) -> Result<CallResult, String> {
        let query_response = self.query(format!("call/{}/{}", account_id, method_name), args)?;
        match query_response.kind {
            near_jsonrpc_primitives::types::query::QueryResponseKind::CallResult(call_result) => {
                Ok(call_result)
            }
            _ => Err("Invalid type of response".into()),
        }
    }

    fn add_transaction(&self, transaction: SignedTransaction) {
        let _ = self
            .actix(move |client| {
                client.call(methods::broadcast_tx_async::RpcBroadcastTxAsyncRequest {
                    signed_transaction: transaction,
                })
            })
            .unwrap();
    }

    fn commit_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<FinalExecutionOutcomeView, RpcTransactionError> {
        let result = self
            .actix(move |client| {
                client.call(methods::broadcast_tx_commit::RpcBroadcastTxCommitRequest {
                    signed_transaction: transaction,
                })
            })
            .map_err(|err| err.handler_error().unwrap())?;
        // Wait for one more block, to make sure all nodes actually apply the state transition.
        let height = self.get_best_height().unwrap();
        while height == self.get_best_height().unwrap() {
            thread::sleep(Duration::from_millis(50));
        }
        Ok(result)
    }

    fn add_receipt(&self, _receipt: Receipt) {
        // TDDO: figure out if rpc will support this
        unimplemented!()
    }

    fn get_best_height(&self) -> Option<BlockHeight> {
        self.get_status().map(|status| status.sync_info.latest_block_height)
    }

    fn get_best_block_hash(&self) -> Option<CryptoHash> {
        self.get_status().map(|status| status.sync_info.latest_block_hash)
    }

    fn get_block(&self, height: BlockHeight) -> Option<BlockView> {
        self.actix(move |client| {
            client.call(methods::block::RpcBlockRequest {
                block_reference: BlockReference::BlockId(BlockId::Height(height)),
            })
        })
        .ok()
    }

    fn get_block_by_hash(&self, block_hash: CryptoHash) -> Option<BlockView> {
        self.actix(move |client| {
            client.call(methods::block::RpcBlockRequest {
                block_reference: BlockReference::BlockId(BlockId::Hash(block_hash)),
            })
        })
        .ok()
    }

    fn get_chunk(&self, height: BlockHeight, shard_id: ShardId) -> Option<ChunkView> {
        self.actix(move |client| {
            client.call(methods::chunk::RpcChunkRequest {
                chunk_reference:
                    near_jsonrpc_primitives::types::chunks::ChunkReference::BlockShardId {
                        block_id: BlockId::Height(height),
                        shard_id,
                    },
            })
        })
        .ok()
    }

    fn get_transaction_result(&self, _hash: &CryptoHash) -> ExecutionOutcomeView {
        unimplemented!()
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalExecutionOutcomeView {
        let (hash, account_id) = (*hash, self.account_id.clone());
        self.actix(move |client| {
            client.call(methods::tx::RpcTransactionStatusRequest {
                transaction_info:
                    near_jsonrpc_primitives::types::transactions::TransactionInfo::TransactionId {
                        hash,
                        account_id,
                    },
            })
        })
        .unwrap()
    }

    fn get_state_root(&self) -> CryptoHash {
        self.get_status().map(|status| status.sync_info.latest_state_root).unwrap()
    }

    fn get_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKeyView, String> {
        let query_response =
            self.query(format!("access_key/{}/{}", account_id, public_key), &[])?;
        match query_response.kind {
            near_jsonrpc_primitives::types::query::QueryResponseKind::AccessKey(access_key) => {
                Ok(access_key)
            }
            _ => Err("Invalid type of response".into()),
        }
    }

    fn signer(&self) -> Arc<dyn Signer> {
        self.signer.clone()
    }

    fn set_signer(&mut self, signer: Arc<dyn Signer>) {
        self.signer = signer;
    }
}
