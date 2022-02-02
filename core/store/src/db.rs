use borsh::{BorshDeserialize, BorshSerialize};
use near_experimental_storage::Storage;
use std::io;
use std::path::Path;
use strum::EnumIter;

/// This enum holds the information about the columns that we use within the RocksDB storage.
/// You can think about our storage as 2-dimensional table (with key and column as indexes/coordinates).
// TODO(mm-near): add info about the RC in the columns.
#[derive(PartialEq, Debug, Copy, Clone, EnumIter, BorshDeserialize, BorshSerialize, Hash, Eq)]
pub enum DBCol {
    /// Column to indicate which version of database this is.
    /// - *Rows*: single row [VERSION_KEY]
    /// - *Content type*: The version of the database (u32), serialized as JSON.
    ColDbVersion = 0,
    /// Column that store Misc cells.
    /// - *Rows*: multiple, for example "GENESIS_JSON_HASH", "HEAD_KEY", [LATEST_KNOWN_KEY] etc.
    /// - *Content type*: cell specific.
    ColBlockMisc = 1,
    /// Column that stores Block content.
    /// - *Rows*: block hash (CryptHash)
    /// - *Content type*: [near_primitives::block::Block]
    ColBlock = 2,
    /// Column that stores Block headers.
    /// - *Rows*: block hash (CryptoHash)
    /// - *Content type*: [near_primitives::block_header::BlockHeader]
    ColBlockHeader = 3,
    /// Column that stores mapping from block height to block hash.
    /// - *Rows*: height (u64)
    /// - *Content type*: block hash (CryptoHash)
    ColBlockHeight = 4,
    /// Column that stores the Trie state.
    /// - *Rows*: trie_node_or_value_hash (CryptoHash)
    /// - *Content type*: Serializd RawTrieNodeWithSize or value ()
    ColState = 5,
    /// Mapping from BlockChunk to ChunkExtra
    /// - *Rows*: BlockChunk (block_hash, shard_uid)
    /// - *Content type*: [near_primitives::types::ChunkExtra]
    ColChunkExtra = 6,
    /// Mapping from transaction outcome id (CryptoHash) to list of outcome ids with proofs.
    /// - *Rows*: outcome id (CryptoHash)
    /// - *Content type*: Vec of [near_primitives::transactions::ExecutionOutcomeWithIdAndProof]
    ColTransactionResult = 7,
    /// Mapping from Block + Shard to list of outgoing receipts.
    /// - *Rows*: block + shard
    /// - *Content type*: Vec of [near_primitives::receipt::Receipt]
    ColOutgoingReceipts = 8,
    /// Mapping from Block + Shard to list of incoming receipt proofs.
    /// Each proof might prove multiple receipts.
    /// - *Rows*: (block, shard)
    /// - *Content type*: Vec of [near_primitives::sharding::ReceiptProof]
    ColIncomingReceipts = 9,
    /// Info about the peers that we are connected to. Mapping from peer_id to KnownPeerState.
    /// - *Rows*: peer_id (PublicKey)
    /// - *Content type*: [network_primitives::types::KnownPeerState]
    ColPeers = 10,
    /// Mapping from EpochId to EpochInfo
    /// - *Rows*: EpochId (CryptoHash)
    /// - *Content type*: [near_primitives::epoch_manager::EpochInfo]
    ColEpochInfo = 11,
    /// Mapping from BlockHash to BlockInfo
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: [near_primitives::epoch_manager::BlockInfo]
    ColBlockInfo = 12,
    /// Mapping from ChunkHash to ShardChunk.
    /// - *Rows*: ChunkHash (CryptoHash)
    /// - *Content type*: [near_primitives::sharding::ShardChunk]
    ColChunks = 13,
    /// Storage for  PartialEncodedChunk.
    /// - *Rows*: ChunkHash (CryptoHash)
    /// - *Content type*: [near_primitives::sharding::PartialEncodedChunk]
    ColPartialChunks = 14,
    /// Blocks for which chunks need to be applied after the state is downloaded for a particular epoch
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: Vec of BlockHash (CryptoHash)
    ColBlocksToCatchup = 15,
    /// Blocks for which the state is being downloaded
    /// - *Rows*: First block of the epoch (CryptoHash)
    /// - *Content type*: StateSyncInfo
    ColStateDlInfos = 16,
    /// Blocks that were ever challenged.
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: 'true' (bool)
    ColChallengedBlocks = 17,
    /// Contains all the Shard State Headers.
    /// - *Rows*: StateHeaderKey (ShardId || BlockHash)
    /// - *Content type*: ShardStateSyncResponseHeader
    ColStateHeaders = 18,
    /// Contains all the invalid chunks (that we had trouble decoding or verifying).
    /// - *Rows*: ShardChunkHeader object
    /// - *Content type*: EncodedShardChunk
    ColInvalidChunks = 19,
    /// Contains 'BlockExtra' information that is computed after block was processed.
    /// Currently it stores only challenges results.
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: BlockExtra
    ColBlockExtra = 20,
    /// Store hash of all block per each height, to detect double signs.
    /// - *Rows*: int (height of the block)
    /// - *Content type*: Map: EpochId -> Set of BlockHash(CryptoHash)
    ColBlockPerHeight = 21,
    /// Contains State parts that we've received.
    /// - *Rows*: StatePartKey (BlockHash || ShardId || PartId (u64))
    /// - *Content type*: state part (bytes)
    ColStateParts = 22,
    /// Contains mapping from epoch_id to epoch start (first block height of the epoch)
    /// - *Rows*: EpochId (CryptoHash)  -- TODO: where does the epoch_id come from? it looks like blockHash..
    /// - *Content type*: BlockHeight (int)
    ColEpochStart = 23,
    /// Map account_id to announce_account (which peer has announced which account in the current epoch). // TODO: explain account annoucement
    /// - *Rows*: AccountId (str)
    /// - *Content type*: AnnounceAccount
    ColAccountAnnouncements = 24,
    /// Next block hashes in the sequence of the canonical chain blocks.
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Content type*: next block: BlockHash (CryptoHash)
    ColNextBlockHashes = 25,
    /// `LightClientBlock`s corresponding to the last final block of each completed epoch.
    /// - *Rows*: EpochId (CryptoHash)
    /// - *Content type*: LightClientBlockView
    ColEpochLightClientBlocks = 26,
    /// Mapping from Receipt id to Shard id
    /// - *Rows*: ReceiptId (CryptoHash)
    /// - *Content type*: Shard Id || ref_count (u64 || u64)
    ColReceiptIdToShardId = 27,
    // Deprecated.
    _ColNextBlockWithNewChunk = 28,
    // Deprecated.
    _ColLastBlockWithNewChunk = 29,
    /// Network storage:
    ///   When given edge is removed (or we didn't get any ping from it for a while), we remove it from our 'in memory'
    ///   view and persist into storage.
    ///
    ///   This is done, so that we prevent the attack, when someone tries to introduce the edge/peer again into the network,
    ///   but with the 'old' nonce.
    ///
    ///   When we write things to storage, we do it in groups (here they are called 'components') - this naming is a little bit
    ///   unfortunate, as the peers/edges that we persist don't need to be connected or form any other 'component' (in a graph theory sense).
    ///
    ///   Each such component gets a new identifier (here called 'nonce').
    ///
    ///   We store this info in the three columns below:
    ///     - LastComponentNonce: keeps info on what is the next identifier (nonce) that can be used.
    ///     - PeerComponent: keep information on mapping from the peer to the last component that it belonged to (so that if a new peer shows
    ///         up we know which 'component' to load)
    ///     - ComponentEdges: keep the info about the edges that were connecting these peers that were removed.

    /// Map each saved peer on disk with its component id (a.k.a. nonce).
    /// - *Rows*: peer_id
    /// - *Column type*:  (nonce) u64
    ColPeerComponent = 30,
    /// Map component id  (a.k.a. nonce) with all edges in this component.
    /// These are all the edges that were purged and persisted to disk at the same time.
    /// - *Rows*: nonce
    /// - *Column type*: `Vec<near_network::routing::Edge>`
    ColComponentEdges = 31,
    /// Biggest component id (a.k.a nonce) used.
    /// - *Rows*: single row (empty row name)
    /// - *Column type*: (nonce) u64
    ColLastComponentNonce = 32,
    /// Map of transactions
    /// - *Rows*: transaction hash
    /// - *Column type*: SignedTransaction
    ColTransactions = 33,
    /// Mapping from a given (Height, ShardId) to the Chunk hash.
    /// - *Rows*: (Height || ShardId) - (u64 || u64)
    /// - *Column type*: ChunkHash (CryptoHash)
    ColChunkPerHeightShard = 34,
    /// Changes to state (Trie) that we have recorded.
    /// - *Rows*: BlockHash || TrieKey (TrieKey is written via custom to_vec)
    /// - *Column type*: TrieKey, new value and reason for change (RawStateChangesWithTrieKey)
    ColStateChanges = 35,
    /// Mapping from Block to its refcount. (Refcounts are used in handling chain forks)
    /// - *Rows*: BlockHash (CryptoHash)
    /// - *Column type*: refcount (u64)
    ColBlockRefCount = 36,
    /// Changes to Trie that we recorded during given block/shard processing.
    /// - *Rows*: BlockHash || ShardId
    /// - *Column type*: old root, new root, list of insertions, list of deletions (TrieChanges)
    ColTrieChanges = 37,
    /// Mapping from a block hash to a merkle tree of block hashes that are in the chain before it.
    /// - *Rows*: BlockHash
    /// - *Column type*: PartialMerkleTree - MerklePath to the leaf + number of leaves in the whole tree.
    ColBlockMerkleTree = 38,
    /// Mapping from height to the set of Chunk Hashes that were included in the block at that height.
    /// - *Rows*: height (u64)
    /// - *Column type*: Vec<ChunkHash (CryptoHash)>
    ColChunkHashesByHeight = 39,
    /// Mapping from block ordinal number (number of the block in the chain) to the BlockHash.
    /// - *Rows*: ordinal (u64)
    /// - *Column type*: BlockHash (CryptoHash)
    ColBlockOrdinal = 40,
    /// GC Count for each column - number of times we did the GarbageCollection on the column.
    /// - *Rows*: column id (byte)
    /// - *Column type*: u64
    ColGCCount = 41,
    /// All Outcome ids by block hash and shard id. For each shard it is ordered by execution order.
    /// TODO: seems that it has only 'transaction ids' there (not sure if intentional)
    /// - *Rows*: BlockShardId (BlockHash || ShardId) - 40 bytes
    /// - *Column type*: Vec <OutcomeId (CryptoHash)>
    ColOutcomeIds = 42,
    /// Deprecated
    _ColTransactionRefCount = 43,
    /// Heights of blocks that have been processed.
    /// - *Rows*: height (u64)
    /// - *Column type*: empty
    ColProcessedBlockHeights = 44,
    /// Mapping from receipt hash to Receipt.
    /// - *Rows*: receipt (CryptoHash)
    /// - *Column type*: Receipt
    ColReceipts = 45,
    /// Precompiled machine code of the contract, used by StoreCompiledContractCache.
    /// - *Rows*: ContractCacheKey or code hash (not sure)
    /// - *Column type*: near-vm-runner CacheRecord
    ColCachedContractCode = 46,
    /// Epoch validator information used for rpc purposes.
    /// - *Rows*: epoch id (CryptoHash)
    /// - *Column type*: EpochSummary
    ColEpochValidatorInfo = 47,
    /// Header Hashes indexed by Height.
    /// - *Rows*: height (u64)
    /// - *Column type*: Vec<HeaderHashes (CryptoHash)>
    ColHeaderHashesByHeight = 48,
    /// State changes made by a chunk, used for splitting states
    /// - *Rows*: BlockShardId (BlockHash || ShardId) - 40 bytes
    /// - *Column type*: StateChangesForSplitStates
    ColStateChangesForSplitStates = 49,
}

// Do not move this line from enum DBCol
pub const NUM_COLS: usize = 50;

impl std::fmt::Display for DBCol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let desc = match self {
            Self::ColDbVersion => "db version",
            Self::ColBlockMisc => "miscellaneous block data",
            Self::ColBlock => "block data",
            Self::ColBlockHeader => "block header data",
            Self::ColBlockHeight => "block height",
            Self::ColState => "blockchain state",
            Self::ColChunkExtra => "extra information of trunk",
            Self::ColTransactionResult => "transaction results",
            Self::ColOutgoingReceipts => "outgoing receipts",
            Self::ColIncomingReceipts => "incoming receipts",
            Self::ColPeers => "peer information",
            Self::ColEpochInfo => "epoch information",
            Self::ColBlockInfo => "block information",
            Self::ColChunks => "chunks",
            Self::ColPartialChunks => "partial chunks",
            Self::ColBlocksToCatchup => "blocks need to apply chunks",
            Self::ColStateDlInfos => "blocks downloading",
            Self::ColChallengedBlocks => "challenged blocks",
            Self::ColStateHeaders => "state headers",
            Self::ColInvalidChunks => "invalid chunks",
            Self::ColBlockExtra => "extra block information",
            Self::ColBlockPerHeight => "hash of block per height",
            Self::ColStateParts => "state parts",
            Self::ColEpochStart => "epoch start",
            Self::ColAccountAnnouncements => "account announcements",
            Self::ColNextBlockHashes => "next block hash",
            Self::ColEpochLightClientBlocks => "epoch light client block",
            Self::ColReceiptIdToShardId => "receipt id to shard id",
            Self::_ColNextBlockWithNewChunk => "next block with new chunk (deprecated)",
            Self::_ColLastBlockWithNewChunk => "last block with new chunk (deprecated)",
            Self::ColPeerComponent => "peer components",
            Self::ColComponentEdges => "component edges",
            Self::ColLastComponentNonce => "last component nonce",
            Self::ColTransactions => "transactions",
            Self::ColChunkPerHeightShard => "hash of chunk per height and shard_id",
            Self::ColStateChanges => "key value changes",
            Self::ColBlockRefCount => "refcount per block",
            Self::ColTrieChanges => "trie changes",
            Self::ColBlockMerkleTree => "block merkle tree",
            Self::ColChunkHashesByHeight => "chunk hashes indexed by height_created",
            Self::ColBlockOrdinal => "block ordinal",
            Self::ColGCCount => "gc count",
            Self::ColOutcomeIds => "outcome ids",
            Self::_ColTransactionRefCount => "refcount per transaction (deprecated)",
            Self::ColProcessedBlockHeights => "processed block heights",
            Self::ColReceipts => "receipts",
            Self::ColCachedContractCode => "cached code",
            Self::ColEpochValidatorInfo => "epoch validator info",
            Self::ColHeaderHashesByHeight => "header hashes indexed by their height",
            Self::ColStateChangesForSplitStates => {
                "state changes indexed by block hash and shard id"
            }
        };
        write!(formatter, "{}", desc)
    }
}

impl DBCol {
    pub fn is_rc(&self) -> bool {
        IS_COL_RC[*self as usize]
    }
}

// List of columns for which GC should be implemented

pub static SHOULD_COL_GC: [bool; NUM_COLS] = {
    let mut col_gc = [true; NUM_COLS];
    col_gc[DBCol::ColDbVersion as usize] = false; // DB version is unrelated to GC
    col_gc[DBCol::ColBlockMisc as usize] = false;
    // TODO #3488 remove
    col_gc[DBCol::ColBlockHeader as usize] = false; // header sync needs headers
    col_gc[DBCol::ColGCCount as usize] = false; // GC count it self isn't GCed
    col_gc[DBCol::ColBlockHeight as usize] = false; // block sync needs it + genesis should be accessible
    col_gc[DBCol::ColPeers as usize] = false; // Peers is unrelated to GC
    col_gc[DBCol::ColBlockMerkleTree as usize] = false;
    col_gc[DBCol::ColAccountAnnouncements as usize] = false;
    col_gc[DBCol::ColEpochLightClientBlocks as usize] = false;
    col_gc[DBCol::ColPeerComponent as usize] = false; // Peer related info doesn't GC
    col_gc[DBCol::ColLastComponentNonce as usize] = false;
    col_gc[DBCol::ColComponentEdges as usize] = false;
    col_gc[DBCol::ColBlockOrdinal as usize] = false;
    col_gc[DBCol::ColEpochInfo as usize] = false; // https://github.com/nearprotocol/nearcore/pull/2952
    col_gc[DBCol::ColEpochValidatorInfo as usize] = false; // https://github.com/nearprotocol/nearcore/pull/2952
    col_gc[DBCol::ColEpochStart as usize] = false; // https://github.com/nearprotocol/nearcore/pull/2952
    col_gc[DBCol::ColCachedContractCode as usize] = false;
    col_gc
};

// List of columns for which GC may not be executed even in fully operational node

pub static SKIP_COL_GC: [bool; NUM_COLS] = {
    let mut col_gc = [false; NUM_COLS];
    // A node may never restarted
    col_gc[DBCol::ColStateHeaders as usize] = true;
    // True until #2515
    col_gc[DBCol::ColStateParts as usize] = true;
    col_gc
};

// List of reference counted columns

pub static IS_COL_RC: [bool; NUM_COLS] = {
    let mut col_rc = [false; NUM_COLS];
    col_rc[DBCol::ColState as usize] = true;
    col_rc[DBCol::ColTransactions as usize] = true;
    col_rc[DBCol::ColReceipts as usize] = true;
    col_rc[DBCol::ColReceiptIdToShardId as usize] = true;
    col_rc
};

pub const HEAD_KEY: &[u8; 4] = b"HEAD";
pub const TAIL_KEY: &[u8; 4] = b"TAIL";
pub const CHUNK_TAIL_KEY: &[u8; 10] = b"CHUNK_TAIL";
pub const FORK_TAIL_KEY: &[u8; 9] = b"FORK_TAIL";
pub const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";
pub const FINAL_HEAD_KEY: &[u8; 10] = b"FINAL_HEAD";
pub const LATEST_KNOWN_KEY: &[u8; 12] = b"LATEST_KNOWN";
pub const LARGEST_TARGET_HEIGHT_KEY: &[u8; 21] = b"LARGEST_TARGET_HEIGHT";
pub const GENESIS_JSON_HASH_KEY: &[u8; 17] = b"GENESIS_JSON_HASH";
pub const GENESIS_STATE_ROOTS_KEY: &[u8; 19] = b"GENESIS_STATE_ROOTS";

// Hashtable storage

pub enum StorageOp {
    Put { col: DBCol, key: Vec<u8>, value: Vec<u8> },
    Delete { col: DBCol, key: Vec<u8> },
    UpdateRC { col: DBCol, key: Vec<u8>, value: Vec<u8>, add_rc: i64 },
}

pub struct StorageTxInner(pub Vec<StorageOp>);

impl StorageTxInner {
    pub fn new() -> Self {
        StorageTxInner(Vec::new())
    }

    pub fn put(&mut self, col: DBCol, key: Vec<u8>, value: Vec<u8>) {
        assert!(!col.is_rc());
        self.0.push(StorageOp::Put { col, key, value });
    }

    pub fn delete(&mut self, col: DBCol, key: Vec<u8>) {
        assert!(!col.is_rc());
        self.0.push(StorageOp::Delete { col, key });
    }

    pub fn update_rc(&mut self, col: DBCol, key: Vec<u8>, value: Vec<u8>, add_rc: i64) {
        assert!(col.is_rc());
        if add_rc != 0 {
            self.0.push(StorageOp::UpdateRC { col, key, value, add_rc });
        }
    }

    pub fn merge(&mut self, mut tx: Self) {
        self.0.append(&mut tx.0);
    }
}

const NUM_BUCKETS: u64 = 1 << 30;
const NUM_LOCKS: usize = 1 << 10;

pub struct StorageInner(Storage);

fn mkkey(col: DBCol, key: &[u8]) -> Vec<u8> {
    let mut res = Vec::with_capacity(key.len() + 1);
    res.push(col as u8);
    res.extend_from_slice(key);
    res
}

impl StorageInner {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(StorageInner(Storage::new(AsRef::<Path>::as_ref(&path), NUM_BUCKETS, NUM_LOCKS)?))
    }

    pub fn get(&self, col: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.0.get(&mkkey(col, key)).map(|v| {
            v.map(|mut v| {
                if col.is_rc() {
                    let len = v.len() - 8;
                    v.truncate(len);
                }
                v
            })
        })
    }

    pub fn write(&self, transaction: StorageTxInner) -> io::Result<()> {
        for op in transaction.0.into_iter() {
            match op {
                StorageOp::Put { col, key, value } => self.0.put(&mkkey(col, &key), value)?,
                StorageOp::Delete { col, key } => self.0.delete(&mkkey(col, &key))?,
                StorageOp::UpdateRC { col, key, value: mut new_value, add_rc } => {
                    let k = mkkey(col, &key);
                    let v = if let Some(mut old_value) = self.0.get(&k)? {
                        let len = old_value.len();
                        let old_rc_ref: &mut [u8; 8] =
                            (&mut old_value[len - 8..]).try_into().unwrap();
                        let old_rc = i64::from_le_bytes(*old_rc_ref);
                        let new_rc = old_rc + add_rc;
                        if new_rc == 0 {
                            self.0.delete(&k)?;
                            continue;
                        }
                        *old_rc_ref = new_rc.to_le_bytes();
                        old_value
                    } else {
                        // add_rc != 0
                        new_value.extend_from_slice(&add_rc.to_le_bytes());
                        new_value
                    };
                    self.0.put(&k, v)?
                }
            }
        }
        Ok(())
    }
}
