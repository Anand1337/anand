#![allow(unused_imports)]
#![allow(unused_variables)]
use std::collections::{HashSet,HashMap};
use std::io;
use std::pin::{Pin};
use std::sync::{Arc};
use std::sync::atomic::{AtomicU64,Ordering};
use std::future::Future;

use parking_lot::{Mutex};
use futures::future::{BoxFuture,FutureExt};
use bytes::{BytesMut,BufMut};
use bytesize::{GIB, MIB};
use borsh::{BorshDeserialize, BorshSerialize};
use tokio_util::codec::{Decoder,Encoder};
use tracing::metadata;
use tracing::{info};
use clap::{Clap};
use tokio::time;
use tokio::net;
use tokio::sync::{Semaphore};
use tokio::io::{AsyncReadExt,AsyncWriteExt,AsyncRead,AsyncWrite,BufStream};
use anyhow::{anyhow,Context};

use nearcore::config::{NearConfig};
use near_network_primitives::types::{PeerInfo,PartialEdgeInfo,PeerChainInfoV2,
    PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
};
use near_primitives::network::{PeerId};
use near_primitives::version::{PEER_MIN_ALLOWED_PROTOCOL_VERSION,PROTOCOL_VERSION};
use near_primitives::hash::CryptoHash;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use crate::peer_manager::peer::codec::{Codec};
use crate::peer_manager::types::{PeerMessage,Handshake,HandshakeFailureReason,RoutingTableUpdate};
use crate::concurrency::{Ctx,Scope,RateLimit,RateLimiter,WeakMap,Once};

/// Maximum size of network message in encoded format.
/// We encode length as `u32`, and therefore maximum size can't be larger than `u32::MAX`.
const NETWORK_MESSAGE_MAX_SIZE_BYTES: usize = 512 * MIB as usize;

// TODO: you have to split the stream into (read,write)
// and wrap write into an AsyncMutex
struct Stream<RW>(BufStream<RW>);

impl<RW:AsyncRead+AsyncWrite+Unpin> Stream<RW> {
    fn new(rw:RW) -> Self { Self(BufStream::new(rw)) }
    
    async fn read(&mut self, ctx: &Ctx) -> anyhow::Result<PeerMessage> {
        let n = ctx.wrap(self.0.read_u32_le()).await?? as usize;
		// TODO: if this check fails, the stream is broken, because we read some bytes already.
		if n>NETWORK_MESSAGE_MAX_SIZE_BYTES { return Err(anyhow!("message size too large")); }
        let mut buf = BytesMut::new();
	    buf.resize(n,0);	
        ctx.wrap(self.0.read_exact(&mut buf[..])).await??;
        Ok(PeerMessage::try_from_slice(&buf[..])?)
    }
    
    async fn write(&mut self, ctx: &Ctx, msg: &PeerMessage) -> anyhow::Result<()> {
        let msg = msg.try_to_vec()?;
        // TODO: If writing fails in the middle, then the stream is broken.
        ctx.wrap(self.0.write_u32_le(msg.len() as u32)).await??;
        ctx.wrap(self.0.write_all(&msg[..])).await??;
        ctx.wrap(self.0.flush()).await??;
        Ok(())
    } 
}

trait NodeServer {
    // Here is the interface that a node should expose to the network.
}

struct NodeClientConfig {
    near : NearConfig,
    peer_info : PeerInfo,
    rate_limit : RateLimit,
}

struct NodeClient {
    block_headers: Arc<WeakMap<CryptoHash, Once<anyhow::Result<Vec<BlockHeader>>>>>,
    blocks: Arc<WeakMap<CryptoHash, Once<anyhow::Result<Block>>>>,
    chunks: Arc<WeakMap<ChunkHash, Once<anyhow::Result<PartialEncodedChunkResponseMsg>>>>,
    stream: Stream<net::TcpStream>,
    rate_limiter : RateLimiter,
}

type EventLoop = Box<dyn FnOnce(Ctx,Arc<dyn NodeServer>) -> BoxFuture<'static,anyhow::Result<()>>>;

impl NodeClient {
    async fn connect(ctx:&Ctx, cfg:NodeClientConfig) -> anyhow::Result<(Arc<NodeClient>,EventLoop)> {
        // TCP connect.
        let peer_addr = cfg.peer_info.addr.ok_or(anyhow!("missing address"))?;
        let stream = ctx::wrap(net::TcpStream::connect(peer_addr)).await?;
        let my_addr = cfg.near.network_config.addr.unwrap_or(stream.local_addr()?);

        let stream = Stream::new(stream); 
        let cli = Arc::new(NodeClient{
            stream,
            block_headers: WeakMap::new(),
            blocks: WeakMap::new(),
            chunks: WeakMap::new(),
            rate_limiter: RateLimiter::new(cfg.rate_limit),
        });

        // Handshake
        let my_id = PeerId::new(cfg.near.network_config.public_key.clone());
        let edge_info = PartialEdgeInfo::new(&my_id, &cfg.peer_info.id, 1, &cfg.near.network_config.secret_key);
        let genesis_id = crate::config::genesis_id(&cfg.near.client_config.chain_id);
        let mut msg = Handshake::new(
            PROTOCOL_VERSION,
            my_id.clone(),
            cfg.peer_info.id.clone(),
            Some(my_addr.port()), // required 
            PeerChainInfoV2 {
                genesis_id: genesis_id.clone(), 
                height: 0,
                tracked_shards: Default::default(),
                archival: false,
            },
            edge_info,
        );
        'handshake: loop {
            cli.stream.write(ctx,&PeerMessage::Handshake(msg.clone())).await.context("stream.write(Handshake)")?;
            match cli.stream.read(ctx).await.context("stream.read(Handshake)")? {
                PeerMessage::Handshake(h) => {
                    let (got,want) = (&h.target_peer_id,&my_id); 
                    if got!=want { return Err(anyhow!("my peer id mismatch: got = {:?}, want {:?}",got,want)); }
                    let (got,want) = (&h.sender_chain_info.genesis_id,&genesis_id);
                    if got!=want { return Err(anyhow!("genesis mismatch: got = {:?}, want {:?}",got,want)); }
                    break 'handshake;
                }
                PeerMessage::HandshakeFailure(peer_info,reason) => match reason {
                    HandshakeFailureReason::ProtocolVersionMismatch{version,oldest_supported_version} => {
                        // TODO: narrow down the situations when we should agree.
                        msg.protocol_version = version;
                        msg.oldest_supported_version = oldest_supported_version;
                    },
                    // TODO: improve the content of the errors:
                    HandshakeFailureReason::GenesisMismatch(_) => { return Err(anyhow!("genesis mismatch")); }
                    HandshakeFailureReason::InvalidTarget => { return Err(anyhow!("unexpected peer id")); }
                }
                unexpected_msg => { return Err(anyhow!("unexpected message : {:?}",unexpected_msg)); }
            }
        }
        let event_loop = Box::new(|ctx,server| async move {
            // TODO: consider graceful disconnect (informing the peer)
            // event loop
            // + monitoring of the peer responsiveness.
            loop {
                match cli.stream.read(&ctx).await? {
                    unexpected_msg => { return Err(anyhow!("unexpected message : {:?}",unexpected_msg)); }
                }
            }
        }.boxed());
        return Ok((cli,event_loop));
    }
}

impl NodeClient {
    // TODO: these methods have to make sure that they are not waiting on a closed connection.

    async fn fetch_block_headers(self:&Arc<Self>,ctx:&Ctx,hash:CryptoHash) -> anyhow::Result<Vec<BlockHeader>> {
        panic!("unimplemented");
        /*NetworkRequests::BlockHeadersRequest {
            hashes: vec![hash.clone()],
            peer_id: self.peer_info.id.clone(),
        }*/
    }
    async fn fetch_block(self:&Arc<Self>,ctx:&Ctx,hash:CryptoHash)  -> anyhow::Result<Block>  {
        panic!("unimplemented");
        /*NetworkRequests::BlockRequest {
            hash: hash.clone(),
            peer_id: peer.peer_info.id.clone(),
        }*/
    }
    async fn fetch_chunk(self:&Arc<Self>,ctx:&Ctx,ch:&ShardChunkHeader) -> anyhow::Result<PartialEncodedChunkResponseMsg> {
        panic!("unimplemented");
        /*NetworkRequests::PartialEncodedChunkRequest {
                            target: peer.peer_info.id.clone(),
                            request: PartialEncodedChunkRequestMsg {
                                chunk_hash: ch.chunk_hash(),
                                part_ords: (0..ppc).collect(),
                                tracking_shards: Default::default(),
                            },
                        }
        */
    }
}

pub struct NetworkConfig {
    pub near : NearConfig,
    pub known_peers : Vec<PeerInfo>,
    pub conn_count : usize,
    pub per_conn_rate_limit : RateLimit,
}

pub struct Network {
    cfg : NetworkConfig,
    peers_queue : Mutex<Vec<PeerInfo>>,
    conn_peers : Mutex<HashMap<PeerId,Arc<NodeClient>>>,
    conn_peers_sem : Semaphore,
    // Obtains a list of PeerInfos, that we managed to perform a handshake with.
    //
    // maintain a pool of connections of size at least n
    // that is good enough: last x requests have latency at most M at y%.
    // spawn new connection otherwise.
}

impl Network {
    fn new(cfg :NetworkConfig) -> Network {
        Network{
            peers_queue: Mutex::new(cfg.known_peers.clone()),
            conn_peers: Mutex::new(HashMap::new()),
            conn_peers_sem: Semaphore::new(cfg.conn_count),
            cfg,
        }
    }

    async fn any_peer() -> Arc<NodeClient> {
        panic!("unimplemented");
    }

    async fn run(&self,ctx:&Ctx, server: Arc<dyn NodeServer>) -> anyhow::Result<()> {
        Scope::run(ctx,|ctx,s|async move{
            loop {
                let permit = ctx.wrap(self.conn_peers_sem.acquire()).await?;
                let pi = self.peers_queue.lock().pop().ok_or(anyhow!("list of known peers has been exhausted"))?;
                s.spawn_weak(||async move{
                    let permit = permit;
                    // TODO: detect missing address earlier - either skip the peer, or fail
                    // PeerManager::new().
                    let (client,event_loop) = NodeClient::connect(ctx,NodeClientConfig{
                        near: self.cfg.near,
                        peer_info: pi,
                        rate_limit: self.cfg.per_conn_rate_limit,
                    }).await?;
                    self.conn_peers.lock().insert(pi.id.clone(),client);
                    let err = event_loop(ctx,server).await;
                    self.conn_peers.lock().erase(&pi.id);
                    // TODO: ignore/log only expected errors.
                    Ok(())
                });
            }
        })
    }
}
