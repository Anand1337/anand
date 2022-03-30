#![allow(unused_imports)]
#![allow(unused_variables)]
use std::fmt;
use std::ops::{Deref};
use std::collections::{HashSet,HashMap};
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
use tokio::io;
use tokio::io::{AsyncReadExt,AsyncWriteExt};
use tokio::sync::{Semaphore,Mutex as AsyncMutex};
use anyhow::{anyhow,Context};

use nearcore::config::{NearConfig};
use near_network_primitives::types::{PeerInfo,PartialEdgeInfo,PeerChainInfoV2,
    PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
    AccountOrPeerIdOrHash, RawRoutedMessage, RoutedMessageBody,
};
use near_primitives::network::{PeerId};
use near_primitives::version::{PEER_MIN_ALLOWED_PROTOCOL_VERSION,PROTOCOL_VERSION};
use near_primitives::hash::CryptoHash;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use crate::peer_manager::peer::codec::{Codec};
use crate::peer_manager::types::{PeerMessage,Handshake,HandshakeFailureReason,RoutingTableUpdate};
use crate::concurrency::{Ctx,CtxWithCancel,Scope,RateLimit,RateLimiter,WeakMap,Once};

/// Maximum size of network message in encoded format.
/// We encode length as `u32`, and therefore maximum size can't be larger than `u32::MAX`.
const NETWORK_MESSAGE_MAX_SIZE_BYTES: usize = 512 * MIB as usize;

struct Stream {
    reader : AsyncMutex<io::BufReader<io::ReadHalf<net::TcpStream>>>,
    writer : AsyncMutex<io::BufWriter<io::WriteHalf<net::TcpStream>>>,
}

impl Stream {
    fn new(stream: net::TcpStream) -> Self {
        let (reader,writer) = io::split(stream);
        Self{
            reader: AsyncMutex::new(io::BufReader::new(reader)),
            writer: AsyncMutex::new(io::BufWriter::new(writer)),
        }
    }
    
    async fn read(&self, ctx: &Ctx) -> anyhow::Result<PeerMessage> {
        let mut reader = ctx.wrap(self.reader.lock()).await?;
        let n = ctx.wrap(reader.read_u32_le()).await?? as usize;
		// TODO: if this check fails, the stream is broken, because we read some bytes already.
		if n>NETWORK_MESSAGE_MAX_SIZE_BYTES { return Err(anyhow!("message size too large")); }
        let mut buf = BytesMut::new();
	    buf.resize(n,0);	
        ctx.wrap(reader.read_exact(&mut buf[..])).await??;
        Ok(PeerMessage::try_from_slice(&buf[..])?)
    }
    
    async fn write(&self, ctx: &Ctx, msg: &PeerMessage) -> anyhow::Result<()> {
        let msg = msg.try_to_vec()?;
        let mut writer = ctx.wrap(self.writer.lock()).await?; 
        // TODO: If writing fails in the middle, then the stream is broken.
        ctx.wrap(writer.write_u32_le(msg.len() as u32)).await??;
        ctx.wrap(writer.write_all(&msg[..])).await??;
        ctx.wrap(writer.flush()).await??;
        Ok(())
    } 
}

pub trait NodeServer : Sync + Send {
    fn sync_routing_table(&self,ctx:&Ctx,u:RoutingTableUpdate) -> anyhow::Result<()> {
        Err(anyhow!("unimplemented"))
    }
}

pub struct NodeClientConfig {
    pub near : Arc<NearConfig>,
    pub peer_info : PeerInfo,
    pub rate_limit : RateLimit,
    pub allow_peer_mismatch : bool,
    pub allow_protocol_mismatch : bool,
    pub allow_genesis_mismatch : bool,
}

impl NodeClientConfig {
    // Currently it is equivalent to genesis_config.num_block_producer_seats,
    // (see https://cs.github.com/near/nearcore/blob/dae9553670de13c279d3ebd55f17da13d94fa691/nearcore/src/runtime/mod.rs#L1114).
    // AFAICT eventually it will change dynamically (I guess it will be provided in the Block).
    fn parts_per_chunk(&self) -> u64 {
        self.near.genesis.config.num_block_producer_seats
    }
}

#[derive(Default)]
struct Stats {
    send_count:AtomicU64,
}

pub struct NodeClient {
    cfg : NodeClientConfig,
    my_id : PeerId,
    event_loop_ctx : Once<CtxWithCancel>,
    block_headers: Arc<WeakMap<CryptoHash, Once<Vec<BlockHeader>>>>,
    blocks: Arc<WeakMap<CryptoHash, Once<Block>>>,
    chunks: Arc<WeakMap<ChunkHash, Once<PartialEncodedChunkResponseMsg>>>,
    stream: Stream,
    rate_limiter : RateLimiter,
    stats : Stats,
}

type EventLoop = Box<dyn FnOnce(Ctx,Arc<dyn NodeServer>) -> BoxFuture<'static,anyhow::Result<()>> + Send>;

impl NodeClient {
    pub async fn connect(ctx:&Ctx, cfg:NodeClientConfig) -> anyhow::Result<(Arc<NodeClient>,EventLoop)> {
        // TCP connect.
        let peer_addr = cfg.peer_info.addr.ok_or(anyhow!("missing address"))?;
        let stream = ctx.wrap(net::TcpStream::connect(peer_addr)).await??;
        let my_id = PeerId::new(cfg.near.network_config.public_key.clone());
        let my_addr = cfg.near.network_config.addr.unwrap_or(stream.local_addr()?);

        let stream = Stream::new(stream); 
        let cli = Arc::new(NodeClient{
            my_id,
            event_loop_ctx: Once::new(),
            stream,
            block_headers: WeakMap::new(),
            blocks: WeakMap::new(),
            chunks: WeakMap::new(),
            rate_limiter: RateLimiter::new(cfg.rate_limit.clone()),
            stats: Stats::default(),
            cfg,
        });

        // Handshake
        let edge_info = PartialEdgeInfo::new(&cli.my_id, &cli.cfg.peer_info.id, 1, &cli.cfg.near.network_config.secret_key);
        let genesis_id = crate::config::genesis_id(&cli.cfg.near.client_config.chain_id);
        let mut msg = Handshake::new(
            PROTOCOL_VERSION,
            cli.my_id.clone(),
            cli.cfg.peer_info.id.clone(),
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
            info!("read next");
            let recv_msg = cli.stream.read(ctx).await.context("stream.read(Handshake)")?;
            info!("recv_msg = {:?}",recv_msg);
            match recv_msg {
                PeerMessage::Handshake(h) => {
                    if !cli.cfg.allow_peer_mismatch {
                        let (got,want) = (&h.target_peer_id,&cli.my_id); 
                        if got!=want { return Err(anyhow!("my peer id mismatch: got = {:?}, want {:?}",got,want)); }
                    }
                    if !cli.cfg.allow_genesis_mismatch {
                        let (got,want) = (&h.sender_chain_info.genesis_id,&genesis_id);
                        if got!=want { return Err(anyhow!("genesis mismatch: got = {:?}, want {:?}",got,want)); }
                    }
                    break 'handshake;
                }
                PeerMessage::HandshakeFailure(peer_info,reason) => match reason {
                    HandshakeFailureReason::ProtocolVersionMismatch{version,oldest_supported_version} => {
                        // TODO: narrow down the situations when we should agree.
                        msg.protocol_version = version;
                        msg.oldest_supported_version = oldest_supported_version;
                    },
                    HandshakeFailureReason::GenesisMismatch(peer_genesis_id) => {
                        if !cli.cfg.allow_genesis_mismatch {
                            return Err(anyhow!("genesis mismatch: got {:?} want {:?}",peer_genesis_id,genesis_id));
                        }
                        msg.sender_chain_info.genesis_id = peer_genesis_id;
                    }
                    HandshakeFailureReason::InvalidTarget => {
                        if !cli.cfg.allow_peer_mismatch {
                            return Err(anyhow!("peer_id mismatch: got {} want {}",peer_info.id,cli.cfg.peer_info.id));
                        }
                        msg.target_peer_id = peer_info.id.clone();
                        msg.partial_edge_info = PartialEdgeInfo::new(&cli.my_id, &peer_info.id, 1, &cli.cfg.near.network_config.secret_key);
                    }
                }
                unexpected_msg => {
                    info!("unexpected message during handshake, ignoring : {:?}",unexpected_msg);
                }
            }
        }
        let event_loop = Box::new({
            let cli = cli.clone();
            |ctx:Ctx,server:Arc<dyn NodeServer>| async move {
                let ctx = ctx.with_cancel();
                if let Err(_) = cli.event_loop_ctx.set(ctx.clone()) {
                    panic!("cli.event_loop_ctx.set() failed unexpectedly");
                }
                let res = async {
                    // TODO: consider graceful disconnect (informing the peer)
                    // event loop
                    // + monitoring of the peer responsiveness.
                    loop {
                        match cli.stream.read(&ctx).await? {
                            PeerMessage::Block(block) => {
                                cli.blocks.get(&block.hash().clone()).map(|once|{
                                    once.set(block)
                                });
                            }
                            PeerMessage::BlockHeaders(headers) => {
                                if let Some(h) = headers.iter().min_by_key(|h| h.height()) {
                                    let hash = h.prev_hash().clone();
                                    cli.block_headers.get(&hash).map(|once|{
                                        once.set(headers)
                                    });
                                }
                            }
                            PeerMessage::Routed(msg) => match msg.body {
                                // TODO: add stats for dropped received messages.
                                RoutedMessageBody::PartialEncodedChunkResponse(resp) => {
                                    cli.chunks.get(&resp.chunk_hash.clone()).map(|once|{
                                        once.set(resp)
                                    });
                                }
                                _ => {}
                            }
                            PeerMessage::SyncRoutingTable(u) => {
                                if let Err(err) = server.sync_routing_table(&ctx,u) {
                                    info!("serve.sync_routing_table(): {:#}",err);
                                }
                            }
                            unexpected_msg => { return Err(anyhow!("unexpected message : {:?}",unexpected_msg)); }
                        }
                    }
                }.await;
                ctx.cancel();
                return res;
            }.boxed()
        });
        return Ok((cli,event_loop));
    }
}

impl NodeClient {
    pub async fn call<T>(self:&Arc<Self>,ctx:&Ctx,msg:PeerMessage,recv:impl Future<Output=T>) -> anyhow::Result<T> {
        self.rate_limiter.allow(ctx).await?;
        self.stream.write(ctx,&msg).await?;
        self.stats.send_count.fetch_add(1,Ordering::Relaxed);
        // wait for the event loop to start. 
        let event_loop_ctx = ctx.wrap(self.event_loop_ctx.wait()).await?;
        // TODO update stats. Notify the stream to close if needed (i.e. cancel the event_loop_ctx)
        // if ctx cancelled before the NodeClient RPC timeout, it shouldn't be accounted as peer's
        // fault.
        Ok(ctx.wrap(event_loop_ctx.wrap(recv)).await??) 
    }

    pub async fn fetch_block_headers(self:&Arc<Self>,ctx:&Ctx,hash:CryptoHash) -> anyhow::Result<Vec<BlockHeader>> {
        let msg = PeerMessage::BlockHeadersRequest(vec![hash.clone()]);
        let recv = self.block_headers.get_or_insert(&hash,Once::new);
        self.call(ctx,msg,recv.wait()).await
    }

    pub async fn fetch_block(self:&Arc<Self>,ctx:&Ctx,hash:CryptoHash)  -> anyhow::Result<Block>  {
        let msg = PeerMessage::BlockRequest(hash.clone());
        let recv = self.blocks.get_or_insert(&hash,Once::new);
        self.call(ctx,msg,recv.wait()).await
    }

    pub async fn fetch_chunk(self:&Arc<Self>,ctx:&Ctx,ch:&ShardChunkHeader) -> anyhow::Result<PartialEncodedChunkResponseMsg> {
        let msg = RawRoutedMessage{
            target: AccountOrPeerIdOrHash::PeerId(self.cfg.peer_info.id.clone()),
            body: RoutedMessageBody::PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg {
                chunk_hash: ch.chunk_hash(),
                part_ords: (0..self.cfg.parts_per_chunk()).collect(),
                tracking_shards: Default::default(),
            }),
        }.sign(self.my_id.clone(),&self.cfg.near.network_config.secret_key,/*ttl=*/1);
        let msg = PeerMessage::Routed(msg);
        let recv = self.chunks.get_or_insert(&ch.chunk_hash(),Once::new);
        self.call(ctx,msg,recv.wait()).await
    }
}

pub struct NetworkConfig {
    pub near : Arc<NearConfig>,
    pub known_peers : Vec<PeerInfo>,
    pub conn_count : usize,
    pub per_conn_rate_limit : RateLimit,
}

pub struct Network {
    cfg : NetworkConfig,
    peers_queue : Mutex<Vec<PeerInfo>>,
    conn_peers : Mutex<HashMap<PeerId,Arc<NodeClient>>>,
    // Arc is required to use acquire_owned() instead of acquire().
    // And we need acquire_owned() to pass a permit to a coroutine.
    conn_peers_sem : Arc<Semaphore>,
    // Obtains a list of PeerInfos, that we managed to perform a handshake with.
    //
    // maintain a pool of connections of size at least n
    // that is good enough: last x requests have latency at most M at y%.
    // spawn new connection otherwise.
}

impl Network {
    pub fn new(cfg :NetworkConfig) -> Arc<Network> {
        Arc::new(Network{
            peers_queue: Mutex::new(cfg.known_peers.clone()),
            conn_peers: Mutex::new(HashMap::new()),
            conn_peers_sem: Arc::new(Semaphore::new(cfg.conn_count)),
            cfg,
        })
    }

    pub async fn any_peer() -> Arc<NodeClient> {
        panic!("unimplemented");
    }

    pub async fn run(self:&Arc<Self>,ctx:&Ctx, server: &Arc<dyn NodeServer>) -> anyhow::Result<()> {
        let self_ = self.clone();
        let server = server.clone();
        Scope::run(ctx,|ctx,s|async move{
            loop {
                let self_ = self_.clone();
                let permit = ctx.wrap(self_.conn_peers_sem.clone().acquire_owned()).await?;
                let pi = self_.peers_queue.lock().pop().ok_or(anyhow!("list of known peers has been exhausted"))?;
                let server = server.clone();
                s.spawn_weak(|ctx|async move{
                    let permit = permit;
                    // TODO: detect missing address earlier - either skip the peer, or fail
                    // PeerManager::new().
                    let (client,event_loop) = NodeClient::connect(&ctx,NodeClientConfig{
                        near: self_.cfg.near.clone(),
                        peer_info: pi.clone(),
                        rate_limit: self_.cfg.per_conn_rate_limit.clone(),
                        allow_peer_mismatch: false,
                        allow_protocol_mismatch: false,
                        allow_genesis_mismatch: false,
                    }).await?;
                    self_.conn_peers.lock().insert(pi.id.clone(),client);
                    let err = event_loop(ctx,server).await;
                    self_.conn_peers.lock().remove(&pi.id);
                    // TODO: ignore/log only expected errors.
                    Ok(())
                });
            }
        }).await
    }
}
