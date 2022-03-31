use std::sync::{Arc};
use std::sync::atomic::{AtomicU64,Ordering};
use std::future::Future;

use futures::future::{BoxFuture,FutureExt};
use tracing::{info};
use tokio::net;
use anyhow::{anyhow,Context};

use nearcore::config::{NearConfig};
use near_network_primitives::types::{PeerInfo,PartialEdgeInfo,PeerChainInfoV2,
    PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
    AccountOrPeerIdOrHash, RawRoutedMessage, RoutedMessageBody,
};
use near_primitives::network::{PeerId};
use near_primitives::version::{PROTOCOL_VERSION};
use near_primitives::hash::CryptoHash;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use crate::peer_manager::types::{PeerMessage,Handshake,HandshakeFailureReason,RoutingTableUpdate};
use crate::concurrency::{Ctx,CtxWithCancel,RateLimit,RateLimiter,WeakMap,Once};
use crate::network2::{Stream};

pub trait NodeServer : Sync + Send {
    fn sync_routing_table(&self,_:&Ctx,_:RoutingTableUpdate) -> anyhow::Result<()> {
        Err(anyhow!("unimplemented"))
    }
    fn fetch_peers(&self,_:&Ctx) -> anyhow::Result<Vec<PeerInfo>> {
        Err(anyhow!("unimplemented"))
    }
}

pub struct UnimplementedNodeServer();
impl NodeServer for UnimplementedNodeServer {}

pub struct NodeClientConfig {
    pub near : Arc<NearConfig>,
    pub peer_addr : std::net::SocketAddr,
    pub peer_id : Option<PeerId>, // expected peer_id
    
    pub rate_limit : RateLimit,
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
    pub my_id : PeerId,
    pub peer_id : PeerId,
    event_loop_ctx : Once<CtxWithCancel>,

    block_headers: Arc<WeakMap<CryptoHash, Once<Vec<BlockHeader>>>>,
    blocks: Arc<WeakMap<CryptoHash, Once<Block>>>,
    chunks: Arc<WeakMap<ChunkHash, Once<PartialEncodedChunkResponseMsg>>>,
    peers: Arc<WeakMap<(),Once<Vec<PeerInfo>>>>,

    stream: Stream,
    rate_limiter : RateLimiter,
    stats : Stats,
}

type EventLoop = Box<dyn FnOnce(Ctx,Arc<dyn NodeServer>) -> BoxFuture<'static,anyhow::Result<()>> + Send>;

impl NodeClient {
    pub async fn connect(ctx:&Ctx, cfg:NodeClientConfig) -> anyhow::Result<(Arc<NodeClient>,EventLoop)> {
        // TCP connect.
        let stream = ctx.wrap(net::TcpStream::connect(cfg.peer_addr)).await??;
        let my_id = PeerId::new(cfg.near.network_config.public_key.clone());
        let my_addr = cfg.near.network_config.addr.unwrap_or(stream.local_addr()?);
        let stream = Stream::new(stream); 

        // Handshake
        let fake_peer_id = ||PeerId::new(near_crypto::InMemorySigner::from_random("node".parse().unwrap(), near_crypto::KeyType::ED25519).public_key);
        let peer_id = cfg.peer_id.clone().unwrap_or_else(fake_peer_id);

        let edge_info = PartialEdgeInfo::new(&my_id, &peer_id, 1, &cfg.near.network_config.secret_key);
        let genesis_id = crate::config::genesis_id(&cfg.near.client_config.chain_id);
        let mut msg = Handshake::new(
            PROTOCOL_VERSION,
            my_id.clone(),
            peer_id,
            Some(my_addr.port()), // required 
            PeerChainInfoV2 {
                genesis_id: genesis_id.clone(), 
                height: 0,
                tracked_shards: Default::default(),
                archival: false,
            },
            edge_info,
        );
        let mut rt_updates = vec![];
        let peer_id = async { loop {
            stream.write(ctx,&PeerMessage::Handshake(msg.clone())).await.context("stream.write(Handshake)")?;
            let recv_msg = stream.read(ctx).await.context("stream.read(Handshake)")?;
            match recv_msg {
                PeerMessage::Handshake(h) => {
                    let (got,want) = (&h.target_peer_id,&my_id); 
                    if got!=want { return Err(anyhow!("my peer id mismatch: got = {:?}, want {:?}",got,want)); }
                    if let Some(want) = cfg.peer_id.as_ref() {
                        let got = &h.sender_peer_id;
                        if got!=want { return Err(anyhow!("peer id mismatch: got = {:?}, want {:?}",got,want)); }
                    }
                    if !cfg.allow_genesis_mismatch {
                        let (got,want) = (&h.sender_chain_info.genesis_id,&genesis_id);
                        if got!=want { return Err(anyhow!("genesis mismatch: got = {:?}, want {:?}",got,want)); }
                    }
                    return Ok(h.sender_peer_id);
                }
                PeerMessage::HandshakeFailure(peer_info,reason) => match reason {
                    HandshakeFailureReason::ProtocolVersionMismatch{version,oldest_supported_version} => {
                        // TODO: narrow down the situations when we should agree.
                        msg.protocol_version = version;
                        msg.oldest_supported_version = oldest_supported_version;
                    },
                    HandshakeFailureReason::GenesisMismatch(peer_genesis_id) => {
                        if !cfg.allow_genesis_mismatch {
                            return Err(anyhow!("genesis mismatch: got {:?} want {:?}",peer_genesis_id,genesis_id));
                        }
                        msg.sender_chain_info.genesis_id = peer_genesis_id;
                    }
                    HandshakeFailureReason::InvalidTarget => {
                        if let Some(want) = cfg.peer_id.as_ref() {
                            return Err(anyhow!("peer_id mismatch: got {} want {}",peer_info.id,want));
                        }
                        msg.target_peer_id = peer_info.id.clone();
                        msg.partial_edge_info = PartialEdgeInfo::new(&my_id, &peer_info.id, 1, &cfg.near.network_config.secret_key);
                    }
                }
                // TODO: do not resend handshake upon unexpected messages
                PeerMessage::SyncRoutingTable(u) => {
                    rt_updates.push(u)
                }
                unexpected_msg => {
                    info!("unexpected message during handshake, ignoring : {:?}",unexpected_msg);
                }
            }
        }}.await?;
        let cli = Arc::new(NodeClient{
            my_id,
            peer_id,
            event_loop_ctx: Once::new(),
            stream,
            block_headers: WeakMap::new(),
            blocks: WeakMap::new(),
            chunks: WeakMap::new(),
            peers: WeakMap::new(),
            rate_limiter: RateLimiter::new(cfg.rate_limit.clone()),
            stats: Stats::default(),
            cfg,
        });
        let event_loop = Box::new({
            let cli = cli.clone();
            |ctx:Ctx,server:Arc<dyn NodeServer>| async move {
                let ctx = ctx.with_cancel();
                if let Err(_) = cli.event_loop_ctx.set(ctx.clone()) {
                    panic!("cli.event_loop_ctx.set() failed unexpectedly");
                }
                let res = async {
                    for u in rt_updates {
                        if let Err(err) = server.sync_routing_table(&ctx,u) {
                            info!("serve.sync_routing_table(): {:#}",err);
                        }
                    }
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
                            PeerMessage::PeersResponse(infos) => {
                                cli.peers.get(&()).map(|once|once.set(infos));
                            }
                            // TODO: the requests should be handled asynchronously
                            // TODO: implement throttling (too many inflight/too many qps)
                            PeerMessage::PeersRequest{} => {
                                match server.fetch_peers(&ctx) {
                                    Ok(infos) => { cli.stream.write(&ctx,&PeerMessage::PeersResponse(infos)).await?; }
                                    Err(err) => { info!("server.fetch_peers(): {:#}",err); }
                                }
                            }
                            PeerMessage::SyncRoutingTable(u) => {
                                if let Err(err) = server.sync_routing_table(&ctx,u) {
                                    info!("server.sync_routing_table(): {:#}",err);
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

    pub async fn fetch_block(self:&Arc<Self>,ctx:&Ctx,hash:CryptoHash)  -> anyhow::Result<Block> {
        let msg = PeerMessage::BlockRequest(hash.clone());
        let recv = self.blocks.get_or_insert(&hash,Once::new);
        self.call(ctx,msg,recv.wait()).await
    }

    pub async fn fetch_chunk(self:&Arc<Self>,ctx:&Ctx,ch:&ShardChunkHeader) -> anyhow::Result<PartialEncodedChunkResponseMsg> {
        let msg = RawRoutedMessage{
            target: AccountOrPeerIdOrHash::PeerId(self.peer_id.clone()),
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

    pub async fn fetch_peers(self:&Arc<Self>,ctx:&Ctx) -> anyhow::Result<Vec<PeerInfo>> {
        let msg = PeerMessage::PeersRequest{};
        let recv = self.peers.get_or_insert(&(),Once::new);
        self.call(ctx,msg,recv.wait()).await
    }
}
