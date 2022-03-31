#![allow(unused_imports)]
#![allow(unused_variables)]
use std::fmt;
use std::ops::{Deref};
use std::collections::{HashSet,HashMap};
use std::pin::{Pin};
use std::sync::{Arc};
use std::sync::atomic::{AtomicU64,Ordering};
use std::future::Future;

use parking_lot::{Mutex,RwLock};
use futures::future::{BoxFuture,FutureExt};
use bytes::{BytesMut,BufMut};
use bytesize::{GIB, MIB};
use borsh::{BorshDeserialize, BorshSerialize};
use rand::{Rng,thread_rng};
use rand::seq::{IteratorRandom};
use tokio_util::codec::{Decoder,Encoder};
use tracing::metadata;
use tracing::{info};
use clap::{Clap};
use tokio::time;
use tokio::net;
use tokio::io;
use tokio::io::{AsyncReadExt,AsyncWriteExt};
use tokio::sync::{Semaphore,Mutex as AsyncMutex,Notify};
use tokio::sync::watch;
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
use crate::concurrency::{Ctx,CtxWithCancel,Scope,Spawnable,spawnable,noop,RateLimit,RateLimiter,WeakMap,Once};
use crate::network2::{NodeClient,NodeClientConfig,NodeServer};

// TODO: use the network protocol knowledge from here to reimplement the loadtest without using the
// PeerManager at all: do a discovery and connect directly to peers. If discovery is too expensive
// to run every time, dump the peer_info: hash@addr for each relevant peer and then reuse it at
// startup.

pub struct ClientManagerConfig {
    pub near : Arc<NearConfig>,
    pub known_peers : Vec<(std::net::SocketAddr,PeerId)>,
    pub conn_count : usize,
    pub per_conn_rate_limit : RateLimit,
}

pub struct ClientManager {
    cfg : ClientManagerConfig,
    peers_queue : Mutex<Vec<(std::net::SocketAddr,PeerId)>>,
    clients: RwLock<HashMap<usize,Arc<NodeClient>>>,
    clients_notify: Notify,
    // Obtains a list of PeerInfos, that we managed to perform a handshake with.
    //
    // maintain a pool of connections of size at least n
    // that is good enough: last x requests have latency at most M at y%.
    // spawn new connection otherwise.
}

impl ClientManager {
    pub fn new(cfg :ClientManagerConfig) -> Arc<ClientManager> {
        Arc::new(ClientManager{
            peers_queue: Mutex::new(cfg.known_peers.clone()),
            clients: RwLock::new(HashMap::new()), 
            clients_notify: Notify::new(),
            cfg,
        })
    }

    pub async fn any(&self,ctx:&Ctx) -> anyhow::Result<Arc<NodeClient>> {
        loop {
            if let Some((_,v)) = self.clients.read().iter().choose(&mut thread_rng()) {
                return Ok(v.clone());
            }
            ctx.wrap(self.clients_notify.notified()).await?;
        }
    }

    pub async fn run(self:&Arc<Self>,ctx:&Ctx, server: &Arc<dyn NodeServer>) -> anyhow::Result<()> {
        let self_ = self.clone();
        let server = server.clone();
        Scope::run(ctx,|ctx,s|async move{
            for i in 0..self_.cfg.conn_count {
                s.spawn({
                    let self_ = self_.clone();
                    let server = server.clone();
                    |ctx,s|async move{
                        loop {
                            let (addr,id) = self_.peers_queue.lock().pop().ok_or(anyhow!("list of known peers has been exhausted"))?;
                            let (client,event_loop) = NodeClient::connect(&ctx,NodeClientConfig{
                                near: self_.cfg.near.clone(),
                                peer_addr: addr,
                                peer_id: Some(id.clone()),
                                rate_limit: self_.cfg.per_conn_rate_limit.clone(),
                                allow_protocol_mismatch: false,
                                allow_genesis_mismatch: false,
                            }).await?;
                            self_.clients.write().insert(i,client.clone());
                            if let Err(err) = event_loop(ctx.clone(),server.clone()).await {
                                // TODO: ignore/log only expected errors.
                                info!("event_loop(): {:#}",err);
                            }
                            self_.clients.write().remove(&i);
                        }
                    }
                });
            }
            Ok(())
        }).await
    }
}
