use std::collections::{HashMap};
use std::sync::{Arc};
// use std::sync::atomic::{AtomicU64,Ordering};

use parking_lot::{Mutex,RwLock};
use rand::{thread_rng};
use rand::seq::{IteratorRandom};
use tracing::{info};
use tokio::time;
use tokio::sync::{Notify};
use anyhow::{anyhow};

use nearcore::config::{NearConfig};
use near_primitives::network::{PeerId};
use crate::concurrency::{Ctx,Scope,RateLimit};
use crate::network2::{NodeClient,NodeClientConfig,NodeServer};

pub struct ClientManagerConfig {
    pub near : Arc<NearConfig>,
    pub known_peers : Vec<(std::net::SocketAddr,PeerId)>,
    pub conn_count : usize,
    pub handshake_timeout: time::Duration,
    pub per_conn_rate_limit : RateLimit,
}

// maintains a pool of connections of size at least n
// that is good enough: last x requests have latency at most M at y%.
// spawn new connection otherwise.
pub struct ClientManager {
    cfg : ClientManagerConfig,
    peers_queue : Mutex<Vec<(std::net::SocketAddr,PeerId)>>,
    clients: RwLock<HashMap<usize,Arc<NodeClient>>>,
    clients_notify: Notify,
    pub stats : Stats,
}

#[derive(Default,Debug)]
pub struct Stats {

}

impl ClientManager {
    pub fn new(cfg :ClientManagerConfig) -> Arc<ClientManager> {
        Arc::new(ClientManager{
            peers_queue: Mutex::new(cfg.known_peers.clone()),
            clients: RwLock::new(HashMap::new()), 
            clients_notify: Notify::new(),
            cfg,
            stats: Stats::default(),
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
        Scope::run(ctx,|_,s|async move{
            for i in 0..self_.cfg.conn_count {
                s.spawn({
                    let self_ = self_.clone();
                    let server = server.clone();
                    |ctx,_|async move{
                        loop {
                            let (addr,id) = self_.peers_queue.lock().pop().ok_or(anyhow!("list of known peers has been exhausted"))?;
                            let (client,event_loop) = NodeClient::connect(&ctx.with_timeout(self_.cfg.handshake_timeout),NodeClientConfig{
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
