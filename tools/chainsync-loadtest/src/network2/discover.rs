use std::collections::{HashSet,HashMap};
use std::sync::{Arc};

use parking_lot::{Mutex};
use tracing::{info};
use tokio::time;
use tokio::sync::{Semaphore};

use nearcore::config::{NearConfig};
use near_network_primitives::types::{PeerInfo};
use near_primitives::network::{PeerId};
use crate::concurrency::{Ctx,Scope,Spawnable,spawnable,noop,RateLimit};
use crate::network2::{NodeClient,NodeClientConfig,UnimplementedNodeServer};

struct DiscoverPeers {
    cfg:Arc<NearConfig>,
    per_peer_timeout : time::Duration,
    per_peer_calls : usize,

    started:Mutex<HashSet<std::net::SocketAddr>>,
    result:Mutex<HashMap<std::net::SocketAddr,FetchResult>>,
    sem:Semaphore,
}

struct FetchResult {
    id:PeerId,
    peers:Vec<PeerInfo>,
}

impl DiscoverPeers {
    // fetch connects to <addr> and calls fetch_peers <calls> times.
    // Returns the received peer_infos. Returns an error iff failed to connect & perform Handshake.
    async fn fetch(self:Arc<Self>, ctx:&Ctx, addr:std::net::SocketAddr) -> anyhow::Result<FetchResult> {
        Scope::run(&ctx.with_timeout(self.per_peer_timeout),|ctx,s|async move{
            let (cli,event_loop) = NodeClient::connect(&ctx,NodeClientConfig{
                near: self.cfg.clone(),
                peer_addr: addr,
                peer_id: None,
                rate_limit: RateLimit{burst:10,qps:10.},
                allow_protocol_mismatch:true,
                allow_genesis_mismatch:false,
            }).await?;
            s.spawn_weak(|ctx|event_loop(ctx,Arc::new(UnimplementedNodeServer())));
            // Try to fetch peers <calls> times. Ignore the errors inbetween.
            let mut res = FetchResult{id:cli.peer_id.clone(),peers:vec![]};
            for _ in 0..self.per_peer_calls {
                match cli.fetch_peers(&ctx).await {
                    Ok(infos) => for x in infos { res.peers.push(x) }
                    Err(err) => { info!("cli.fetch_peers(): {:#}",err); }
                }
            }
            Ok(res)
        }).await
    }

    // fetch returns a Spawnable, which connects to pi.add, asks for more peers
    // and spawns discover recursively.
    fn discover(self:Arc<Self>,pi:PeerInfo) -> Spawnable {
        let addr = if let Some(addr) = &pi.addr { addr.clone() } else { return noop() };
        {
            let mut started = self.started.lock();
            if started.contains(&addr){ return noop(); }
            started.insert(addr.clone());
        }
        spawnable(move |ctx,s| async move {
            // Acquire a permit.
            let _permit = self.sem.acquire().await;
            match self.clone().fetch(&ctx,addr).await {
                Err(err) => { info!("self.fetch(): {:#}",err); }
                Ok(res) => {
                    for pi in &res.peers {
                        s.spawn(self.clone().discover(pi.clone()));
                    }
                    self.result.lock().insert(addr,res);
                }
            }
            return Ok(());
        })
    }
}

pub type DiscoverPeersResult = HashMap<std::net::SocketAddr,PeerId>;

// discover_peers scans recursively the network and returns a set of all peers it managed to connect to. 
pub async fn discover_peers(ctx:&Ctx, cfg:Arc<NearConfig>, per_peer_timeout:time::Duration, per_peer_calls:usize, inflight_limit:usize) -> anyhow::Result<DiscoverPeersResult> {
    let d = Scope::run(ctx,|_,s|async move{
        let d = Arc::new(DiscoverPeers{
            started: Mutex::new(HashSet::new()),
            result: Mutex::new(HashMap::new()),
            per_peer_timeout,
            per_peer_calls,
            cfg,
            sem:Semaphore::new(inflight_limit),
        });
        for pi in &d.cfg.network_config.boot_nodes {
            s.spawn(d.clone().discover(pi.clone()));
        }
        Ok(d)
    }).await?;
    let mut res = HashMap::new();
    for (k,v) in d.result.lock().drain() {
        res.insert(k,v.id);
    }
    return Ok(res); 
}
