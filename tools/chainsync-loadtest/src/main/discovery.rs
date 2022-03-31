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

use near_crypto::{PublicKey};
use nearcore::config::{NearConfig};
use near_network_primitives::types::{EdgeState,PeerInfo,PartialEdgeInfo,PeerChainInfoV2};
use near_primitives::types::{AccountId};
use near_primitives::network::{PeerId};
use near_primitives::version::{PEER_MIN_ALLOWED_PROTOCOL_VERSION,PROTOCOL_VERSION};
use crate::peer_manager::peer::codec::{Codec};
use crate::peer_manager::types::{PeerMessage,Handshake,HandshakeFailureReason,RoutingTableUpdate};
use crate::concurrency::{Ctx,Scope,RateLimit,Once,noop,Spawnable,spawnable};
use crate::network2 as network;
use near_jsonrpc_primitives::types::network_info::{RpcNetworkInfoResponse};
use near_jsonrpc_primitives::types::validator::RpcValidatorResponse;
use near_jsonrpc_primitives::message;

fn init_logging() {
    let env_filter = tracing_subscriber::EnvFilter::from_default_env().add_directive(metadata::LevelFilter::INFO.into());
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(io::stderr)
        .init();
}


#[derive(Copy,Clone,Debug,Eq,PartialEq,Hash)]
enum HandshakeFailure {
    Genesis{local:bool},
    Target{local:bool},
}

#[derive(Default)]
struct AskForPeersResult {
    expected_info:Option<PeerInfo>,
    actual_info:Option<PeerInfo>,
    connect:bool,
    handshake:bool,
    handshake_failure:Option<HandshakeFailure>,
    peers_responses:usize,
    peers: Vec<PeerInfo>,
    routing_table : Option<RoutingTableUpdate>,
    err : Option<anyhow::Error>,
    
    network_info: Option<RpcNetworkInfoResponse>,
}

struct Server {
    routing_table : Once<RoutingTableUpdate>,
}

impl network::NodeServer for Server {
    fn sync_routing_table(&self,ctx:&Ctx,update:RoutingTableUpdate) -> anyhow::Result<()> {
        if update.accounts.len()>0 { 
            info!("SyncRoutingTable = (edges={},accounts={})",update.edges.len(),update.accounts.len());
            // Ignore subsequent setting attempts.
            let _ = self.routing_table.set(update);
        }
        Ok(())
    }
}

async fn ask_for_peers(ctx:&Ctx, cfg :Arc<NearConfig>, peer_info:PeerInfo, peers_responses:usize) -> AskForPeersResult {
    let res = Arc::new(Mutex::new(AskForPeersResult::default()));
    let err = Scope::run(ctx,{
        let res = res.clone();
        |ctx,s|async move{
            res.lock().expected_info = Some(peer_info.clone());
            let addr = peer_info.addr.expect("missing addr");
            let (cli,event_loop) = network::NodeClient::connect(&ctx,network::NodeClientConfig{
                near: cfg.clone(),
                peer_addr: addr,
                peer_id: None,
                rate_limit: RateLimit{burst:10,qps:10.},
                allow_protocol_mismatch:true,
                allow_genesis_mismatch:false,
            }).await?;
            // TODO: distinguish between these by augmenting the error.
            res.lock().connect = true;
            res.lock().handshake = true;
            let server = Arc::new(Server{routing_table: Once::new()});
            s.spawn_weak(|ctx|event_loop(ctx,server.clone()));
        
            while res.lock().peers_responses<peers_responses{
                let infos = cli.fetch_peers(&ctx).await?;
                res.lock().peers_responses += 1;
                for info in infos { res.lock().peers.push(info); }
            }
            res.lock().routing_table = Some(ctx.wrap(server.routing_table.wait()).await?);
            Ok(())
        }
    }).await;
    if let Err(err) = err {
        res.lock().err = Some(err);
    }
    return std::mem::take(&mut *res.lock());
}

#[derive(Clap, Debug)]
struct Cmd {
    #[clap(long)]
    pub chain_id: String,
}

#[derive(Default,Debug)]
struct Stats {
    scan_calls:AtomicU64,
    ask_for_peers_calls:AtomicU64,
}

struct Scanner {
    per_peer_timeout:time::Duration,
    per_peer_requests:usize,
    cfg:Arc<NearConfig>,

    started:Mutex<HashSet<std::net::SocketAddr>>,
    result:Mutex<HashMap<std::net::SocketAddr,AskForPeersResult>>,
    sem:Semaphore,
    stats:Stats,
}

impl Scanner {
    fn new(cfg:Arc<NearConfig>, per_peer_timeout:time::Duration, per_peer_requests:usize, inflight_limit:usize) -> Arc<Self> {
        Arc::new(Self{
            started: Mutex::new(HashSet::new()),
            result: Mutex::new(HashMap::new()),
            per_peer_timeout,
            per_peer_requests,
            cfg,
            sem:Semaphore::new(inflight_limit),
            stats:Stats::default(),
        })
    }

    fn scan(self:Arc<Scanner>,info:PeerInfo) -> Spawnable {
        self.stats.scan_calls.fetch_add(1,Ordering::Relaxed);
        let addr = if let Some(addr) = &info.addr { addr.clone() } else { return noop() };
        // Check if this peer has been already processed.
        {
            let mut started = self.started.lock();
            if started.contains(&addr){ return noop(); }
            started.insert(addr);
        }
        spawnable(move |ctx,s| async move {
            // Acquire a permit.
            let permit = self.sem.acquire().await;
            self.stats.ask_for_peers_calls.fetch_add(1,Ordering::Relaxed);
            let mut result = ask_for_peers(&ctx.with_timeout(self.per_peer_timeout),self.cfg.clone(),info.clone(),self.per_peer_requests).await;
            match crate::diagnostics::fetch_network_info(
                &ctx.with_timeout(time::Duration::from_secs(2)),
                std::net::SocketAddr::new(addr.ip(),3030),
            ).await {
                Ok(ni) => {
                    // info!("accounts.len() = {}",ni.known_producers.len());
                    result.network_info = Some(ni);
                }
                Err(err) => {} //{ info!("err = {:#}",err); }
            }
            for info in &result.peers {
                s.spawn(self.clone().scan(info.clone()));
            }
            self.result.lock().insert(addr,result);
            return Ok(());
        })
    }
}

pub fn main() -> anyhow::Result<()> {
    init_logging();
    let cmd = Cmd::parse();
    let cfg = Arc::new(crate::config::download(&cmd.chain_id)?);
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move{
        let vi = crate::diagnostics::fetch_validators(hyper::Uri::from_static("http://rpc.mainnet.near.org")).await?;
        let mut validators : HashMap<AccountId,PublicKey> = HashMap::new();
        for v in &vi.validator_info.current_validators {
            validators.insert(v.account_id.clone(),v.public_key.clone());
        }
        let scanner = Scanner::new(cfg.clone(),time::Duration::from_secs(10),5,1000);
        Scope::run(&Ctx::background(),{
            let scanner = scanner.clone();
            |ctx,s|async move{
                s.spawn_weak({
                    let scanner = scanner.clone();
                    |ctx|async move{
                        loop {
                            info!("stats = {:?}",scanner.stats);
                            ctx.wait(time::Duration::from_secs(2)).await?;
                        }
                    }
                });
                
                for info in &scanner.cfg.network_config.boot_nodes {
                    let info = info.clone();
                    let scanner = scanner.clone();
                    s.spawn(scanner.scan(info.clone()));
                }
                Ok(())
            }
        }).await?;
        let mut found = HashSet::new();
        let mut addrs = HashSet::new();
        let mut network_info_count = 0;
        let mut routing_tables_count = 0;
        let mut accounts = HashMap::new();
        let mut addr_count = 0;
        let mut peer_id_mismatch_count = 0;
        let mut connected_count = 0;
        let mut handshake_count = 0;
        let mut handshakes = HashSet::new();
        let mut handshake_failures = HashMap::<_,usize>::new();
        let mut fetch_completed_count = 0;
        let mut graph : Option<RoutingTableUpdate> = None;
        for (_,v) in &*scanner.result.lock() {
            addr_count += 1;
            if v.network_info.is_some() { network_info_count += 1; }
            if v.connect { connected_count += 1; }
            if v.handshake {
                handshake_count += 1;
                handshakes.insert(v.actual_info.as_ref().or(v.expected_info.as_ref()).unwrap().id.clone());
            }
            if let Some(rt) = &v.routing_table {
                routing_tables_count += 1;
                for a in &rt.accounts {
                    if let Some(public_key) = validators.get(&a.account_id) {
                        if !a.signature.verify(a.hash().as_ref(),public_key) {
                            info!("signature mismatch for {}",a.account_id);
                            continue;
                        }
                        //info!("signature OK for {}",a.account_id);
                        accounts.insert(a.account_id.clone(),a.peer_id.clone());
                    }
                }
                if graph.as_ref().map(|g|g.edges.len()<rt.edges.len()).unwrap_or(true) {
                    graph = Some(rt.clone());    
                }
            }
            if v.actual_info.is_some() {
                peer_id_mismatch_count += 1;
            }
            if let Some(reason) = v.handshake_failure {
                *handshake_failures.entry(reason).or_default() += 1;
            }
            if v.peers_responses>=scanner.per_peer_requests { fetch_completed_count += 1; }
            for info in &v.peers {
                found.insert(info.id.clone());
                if info.addr.is_some() { addrs.insert(info.id.clone()); }
            }
        }
        let graph = crate::graph::Graph::new(&graph.ok_or(anyhow!("no peer provided a graph"))?);
        let (dist,unknown_nodes) = graph.dist_from(&handshakes);
        let mut dist_hist : HashMap<usize,usize> = HashMap::new();
        for (_,v) in &dist { *dist_hist.entry(*v).or_default() += 1; }

        for (account_id,_) in &validators {
            let peer_id = accounts.get(account_id);
            info!("validator = {}, peer = {:?}, found = {}, addr = {}, handshake = {}, dist = {:?}",
                  account_id,
                  peer_id,
                  peer_id.map(|i|found.contains(&i)).unwrap_or(false),
                  peer_id.map(|i|addrs.contains(&i)).unwrap_or(false),
                  peer_id.map(|i|handshakes.contains(&i)).unwrap_or(false),
                  peer_id.map(|i|dist.get(i)).flatten(),
            );
        }
        info!("network_info_count = {}",network_info_count);
        info!("routing_tables_count = {}",routing_tables_count);
        info!("fetch_completed_count = {}",fetch_completed_count);
        info!("handshake_count = {}",handshake_count);
        info!("handshake_failures = {:?}",handshake_failures);
        info!("connected_count = {}",connected_count);
        info!("addr_count = {}",addr_count);
        info!("handshakes_not_in_the_graph = {}",unknown_nodes);
        info!("graph_node_count = {}",graph.node_count());
        info!("graph_isolated_node_count = {}",graph.isolated_node_count());
        // TODO: Next step will be to send routing requests to them (but that requires finalizing network2).
        info!("graph_reachable_count = {}",dist.len());
        for (k,v) in &dist_hist {
            info!("graph_reachable[{}] = {}",k,v);
        }
        info!("found_count = {}",found.len());
        info!("peer_id_mismatch_count = {}",peer_id_mismatch_count);
        //for (_,v) in &*scanner.started.lock() { println!("{}",v.ip()); }
        //for id in &found { println!("{}",id); }
        //for id in &accounts { println!("{}",id); }
        Ok(())
    })
}
