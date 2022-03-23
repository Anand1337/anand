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
use crate::concurrency::{Ctx,Scope};
use near_jsonrpc_primitives::types::network_info::{RpcNetworkInfoResponse};
use near_jsonrpc_primitives::types::validator::RpcValidatorResponse;
use near_jsonrpc_primitives::message;


/// Maximum size of network message in encoded format.
/// We encode length as `u32`, and therefore maximum size can't be larger than `u32::MAX`.
const NETWORK_MESSAGE_MAX_SIZE_BYTES: usize = 512 * MIB as usize;

fn init_logging() {
    let env_filter = tracing_subscriber::EnvFilter::from_default_env().add_directive(metadata::LevelFilter::INFO.into());
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(io::stderr)
        .init();
}

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

// TODO: use the network protocol knowledge from here to reimplement the loadtest without using the
// PeerManager at all: do a discovery and connect directly to peers. If discovery is too expensive
// to run every time, dump the peer_info: hash@addr for each relevant peer and then reuse it at
// startup.

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

async fn ask_for_peers(ctx:&Ctx, result:&mut AskForPeersResult, cfg :&NearConfig, peer_info:&PeerInfo, peers_responses:usize) -> anyhow::Result<()> {
    // The `connect` may take several minutes. This happens when the
    // `SYN` packet for establishing a TCP connection gets silently
    // dropped, in which case the default TCP timeout is applied. That's
    // too long for us, so we shorten it to one second.
    //
    // Why exactly a second? It was hard-coded in a library we used
    // before, so we keep it to preserve behavior. Removing the timeout
    // completely was observed to break stuff for real on the testnet.
    result.expected_info = Some(peer_info.clone());
    let addr = peer_info.addr.expect("missing addr");
    let stream = ctx.wrap(net::TcpStream::connect(addr)).await.context("connect()")??;
    result.connect = true;
    
    let my_peer_id = PeerId::new(cfg.network_config.public_key.clone());
    let my_addr = cfg.network_config.addr.unwrap_or(stream.local_addr()?);
    let edge_info = PartialEdgeInfo::new(&my_peer_id, &peer_info.id, 1, &cfg.network_config.secret_key);
    
	let genesis_id = crate::config::genesis_id(&cfg.client_config.chain_id); 
    let mut msg = Handshake::new(
        PROTOCOL_VERSION, // TODO: check when exactly the handshake fails on protocol mismatch
        my_peer_id.clone(),
        peer_info.id.clone(),
        Some(my_addr.port()), // required 
        PeerChainInfoV2 {
			genesis_id: genesis_id.clone(), 
			height: 0,
            tracked_shards: Default::default(),
            archival: false,
        },
        edge_info,
    );

    let mut stream = Stream::new(stream);
    stream.write(ctx,&PeerMessage::Handshake(msg.clone())).await.context("stream.write()")?;
    while result.peers_responses<peers_responses || result.routing_table.is_none() {
        match stream.read(ctx).await.context("stream.read()")? {
            PeerMessage::Handshake(h) => {
                let (got,want) = (&h.target_peer_id,&my_peer_id); 
                if got!=want {
                    result.handshake_failure = Some(HandshakeFailure::Target{local:true});
                    return Err(anyhow!("got = {:?}, want {:?}",got,want));
                }
                let (got,want) = (&h.sender_chain_info.genesis_id,&genesis_id);
                if got!=want {
                    result.handshake_failure = Some(HandshakeFailure::Genesis{local:true});
                    return Err(anyhow!("got = {:?}, want {:?}",got,want));
                }
                result.handshake = true;
                stream.write(ctx,&PeerMessage::PeersRequest{}).await?;
            }
            PeerMessage::HandshakeFailure(info,reason) => { // TODO: use the info to restart the handshake (from within scan)
                result.handshake_failure = Some(match reason {
                    HandshakeFailureReason::ProtocolVersionMismatch{version,oldest_supported_version} => {
                        msg.protocol_version = version;
                        msg.oldest_supported_version = oldest_supported_version;
                        stream.write(ctx,&PeerMessage::Handshake(msg.clone())).await.context("stream.write(Handshake)")?;
                        continue
                    },
                    HandshakeFailureReason::GenesisMismatch(_) => HandshakeFailure::Genesis{local:false},
                    HandshakeFailureReason::InvalidTarget => {
                        if let Some(actual_info) = &result.actual_info { panic!("received InvalidTarget for the second time: actual_info.id = {}, info.id = {}",&actual_info.id,&info.id); }
                        result.actual_info = Some(info.clone());
                        msg.target_peer_id = info.id.clone();
                        msg.partial_edge_info = PartialEdgeInfo::new(&my_peer_id, &info.id, 1, &cfg.network_config.secret_key);
                        stream.write(ctx,&PeerMessage::Handshake(msg.clone())).await.context("stream.write(Handshake)")?;
                        continue
                    },
                });
                return Err(anyhow!("handshake failure: {:?}",reason));
            }
            PeerMessage::SyncRoutingTable(update) => {
                if update.accounts.len()>0 { 
                    info!("SyncRoutingTable = (edges={},accounts={})",update.edges.len(),update.accounts.len());
                    result.routing_table = Some(update);
                }
                /*
                let mut peers = HashSet::<PeerId>::new();
                for e in &update.edges {
                    let (p1,p2) = e.key();
                    peers.insert(p1.clone());
                    peers.insert(p2.clone());
                }*/
                //info!("SyncRoutingTable = (edges={},accounts={},peers={})",update.edges.len(),update.accounts.len(),peers.len());
            }
            // contains peer_store.healthy_peers(config.max_send_peers)
            PeerMessage::PeersResponse(infos) => {
                result.peers_responses += 1;
                for info in infos { result.peers.push(info); }
                if result.peers_responses < peers_responses {
                    stream.write(ctx,&PeerMessage::PeersRequest{}).await?;
                }
            }
            _ => {}
        }
    }
    return Ok(());
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
    cfg:NearConfig,

    started:Mutex<HashSet<std::net::SocketAddr>>,
    result:Mutex<HashMap<std::net::SocketAddr,AskForPeersResult>>,
    sem:Semaphore,
    stats:Stats,
}

type Spawnable = Box<dyn FnOnce(Ctx,Arc<Scope>) -> BoxFuture<'static,anyhow::Result<()>> + Send>;

fn noop() -> Spawnable {
    spawnable(|_ctx,_s| async { Ok(()) })
}

fn spawnable<F,G>(g:G) -> Spawnable where
    G:FnOnce(Ctx,Arc<Scope>) -> F + 'static + Send,
    F:Future<Output=anyhow::Result<()>> + 'static + Send,
{
    return Box::new(|ctx,s|g(ctx,s).boxed());
}

impl Scanner {
    fn new(cfg:NearConfig, per_peer_timeout:time::Duration, per_peer_requests:usize, inflight_limit:usize) -> Arc<Self> {
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
            let mut result = AskForPeersResult::default();
            if let Err(err) = ask_for_peers(&ctx.with_timeout(self.per_peer_timeout),&mut result,&self.cfg,&info,self.per_peer_requests).await {
                result.err = Some(err);
            }
            match fetch_network_info(&ctx.with_timeout(time::Duration::from_secs(2)),addr.ip()).await {
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

async fn fetch_network_info(ctx :&Ctx, ip: std::net::IpAddr) -> anyhow::Result<RpcNetworkInfoResponse> {
    let uri = hyper::Uri::builder()
        .scheme("http")
        .authority(std::net::SocketAddr::new(ip,3030).to_string())
        .path_and_query("/")
        .build()?;
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "network_info",
        "params": [],
    });
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .header("Content-Type","application/json")
        .uri(uri)
        .body(hyper::Body::from(serde_json::to_string(&req)?))?;
    
    let resp = ctx.wrap(hyper::Client::new().request(req)).await??;
    let resp = ctx.wrap(hyper::body::to_bytes(resp)).await??;
    //info!("resp = {}",std::str::from_utf8(&resp).unwrap());
    let resp = serde_json::from_slice::<message::Response>(&*resp)?;
    let resp = match resp.result {
        Ok(resp) => resp,
        Err(err) => { return Err(anyhow!(serde_json::to_string(&err)?)) }
    };
    return Ok(serde_json::from_value::<RpcNetworkInfoResponse>(resp)?);
}

async fn fetch_validators(uri: hyper::Uri) -> anyhow::Result<RpcValidatorResponse> {
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "validators",
        "id": "dontcare",
        "params": [null],
    });
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .header("Content-Type","application/json")
        .uri(uri)
        .body(hyper::Body::from(serde_json::to_string(&req)?))?;
    
    let resp = hyper::Client::new().request(req).await?;
    let resp = hyper::body::to_bytes(resp).await?;
    let resp = serde_json::from_slice::<message::Response>(&*resp)?;
    let resp = match resp.result {
        Ok(resp) => resp,
        Err(err) => { return Err(anyhow!(serde_json::to_string(&err)?)) }
    };
    return Ok(serde_json::from_value::<RpcValidatorResponse>(resp)?);
}

fn bfs(graph: &RoutingTableUpdate, start: &HashSet<PeerId>) -> HashMap<PeerId,usize> {
    let mut edges : HashMap<PeerId,HashSet<PeerId>> = HashMap::new();
    let mut edges_list = graph.edges.clone();
    edges_list.sort_by_key(|v|v.nonce());
    for e in &edges_list {
        let (p0,p1) = e.key();
        match e.edge_type() {
            EdgeState::Active => {
                edges.entry(p0.clone()).or_default().insert(p1.clone());
                edges.entry(p1.clone()).or_default().insert(p0.clone());
            }
            EdgeState::Removed => {
                edges.entry(p0.clone()).or_default().remove(p1);
                edges.entry(p1.clone()).or_default().remove(p0);
            }
        }
    }
    let mut dist : HashMap<PeerId,usize> = HashMap::new();
    let mut queue = vec![];
    for p in start {
        dist.insert(p.clone(),1);
        queue.push(p);
    }
    let mut i = 0;
    while i<queue.len() {
        let v = queue[i];
        i += 1;
        for w in edges.get(v).iter().map(|s|s.iter()).flatten() {
            if dist.contains_key(w) { continue }
            dist.insert(w.clone(),dist.get(v).unwrap()+1);
            queue.push(w);
        }
    }
    return dist;
}

pub fn main() -> anyhow::Result<()> {
    init_logging();
    let cmd = Cmd::parse();
    let cfg = crate::config::download(&cmd.chain_id)?;
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move{
        let vi = fetch_validators(hyper::Uri::from_static("http://rpc.mainnet.near.org")).await?;
        let mut validators : HashMap<AccountId,PublicKey> = HashMap::new();
        for v in &vi.validator_info.current_validators {
            validators.insert(v.account_id.clone(),v.public_key.clone());
        }
        let scanner = Scanner::new(cfg.clone(),time::Duration::from_secs(20),5,1000);
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
        let dist = bfs(&graph.ok_or(anyhow!("no peer provided a graph"))?,&handshakes);
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
        // TODO: how many nodes are in the RoutingTable graph?
        // TODO: how many nodes are not isolated in the RoutingTable graph?
        // Next step will be to send routing requests to them (but that requires finalizing network2).
        info!("reachable_count = {}",dist.len());
        info!("found_count = {}",found.len());
        info!("peer_id_mismatch_count = {}",peer_id_mismatch_count);
        //for (_,v) in &*scanner.started.lock() { println!("{}",v.ip()); }
        //for id in &found { println!("{}",id); }
        //for id in &accounts { println!("{}",id); }
        Ok(())
    })
}
