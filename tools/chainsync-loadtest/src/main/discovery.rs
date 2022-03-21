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
use near_network_primitives::types::{PeerInfo,PartialEdgeInfo,PeerChainInfoV2};
use near_primitives::network::{PeerId};
use near_primitives::version::{PEER_MIN_ALLOWED_PROTOCOL_VERSION,PROTOCOL_VERSION};
use crate::peer_manager::peer::codec::{Codec};
use crate::peer_manager::types::{PeerMessage,Handshake};
use crate::concurrency::{Ctx,Scope};

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

#[derive(Default)]
struct AskForPeersResult {
    connect:bool,
    handshake:bool,
    handshake_failure:bool,
    peers_responses:usize,
    peers: Vec<PeerInfo>,
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
    let addr = peer_info.addr.expect("missing addr");
    let stream = ctx.wrap(net::TcpStream::connect(addr)).await.context("connect()")??;
    result.connect = true;
    
    let my_peer_id = PeerId::new(cfg.network_config.public_key.clone());
    let my_addr = cfg.network_config.addr.unwrap_or(stream.local_addr()?);
    let edge_info = PartialEdgeInfo::new(&my_peer_id, &peer_info.id, 1, &cfg.network_config.secret_key);
    
	let genesis_id = crate::config::genesis_id(&cfg.client_config.chain_id); 
    let msg = PeerMessage::Handshake(Handshake::new(
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
    ));
    let mut stream = Stream::new(stream);
    stream.write(ctx,&msg).await.context("stream.write()")?;
    while result.peers_responses<peers_responses {
        match stream.read(ctx).await.context("stream.read()")? {
            PeerMessage::Handshake(h) => {
                let got = (&h.target_peer_id,&h.sender_chain_info.genesis_id);
                let want = (&my_peer_id,&genesis_id);
                if got!=want {
                    result.handshake_failure = true;
                    return Err(anyhow!("got = {:?}, want {:?}",got,want));
                }
                result.handshake = true;
                stream.write(ctx,&PeerMessage::PeersRequest{}).await?;
            }
            PeerMessage::HandshakeFailure(_,reason) => {
                result.handshake_failure = true;
                return Err(anyhow!("handshake failure: {:?}",reason));
            }
            PeerMessage::SyncRoutingTable(update) => {
                let mut peers = HashSet::<PeerId>::new();
                for e in &update.edges {
                    let (p1,p2) = e.key();
                    peers.insert(p1.clone());
                    peers.insert(p2.clone());
                }
                //info!("SyncRoutingTable = (edges={},accounts={},peers={})",update.edges.len(),update.accounts.len(),peers.len());
            }
            // contains peer_store.healthy_peers(config.max_send_peers)
            PeerMessage::PeersResponse(infos) => {
                result.peers_responses += 1;
                for info in infos { result.peers.push(info); }
                stream.write(ctx,&PeerMessage::PeersRequest{}).await?;
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

    started:Mutex<HashMap<PeerId,std::net::SocketAddr>>,
    result:Mutex<HashMap<PeerId,AskForPeersResult>>,
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
            started: Mutex::new(HashMap::new()),
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
        if info.addr.is_none() { return noop(); }
        // Check if this peer has been already processed.
        {
            let mut started = self.started.lock();
            if started.contains_key(&info.id){ return noop(); }
            started.insert(info.id.clone(),info.addr.unwrap());
        }
        spawnable(move |ctx,s| async move {
            // Acquire a permit.
            let permit = self.sem.acquire().await;
            self.stats.ask_for_peers_calls.fetch_add(1,Ordering::Relaxed);
            let mut result = AskForPeersResult::default();
            let _ = ask_for_peers(&ctx.with_timeout(self.per_peer_timeout),&mut result,&self.cfg,&info,self.per_peer_requests).await;
            for info in &result.peers {
                s.spawn(self.clone().scan(info.clone()));
            }
            self.result.lock().insert(info.id,result);
            return Ok(());
        })
    }
}

pub fn main() -> anyhow::Result<()> {
    init_logging();
    let cmd = Cmd::parse();
    let cfg = crate::config::download(&cmd.chain_id)?;
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move{
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
        let mut addr_count = 0;
        let mut connected_count = 0;
        let mut handshake_count = 0;
        let mut handshake_failure_count = 0;
        let mut fetch_completed_count = 0;
        for (_,v) in &*scanner.result.lock() {
            addr_count += 1;
            if v.connect { connected_count += 1; }
            if v.handshake { handshake_count += 1; }
            if v.handshake_failure { handshake_failure_count += 1; }
            if v.peers_responses>=scanner.per_peer_requests { fetch_completed_count += 1; }
            for info in &v.peers {
                found.insert(info.id.clone());
            }
        }
        info!("fetch_completed_count = {}",fetch_completed_count);
        info!("handshake_count = {}",handshake_count);
        info!("handshake_failure_count = {}",handshake_failure_count);
        info!("connected_count = {}",connected_count);
        info!("addr_count = {}",addr_count);
        info!("found_count = {}",found.len());
        for (_,v) in &*scanner.started.lock() {
            println!("{}",v.ip());
        }
        Ok(())
    })
}
