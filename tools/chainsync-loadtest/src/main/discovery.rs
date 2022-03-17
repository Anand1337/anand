#![allow(unused_imports)]
#![allow(unused_variables)]
use std::collections::HashSet;
use std::io;
use std::pin::{Pin};
use std::sync::{Arc,Mutex};
use std::future::Future;

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
    
    async fn read(&mut self) -> anyhow::Result<PeerMessage> {
        let n = self.0.read_u32_le().await? as usize;
		// TODO: if this check fails, the stream is broken, because we read some bytes already.
		if n>NETWORK_MESSAGE_MAX_SIZE_BYTES { return Err(anyhow!("message size too large")); }
        let mut buf = BytesMut::new();
	    buf.resize(n,0);	
        self.0.read_exact(&mut buf[..]).await?;
        Ok(PeerMessage::try_from_slice(&buf[..])?)
    }
    
    async fn write(&mut self, msg: &PeerMessage) -> anyhow::Result<()> {
        let msg = msg.try_to_vec()?;
        // TODO: If writing fails in the middle, then the stream is broken.
        self.0.write_u32_le(msg.len() as u32).await?;
        self.0.write_all(&msg[..]).await?;
        self.0.flush().await?;
        Ok(())
    }
}

async fn ask_for_peers(ctx:&Ctx, cfg :&NearConfig, peer_info:&PeerInfo) -> anyhow::Result<Vec<PeerInfo>> {
    // The `connect` may take several minutes. This happens when the
    // `SYN` packet for establishing a TCP connection gets silently
    // dropped, in which case the default TCP timeout is applied. That's
    // too long for us, so we shorten it to one second.
    //
    // Why exactly a second? It was hard-coded in a library we used
    // before, so we keep it to preserve behavior. Removing the timeout
    // completely was observed to break stuff for real on the testnet.
    let connect_timeout = time::Duration::from_secs(1);
    let addr = peer_info.addr.ok_or(anyhow!("missing addr"))?;
    let stream = ctx.with_timeout(connect_timeout).wrap(net::TcpStream::connect(addr)).await??;
    
    info!("connected!");
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
    stream.write(&msg).await?;
    info!("sent");
    loop {
        match stream.read().await? {
            PeerMessage::Handshake(h) => {
                info!("Hanshake");
                let (got,want) = (&h.sender_chain_info.genesis_id,&genesis_id);
                if got!=want { return Err(anyhow!("got = {:?}, want {:?}",got,want)); }
                let (got,want) = (&h.target_peer_id,&my_peer_id);
                if got!=want { return Err(anyhow!("got = {}, want = {}",got,want)); }
                stream.write(&PeerMessage::PeersRequest{}).await?;
            }
            PeerMessage::HandshakeFailure(_,reason) => {
                return Err(anyhow!("handshake failure: {:?}",reason));
            }
            PeerMessage::SyncRoutingTable(update) => {
                let mut peers = HashSet::<PeerId>::new();
                for e in &update.edges {
                    let (p1,p2) = e.key();
                    peers.insert(p1.clone());
                    peers.insert(p2.clone());
                }
                info!("SyncRoutingTable = (edges={},accounts={},peers={})",update.edges.len(),update.accounts.len(),peers.len());
            }
            // contains peer_store.healthy_peers(config.max_send_peers)
            PeerMessage::PeersResponse(infos) => { 
                let mut peers = HashSet::<PeerId>::new();
                for info in &infos {
                    if info.addr.is_some() {
                        peers.insert(info.id.clone());
                    }
                }
                info!("PeersResponse = (with_addr={},total={})",peers.len(),infos.len());
                return Ok(infos);
            }
            msg => { info!("ignoring {:?}",msg); }
        }
    }
}

#[derive(Clap, Debug)]
struct Cmd {
    #[clap(long)]
    pub chain_id: String,
}

struct Scanner {
    done:Mutex<HashSet<PeerId>>,
    cfg:NearConfig,
    sem:Semaphore,
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
    fn new(cfg:NearConfig) -> Self {
        Self{
            done: Mutex::new(HashSet::new()),
            cfg:cfg,
            sem:Semaphore::new(100),
        }
    }

    fn scan(self:Arc<Scanner>,info:PeerInfo) -> Spawnable {
        if info.addr.is_none(){ return noop(); }
        // Check if this peer has been already processed.
        {
            let mut done = self.done.lock().unwrap();
            if done.contains(&info.id){ return noop(); }
            done.insert(info.id.clone());
        }
        spawnable(move |ctx,s| async move {
            // Acquire a permit. TODO: consider acquiring the permit before spawning scan.
            let permit = self.sem.acquire().await;
            let infos = match ask_for_peers(&ctx,&self.cfg,&info).await {
                Ok(infos) => infos,
                Err(err) => {
                    info!("connect({}): {:#}",&info.addr.unwrap(),err);
                    return Ok(());
                }
            };
            for info in infos {
                s.spawn(self.clone().scan(info));
            }
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
        let scanner = Arc::new(Scanner::new(cfg.clone()));
        Scope::run(&Ctx::background(),{
            let scanner = scanner.clone();
            |ctx,s|async move{
                for info in &scanner.cfg.network_config.boot_nodes {
                    let info = info.clone();
                    let scanner = scanner.clone();
                    s.spawn(scanner.scan(info.clone()));
                }
                Ok(())
            }
        }).await?;
        info!("total peers = {}",scanner.done.lock().unwrap().len());
        Ok(())
    })
}
