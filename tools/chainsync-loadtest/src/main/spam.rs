#![allow(unused_imports)]
#![allow(unused_variables)]
use std::collections::{HashSet,HashMap};
use std::io;
use std::pin::{Pin};
use std::sync::{Arc};
use std::sync::atomic::{AtomicU64,Ordering};
use std::future::Future;
use std::str::{FromStr};

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
use tokio::sync::{Semaphore,Mutex as AsyncMutex};
use tokio::io::{AsyncReadExt,AsyncWriteExt,AsyncRead,AsyncWrite,BufReader,BufWriter,ReadHalf,WriteHalf};
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

struct Stream {
    reader: AsyncMutex<BufReader<ReadHalf<net::TcpStream>>>,
    writer: AsyncMutex<BufWriter<WriteHalf<net::TcpStream>>>,
}

impl Stream {
    fn new(stream: net::TcpStream) -> Self {
        let (reader,writer) = tokio::io::split(stream);
        Self{
            reader: AsyncMutex::new(BufReader::new(reader)),
            writer: AsyncMutex::new(BufWriter::new(writer)),
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

async fn spam(ctx:&Ctx, cfg :&NearConfig, peer_info:&PeerInfo, stats: Arc<Stats>) -> anyhow::Result<()> {
    let account_id = "node".parse().unwrap();
    let node_signer = near_crypto::InMemorySigner::from_random(account_id, near_crypto::KeyType::ED25519);
    stats.connection_attempts.fetch_add(1,Ordering::Relaxed);
    let addr = peer_info.addr.expect("missing addr");
    let stream = ctx.wrap(net::TcpStream::connect(addr)).await.context("connect()")??;
    let my_peer_id = PeerId::new(node_signer.public_key.clone());
    let my_addr = stream.local_addr()?;
    let edge_info = PartialEdgeInfo::new(&my_peer_id, &peer_info.id, 1, &node_signer.secret_key);
    let stream = Arc::new(Stream::new(stream));
   
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

    'handshake: loop {
        stream.write(ctx,&PeerMessage::Handshake(msg.clone())).await.context("stream.write()")?;
        match stream.read(ctx).await.context("stream.read()")? {
            PeerMessage::Handshake(h) => { break 'handshake }
            PeerMessage::HandshakeFailure(info,reason) => {
                match reason {
                    HandshakeFailureReason::ProtocolVersionMismatch{version,oldest_supported_version} => {
                        msg.protocol_version = version;
                        msg.oldest_supported_version = oldest_supported_version;
                    }
                    HandshakeFailureReason::GenesisMismatch(genesis_id) => {
                        msg.sender_chain_info.genesis_id = genesis_id;
                    }
                    HandshakeFailureReason::InvalidTarget => {
                        msg.target_peer_id = info.id.clone();
                        msg.partial_edge_info = PartialEdgeInfo::new(&my_peer_id, &info.id, 1, &node_signer.secret_key);
                    }
                }
            }
            _ => {}
        }

    }
    //info!("handshake done");
    stats.connections.fetch_add(1,Ordering::Relaxed);
    let err = Scope::run(ctx,{
        let stats = stats.clone();
        |ctx,s| async move {
            s.spawn({
                let stream = stream.clone();
                let stats = stats.clone();
                |ctx,s| async move {
                    loop {
                        stream.write(&ctx,&PeerMessage::PeersRequest{}).await?;
                        stats.requests_sent.fetch_add(1,Ordering::Relaxed);
                        ctx.wait(time::Duration::from_secs(1)).await?;
                    }
                }
            });
            loop {
                stream.read(&ctx).await?;
                stats.requests_recv.fetch_add(1,Ordering::Relaxed);
            }
        }
    }).await;
    stats.connections.fetch_sub(1,Ordering::Relaxed);
    err
}

#[derive(Clap, Debug)]
struct Cmd {
    #[clap(long)]
    pub chain_id: String,
}

#[derive(Default,Debug)]
struct Stats {
    requests_sent: AtomicU64,
    requests_recv: AtomicU64,
    connection_attempts: AtomicU64,
    connections: AtomicU64,
}

pub fn main() -> anyhow::Result<()> {
    init_logging();
    let cmd = Cmd::parse();
    let cfg = Arc::new(crate::config::download(&cmd.chain_id)?);
    let rt = tokio::runtime::Runtime::new()?;

    let addrs = vec![
        "34.83.255.174:24567",
        "34.83.72.115:24567",
        "35.229.125.219:24567",
        "104.196.140.215:24567",
        "34.91.163.21:24567",
        "34.75.95.45:24567",
        "35.231.62.215:24567",
        "34.82.155.46:24567",
        "34.90.229.36:24567",
        "35.197.60.48:24567",
        "35.196.98.160:24567",
        "34.90.177.10:24567",
        "35.197.69.22:24567",
        "34.91.107.215:24567",
        "35.243.190.122:24567",
        "34.139.204.247:24567",
        "35.233.179.86:24567",
        "104.196.41.204:24567",
        "34.91.145.199:24567",
        "34.73.198.234:24567",
        "35.237.214.215:24567",
        "34.148.165.160:24567",
        "34.91.236.145:24567",
        "34.148.70.244:24567",
        "35.196.96.251:24567",
        "34.139.80.255:24567",
        "34.145.87.183:24567",
        "34.83.78.151:24567",
        "34.82.121.231:24567",
        "34.85.142.58:24567",
        "34.91.192.144:24567",
        "35.233.238.132:24567",
        "34.148.158.224:24567",
        "35.247.114.12:24567",
        "34.91.180.20:24567",
        "34.148.114.250:24567",
        "35.196.158.54:24567",
        "34.78.228.76:24567",
        "34.83.255.168:24567",
        "35.230.97.117:24567",
        "34.83.4.245:24567",
        "34.91.192.16:24567",
        "35.233.176.198:24567",
        "35.203.187.11:24567",
        "34.91.9.69:24567",
        "104.196.206.153:24567",
        "34.148.189.36:24567",
        "35.227.161.23:24567",
        "34.91.199.159:24567",
        "34.75.2.191:24567",
        "34.82.56.106:24567",
        "34.150.212.111:24567",
        "34.82.159.27:24567",
        "34.147.80.237:24567",
        "34.127.33.75:24567",
        "34.145.81.69:24567",
        "34.73.2.96:24567",
        "34.91.105.2:24567",
        "34.105.116.254:24567",
        "34.90.248.188:24567",
        "34.141.153.121:24567",
        "35.188.237.4:24567",
        "34.75.26.179:24567",
        "34.90.39.182:24567",
        "35.233.245.2:24567",
        "34.91.215.41:24567",
        "34.85.183.44:24567",
        "34.75.102.10:24567",
        "35.227.171.68:24567",
        "35.227.169.59:24567",
        "35.221.3.91:24567",
        "35.233.165.43:24567",
        "34.85.163.141:24567",
        "34.83.189.153:24567",
        "35.197.34.125:24567",
        "34.150.236.200:24567",
        "34.127.50.222:24567",
        "35.237.186.164:24567",
        "34.91.170.176:24567",
        "34.139.94.215:24567",
        "34.75.53.210:24567",
        "34.139.216.182:24567",
        "34.105.54.56:24567",
        "35.204.36.65:24567",
        "35.204.98.153:24567",
        "34.85.175.243:24567",
        "35.196.253.176:24567",
        "34.127.74.54:24567",
        "35.230.22.3:24567",
        "35.203.185.132:24567",
        "34.83.157.230:24567",
        "34.145.121.86:24567",
        "34.83.88.182:24567",
        "34.90.68.226:24567",
        "34.141.200.85:24567",
        "34.147.89.26:24567",
        "35.243.141.99:24567",
        "35.185.209.215:24567",
        "34.90.143.134:24567",
        "34.148.21.208:24567",
        "34.141.248.59:24567",
        "34.138.200.73:24567",
        "34.75.190.181:24567",
        "104.196.22.233:24567",
        "34.82.208.89:24567",
        "34.75.118.96:24567",
        "79.191.59.148:24567",
        "34.141.244.62:24567",
        "34.91.152.2:24567",
        "34.139.38.91:24567",
        "35.197.23.74:24567",
        "34.141.135.24:24567",
        "35.236.192.136:24567",
        "34.85.233.231:24567",
        "34.127.109.200:24567",
        "34.139.217.255:24567",
        "34.91.159.134:24567",
        "34.83.133.0:24567",
        "35.204.95.152:24567",
        "34.138.48.158:24567",
        "34.148.142.80:24567",
        "34.91.82.50:24567",
        "34.148.0.247:24567",
        "34.138.255.60:24567",
        "34.139.170.21:24567",
    ];
    let mut infos = vec![];
    for addr in addrs {
        let s = "ed25519:DRimR9ArFo8iG1HFEHHz5fCv61tciRBbpXBM4rKY4Kix@".to_string() + &addr.to_string();
        infos.push(PeerInfo::from_str(&s).unwrap());
    }
        /*PeerInfo::from_str("ed25519:DRimR9ArFo8iG1HFEHHz5fCv61tciRBbpXBM4rKY4Kix@35.247.114.12:24567").unwrap(),
        PeerInfo::from_str("ed25519:DRimR9ArFo8iG1HFEHHz5fCv61tciRBbpXBM4rKY4Kix@34.105.116.254:24567").unwrap(),
        PeerInfo::from_str("ed25519:DRimR9ArFo8iG1HFEHHz5fCv61tciRBbpXBM4rKY4Kix@35.237.214.215:24567").unwrap(),
        PeerInfo::from_str("ed25519:DRimR9ArFo8iG1HFEHHz5fCv61tciRBbpXBM4rKY4Kix@34.78.228.76:24567").unwrap(),*/
    let stats = Arc::new(Stats::default());
    rt.block_on(async move {
        Scope::run(&Ctx::background(),|ctx,s|async move{
            s.spawn_weak({
                let stats = stats.clone();
                |ctx|async move{
                    loop {
                        info!("stats = {:?}",stats);
                        ctx.wait(time::Duration::from_secs(2)).await?;
                    }
                }
            });
          
            for i in 0..100 {
                for info in &infos {
                    s.spawn({
                        let cfg = cfg.clone();
                        let info = info.clone();
                        let stats = stats.clone();
                        |ctx,s| async move {
                            loop {
                                match spam(&ctx,&cfg,&info,stats.clone()).await {
                                    Ok(_) => { return Ok(()); }
                                    Err(err) => { } // info!("err = {:#}",err); }
                                }
                            }
                        }
                    });
                }
            }
            return Ok(());
        }).await
    })
}
