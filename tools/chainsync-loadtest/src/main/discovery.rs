#![allow(unused_imports)]
#![allow(unused_variables)]
use std::io;

use tracing::{info,metadata};
use clap::Clap;
use near_network_primitives::types::{PeerInfo};
use tokio::time;
use tokio::net;
use anyhow::{anyhow};

use crate::peer_manager::peer::codec::Codec;
use crate::concurrency::{Ctx};

fn init_logging() {
    let env_filter = tracing_subscriber::EnvFilter::from_default_env().add_directive(metadata::LevelFilter::INFO.into());
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(io::stderr)
        .init();
}

async fn connect(ctx:&Ctx, peer_info:&PeerInfo) -> anyhow::Result<()> {
    // The `connect` may take several minutes. This happens when the
    // `SYN` packet for establishing a TCP connection gets silently
    // dropped, in which case the default TCP timeout is applied. That's
    // too long for us, so we shorten it to one second.
    //
    // Why exactly a second? It was hard-coded in a library we used
    // before, so we keep it to preserve behavior. Removing the timeout
    // completely was observed to break stuff for real on the testnet.
    let CONNECT_TIMEOUT = time::Duration::from_secs(1);
    let addr = peer_info.addr.ok_or(anyhow!("missing addr"))?;
    let stream = ctx.with_timeout(CONNECT_TIMEOUT).wrap(net::TcpStream::connect(addr)).await??;
    info!("connected!");
    Ok(())
}
/*
    let edge_info = act.propose_edge(&msg.peer_info.id, None);
    
    let my_peer_id = self.my_peer_id.clone();
    let account_id = self.config.account_id.clone();
    let handshake_timeout = self.config.handshake_timeout;
    let server_addr = self.config.addr.or(stream.local_addr());
    let remote_addr = stream.peer_addr()?;
    let stream = tokio_util::codec::Framed::new(stream, Codec::default());
    //let (read, write) = tokio::io::split(stream);

    let chain_info = self.view_client_addr.get_chain_info();
    if protocol_version<39 { error }
    let msg = PeerMessage::Handshake(Handshake::new(
        protocol_version,
        my_node_id.clone(),
        peer_info.id.clone(),
        addr,
        PeerChainInfoV2 {
            chain_info.genesis_id,
            chain_info.height,
            chain_info.tracked_shards,
            chain_info.archival,
        },
        partial_edge_info,
    ));
    stream.write(msg.try_to_vec()?)?;
    let resp = stream.read().await?;
    let resp = PeerMessage::try_from_slice(&resp)?;
    match resp {
        PeerMessage::Handshake(h) => {
            if handshake.sender_chain_info.genesis_id != self.genesis_id { error }
            if handshake.sender_peer_id == self.my_node_info.id { error }
            if handshake.target_peer_id != self.my_node_info.id { error }
            // Check that received nonce on handshake match our proposed nonce.
            if self.peer_type == PeerType::Outbound
                && handshake.partial_edge_info.nonce
                    != self.partial_edge_info.as_ref().map(|edge_info| edge_info.nonce).unwrap()
            // should happen before HANDSHAKE TIMEOUT
        PeerMessage::HandshakeFailure => { error }
        PeerMessage::SyncRoutingTable => { /* should happen after handshake */ }
        PeerMessage::PeersResponse => { /* contains peer_store.healthy_peers(config.max_send_peers) */ }
        PeerMessage::PeersRequest => { ignore }
    }
}*/

#[derive(Clap, Debug)]
struct Cmd {
    #[clap(long)]
    pub chain_id: String,
}

pub fn main() -> anyhow::Result<()> {
    init_logging();
    let cmd = Cmd::parse();
    let cfg = crate::config::download(&cmd.chain_id)?;
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move{
        connect(&Ctx::background(),&cfg.network_config.boot_nodes[0]).await?;
        Ok(())
    })
}
