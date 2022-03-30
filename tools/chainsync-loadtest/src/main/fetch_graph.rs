use crate::network2 as network;
use clap::{Clap};
use crate::concurrency::{Ctx,Scope,RateLimit};
use near_network_primitives::types::{PeerInfo};
use near_primitives::network::{PeerId};
use crate::peer_manager::types::{RoutingTableUpdate};
use tracing::metadata;
use tracing::{info};
use std::sync::{Arc};

fn init_logging() {
    let env_filter = tracing_subscriber::EnvFilter::from_default_env().add_directive(metadata::LevelFilter::INFO.into());
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();
}

#[derive(Clap, Debug)]
struct Cmd {
    #[clap(long)]
    pub chain_id: String,
    
    #[clap(long)]
    pub addr:String,
}

struct Server();

impl network::NodeServer for Server {
    fn sync_routing_table(&self,_ctx:&Ctx,u:RoutingTableUpdate) -> anyhow::Result<()> {
        info!("u.edges.len() = {}",u.edges.len());
        Ok(())
    }
}

pub fn main() -> anyhow::Result<()> {
    init_logging();
    let cmd = Cmd::parse();
    let cfg = Arc::new(crate::config::download(&cmd.chain_id)?);
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move{
        Scope::run(&Ctx::background(),|ctx,s|async move{
            let account_id = "dupa123".parse()?;
            let node_signer =
                near_crypto::InMemorySigner::from_random(account_id, near_crypto::KeyType::ED25519);
            let peer_id = PeerId::new(node_signer.public_key.clone());
            let (cli,event_loop) = network::NodeClient::connect(&ctx,network::NodeClientConfig{
                near: cfg,
                peer_info: PeerInfo{
                    account_id: None,
                    addr: Some(cmd.addr.parse()?),
                    id: peer_id,
                },
                rate_limit: RateLimit{burst:100,qps:100.},
                allow_peer_mismatch: true,
                allow_protocol_mismatch: false,
                allow_genesis_mismatch: false,
            }).await?;
            s.spawn_weak(|ctx|event_loop(ctx,Arc::new(Server())));
            info!("connected");
            Ok(())
        }).await
    })
}
