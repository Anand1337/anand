use crate::concurrency;
use crate::fetch_chain;
use crate::config;

use std::io;
use std::sync::Arc;
use std::collections::HashSet;
use tokio::time;

use anyhow::{anyhow, Context};
use clap::Clap;
use openssl_probe;
use tracing::metadata::LevelFilter;
use tracing::{info};
use tracing_subscriber::EnvFilter;

use concurrency::{Ctx, RateLimit, Scope};
use crate::network2 as network;

use near_primitives::hash::CryptoHash;

#[derive(Clap, Debug)]
struct Cmd {
    #[clap(long)]
    pub chain_id: String,
    #[clap(long)]
    pub start_block_hash: String,
    #[clap(long, default_value = "200")]
    pub qps_limit: u32,
    #[clap(long, default_value = "2000")]
    pub block_limit: u64,
    #[clap(long)]
    pub peers_filepath: String,
}

fn init_logging() {
    let env_filter = EnvFilter::from_default_env().add_directive(LevelFilter::INFO.into());
    tracing_subscriber::fmt::Subscriber::builder()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::ENTER
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_env_filter(env_filter)
        .with_writer(io::stderr)
        .init();
}

pub fn main() -> anyhow::Result<()> {
    init_logging();
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        std::process::exit(1);
    }));
    openssl_probe::init_ssl_cert_env_vars();
    
    let cmd = Cmd::parse();
    let start_block_hash =
        cmd.start_block_hash.parse::<CryptoHash>().map_err(|x| anyhow!(x.to_string())).context("start_block_hash.parse")?;

    let mut peers : HashSet<std::net::IpAddr> = HashSet::new();
    let peers_raw = std::fs::read_to_string(cmd.peers_filepath).context("fs::read_to_string()")?;
    for ip in peers_raw.split_whitespace() {
        if ip.len()==0 { continue }
        peers.insert(ip.parse().with_context(||format!("parse('{}')",ip))?);
    }

    info!("downloading configs for chain {}", cmd.chain_id);
    let near_config = Arc::new(config::download(&cmd.chain_id).context("download_configs")?);

    info!("#boot nodes = {}", near_config.network_config.boot_nodes.len());
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(Scope::run(&Ctx::background(),|ctx,s|async move {
        let mut peers = network::discover_peers(
            &ctx,
            near_config.clone(),
            time::Duration::from_secs(10),
            3,
            1000,
        ).await?;
        let (network,event_loop) = network::ClientManager::start(network::ClientManagerConfig{
            near: near_config,
            known_peers: peers.drain().collect(),
            conn_count: 10,
            handshake_timeout: time::Duration::from_secs(5),
            per_conn_rate_limit: RateLimit{burst:cmd.qps_limit as u64 ,qps:cmd.qps_limit as f64},
        });
        let server = Arc::new(network::UnimplementedNodeServer());
        s.spawn_weak(|ctx|event_loop(ctx,server));

        fetch_chain::run(ctx.clone(), network, start_block_hash, cmd.block_limit).await?;
        info!("Fetch completed");
        Ok(())
    }))
}
