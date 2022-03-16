use crate::concurrency;
use crate::fetch_chain;
use crate::network;
use crate::config;

use std::io;
use std::sync::Arc;
use std::net;
use std::collections::HashSet;

use actix::{Actor, Arbiter};
use anyhow::{anyhow, Context};
use clap::Clap;
use openssl_probe;
use tracing::metadata::LevelFilter;
use tracing::{info};
use tracing_subscriber::EnvFilter;

use concurrency::{Ctx, Scope};
use network::{FakeClientActor, Network};

use crate::peer_manager::routing::start_routing_table_actor;
use crate::peer_manager::test_utils::NetworkRecipient;
use crate::peer_manager::PeerManagerActor;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_store::{db, Store};
use nearcore::config::NearConfig;

pub fn start_with_config(config: NearConfig, qps_limit: u32, peer_whitelist: HashSet<net::IpAddr>) -> anyhow::Result<Arc<Network>> {
    config.network_config.verify().context("start_with_config")?;
    let node_id = PeerId::new(config.network_config.public_key.clone());
    let store = Store::new(Arc::new(db::TestDB::new()));

    let network_adapter = Arc::new(NetworkRecipient::default());
    let network = Network::new(&config, network_adapter.clone(), qps_limit, peer_whitelist.clone());
    let client_actor = FakeClientActor::start_in_arbiter(&Arbiter::new().handle(), {
        let network = network.clone();
        move |_| FakeClientActor::new(network)
    });

    let routing_table_addr = start_routing_table_actor(node_id, store.clone());
    let network_actor = PeerManagerActor::start_in_arbiter(&Arbiter::new().handle(), move |_ctx| {
        PeerManagerActor::new(
            store,
            config.network_config,
            client_actor.clone().recipient(),
            client_actor.clone().recipient(),
            routing_table_addr,
            peer_whitelist.clone(),
        )
        .unwrap()
    })
    .recipient();
    network_adapter.set_recipient(network_actor);
    return Ok(network);
}



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

impl Cmd {
    fn parse_and_run() -> anyhow::Result<()> {
        let cmd = Self::parse();
        let start_block_hash =
            cmd.start_block_hash.parse::<CryptoHash>().map_err(|x| anyhow!(x.to_string())).context("start_block_hash.parse")?;

        let mut peers = HashSet::new();
        let peers_raw = std::fs::read_to_string(cmd.peers_filepath).context("fs::read_to_string()")?;
        for ip in peers_raw.split_whitespace() {
            if ip.len()==0 { continue }
            peers.insert(ip.parse().with_context(||format!("parse('{}')",ip))?);
        }


        info!("downloading configs for chain {}", cmd.chain_id);
        let near_config = config::download(&cmd.chain_id).context("download_configs")?;

        info!("#boot nodes = {}", near_config.network_config.boot_nodes.len());
        // Dropping Runtime is blocking, while futures should never be blocking.
        // Tokio has a runtime check which panics if you drop tokio Runtime from a future executed
        // on another Tokio runtime.
        // To avoid that, we create a runtime within the synchronous code and pass just an Arc
        // inside of it.
        let rt_ = Arc::new(tokio::runtime::Runtime::new()?);
        let rt = rt_.clone();
        return actix::System::new().block_on(async move {
            let network =
                start_with_config(near_config, cmd.qps_limit, peers).context("start_with_config")?;

            // We execute the chain_sync on a totally separate set of system threads to minimize
            // the interaction with actix.
            rt.spawn(async move {
                Scope::run(&Ctx::background(), move |ctx, s| async move {
                    s.spawn_weak(|ctx| async move {
                        ctx.wrap(tokio::signal::ctrl_c()).await?.unwrap();
                        info!("Got CTRL+C, stopping...");
                        return Err(anyhow!("Got CTRL+C"));
                    });
                    fetch_chain::run(ctx.clone(), network, start_block_hash, cmd.block_limit)
                        .await?;
                    info!("Fetch completed");
                    anyhow::Ok(())
                })
                .await
            })
            .await??;
            return Ok(());
        });
    }
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
    Cmd::parse_and_run().context("Cmd::parse_and_run")
}
