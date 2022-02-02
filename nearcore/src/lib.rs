use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use actix::{Actor, Addr, Arbiter};
use actix_rt::ArbiterHandle;
use actix_web;
use anyhow::Context;
#[cfg(feature = "performance_stats")]
use near_rust_allocator_proxy::reset_memory_usage_max;
use tracing::{info, trace};

use near_chain::ChainGenesis;
#[cfg(feature = "test_features")]
use near_client::AdversarialControls;
use near_client::{start_client, start_view_client, ClientActor, ViewClientActor};

use near_network::routing::start_routing_table_actor;
use near_network::test_utils::NetworkRecipient;
use near_network::PeerManagerActor;
use near_primitives::network::PeerId;
#[cfg(feature = "rosetta_rpc")]
use near_rosetta_rpc::start_rosetta_rpc;
use near_store::{create_store, Store};
use near_telemetry::TelemetryActor;

pub use crate::config::{init_configs, load_config, load_test_config, NearConfig, NEAR_BASE};
pub use crate::runtime::NightshadeRuntime;
pub use crate::shard_tracker::TrackedConfig;

pub mod append_only_map;
pub mod config;
mod migrations;
mod runtime;
mod shard_tracker;

const STORE_PATH: &str = "data";

pub fn store_path_exists<P: AsRef<Path>>(path: P) -> bool {
    fs::canonicalize(path).is_ok()
}

pub fn get_store_path(base_path: &Path) -> PathBuf {
    let mut store_path = base_path.to_owned();
    store_path.push(STORE_PATH);
    if store_path_exists(&store_path) {
        info!(target: "near", "Opening store database at {:?}", store_path);
    } else {
        info!(target: "near", "Did not find {:?} path, will be creating new store database", store_path);
    }
    store_path
}

pub fn get_default_home() -> PathBuf {
    if let Ok(near_home) = std::env::var("NEAR_HOME") {
        return near_home.into();
    }

    if let Some(mut home) = dirs::home_dir() {
        home.push(".near");
        return home;
    }

    PathBuf::default()
}

pub fn init_and_migrate_store(home_dir: &Path, _near_config: &NearConfig) -> Store {
    create_store(&get_store_path(home_dir))
}

pub struct NearNode {
    pub client: Addr<ClientActor>,
    pub view_client: Addr<ViewClientActor>,
    pub arbiters: Vec<ArbiterHandle>,
    pub rpc_servers: Vec<(&'static str, actix_web::dev::Server)>,
}

pub fn start_with_config(home_dir: &Path, config: NearConfig) -> Result<NearNode, anyhow::Error> {
    let store = init_and_migrate_store(home_dir, &config);

    let runtime = Arc::new(NightshadeRuntime::with_config(
        home_dir,
        store.clone(),
        &config,
        config.client_config.trie_viewer_state_size_limit,
        config.client_config.max_gas_burnt_view,
    ));

    let telemetry = TelemetryActor::new(config.telemetry_config.clone()).start();
    let chain_genesis = ChainGenesis::from(&config.genesis);

    let node_id = PeerId::new(config.network_config.public_key.clone().into());
    let network_adapter = Arc::new(NetworkRecipient::default());
    #[cfg(feature = "test_features")]
    let adv = Arc::new(std::sync::RwLock::new(AdversarialControls::default()));

    let view_client = start_view_client(
        config.validator_signer.as_ref().map(|signer| signer.validator_id().clone()),
        chain_genesis.clone(),
        runtime.clone(),
        network_adapter.clone(),
        config.client_config.clone(),
        #[cfg(feature = "test_features")]
        adv.clone(),
    );
    let (client_actor, client_arbiter_handle) = start_client(
        config.client_config,
        chain_genesis,
        runtime,
        node_id,
        network_adapter.clone(),
        config.validator_signer,
        telemetry,
        #[cfg(feature = "test_features")]
        adv.clone(),
    );

    #[allow(unused_mut)]
    let mut rpc_servers = Vec::new();
    let arbiter = Arbiter::new();
    let client_actor1 = client_actor.clone().recipient();
    let view_client1 = view_client.clone().recipient();
    config.network_config.verify().with_context(|| "start_with_config")?;
    let network_config = config.network_config;
    let routing_table_addr =
        start_routing_table_actor(PeerId::new(network_config.public_key.clone()), store.clone());
    #[cfg(all(feature = "json_rpc", feature = "test_features"))]
    let routing_table_addr2 = routing_table_addr.clone();
    let network_actor = PeerManagerActor::start_in_arbiter(&arbiter.handle(), move |_ctx| {
        PeerManagerActor::new(
            store,
            network_config,
            client_actor1,
            view_client1,
            routing_table_addr,
        )
        .unwrap()
    });

    #[cfg(feature = "json_rpc")]
    if let Some(rpc_config) = config.rpc_config {
        rpc_servers.extend_from_slice(&near_jsonrpc::start_http(
            rpc_config,
            config.genesis.config.clone(),
            client_actor.clone(),
            view_client.clone(),
            #[cfg(feature = "test_features")]
            network_actor.clone(),
            #[cfg(feature = "test_features")]
            routing_table_addr2,
        ));
    }

    #[cfg(feature = "rosetta_rpc")]
    if let Some(rosetta_rpc_config) = config.rosetta_rpc_config {
        rpc_servers.push((
            "Rosetta RPC",
            start_rosetta_rpc(
                rosetta_rpc_config,
                Arc::new(config.genesis.clone()),
                client_actor.clone(),
                view_client.clone(),
            ),
        ));
    }

    network_adapter.set_recipient(network_actor.recipient());

    rpc_servers.shrink_to_fit();

    trace!(target: "diagnostic", key="log", "Starting NEAR node with diagnostic activated");

    // We probably reached peak memory once on this thread, we want to see when it happens again.
    #[cfg(feature = "performance_stats")]
    reset_memory_usage_max();

    Ok(NearNode {
        client: client_actor,
        view_client,
        rpc_servers,
        arbiters: vec![client_arbiter_handle, arbiter.handle()],
    })
}
