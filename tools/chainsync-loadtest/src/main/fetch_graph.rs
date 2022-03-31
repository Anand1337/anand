use crate::network2 as network;
use clap::{Clap};
use crate::concurrency::{Ctx,Scope,RateLimit,Once};
use crate::peer_manager::types::{RoutingTableUpdate};
use tracing::metadata;
use tracing::{info};
use std::sync::{Arc};
use std::collections::{HashSet,HashMap};

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

struct Server {
    routing_table : Once<RoutingTableUpdate>,
}

impl network::NodeServer for Server {
    fn sync_routing_table(&self,_ctx:&Ctx,u:RoutingTableUpdate) -> anyhow::Result<()> {
        if u.accounts.len()>0 {
            info!("u.edges.len() = {}",u.edges.len());
            let _ = self.routing_table.set(u);
        }
        Ok(())
    }
}

pub fn main() -> anyhow::Result<()> {
    init_logging();
    let cmd = Cmd::parse();
    let cfg = Arc::new(crate::config::download(&cmd.chain_id)?);
    let rt = tokio::runtime::Runtime::new()?;
    let ctx = Ctx::background();
    rt.block_on(async move{
        let addr : std::net::SocketAddr = cmd.addr.parse()?;
        Scope::run(&ctx,|ctx,s|async move{
            let (cli,event_loop) = network::NodeClient::connect(&ctx,network::NodeClientConfig{
                near: cfg,
                peer_addr: addr,
                peer_id: None,
                rate_limit: RateLimit{burst:100,qps:100.},
                allow_protocol_mismatch: false,
                allow_genesis_mismatch: false,
            }).await?;
            let server = Arc::new(Server{routing_table:Once::new()});
            s.spawn_weak(|ctx|event_loop(ctx,server.clone()));
            info!("connected");
            /*for peer_info in cli.fetch_peers(&ctx).await? {
                info!("peer = {}",peer_info);
            }*/
            let graph = ctx.wrap(server.routing_table.wait()).await?;
            let graph = crate::graph::Graph::new(&graph);
            let mut start = HashSet::new();
            start.insert(cli.peer_id.clone());
            let (dist,unknown_nodes) = graph.dist_from(&start);
            info!("unknown_nodes = {}",unknown_nodes);
            info!("edges = {:?}",graph.edges.get(&cli.peer_id));
            let mut hist = HashMap::<usize,usize>::new();
            for (_,v) in &dist {
                *hist.entry(*v).or_default() += 1;
            }
            for (k,v) in &hist {
                info!("dist[{}] = {}",k,v);
            }
            info!("done");
            Ok(())
        }).await
    })
}
