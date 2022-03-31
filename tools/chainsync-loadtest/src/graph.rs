use std::collections::{HashSet,HashMap};

use near_network_primitives::types::{EdgeState};
use near_primitives::network::{PeerId};
use crate::peer_manager::types::{RoutingTableUpdate};

pub struct Graph {
    pub edges : HashMap<PeerId,HashSet<PeerId>>,
}

impl Graph {
    pub fn node_count(&self) -> usize { self.edges.len() }
    pub fn isolated_node_count(&self) -> usize { self.edges.iter().filter(|(_,v)|v.is_empty()).count() }

    pub fn new(graph: &RoutingTableUpdate) -> Graph {
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
        return Graph{edges}
    }

    pub fn dist_from(&self, start: &HashSet<PeerId>) -> (HashMap<PeerId,usize>,usize) {
        let mut dist : HashMap<PeerId,usize> = HashMap::new();
        let mut queue = vec![];
        let mut unknown_nodes = 0;
        for p in start {
            if self.edges.contains_key(&p) {
                dist.insert(p.clone(),0);
                queue.push(p);
            } else {
                unknown_nodes += 1;
            }
        }
        let mut i = 0;
        while i<queue.len() {
            let v = queue[i];
            i += 1;
            for w in self.edges.get(v).iter().map(|s|s.iter()).flatten() {
                if dist.contains_key(w) { continue }
                dist.insert(w.clone(),dist.get(v).unwrap()+1);
                queue.push(w);
            }
        }
        return (dist,unknown_nodes);
    }
}
