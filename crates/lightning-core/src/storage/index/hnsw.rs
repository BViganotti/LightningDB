use crate::storage::buffer_manager::BufferManager;
use crate::storage::file_handle::FileHandle;
use crate::Result;
use parking_lot::RwLock;
use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

thread_local! {
    static RNG_STATE: RefCell<u64> = const { RefCell::new(12345) };
}

const HNSW_MAGIC: [u8; 4] = *b"HNSW";
const HNSW_VERSION: u8 = 0x01;

/// Distance metric for vector comparison.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DistanceMetric {
    Cosine,
    L2,
    InnerProduct,
}

/// Configuration for the HNSW index.
#[derive(Debug, Clone)]
pub struct HnswConfig {
    pub M: usize,
    pub M_max: usize,
    pub M_max0: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub metric: DistanceMetric,
    pub dimension: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            M: 16,
            M_max: 32,
            M_max0: 64,
            ef_construction: 200,
            ef_search: 50,
            metric: DistanceMetric::Cosine,
            dimension: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct HnswNode {
    id: u64,
    level: usize,
    /// Neighbors per layer: neighbors[layer] = Vec<node_id>
    neighbors: Vec<Vec<u64>>,
}

#[derive(Debug, Clone)]
struct Candidate {
    node_id: u64,
    distance: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.distance.partial_cmp(&other.distance)
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance.partial_cmp(&other.distance).unwrap_or(std::cmp::Ordering::Equal)
    }
}

pub struct HnswIndex {
    config: HnswConfig,
    nodes: RwLock<Vec<HnswNode>>,
    entry_point: RwLock<Option<u64>>,
    max_level: RwLock<usize>,
    embeddings: RwLock<Vec<Vec<f32>>>,
}

impl HnswIndex {
    pub fn new(config: HnswConfig) -> Self {
        Self {
            config,
            nodes: RwLock::new(Vec::new()),
            entry_point: RwLock::new(None),
            max_level: RwLock::new(0),
            embeddings: RwLock::new(Vec::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn config(&self) -> &HnswConfig {
        &self.config
    }

    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.config.metric {
            DistanceMetric::Cosine => {
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|v| v * v).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|v| v * v).sum::<f32>().sqrt();
                1.0 - (dot / (norm_a * norm_b.max(f32::EPSILON)))
            }
            DistanceMetric::L2 => {
                a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum::<f32>().sqrt()
            }
            DistanceMetric::InnerProduct => {
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                -dot
            }
        }
    }

    fn random_level(&self) -> usize {
        let ml = (self.config.M as f64).ln();
        RNG_STATE.with(|state| {
            let mut rng = state.borrow_mut();
            *rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let u = (*rng >> 33) as f64 / (1u64 << 31) as f64;
            (-u.ln() * ml) as usize
        })
    }

    /// Greedy search on a single layer, starting from `entry`.
    /// Returns the ef closest candidates.
    fn search_layer(
        &self,
        query: &[f32],
        entry: u64,
        ef: usize,
        layer: usize,
    ) -> Vec<Candidate> {
        let nodes = self.nodes.read();
        let embeddings = self.embeddings.read();

        let mut visited = std::collections::HashSet::new();
        visited.insert(entry);

        let mut candidates = BinaryHeap::new();
        let mut results = BinaryHeap::new();

        let dist = self.distance(query, &embeddings[entry as usize]);
        candidates.push(Reverse(Candidate { node_id: entry, distance: dist }));
        results.push(Candidate { node_id: entry, distance: dist });

        while let Some(Reverse(current)) = candidates.pop() {
            let furthest = results.peek().map(|c| c.distance).unwrap_or(f32::MAX);
            if current.distance > furthest {
                break;
            }

            if let Some(node) = nodes.get(current.node_id as usize) {
                if layer < node.neighbors.len() {
                    for &neighbor in &node.neighbors[layer] {
                        if visited.insert(neighbor) {
                            let d = self.distance(query, &embeddings[neighbor as usize]);
                            let furthest_in_results = results.peek().map(|c| c.distance).unwrap_or(f32::MAX);
                            if results.len() < ef || d < furthest_in_results {
                                candidates.push(Reverse(Candidate { node_id: neighbor, distance: d }));
                                results.push(Candidate { node_id: neighbor, distance: d });
                                if results.len() > ef {
                                    results.pop();
                                }
                            }
                        }
                    }
                }
            }
        }

        results.into_sorted_vec()
    }

    /// Select the M nearest neighbors using the simple heuristic.
    fn select_neighbors_simple(&self, candidates: &[Candidate], M: usize) -> Vec<Candidate> {
        let mut sorted = candidates.to_vec();
        sorted.sort();
        sorted.truncate(M);
        sorted
    }

    pub fn insert(&self, id: u64, embedding: Vec<f32>) {
        let level = self.random_level();

        // Phase A: Write node data and prepare entry point
        let ep = {
            let mut nodes = self.nodes.write();
            let mut embeddings = self.embeddings.write();
            let mut entry_point = self.entry_point.write();
            let mut max_level = self.max_level.write();

            let mut next_id = nodes.len() as u64;
            while nodes.len() <= id as usize {
                nodes.push(HnswNode {
                    id: next_id, level: 0, neighbors: Vec::new(),
                });
                embeddings.push(vec![0.0; self.config.dimension]);
                next_id += 1;
            }

            nodes[id as usize] = HnswNode {
                id, level,
                neighbors: vec![Vec::new(); level + 1],
            };
            embeddings[id as usize] = embedding;

            if entry_point.is_none() {
                *entry_point = Some(id);
                *max_level = level;
                return;
            }

            let ep = entry_point.unwrap();
            if level > *max_level {
                *max_level = level;
                *entry_point = Some(id);
            }
            ep
        };

        // Phase B: Traverse from top level down to level+1
        let top_level = *self.max_level.read();
        let new_embedding = self.embeddings.read()[id as usize].clone();
        let mut curr_entry = ep;

        for lvl in (level + 1..=top_level).rev() {
            let results = self.search_layer(&new_embedding, curr_entry, 1, lvl);
            if let Some(c) = results.first() {
                curr_entry = c.node_id;
            }
        }

        // Phase C: Insert at each layer
        let max_insert_level = std::cmp::min(level, top_level);
        for lvl in (0..=max_insert_level).rev() {
            let ef = if lvl == 0 { self.config.ef_construction } else { self.config.M };
            let candidates = self.search_layer(&new_embedding, curr_entry, ef, lvl);

            let M = if lvl == 0 { self.config.M_max0 } else { self.config.M_max };
            let neighbors = self.select_neighbors_simple(&candidates, M);
            let neighbor_ids: Vec<u64> = neighbors.iter().map(|c| c.node_id).collect();

            {
                let mut nodes = self.nodes.write();
                if let Some(node) = nodes.get_mut(id as usize) {
                    node.neighbors[lvl] = neighbor_ids.clone();
                }
            }

            // Bidirectional connections: clone embeddings to avoid borrow conflicts
            let emb_snapshot = self.embeddings.read().clone();
            for &neighbor_id in &neighbor_ids {
                let mut nodes = self.nodes.write();
                if let Some(neighbor) = nodes.get_mut(neighbor_id as usize) {
                    if lvl < neighbor.neighbors.len() {
                        neighbor.neighbors[lvl].push(id);
                        let max_for_layer = if lvl == 0 { self.config.M_max0 } else { self.config.M_max };
                        if neighbor.neighbors[lvl].len() > max_for_layer {
                            let dists: Vec<Candidate> = neighbor.neighbors[lvl].iter().map(|&nid| {
                                let d = self.distance(&emb_snapshot[neighbor_id as usize], &emb_snapshot[nid as usize]);
                                Candidate { node_id: nid, distance: d }
                            }).collect();
                            let selected = self.select_neighbors_simple(&dists, max_for_layer);
                            neighbor.neighbors[lvl] = selected.into_iter().map(|c| c.node_id).collect();
                        }
                    }
                }
            }

            if let Some(c) = candidates.first() {
                curr_entry = c.node_id;
            }
        }
    }

    /// Search the HNSW index for k approximate nearest neighbors.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
    ) -> Vec<(u64, f32)> {
        let entry_point = self.entry_point.read();
        let max_level = self.max_level.read();
        let nodes = self.nodes.read();

        let ep = match *entry_point {
            Some(ep) => ep,
            None => return Vec::new(),
        };

        if nodes.is_empty() || embeddings_is_empty(&self.embeddings) {
            return Vec::new();
        }

        let mut curr_entry = ep;

        // Phase 1: Traverse from top level down to 0
        for lvl in (1..=*max_level).rev() {
            let candidates = self.search_layer(query, curr_entry, 1, lvl);
            if let Some(c) = candidates.first() {
                curr_entry = c.node_id;
            }
        }

        // Phase 2: Search at layer 0 with ef_search
        let candidates = self.search_layer(query, curr_entry, self.config.ef_search, 0);

        let mut results: Vec<(u64, f32)> = candidates.into_iter()
            .map(|c| (c.node_id, c.distance))
            .collect();
        results.truncate(k);
        results
    }

    /// Bulk insert multiple embeddings.
    pub fn insert_batch(&self, ids: &[u64], embeddings: &[Vec<f32>]) {
        for (i, id) in ids.iter().enumerate() {
            if i < embeddings.len() {
                self.insert(*id, embeddings[i].clone());
            }
        }
    }
}

fn embeddings_is_empty(embeddings: &RwLock<Vec<Vec<f32>>>) -> bool {
    embeddings.read().is_empty()
}

/// Cosine distance function accessible for external use.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|v| v * v).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|v| v * v).sum::<f32>().sqrt();
    1.0 - (dot / (norm_a * norm_b.max(f32::EPSILON)))
}

/// L2 (Euclidean) distance function.
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum::<f32>().sqrt()
}

/// Inner product distance (negative dot product, so larger = closer).
pub fn inner_product_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    -dot
}
