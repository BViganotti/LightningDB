use crate::processor::arrow_utils;
use crate::Connection;
use serde::Serialize;

fn sq(s: &str) -> String {
    s.replace('\'', "\\'")
}

pub enum ConnectedDirection {
    Incoming,
    Outgoing,
}

#[derive(Debug, Clone, Serialize)]
pub struct ModuleCohesion {
    pub module_path: String,
    pub internal_edges: u64,
    pub external_edges: u64,
    pub cohesion_score: f64,
}

pub struct FusionApp;

impl FusionApp {
    /// Initialize fusion-specific schema (CSR indexes for graph traversal).
    pub fn init_fusion_schema(_conn: &Connection) -> Result<(), crate::LightningError> {
        // Schema initialization is handled by the database catalog on open.
        // No additional fusion-specific schema is needed.
        Ok(())
    }

    /// Find CodeNode IDs by exact name match.
    pub fn find_node_by_name(conn: &Connection, name: &str) -> Result<Vec<String>, crate::LightningError> {
        let q = format!("MATCH (n:CodeNode) WHERE n.name = '{}' RETURN n.id", sq(name));
        let result = conn.query(&q)?;
        let mut ids = Vec::new();
        for batch in &result.batches {
            if let Ok(col) = arrow_utils::str_col(batch, 0) {
                for i in 0..batch.num_rows() {
                    ids.push(col.value(i).to_string());
                }
            }
        }
        Ok(ids)
    }

    /// Find paths between two nodes (simple reachability — returns path descriptions).
    pub fn find_paths(
        conn: &Connection,
        source_id: &str,
        target_id: &str,
        _edge_types: &[&str],
    ) -> Result<Vec<String>, crate::LightningError> {
        // Simple direct connection check
        let q = format!(
            "MATCH (s:CodeNode {{id: '{}'}})-[r]->(t:CodeNode {{id: '{}'}}) RETURN type(r) as rel_type",
            source_id.replace('\'', ""),
            target_id.replace('\'', "")
        );
        let result = conn.query(&q)?;
        let mut paths = Vec::new();
        for batch in &result.batches {
            if let Ok(col) = arrow_utils::str_col(batch, 0) {
                for i in 0..batch.num_rows() {
                    paths.push(format!("{} -[{}]-> {}", source_id, col.value(i), target_id));
                }
            }
        }
        // Also check reverse direction
        let q = format!(
            "MATCH (t:CodeNode {{id: '{}'}})-[r]->(s:CodeNode {{id: '{}'}}) RETURN type(r) as rel_type",
            source_id.replace('\'', ""),
            target_id.replace('\'', "")
        );
        let result = conn.query(&q)?;
        for batch in &result.batches {
            if let Ok(col) = arrow_utils::str_col(batch, 0) {
                for i in 0..batch.num_rows() {
                    paths.push(format!("{} <-[{}]- {}", target_id, col.value(i), source_id));
                }
            }
        }
        if paths.is_empty() {
            paths.push(format!("{} → {}: no direct connection found", source_id, target_id));
        }
        Ok(paths)
    }

    /// Find connected node IDs by edge traversal.
    pub fn find_connected_nodes(
        conn: &Connection,
        node_id: &str,
        edge_types: &[&str],
        direction: ConnectedDirection,
    ) -> Result<Vec<String>, crate::LightningError> {
        let edges = edge_types.join("|");
        let q = match direction {
            ConnectedDirection::Incoming => format!(
                "MATCH (n:CodeNode {{id: '{}'}})<-[r:{edges}]-(connected:CodeNode) RETURN connected.id",
                sq(node_id)
            ),
            ConnectedDirection::Outgoing => format!(
                "MATCH (n:CodeNode {{id: '{}'}})-[r:{edges}]->(connected:CodeNode) RETURN connected.id",
                sq(node_id)
            ),
        };
        let result = conn.query(&q)?;
        let mut ids = Vec::new();
        for batch in &result.batches {
            if let Ok(col) = arrow_utils::str_col(batch, 0) {
                for i in 0..batch.num_rows() {
                    ids.push(col.value(i).to_string());
                }
            }
        }
        Ok(ids)
    }

    /// Look up (id, name, node_type) for a list of node IDs.
    pub fn lookup_node_names(
        conn: &Connection,
        ids: &[String],
    ) -> Result<Vec<(String, String, String)>, crate::LightningError> {
        let mut results = Vec::new();
        for node_id in ids {
            let q = format!(
                "MATCH (n:CodeNode {{id: '{}'}}) RETURN n.id, n.name, n.node_type",
                sq(node_id)
            );
            if let Ok(result) = conn.query(&q) {
                for batch in &result.batches {
                    if let (Ok(id_col), Ok(name_col), Ok(typ_col)) = (
                        arrow_utils::str_col(batch, 0),
                        arrow_utils::str_col(batch, 1),
                        arrow_utils::str_col(batch, 2),
                    ) {
                        for i in 0..batch.num_rows() {
                            results.push((
                                id_col.value(i).to_string(),
                                name_col.value(i).to_string(),
                                typ_col.value(i).to_string(),
                            ));
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    /// Store an observation in the Observation node table.
    pub fn add_observation(
        conn: &Connection,
        id: &str,
        content: &str,
        _parent_id: Option<&str>,
    ) -> Result<(), crate::LightningError> {
        let q = format!(
            "CREATE (o:Observation {{id: '{}', content: '{}', is_stale: false, created_at: '{}'}})",
            sq(&id),
            sq(&content).replace('\n', " "),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs().to_string())
                .unwrap_or_default()
        );
        conn.execute(&q, None)?;
        Ok(())
    }

    /// Get recent observation content strings.
    pub fn get_recent_observations(
        conn: &Connection,
        limit: usize,
    ) -> Result<Vec<String>, crate::LightningError> {
        let q = format!(
            "MATCH (o:Observation) WHERE o.is_stale = false RETURN o.content ORDER BY o.created_at DESC LIMIT {limit}"
        );
        let result = conn.query(&q)?;
        let mut observations = Vec::new();
        for batch in &result.batches {
            if let Ok(col) = arrow_utils::str_col(batch, 0) {
                for i in 0..batch.num_rows() {
                    observations.push(col.value(i).to_string());
                }
            }
        }
        Ok(observations)
    }

    /// Compute architecture cohesion metrics from the module graph.
    pub fn compute_architecture_cohesion(
        conn: &Connection,
    ) -> Result<Vec<ModuleCohesion>, crate::LightningError> {
        let relation_types = ["Calls", "Imports", "References", "Implements", "Contains", "Extends"];
        let mut module_map: std::collections::HashMap<String, (u64, u64)> = std::collections::HashMap::new();
        for rel_type in &relation_types {
            let q = format!(
                "MATCH (n:CodeNode)-[r:{rel_type}]-(m:CodeNode) \
                 WITH n.file_path AS nf, m.file_path AS mf \
                 WITH split(nf, '/') AS np, split(mf, '/') AS mp \
                 WITH np[0] AS n_mod, mp[0] AS m_mod \
                 WHERE n_mod IS NOT NULL AND m_mod IS NOT NULL \
                 RETURN n_mod, m_mod, count(*) AS edge_count \
                 ORDER BY n_mod"
            );
            if let Ok(rows) = conn.query(&q) {
                for batch in &rows.batches {
                    if let (Ok(src_col), Ok(dst_col), Ok(cnt_col)) = (
                        arrow_utils::str_col(batch, 0),
                        arrow_utils::str_col(batch, 1),
                        arrow_utils::i64_col(batch, 2),
                    ) {
                        for i in 0..batch.num_rows() {
                            let src_mod = src_col.value(i).to_string();
                            let dst_mod = dst_col.value(i).to_string();
                            let count = cnt_col.value(i) as u64;
                            let same_module = src_mod == dst_mod;
                            let (internal, external) = module_map.entry(src_mod).or_insert((0, 0));
                            if same_module {
                                *internal += count;
                            } else {
                                *external += count;
                            }
                        }
                    }
                }
            }
        }
        let mut results: Vec<ModuleCohesion> = module_map
            .into_iter()
            .map(|(module_path, (internal_edges, external_edges))| {
                let total = internal_edges + external_edges;
                let cohesion_score = if total > 0 {
                    internal_edges as f64 / total as f64
                } else {
                    0.0
                };
                ModuleCohesion { module_path, internal_edges, external_edges, cohesion_score }
            })
            .collect();
        results.sort_by(|a, b| b.cohesion_score.partial_cmp(&a.cohesion_score).unwrap_or(std::cmp::Ordering::Equal));
        Ok(results)
    }

    /// Export the entire graph as D3 JSON format.
    pub fn export_to_d3_json(
        conn: &Connection,
    ) -> Result<String, crate::LightningError> {
        // Export nodes
        let nodes_result = conn.query("MATCH (n:CodeNode) RETURN n.id, n.name, n.node_type LIMIT 10000")?;
        let mut nodes = Vec::new();
        for batch in &nodes_result.batches {
            if let (Ok(id_col), Ok(name_col), Ok(typ_col)) = (
                arrow_utils::str_col(batch, 0),
                arrow_utils::str_col(batch, 1),
                arrow_utils::str_col(batch, 2),
            ) {
                for i in 0..batch.num_rows() {
                    nodes.push(serde_json::json!({
                        "id": id_col.value(i),
                        "name": name_col.value(i),
                        "type": typ_col.value(i),
                    }));
                }
            }
        }
        // Export links
        let links_result = conn.query(
            "MATCH (s:CodeNode)-[r]->(t:CodeNode) RETURN s.id, t.id, type(r) AS rel_type LIMIT 50000",
        )?;
        let mut links = Vec::new();
        for batch in &links_result.batches {
            if let (Ok(src_col), Ok(tgt_col), Ok(typ_col)) = (
                arrow_utils::str_col(batch, 0),
                arrow_utils::str_col(batch, 1),
                arrow_utils::str_col(batch, 2),
            ) {
                for i in 0..batch.num_rows() {
                    links.push(serde_json::json!({
                        "source": src_col.value(i),
                        "target": tgt_col.value(i),
                        "type": typ_col.value(i),
                    }));
                }
            }
        }
        let graph = serde_json::json!({ "nodes": nodes, "links": links });
        Ok(serde_json::to_string(&graph).unwrap_or_default())
    }

    /// Recompute PageRank scores for all nodes.
    pub fn materialize_pagerank(
        conn: &Connection,
    ) -> Result<(), crate::LightningError> {
        // Damping factor
        let damping = 0.85;
        let max_iterations = 100;
        let convergence_threshold = 0.0001;

        // Get total node count
        let count_result = conn.query("MATCH (n:CodeNode) RETURN count(n.id) AS cnt")?;
        let total_nodes = count_result.batches.first()
            .and_then(|b| arrow_utils::i64_col(b, 0).ok())
            .map(|c| c.value(0))
            .unwrap_or(0);

        if total_nodes == 0 {
            return Err(crate::LightningError::Internal("No nodes to rank".into()));
        }

        let total = total_nodes as f64;
        let mut ranks: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        let damping_leak = (1.0 - damping) / total;

        // Get all node IDs and initialize ranks
        let id_result = conn.query("MATCH (n:CodeNode) RETURN n.id")?;
        let mut all_ids: Vec<String> = Vec::new();
        for batch in &id_result.batches {
            if let Ok(col) = arrow_utils::str_col(batch, 0) {
                for i in 0..batch.num_rows() {
                    let id = col.value(i).to_string();
                    ranks.insert(id.clone(), 1.0 / total);
                    all_ids.push(id);
                }
            }
        }

        // Iterate PageRank
        for _iteration in 0..max_iterations {
            let mut new_ranks: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
            for id in &all_ids {
                new_ranks.insert(id.clone(), damping_leak);
            }

            // For each node, distribute its rank to its outgoing neighbors
            for id in &all_ids {
                let rank = ranks.get(id).copied().unwrap_or(0.0);
                let share = rank * damping;

                let q = format!(
                    "MATCH (n:CodeNode {{id: '{}'}})-[r]->(t:CodeNode) RETURN t.id",
                    id.replace('\'', "")
                );
                if let Ok(result) = conn.query(&q) {
                    let mut neighbors: Vec<String> = Vec::new();
                    for batch in &result.batches {
                        if let Ok(col) = arrow_utils::str_col(batch, 0) {
                            for i in 0..batch.num_rows() {
                                neighbors.push(col.value(i).to_string());
                            }
                        }
                    }
                    if neighbors.is_empty() {
                        // Dangling node: distribute to all nodes
                        let per_node = share / total;
                        for nid in &all_ids {
                            *new_ranks.entry(nid.clone()).or_insert(0.0) += per_node;
                        }
                    } else {
                        let per_neighbor = share / neighbors.len() as f64;
                        for nid in &neighbors {
                            *new_ranks.entry(nid.clone()).or_insert(0.0) += per_neighbor;
                        }
                    }
                }
            }

            // Check convergence
            let mut diff = 0.0;
            for id in &all_ids {
                diff += (ranks.get(id).copied().unwrap_or(0.0)
                    - new_ranks.get(id).copied().unwrap_or(0.0))
                    .abs();
            }
            ranks = new_ranks;
            if diff < convergence_threshold {
                break;
            }
        }

        // Store ranks back to DB
        for (id, rank) in &ranks {
            let q = format!(
                "MATCH (n:CodeNode {{id: '{}'}}) SET n.page_rank = {}",
                id.replace('\'', ""),
                rank
            );
            let _ = conn.execute(&q, None);
        }

        Ok(())
    }
}
