use crate::processor::arrow_utils;
use crate::processor::Value;
use crate::Connection;
use serde::Serialize;
use std::collections::HashMap;

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
    ///
    /// Scans the catalog for the CodeNode and CodeRel tables and creates CSR
    /// (compressed sparse row) indexes on any relationship table that lacks one.
    /// CSR indexes enable efficient graph traversal (BFS, PageRank, path finding)
    /// without full-table scans on every hop. If neither table exists this is a
    /// safe no-op -- the function can be called unconditionally at startup.
    pub fn init_fusion_schema(conn: &Connection) -> Result<(), crate::LightningError> {
        let db = &conn.client_context.database;
        let catalog = db.catalog.read();

        // Check for the expected fusion tables; only create CSR indexes on rel tables.
        let fusion_tables: Vec<String> = catalog
            .node_tables
            .keys()
            .chain(catalog.rel_tables.keys())
            .filter(|n| n.starts_with("Code"))
            .cloned()
            .collect();
        drop(catalog);

        for name in &fusion_tables {
            let mut sm = db.storage_manager.write();
            if !sm.fwd_csr.contains_key(name) && !sm.bwd_csr.contains_key(name) {
                if let Err(e) = sm.create_csr(name) {
                    tracing::warn!(
                        "init_fusion_schema: failed to create CSR index for '{}': {}",
                        name, e
                    );
                } else {
                    tracing::info!(
                        "init_fusion_schema: created CSR index for '{}'",
                        name
                    );
                }
            }
        }

        Ok(())
    }

    /// Find CodeNode IDs by exact name match.
    pub fn find_node_by_name(conn: &Connection, name: &str) -> Result<Vec<String>, crate::LightningError> {
        let q = "MATCH (n:CodeNode) WHERE n.name = $name RETURN n.id".to_string();
        let mut params = HashMap::new();
        params.insert("name".to_string(), Value::String(name.to_string()));
        let result = conn.execute(&q, Some(params))?;
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
        edge_types: &[&str],
    ) -> Result<Vec<String>, crate::LightningError> {
        // Validate and build relationship type filter
        for et in edge_types {
            if et.is_empty() || et.contains(|c: char| !c.is_alphanumeric() && c != '_') {
                return Err(crate::LightningError::Internal(format!(
                    "Invalid edge type: '{}' must be alphanumeric", et
                )));
            }
        }
        let edges = edge_types.join("|");
        let rel_pattern = if edges.is_empty() {
            "r".to_string()
        } else {
            format!("r:{edges}")
        };

        let mut paths = Vec::new();

        // Simple direct connection check (forward direction)
        {
            let q = format!(
                "MATCH (s:CodeNode {{id: $source_id}})-[{rel_pattern}]->(t:CodeNode {{id: $target_id}}) RETURN type(r) as rel_type"
            );
            let mut params = HashMap::new();
            params.insert("source_id".to_string(), Value::String(source_id.to_string()));
            params.insert("target_id".to_string(), Value::String(target_id.to_string()));
            let result = conn.execute(&q, Some(params))?;
            for batch in &result.batches {
                if let Ok(col) = arrow_utils::str_col(batch, 0) {
                    for i in 0..batch.num_rows() {
                        paths.push(format!("{} -[{}]-> {}", source_id, col.value(i), target_id));
                    }
                }
            }
        }

        // Also check reverse direction
        {
            let q = format!(
                "MATCH (t:CodeNode {{id: $source_id}})-[{rel_pattern}]->(s:CodeNode {{id: $target_id}}) RETURN type(r) as rel_type"
            );
            let mut params = HashMap::new();
            params.insert("source_id".to_string(), Value::String(target_id.to_string()));
            params.insert("target_id".to_string(), Value::String(source_id.to_string()));
            let result = conn.execute(&q, Some(params))?;
            for batch in &result.batches {
                if let Ok(col) = arrow_utils::str_col(batch, 0) {
                    for i in 0..batch.num_rows() {
                        paths.push(format!("{} <-[{}]- {}", target_id, col.value(i), source_id));
                    }
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
        // Validate edge types to prevent Cypher injection in relationship patterns
        for et in edge_types {
            if et.is_empty() || et.contains(|c: char| !c.is_alphanumeric() && c != '_') {
                return Err(crate::LightningError::Internal(format!(
                    "Invalid edge type: '{}' must be alphanumeric", et
                )));
            }
        }
        let edges = edge_types.join("|");
        let edges_pattern = if edges.is_empty() { "r".to_string() } else { format!("r:{edges}") };
        let q = match direction {
            ConnectedDirection::Incoming => format!(
                "MATCH (n:CodeNode {{id: $node_id}})<-[{edges_pattern}]-(connected:CodeNode) RETURN connected.id"
            ),
            ConnectedDirection::Outgoing => format!(
                "MATCH (n:CodeNode {{id: $node_id}})-[{edges_pattern}]->(connected:CodeNode) RETURN connected.id"
            ),
        };
        let mut params = HashMap::new();
        params.insert("node_id".to_string(), Value::String(node_id.to_string()));
        let result = conn.execute(&q, Some(params))?;
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
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids_list: Vec<Value> = ids.iter().map(|id| Value::String(id.clone())).collect();
        let q = "WITH $ids AS id_list UNWIND id_list AS id MATCH (n:CodeNode) WHERE n.id = id RETURN n.id, n.name, n.node_type".to_string();
        let mut params = HashMap::new();
        params.insert("ids".to_string(), Value::List(ids_list));
        let mut results = Vec::new();
        if let Ok(result) = conn.execute(&q, Some(params)) {
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
        Ok(results)
    }

    /// Store an observation in the Observation node table.
    pub fn add_observation(
        conn: &Connection,
        id: &str,
        content: &str,
        parent_id: Option<&str>,
    ) -> Result<(), crate::LightningError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        if let Some(pid) = parent_id {
            let q = "CREATE (o:Observation {id: $id, content: $content, is_stale: false, created_at: $created_at}) \
                     WITH o MATCH (p:CodeNode {id: $parent_id}) CREATE (p)-[:HAS_OBSERVATION]->(o)".to_string();
            let mut params = HashMap::new();
            params.insert("id".to_string(), Value::String(id.to_string()));
            params.insert("content".to_string(), Value::String(content.replace('\n', " ")));
            params.insert("created_at".to_string(), Value::Timestamp(now));
            params.insert("parent_id".to_string(), Value::String(pid.to_string()));
            conn.execute(&q, Some(params))?;
        } else {
            let q = "CREATE (o:Observation {id: $id, content: $content, is_stale: false, created_at: $created_at})".to_string();
            let mut params = HashMap::new();
            params.insert("id".to_string(), Value::String(id.to_string()));
            params.insert("content".to_string(), Value::String(content.replace('\n', " ")));
            params.insert("created_at".to_string(), Value::Timestamp(now));
            conn.execute(&q, Some(params))?;
        }
        Ok(())
    }

    /// Get recent observation content strings.
    pub fn get_recent_observations(
        conn: &Connection,
        limit: usize,
    ) -> Result<Vec<String>, crate::LightningError> {
        let limit = limit.min(10000);
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
        let q = "\
MATCH (n:CodeNode)-[r]-(m:CodeNode) \
WITH n.file_path AS nf, m.file_path AS mf \
WHERE nf IS NOT NULL AND mf IS NOT NULL \
WITH CASE WHEN nf ENDS_WITH '.rs' THEN LEFT(nf, LENGTH(nf) - 3) ELSE nf END AS n_clean, \
     CASE WHEN mf ENDS_WITH '.rs' THEN LEFT(mf, LENGTH(mf) - 3) ELSE mf END AS m_clean \
WITH n_clean AS n_mod, m_clean AS m_mod \
WHERE n_mod IS NOT NULL AND m_mod IS NOT NULL \
RETURN n_mod, m_mod, count(*) AS edge_count \
ORDER BY n_mod".to_string();
        let mut module_map: std::collections::HashMap<String, (u64, u64)> = std::collections::HashMap::new();
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
        let json = serde_json::to_string(&graph)
            .map_err(|e| crate::LightningError::Internal(format!("Failed to serialize graph: {e}")))?;
        Ok(json)
    }

    /// Recompute PageRank scores for all nodes.
    /// Loads the entire adjacency graph into memory once, computes locally in Rust,
    /// and writes back ranks in bulk. This eliminates the O(N × iterations) query
    /// storm where each node-per-iteration issued a separate MATCH query.
    pub fn materialize_pagerank(
        conn: &Connection,
    ) -> Result<(), crate::LightningError> {
        let damping = 0.85;
        let max_iterations = 100;
        let convergence_threshold = 0.0001;

        // Load all node IDs and initialize ranks
        let id_result = conn.query("MATCH (n:CodeNode) RETURN n.id")?;
        let mut all_ids: Vec<String> = Vec::new();
        let mut ranks: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for batch in &id_result.batches {
            if let Ok(col) = arrow_utils::str_col(batch, 0) {
                for i in 0..batch.num_rows() {
                    let id = col.value(i).to_string();
                    all_ids.push(id.clone());
                }
            }
        }
        if all_ids.is_empty() {
            return Err(crate::LightningError::Internal("No nodes to rank".into()));
        }
        let total = all_ids.len() as f64;
        let initial = 1.0 / total;
        for id in &all_ids {
            ranks.insert(id.clone(), initial);
        }

        // Load entire adjacency graph into memory: one query for all edges
        let mut adjacency: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let edge_result = conn.query(
            "MATCH (n:CodeNode)-[r]->(t:CodeNode) RETURN n.id, t.id"
        )?;
        for batch in &edge_result.batches {
            if let (Ok(src_col), Ok(dst_col)) = (
                arrow_utils::str_col(batch, 0),
                arrow_utils::str_col(batch, 1),
            ) {
                for i in 0..batch.num_rows() {
                    let src = src_col.value(i).to_string();
                    let dst = dst_col.value(i).to_string();
                    adjacency.entry(src).or_default().push(dst);
                }
            }
        }

        let damping_leak = (1.0 - damping) / total;

        // Iterate PageRank entirely in memory
        for _iteration in 0..max_iterations {
            let mut new_ranks: std::collections::HashMap<String, f64> =
                std::collections::HashMap::new();
            for id in &all_ids {
                new_ranks.insert(id.clone(), damping_leak);
            }

            for id in &all_ids {
                let rank = ranks.get(id).copied().unwrap_or(0.0);
                let share = rank * damping;

                if let Some(neighbors) = adjacency.get(id) {
                    if neighbors.is_empty() {
                        let per_node = share / total;
                        for nid in &all_ids {
                            *new_ranks.entry(nid.clone()).or_insert(0.0) += per_node;
                        }
                    } else {
                        let per_neighbor = share / neighbors.len() as f64;
                        for nid in neighbors {
                            *new_ranks.entry(nid.clone()).or_insert(0.0) += per_neighbor;
                        }
                    }
                } else {
                    let per_node = share / total;
                    for nid in &all_ids {
                        *new_ranks.entry(nid.clone()).or_insert(0.0) += per_node;
                    }
                }
            }

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

        // Bulk write back all ranks using paired UNWIND on a list of {id, rank} structs.
        // Each node gets its own computed rank — unlike the previous buggy approach
        // where $ranks[0] was applied to every matched node, giving all nodes the same rank.
        if !all_ids.is_empty() {
            let mut updates: Vec<Value> = Vec::with_capacity(all_ids.len());
            for id in &all_ids {
                if let Some(rank) = ranks.get(id) {
                    updates.push(Value::Struct(vec![
                        ("id".to_string(), Value::String(id.clone())),
                        ("rank".to_string(), Value::Number(*rank)),
                    ]));
                }
            }
            if !updates.is_empty() {
                let batch_update = "UNWIND $updates AS row MATCH (n:CodeNode {id: row.id}) \
                     SET n.page_rank = row.rank".to_string();
                let mut params = HashMap::new();
                params.insert("updates".to_string(), Value::List(updates));
                conn.execute(&batch_update, Some(params))
                    .map_err(|e| crate::LightningError::Internal(format!(
                        "PageRank writeback failed: {e}"
                    )))?;
            }
        }

        Ok(())
    }
}
