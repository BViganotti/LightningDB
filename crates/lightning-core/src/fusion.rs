use crate::processor::Value;
use crate::Result;
use arrow::array::{Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct ModuleCohesion {
    pub module_path: String,
    pub internal_edges: u64,
    pub external_edges: u64,
    pub cohesion_score: f64,
}

#[derive(Debug, Clone)]
pub struct GraphNode {
    pub id: u64,
    pub name: String,
    pub node_type: String,
}

#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub source_id: u64,
    pub target_id: u64,
    pub edge_type: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectedDirection {
    Incoming,
    Outgoing,
    Both,
}

pub struct FusionApp;

impl FusionApp {
    /// Extremely fast full graph export using Arrow zero-copy scans
    pub fn export_graph(conn: &crate::Connection) -> Result<(Vec<GraphNode>, Vec<GraphEdge>)> {
        let db = conn.client_context.database.clone();
        let storage = db.storage_manager.read();

        let mut nodes = Vec::new();
        if let Some(_) = storage.get_table("CodeNode") {
            let res = conn.query("MATCH (n:CodeNode) RETURN n._id, n.name, n.node_type")?;
            for batch in res.batches {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let names = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let types = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    nodes.push(GraphNode {
                        id: ids.value(i),
                        name: names.value(i).to_string(),
                        node_type: types.value(i).to_string(),
                    });
                }
            }
        }

        let mut edges = Vec::new();
        let rel_tables = [
            "Calls",
            "Imports",
            "Implements",
            "References",
            "PreciseDefines",
        ];
        for rel in rel_tables {
            let query = format!(
                "MATCH (a:CodeNode)-[r:{}]->(b:CodeNode) RETURN a._id, b._id",
                rel
            );
            if let Ok(res) = conn.query(&query) {
                for batch in res.batches {
                    let srcs = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    let dsts = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    for i in 0..batch.num_rows() {
                        edges.push(GraphEdge {
                            source_id: srcs.value(i),
                            target_id: dsts.value(i),
                            edge_type: rel.to_string(),
                        });
                    }
                }
            }
        }
        Ok((nodes, edges))
    }

    /// Fast architecture analysis using Arrow columns to compute cohesion
    /// Uses combined edge query to reduce 4 queries to 2
    pub fn architecture_map(conn: &crate::Connection) -> Result<Vec<ModuleCohesion>> {
        let mut module_map: HashMap<u64, String> = HashMap::new();
        let res = conn.query("MATCH (n:CodeNode) RETURN n._id, n.file_path")?;
        for batch in res.batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let paths = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                if paths.is_null(i) {
                    continue;
                }
                let path = paths.value(i);
                let module = match path.rfind('/') {
                    Some(idx) => &path[0..idx],
                    None => path,
                };
                module_map.insert(ids.value(i), module.to_string());
            }
        }

        // Query each edge type separately to avoid parser issues with combined patterns
        let mut metrics: HashMap<String, (u64, u64)> = HashMap::new();
        let edge_types = ["Calls", "Imports", "References"];
        for edge_type in edge_types {
            let edge_query = format!(
                "MATCH (a:CodeNode)-[:{}]->(b:CodeNode) RETURN a._id, b._id",
                edge_type
            );

            if let Ok(res) = conn.query(&edge_query) {
                for batch in res.batches {
                    if let (Some(srcs), Some(dsts)) = (
                        batch.column(0).as_any().downcast_ref::<UInt64Array>(),
                        batch.column(1).as_any().downcast_ref::<UInt64Array>(),
                    ) {
                        for i in 0..batch.num_rows() {
                            let src_id = srcs.value(i);
                            let dst_id = dsts.value(i);
                            if let (Some(src_mod), Some(dst_mod)) =
                                (module_map.get(&src_id), module_map.get(&dst_id))
                            {
                                let entry = metrics.entry(src_mod.clone()).or_insert((0, 0));
                                if src_mod == dst_mod {
                                    entry.0 += 1;
                                } else {
                                    entry.1 += 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut results = Vec::with_capacity(metrics.len());
        for (module, (internal, external)) in metrics {
            let total = internal + external;
            let cohesion = if total == 0 {
                0.0
            } else {
                (internal as f64) / (total as f64)
            };
            results.push(ModuleCohesion {
                module_path: module,
                internal_edges: internal,
                external_edges: external,
                cohesion_score: cohesion,
            });
        }
        results.sort_by(|a, b| a.cohesion_score.partial_cmp(&b.cohesion_score).unwrap());
        Ok(results)
    }

    /// Single-source multi-hop expansion for agentic retrieval (Parameterized)
    pub fn agentic_expansion(
        conn: &crate::Connection,
        seed_ids: &[u64],
    ) -> Result<Vec<(u64, u32)>> {
        let mut results = Vec::new();
        let mut params = HashMap::new();
        params.insert(
            "seeds".to_string(),
            Value::List(seed_ids.iter().map(|&id| Value::Node(id)).collect()),
        );

        // In Lightning, relationship patterns also need a variable if they have bounds
        let query = "MATCH (seed:CodeNode)-[r:Calls|Imports|References*1..2]->(expanded:CodeNode) \
                     WHERE seed._id IN $seeds \
                     RETURN DISTINCT expanded._id, 1"; // Simplified depth for now

        if let Ok(res) = conn.execute(query, Some(params)) {
            for batch in res.batches {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let depths = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    results.push((ids.value(i), depths.value(i)));
                }
            }
        }
        Ok(results)
    }

    /// Symbol resolution and fuzzy matching for verification
    pub fn verify_symbol(conn: &crate::Connection, symbol: &str) -> Result<Vec<String>> {
        let query = format!(
            "MATCH (n:CodeNode {{name: '{}'}}) RETURN n.name LIMIT 1",
            symbol
        );
        let res = conn.query(&query)?;
        if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
            return Ok(vec![symbol.to_string()]);
        }

        let storage = conn.client_context.database.storage_manager.read();
        if let Some(fts) = storage.fts_indexes.get("CodeNode") {
            let tx = conn
                .client_context
                .database
                .transaction_manager
                .begin(true)?;
            let fuzzy_res =
                fts.search(symbol, 3, &conn.client_context.database.buffer_manager, &tx)?;
            let mut suggestions = Vec::new();
            for (id, _) in fuzzy_res {
                let lookup = format!("MATCH (n:CodeNode) WHERE n._id = {} RETURN n.name", id);
                if let Ok(r) = conn.query(&lookup) {
                    for b in r.batches {
                        let names = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                        for i in 0..b.num_rows() {
                            suggestions.push(names.value(i).to_string());
                        }
                    }
                }
            }
            return Ok(suggestions);
        }
        Ok(vec![])
    }

    /// Initialize the entire Fusion MCP schema
    /// Skips tables that already exist to preserve cardinality and data
    pub fn init_fusion_schema(conn: &crate::Connection) -> Result<()> {
        // Check storage manager for existing tables (more reliable than catalog,
        // since old code created tables directly in storage_manager without catalog entries)
        let db = conn.client_context.database.clone();
        let existing_tables: std::collections::HashSet<String> = {
            let storage = db.storage_manager.read();
            storage.node_tables.keys().chain(storage.rel_tables.keys()).cloned().collect()
        };

        let ddl = [
            "CREATE NODE TABLE CodeNode (id STRING, workspace_id STRING, node_type STRING, name STRING, file_path STRING, start_line INT64, start_col INT64, end_line INT64, docstring STRING, signature STRING, source_hash STRING, PRIMARY KEY (id))",
            "CREATE NODE TABLE Workspace (id STRING, name STRING, root_path STRING, last_indexed_at STRING, PRIMARY KEY (id))",
            "CREATE NODE TABLE Observation (id STRING, content STRING, is_stale BOOL, created_at STRING, PRIMARY KEY (id))",
            "CREATE NODE TABLE FileHash (file_path STRING, hash STRING, workspace_id STRING, PRIMARY KEY (file_path))",
            "CREATE NODE TABLE EnrichmentProgress (workspace_id STRING, enrichment_type STRING, files_processed INT64, total_files INT64, status STRING, PRIMARY KEY (workspace_id))",
            "CREATE NODE TABLE WatcherStats (id INT64, last_update STRING, files_processed INT64, duration_ms INT64, PRIMARY KEY (id))",
            "CREATE REL TABLE Calls (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE Imports (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE Implements (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE References (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE Contains (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE PreciseDefines (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE PreciseRefs (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE LspDefines (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE LspRefs (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE LspCalls (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE LspTypeDefines (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE LspImplements (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE Extends (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE ImplicitCouples (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE EnvContracts (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE ApiContracts (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE CrossRepos (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE ScipRefs (FROM CodeNode TO CodeNode)",
            "CREATE REL TABLE ObservedAt (FROM Observation TO CodeNode)",
            "CREATE REL TABLE BelongsTo (FROM CodeNode TO Workspace)",
        ];

        for query in ddl {
            // Extract table name from CREATE statement to check existence
            let table_name = query.split_whitespace()
                .nth(3) // CREATE NODE/REL TABLE <name>
                .unwrap_or("");
            if !table_name.is_empty() && existing_tables.contains(table_name) {
                continue; // Skip tables that already have data + metadata
            }
            let _ = conn.query(query);
        }

        // Ensure all edge tables have catalog entries (needed by sync_catalog_stats
        // and index_health). Old code created tables directly in storage manager
        // without catalog entries. When adding entries, compute cardinality from
        // file size since storage manager was loaded with 0 cardinality from catalog.
        let db = conn.client_context.database.clone();
        {
            let mut cat = db.catalog.write();
            let storage = db.storage_manager.read();
            for name in ["Calls", "Imports", "Implements", "References", "Contains",
                          "PreciseDefines", "PreciseRefs", "LspDefines", "LspRefs",
                          "LspCalls", "LspTypeDefines", "LspImplements", "Extends",
                          "ImplicitCouples", "EnvContracts", "ApiContracts", "CrossRepos",
                          "ScipRefs", "ObservedAt", "BelongsTo", "HasMessage"] {
                if cat.get_rel_table(name).is_none() {
                    if let Some(table) = storage.rel_tables.get(name) {
                        // Compute cardinality from file size since in-memory stats may be 0
                        let actual_rows = if !table.columns.is_empty() {
                            let fs = table.columns[0].fh.get_file_size();
                            let es = table.columns[0].element_size();
                            if es > 0 && fs > 0 { fs / es as u64 } else { 0 }
                        } else { 0 };
                        cat.add_rel_table(name.to_string(), "CodeNode".to_string(), "CodeNode".to_string(), vec![]);
                        if let Some(entry) = cat.get_rel_table_mut(name) {
                            entry.stats = table.stats.read().clone();
                            entry.stats.cardinality = actual_rows;
                            entry.num_rows = actual_rows;
                        }
                        tracing::info!("Added catalog entry for edge table {} with {} rows", name, actual_rows);
                    }
                }
            }
            db.catalog.mark_dirty();
        }

        // Create CSR indexes for all relationship tables
        let rel_tables = [
            "Calls",
            "Imports",
            "Implements",
            "References",
            "Contains",
            "PreciseDefines",
            "PreciseRefs",
            "LspDefines",
            "LspRefs",
            "LspCalls",
            "LspTypeDefines",
            "LspImplements",
            "Extends",
            "ImplicitCouples",
            "EnvContracts",
            "ApiContracts",
            "CrossRepos",
            "ScipRefs",
            "ObservedAt",
            "BelongsTo",
        ];
        let db = conn.client_context.database.clone();
        let mut storage = db.storage_manager.write();
        for table_name in rel_tables {
            if storage.get_table(table_name).is_some() {
                // Check if CSR already exists
                if !storage.fwd_csr.contains_key(table_name) {
                    if let Err(e) = storage.create_csr(table_name) {
                        eprintln!("Warning: failed to create CSR for {}: {}", table_name, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Add an observation linked to a node
    pub fn add_observation(
        conn: &crate::Connection,
        id: &str,
        content: &str,
        node_id: Option<u64>,
    ) -> Result<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().to_string())
            .unwrap_or_else(|_| "0".to_string());
        let query = format!(
            "CREATE (o:Observation {{id: '{}', content: '{}', is_stale: false, created_at: '{}'}})",
            id, content, timestamp
        );
        conn.query(&query)?;
        if let Some(nid) = node_id {
            let link = format!("MATCH (o:Observation {{id: '{}'}}), (n:CodeNode) WHERE n._id = {} CREATE (o)-[:ObservedAt]->(n)", id, nid);
            conn.query(&link)?;
        }
        Ok(())
    }

    /// Retrieve session context (recent observations)
    pub fn get_recent_observations(conn: &crate::Connection, limit: usize) -> Result<Vec<String>> {
        let query = format!(
            "MATCH (o:Observation) RETURN o.content ORDER BY o.created_at DESC LIMIT {}",
            limit
        );
        let res = conn.query(&query)?;
        let mut observations = Vec::new();
        for batch in res.batches {
            let contents = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                observations.push(contents.value(i).to_string());
            }
        }
        Ok(observations)
    }

    /// Get all file hashes for staleness check
    pub fn get_file_hashes(
        conn: &crate::Connection,
        workspace_id: &str,
    ) -> Result<HashMap<String, String>> {
        let query = format!(
            "MATCH (fh:FileHash {{workspace_id: '{}'}}) RETURN fh.file_path, fh.hash",
            workspace_id
        );
        let res = conn.query(&query)?;
        let mut hashes = HashMap::new();
        for batch in res.batches {
            let paths = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let hashes_arr = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                hashes.insert(paths.value(i).to_string(), hashes_arr.value(i).to_string());
            }
        }
        Ok(hashes)
    }

    /// Hybrid search combining FTS and Vector search with Reciprocal Rank Fusion (RRF)
    pub fn hybrid_search(
        conn: &crate::Connection,
        text_query: &str,
        vector_query: &[f32; 768],
        limit: usize,
    ) -> Result<Vec<(u64, f64)>> {
        let db = conn.client_context.database.clone();
        let storage = db.storage_manager.read();
        let bm = &db.buffer_manager;
        let tx = db.transaction_manager.begin(true)?;
        let mut fts_results = Vec::new();
        if let Some(fts) = storage.fts_indexes.get("CodeNode") {
            fts_results = fts.search(text_query, limit * 2, bm, &tx)?;
        }
        let mut vec_results = Vec::new();
        if let Some(vec_idx) = storage.vector_indexes.get("CodeNode") {
            vec_results = vec_idx.search(vector_query, limit * 2, bm, &tx)?;
        }
        let mut scores: HashMap<u64, f64> = HashMap::new();
        let k = 60.0;
        for (rank, (id, _)) in fts_results.iter().enumerate() {
            let entry = scores.entry(*id).or_insert(0.0);
            *entry += 1.0 / (k + (rank as f64) + 1.0);
        }
        for (rank, (id, _)) in vec_results.iter().enumerate() {
            let entry = scores.entry(*id).or_insert(0.0);
            *entry += 1.0 / (k + (rank as f64) + 1.0);
        }
        let mut fused: Vec<(u64, f64)> = scores.into_iter().collect();
        fused.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        fused.truncate(limit);
        Ok(fused)
    }

    /// Context Bundle Construction: Fetch neighbors via graph relationships
    /// Uses CSR indexes directly for O(1) neighbor lookups instead of query language
    pub fn get_context_bundle(conn: &crate::Connection, pivot_id: u64) -> Result<Vec<u64>> {
        let mut bundle_ids = HashSet::new();
        bundle_ids.insert(pivot_id);

        let db = conn.client_context.database.clone();
        let tx = db.transaction_manager.begin(true)?;
        let bm = &db.buffer_manager;

        let edge_types = ["Calls", "Imports", "References", "Contains"];
        {
            let mut storage = db.storage_manager.write();
            for edge_type in edge_types {
                let _ = storage.rebuild_csr_if_stale(edge_type, bm, &tx);
                if let Some(csr) = storage.fwd_csr.get(edge_type) {
                    let _ = csr.for_each_neighbor(bm, pivot_id, &tx, |n| { bundle_ids.insert(n); });
                }
                if let Some(csr) = storage.bwd_csr.get(edge_type) {
                    let _ = csr.for_each_neighbor(bm, pivot_id, &tx, |n| { bundle_ids.insert(n); });
                }
            }
        }
        db.transaction_manager.rollback(&db, &tx)?;

        Ok(bundle_ids.into_iter().collect())
    }

    /// Materialize PageRank scores into the CodeNode.pagerank property
    /// Uses bulk UNWIND-based update instead of per-node SET queries
    pub fn materialize_pagerank(conn: &crate::Connection) -> Result<()> {
        let query = "MATCH (n:CodeNode) RETURN n._id, pagerank(n)";
        let res = conn.query(query)?;

        // Collect all (id, score) pairs
        let mut pairs: Vec<(u64, f64)> = Vec::new();
        for batch in res.batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let scores = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                pairs.push((ids.value(i), scores.value(i)));
            }
        }

        if pairs.is_empty() {
            return Ok(());
        }

        // Batch update using UNWIND - chunk into 1000 per query to avoid query size limits
        const CHUNK_SIZE: usize = 1000;
        for chunk in pairs.chunks(CHUNK_SIZE) {
            let updates: Vec<String> = chunk
                .iter()
                .map(|(id, score)| format!("{{id: {}, score: {}}}", id, score))
                .collect();
            let unwind_query = format!(
                "UNWIND [{}] AS row MATCH (n:CodeNode) WHERE n._id = row.id SET n.pagerank = row.score",
                updates.join(", ")
            );
            let _ = conn.query(&unwind_query);
        }
        Ok(())
    }

    /// Bulk lookup nodes by internal _id values, returning all properties
    /// This avoids the N+1 query problem when hydrating search results
    pub fn bulk_lookup_nodes_by_ids(
        conn: &crate::Connection,
        ids: &[u64],
    ) -> Result<Vec<RecordBatch>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        const CHUNK_SIZE: usize = 50;
        let mut all_batches = Vec::new();

        for chunk in ids.chunks(CHUNK_SIZE) {
            let conditions: Vec<String> =
                chunk.iter().map(|id| format!("n._id = {}", id)).collect();
            let query = format!(
                "MATCH (n:CodeNode) WHERE {} RETURN n._id, n.id, n.node_type, n.name, n.file_path, n.start_line, n.start_col, n.end_line, n.docstring, n.signature, n.source_hash",
                conditions.join(" OR ")
            );

            let res = conn.query(&query)?;
            all_batches.extend(res.batches);
        }

        Ok(all_batches)
    }

    /// Bulk lookup nodes by string 'id' values (not internal _id)
    pub fn bulk_lookup_nodes_by_string_ids(
        conn: &crate::Connection,
        ids: &[String],
    ) -> Result<Vec<RecordBatch>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        const CHUNK_SIZE: usize = 50;
        let mut all_batches = Vec::new();

        for chunk in ids.chunks(CHUNK_SIZE) {
            let conditions: Vec<String> = chunk
                .iter()
                .map(|s| format!("n.id = '{}'", s.replace('\'', "\\'")))
                .collect();
            let query = format!(
                "MATCH (n:CodeNode) WHERE {} RETURN n._id, n.id, n.node_type, n.name, n.file_path, n.start_line, n.start_col, n.end_line, n.docstring, n.signature, n.source_hash",
                conditions.join(" OR ")
            );

            let res = conn.query(&query)?;
            all_batches.extend(res.batches);
        }

        Ok(all_batches)
    }

    pub fn upsert_node_with_hash(
        conn: &crate::Connection,
        node_id: &str,
        workspace_id: &str,
        node_type: &str,
        name: &str,
        file_path: &str,
        start_line: i64,
        end_line: i64,
        docstring: &str,
        signature: &str,
        source_hash: &str,
    ) -> Result<()> {
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(node_id.to_string()));
        params.insert("ws_id".to_string(), Value::String(workspace_id.to_string()));
        params.insert("nt".to_string(), Value::String(node_type.to_string()));
        params.insert("name".to_string(), Value::String(name.to_string()));
        params.insert("path".to_string(), Value::String(file_path.to_string()));
        params.insert("sl".to_string(), Value::Number(start_line as f64));
        params.insert("el".to_string(), Value::Number(end_line as f64));
        params.insert("doc".to_string(), Value::String(docstring.to_string()));
        params.insert("sig".to_string(), Value::String(signature.to_string()));
        params.insert("hash".to_string(), Value::String(source_hash.to_string()));

        let check = "MATCH (n:CodeNode {id: $id}) RETURN n.source_hash";
        if let Ok(res) = conn.execute(check, Some(params.clone())) {
            if !res.batches.is_empty() && res.batches[0].num_rows() > 0 {
                let hashes = res.batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                if hashes.value(0) == source_hash {
                    return Ok(());
                }
                let _ = conn.execute(
                    "MATCH (n:CodeNode {id: $id}) DELETE n",
                    Some(params.clone()),
                );
            }
        }
        let create = "CREATE (n:CodeNode {id: $id, workspace_id: $ws_id, node_type: $nt, name: $name, file_path: $path, start_line: $sl, end_line: $el, docstring: $doc, signature: $sig, source_hash: $hash})";
        conn.execute(create, Some(params))?;
        Ok(())
    }

    /// Bulk insert nodes using Arrow RecordBatch for maximum throughput
    /// Note: embedding and pagerank are NOT included as they require separate updates
    pub fn bulk_insert_code_nodes(
        conn: &crate::Connection,
        nodes: Vec<(
            String, // id
            String, // workspace_id
            String, // node_type
            String, // name
            String, // file_path
            i64,    // start_line
            i64,    // start_col
            i64,    // end_line
            String, // docstring
            String, // signature
            String, // source_hash
        )>,
    ) -> Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }

        // Invalidate name→_id cache since nodes are changing
        {
            let db = conn.client_context.database.clone();
            db.name_id_cache.write().clear();
        }

        let num_rows = nodes.len();
        let mut ids = Vec::with_capacity(num_rows);
        let mut ws_ids = Vec::with_capacity(num_rows);
        let mut node_types = Vec::with_capacity(num_rows);
        let mut names = Vec::with_capacity(num_rows);
        let mut paths = Vec::with_capacity(num_rows);
        let mut start_lines = Vec::with_capacity(num_rows);
        let mut start_cols = Vec::with_capacity(num_rows);
        let mut end_lines = Vec::with_capacity(num_rows);
        let mut docstrings = Vec::with_capacity(num_rows);
        let mut signatures = Vec::with_capacity(num_rows);
        let mut hashes = Vec::with_capacity(num_rows);

        for (id, ws_id, nt, name, path, sl, sc, el, doc, sig, hash) in nodes {
            ids.push(id);
            ws_ids.push(ws_id);
            node_types.push(nt);
            names.push(name);
            paths.push(path);
            start_lines.push(sl);
            start_cols.push(sc);
            end_lines.push(el);
            docstrings.push(doc);
            signatures.push(sig);
            hashes.push(hash);
        }

        let schema = Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("workspace_id", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("node_type", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("file_path", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("start_line", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("start_col", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("end_line", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("docstring", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("signature", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("source_hash", arrow::datatypes::DataType::Utf8, false),
        ]);

        let batch = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![
                std::sync::Arc::new(arrow::array::StringArray::from(ids)),
                std::sync::Arc::new(arrow::array::StringArray::from(ws_ids)),
                std::sync::Arc::new(arrow::array::StringArray::from(node_types)),
                std::sync::Arc::new(arrow::array::StringArray::from(names)),
                std::sync::Arc::new(arrow::array::StringArray::from(paths)),
                std::sync::Arc::new(arrow::array::Int64Array::from(start_lines)),
                std::sync::Arc::new(arrow::array::Int64Array::from(start_cols)),
                std::sync::Arc::new(arrow::array::Int64Array::from(end_lines)),
                std::sync::Arc::new(arrow::array::StringArray::from(docstrings)),
                std::sync::Arc::new(arrow::array::StringArray::from(signatures)),
                std::sync::Arc::new(arrow::array::StringArray::from(hashes)),
            ],
        )?;

        conn.bulk_insert_batch("CodeNode", &batch)?;
        Ok(())
    }

    /// Bulk insert edges using Arrow RecordBatch per relationship type
    pub fn bulk_insert_edges(
        conn: &crate::Connection,
        _workspace_id: &str,
        edges: Vec<(String, String, String)>, // (source_id, edge_type, target_id)
    ) -> Result<()> {
        if edges.is_empty() {
            return Ok(());
        }

        // Step 1: Resolve all source and target node IDs to internal _id values
        // Source IDs are UUIDs, target IDs are names (need name-based lookup)
        let needed_src_ids: HashSet<String> = edges.iter().map(|(src, _, _)| src.clone()).collect();
        let needed_dst_names: HashSet<String> =
            edges.iter().map(|(_, _, dst)| dst.clone()).collect();

        let mut id_to_internal: HashMap<String, u64> = HashMap::new();
        let mut name_to_internal: HashMap<String, u64> = HashMap::new();

        let res = match conn.query("MATCH (n:CodeNode) RETURN n._id, n.id, n.name") {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Failed to scan CodeNode for edge resolution: {}", e);
                return Ok(());
            }
        };
        for batch in &res.batches {
                let internal_ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let string_ids = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let names = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    let internal_id = internal_ids.value(i);
                    let str_id = string_ids.value(i).to_string();
                    let name = names.value(i).to_string();

                    // Map UUID -> internal ID for source lookup
                    if needed_src_ids.contains(&str_id) {
                        id_to_internal.insert(str_id, internal_id);
                    }
                    // Map name -> internal ID for target lookup
                    if needed_dst_names.contains(&name) {
                        name_to_internal.insert(name, internal_id);
                }
            }
        }

        // Step 2: Group edges by type
        let mut edges_by_type: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
        for (src, et, dst_name) in edges {
            // Source is looked up by UUID, target is looked up by name
            if let (Some(&src_id), Some(&dst_id)) =
                (id_to_internal.get(&src), name_to_internal.get(&dst_name))
            {
                edges_by_type.entry(et).or_default().push((src_id, dst_id));
            }
        }

        // Step 3: Bulk insert per relationship type directly to storage
        // NOTE: We cannot use bulk_insert_batch because it prepends an _id column,
        // but rel tables only have _src and _dst columns (no _id).
        // CSR rebuild is deferred to rebuild_csr_if_stale() - graph traversal operators
        // call it lazily, avoiding O(V+E) rebuild per edge batch during bulk indexing.
        let db = conn.client_context.database.clone();
        // Single transaction for all edge types (avoid per-type commit overhead)
        let tx = db.transaction_manager.begin(false)?;
        let bm = db.buffer_manager.clone();
        let total_edges: usize = edges_by_type.values().map(|e| e.len()).sum();
        {
            for (edge_type, edge_list) in &edges_by_type {
                let num_rows = edge_list.len();
                if num_rows == 0 {
                    continue;
                }

                let mut src_ids = Vec::with_capacity(num_rows);
                let mut dst_ids = Vec::with_capacity(num_rows);
                for (s, d) in edge_list {
                    src_ids.push(*s);
                    dst_ids.push(*d);
                }

                let batch = arrow::record_batch::RecordBatch::try_new(
                    std::sync::Arc::new(Schema::new(vec![
                        arrow::datatypes::Field::new("_src", arrow::datatypes::DataType::UInt64, false),
                        arrow::datatypes::Field::new("_dst", arrow::datatypes::DataType::UInt64, false),
                    ])),
                    vec![
                        std::sync::Arc::new(arrow::array::UInt64Array::from(src_ids)),
                        std::sync::Arc::new(arrow::array::UInt64Array::from(dst_ids)),
                    ],
                )?;

                // Insert edges into column storage
                let storage = db.storage_manager.read();
                let table = match storage.get_table(edge_type) {
                    Some(t) => t,
                    None => {
                        return Err(crate::LightningError::Query(format!(
                            "Table {} not found",
                            edge_type
                        )));
                    }
                };
                table.bulk_append_batch(&bm, &batch, 0, &tx)?;
            }
        }
        // Single commit for all edge types
        db.storage_manager.read().flush_all_pending(&bm, &tx)?;
        db.transaction_manager.commit(&tx, &bm, &db)?;

        // Batch CSR rebuild - once for ALL edge types (not once per type).
        // Checkpoint first to flush any dirty BM pages to disk so CSR rebuild
        // can read clean data via column scans.
        {
            let bm = db.buffer_manager.clone();
            let _ = db.checkpoint();
            let tx = db.transaction_manager.begin(false)?;
            let edge_types: Vec<String> = {
                let storage = db.storage_manager.read();
                storage.fwd_csr.keys().cloned().collect()
            };
            for et in &edge_types {
                let mut storage = db.storage_manager.write();
                let _ = storage.rebuild_csr_if_stale(et, &bm, &tx);
            }
            db.transaction_manager.commit(&tx, &bm, &db)?;
        }

        // Sync catalog stats from table stats after all inserts
        Self::sync_catalog_stats(&db);

        Ok(())
    }

    /// Look up nodes connected to a given node via specific edge types.
    /// Uses direct column scans on edge tables for reliability (no CSR dependency).
    pub fn find_connected_nodes(
        conn: &crate::Connection,
        node_id: u64,
        edge_types: &[&str],
        direction: ConnectedDirection,
    ) -> Result<Vec<u64>> {
        let mut connected = HashSet::new();
        let db = conn.client_context.database.clone();
        for et in edge_types {
            let table = {
                let storage = db.storage_manager.read();
                storage.rel_tables.get(*et).cloned()
            };
            if let Some(table) = table {
                let bm = &db.buffer_manager;
                let tx = db.transaction_manager.begin(true)?;
                let cardinality = table.stats.read().cardinality;
                if cardinality > 0 {
                    let mut col0 = Vec::new();
                    let mut col1 = Vec::new();
                    let _ = table.columns[0].scan(bm, 0, cardinality, &tx, &mut col0);
                    let _ = table.columns[1].scan(bm, 0, cardinality, &tx, &mut col1);
                    for (src, dst) in col0.iter().zip(col1.iter()) {
                        let s = src.as_node();
                        let d = dst.as_node();
                        match direction {
                            ConnectedDirection::Incoming => { if d == node_id { connected.insert(s); } }
                            ConnectedDirection::Outgoing => { if s == node_id { connected.insert(d); } }
                            ConnectedDirection::Both => { if s == node_id || d == node_id { connected.insert(if s == node_id { d } else { s }); } }
                        }
                    }
                }
                let _ = db.transaction_manager.rollback(&db, &tx);
            }
        }
        Ok(connected.into_iter().collect())
    }

    /// Bulk lookup node names by internal _id values
    pub fn lookup_node_names(
        conn: &crate::Connection,
        ids: &[u64],
    ) -> Result<Vec<(u64, String, String)>> {
        if ids.is_empty() { return Ok(Vec::new()); }
        let mut results = Vec::new();
        for chunk in ids.chunks(100) {
            let conditions: Vec<String> = chunk.iter().map(|id| format!("n._id = {}", id)).collect();
            let q = format!("MATCH (n:CodeNode) WHERE {} RETURN n._id, n.name, n.node_type, n.file_path", conditions.join(" OR "));
            if let Ok(res) = conn.query(&q) {
                for batch in res.batches {
                    let ids_arr = batch.column(0).as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap();
                    let names_arr = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                    let types_arr = batch.column(2).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                    for i in 0..batch.num_rows() {
                        results.push((ids_arr.value(i), names_arr.value(i).to_string(), types_arr.value(i).to_string()));
                    }
                }
            }
        }
        Ok(results)
    }

    /// Find logic flow paths between two symbols
    pub fn find_paths(
        conn: &crate::Connection,
        source_id: u64,
        target_id: u64,
        edge_types: &[&str],
    ) -> Result<Vec<String>> {
        let db = conn.client_context.database.clone();
        let mut paths = Vec::new();
        for et in edge_types {
            let table = {
                let storage = db.storage_manager.read();
                storage.rel_tables.get(*et).cloned()
            };
            if let Some(table) = table {
                let bm = &db.buffer_manager;
                let tx = db.transaction_manager.begin(true)?;
                let cardinality = table.stats.read().cardinality;
                if cardinality > 0 {
                    let mut col0 = Vec::new();
                    let mut col1 = Vec::new();
                    let _ = table.columns[0].scan(bm, 0, cardinality, &tx, &mut col0);
                    let _ = table.columns[1].scan(bm, 0, cardinality, &tx, &mut col1);
                    for (src, dst) in col0.iter().zip(col1.iter()) {
                        let s = src.as_node();
                        let d = dst.as_node();
                        if s == source_id && d == target_id {
                            paths.push(format!("{} --{}--> {}", source_id, et, target_id));
                        }
                    }
                }
                let _ = db.transaction_manager.rollback(&db, &tx);
            }
        }
        Ok(paths)
    }

    /// Compute module cohesion using edge column scans
    pub fn compute_architecture_cohesion(
        conn: &crate::Connection,
    ) -> Result<Vec<ModuleCohesion>> {
        let db = conn.client_context.database.clone();

        // Build module map: node_id -> module_path
        let mut module_map: HashMap<u64, String> = HashMap::new();
        if let Ok(res) = conn.query("MATCH (n:CodeNode) RETURN n._id, n.file_path") {
            for batch in res.batches {
                let ids = batch.column(0).as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap();
                let paths = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                for i in 0..batch.num_rows() {
                    let p = paths.value(i);
                    let module = match p.rfind('/') { Some(idx) => &p[0..idx], None => p };
                    module_map.insert(ids.value(i), module.to_string());
                }
            }
        }

        let mut metrics: HashMap<String, (u64, u64)> = HashMap::new();
        let bm = &db.buffer_manager;
        let edge_types = ["Calls", "Imports", "References"];

        for et in edge_types {
            let table = {
                let storage = db.storage_manager.read();
                storage.rel_tables.get(et).cloned()
            };
            if let Some(table) = table {
                let tx = db.transaction_manager.begin(true)?;
                let cardinality = table.stats.read().cardinality;
                if cardinality > 0 {
                    let mut col0 = Vec::new();
                    let mut col1 = Vec::new();
                    let _ = table.columns[0].scan(bm, 0, cardinality, &tx, &mut col0);
                    let _ = table.columns[1].scan(bm, 0, cardinality, &tx, &mut col1);
                    for (src, dst) in col0.iter().zip(col1.iter()) {
                        let s = src.as_node();
                        let d = dst.as_node();
                        if let (Some(src_mod), Some(dst_mod)) = (module_map.get(&s), module_map.get(&d)) {
                            let entry = metrics.entry(src_mod.clone()).or_insert((0, 0));
                            if src_mod == dst_mod { entry.0 += 1; } else { entry.1 += 1; }
                        }
                    }
                }
                let _ = db.transaction_manager.rollback(&db, &tx);
            }
        }

        let mut results: Vec<ModuleCohesion> = metrics.into_iter().map(|(module, (internal, external))| {
            let total = internal + external;
            let cohesion = if total == 0 { 0.0 } else { internal as f64 / total as f64 };
            ModuleCohesion { module_path: module, internal_edges: internal, external_edges: external, cohesion_score: cohesion }
        }).collect();
        results.sort_by(|a, b| a.cohesion_score.partial_cmp(&b.cohesion_score).unwrap());
        Ok(results)
    }

    pub fn find_node_by_name(
        conn: &crate::Connection,
        name: &str,
    ) -> Result<Vec<u64>> {
        let db = conn.client_context.database.clone();
        let name_lower = name.to_lowercase();
        
        // Check name→_id cache first
        {
            let cache = db.name_id_cache.read();
            if let Some(cached) = cache.get(&name_lower) {
                return Ok(cached.clone());
            }
        }
        
        let mut ids = Vec::new();
        // Direct column scan: bypass query engine which can't resolve n._id
        if let Some(table) = { let s = db.storage_manager.read(); s.node_tables.get("CodeNode").cloned() } {
            let bm = &db.buffer_manager;
            let tx = match db.transaction_manager.begin(true) {
                Ok(t) => t,
                Err(_) => return Ok(ids),
            };
            // Get actual row count from _id column file size
            let actual_rows = {
                let fs = table.columns[0].fh.get_file_size();
                let es = table.columns[0].element_size();
                if es > 0 && fs > 0 { fs / es as u64 } else { 0 }
            };
            if actual_rows > 0 {
                // Scan _id (col 0) and name (col 4)
                let mut row_ids = Vec::new();
                let mut names = Vec::new();
                // Silently handle scan errors for cache population
                let _ = table.columns[0].scan(bm, 0, actual_rows, &tx, &mut row_ids);
                if table.columns.len() > 4 {
                    let _ = table.columns[4].scan(bm, 0, actual_rows, &tx, &mut names);
                }
                // Build full name→_id cache while searching
                let mut full_cache: std::collections::HashMap<String, Vec<u64>> = std::collections::HashMap::new();
                for i in 0..names.len().min(row_ids.len()) {
                    if let crate::processor::Value::String(ref n) = names[i] {
                        let n_lower = n.to_lowercase();
                        let rid = row_ids[i].as_node();
                        // Cache each word and the full name
                        for token in n_lower.split(&['_', ' ', '.', ':', '/'][..]) {
                            if !token.is_empty() && token.len() >= 2 {
                                full_cache.entry(token.to_string()).or_default().push(rid);
                            }
                        }
                        full_cache.entry(n_lower.clone()).or_default().push(rid);
                        // Check if this matches our query
                        if n_lower.contains(&name_lower) {
                            ids.push(rid);
                        }
                    }
                }
                // Populate global cache with all entries
                {
                    let mut cache = db.name_id_cache.write();
                    for (key, val) in full_cache {
                        cache.entry(key).or_insert_with(|| val);
                    }
                }
            }
            let _ = db.transaction_manager.rollback(&db, &tx);
        }
        Ok(ids)
    }

    /// Sync catalog stats from storage manager table stats
    pub fn sync_catalog_stats(db: &crate::Database) {
        let storage = db.storage_manager.read();
        let mut catalog = db.catalog.write();
        for (name, table) in storage.rel_tables.iter() {
            if let Some(entry) = catalog.get_rel_table_mut(name) {
                entry.stats = table.stats.read().clone();
            }
        }
        for (name, table) in storage.node_tables.iter() {
            if let Some(entry) = catalog.get_node_table_mut(name) {
                entry.stats = table.stats.read().clone();
            }
        }
        db.catalog.mark_dirty();
    }

    /// Bulk delete CodeNodes by file paths and workspace_id
    pub fn bulk_delete_nodes_by_files(
        conn: &crate::Connection,
        file_paths: &[String],
        workspace_id: &str,
    ) -> Result<()> {
        if file_paths.is_empty() {
            return Ok(());
        }
        // Invalidate name→_id cache since nodes are changing
        {
            let db = conn.client_context.database.clone();
            db.name_id_cache.write().clear();
        }
        // Batch files into chunks to avoid overly large queries
        for chunk in file_paths.chunks(50) {
            let conditions: Vec<String> = chunk
                .iter()
                .map(|p| format!("n.file_path = '{}'", p.replace('\'', "\\'")))
                .collect();
            let query = format!(
                "MATCH (n:CodeNode {{workspace_id: '{}'}}) WHERE {} DELETE n",
                workspace_id,
                conditions.join(" OR ")
            );
            conn.query(&query)?;
        }
        Ok(())
    }

    /// Bulk upsert file hashes
    /// Removed broken delete step that was scanning CodeNode table with wrong variable name
    pub fn bulk_upsert_file_hashes(
        conn: &crate::Connection,
        hashes: &[(String, String)],
        workspace_id: &str,
    ) -> Result<()> {
        if hashes.is_empty() {
            return Ok(());
        }

        // First, delete old FileHash entries with matching paths using direct MATCH
        for chunk in hashes.chunks(100) {
            let conditions: Vec<String> = chunk
                .iter()
                .map(|(p, _)| format!("n.file_path = '{}'", p.replace('\'', "\\'")))
                .collect();
            let _ = conn.query(&format!(
                "MATCH (n:FileHash) WHERE {} DELETE n",
                conditions.join(" OR ")
            ));
        }

        // Then bulk insert new hashes
        let num_rows = hashes.len();
        let mut paths = Vec::with_capacity(num_rows);
        let mut hash_vals = Vec::with_capacity(num_rows);
        let mut ws_ids = Vec::with_capacity(num_rows);
        for (p, h) in hashes {
            paths.push(p.clone());
            hash_vals.push(h.clone());
            ws_ids.push(workspace_id.to_string());
        }

        let schema = Schema::new(vec![
            arrow::datatypes::Field::new("file_path", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("hash", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("workspace_id", arrow::datatypes::DataType::Utf8, false),
        ]);

        let batch = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![
                std::sync::Arc::new(arrow::array::StringArray::from(paths)),
                std::sync::Arc::new(arrow::array::StringArray::from(hash_vals)),
                std::sync::Arc::new(arrow::array::StringArray::from(ws_ids)),
            ],
        )?;

        conn.bulk_insert_batch("FileHash", &batch)?;
        Ok(())
    }

    /// Bulk insert edges using Arrow RecordBatch

    pub fn update_progress(
        conn: &crate::Connection,
        workspace_id: &str,
        enrichment_type: &str,
        processed: i64,
        total: i64,
        status: &str,
    ) -> Result<()> {
        let cleanup = format!(
            "MATCH (p:EnrichmentProgress {{workspace_id: '{}'}}) DELETE p",
            workspace_id
        );
        let _ = conn.query(&cleanup);
        let query = format!("CREATE (p:EnrichmentProgress {{workspace_id: '{}', enrichment_type: '{}', files_processed: {}, total_files: {}, status: '{}'}})", workspace_id, enrichment_type, processed, total, status);
        conn.query(&query)?;
        Ok(())
    }

    /// Record watcher statistics
    pub fn update_watcher_stats(
        conn: &crate::Connection,
        processed: i64,
        duration_ms: i64,
    ) -> Result<()> {
        let cleanup = "MATCH (s:WatcherStats) WHERE s.id = 1 DELETE s";
        let _ = conn.query(cleanup);
        let query = format!("CREATE (s:WatcherStats {{id: 1, last_update: current_timestamp, files_processed: {}, duration_ms: {}}})", processed, duration_ms);
        conn.query(&query)?;
        Ok(())
    }

    /// Export graph to a JSON string compatible with D3.js
    pub fn export_to_d3_json(conn: &crate::Connection) -> Result<String> {
        let (nodes, edges) = Self::export_graph(conn)?;
        let mut json = String::from("{\n  \"nodes\": [\n");
        for (i, node) in nodes.iter().enumerate() {
            json.push_str(&format!(
                "    {{\"id\": \"{}\", \"name\": \"{}\", \"group\": \"{}\"}}{}",
                node.id,
                node.name.replace("\"", "\\\""),
                node.node_type,
                if i == nodes.len() - 1 { "" } else { "," }
            ));
            json.push('\n');
        }
        json.push_str("  ],\n  \"links\": [\n");
        for (i, edge) in edges.iter().enumerate() {
            json.push_str(&format!(
                "    {{\"source\": \"{}\", \"target\": \"{}\", \"type\": \"{}\"}}{}",
                edge.source_id,
                edge.target_id,
                edge.edge_type,
                if i == edges.len() - 1 { "" } else { "," }
            ));
            json.push('\n');
        }
        json.push_str("  ]\n}");
        Ok(json)
    }

    pub fn export_graph_to_string(conn: &crate::Connection) -> Result<String> {
        Self::export_to_d3_json(conn)
    }

    pub fn export_graph_v2(conn: &crate::Connection) -> Result<(Vec<GraphNode>, Vec<GraphEdge>)> {
        Self::export_graph(conn)
    }
}
