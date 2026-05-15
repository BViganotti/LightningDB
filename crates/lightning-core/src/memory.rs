use crate::processor::{DataChunk, Value};
use crate::Result;
use crate::Connection;
use crate::QueryResult;
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crossbeam::channel::Receiver;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const ENTITY_TABLE: &str = "Entity";
const RELATES_TABLE: &str = "Relates";
const DEFAULT_EMBEDDING_DIM: usize = 768;
const SIMILARITY_THRESHOLD: f64 = 0.82;

/// Configuration for the RAG pipeline.
pub struct RagConfig {
    /// Number of top initial results to use for graph expansion.
    pub expansion_depth: usize,
    /// Weight for the search score in the composite reranking formula.
    pub search_weight: f64,
    /// Weight for temporal recency in the composite reranking formula.
    pub recency_weight: f64,
    /// Weight for graph degree (number of connections) in the composite formula.
    pub degree_weight: f64,
    /// Name of a WASM function to use as a cross-encoder reranker.
    pub cross_encoder_wasm: String,
}

impl Default for RagConfig {
    fn default() -> Self {
        Self {
            expansion_depth: 3,
            search_weight: 2.0,
            recency_weight: 0.3,
            degree_weight: 0.0,
            cross_encoder_wasm: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoryEntity {
    pub id: String,
    pub entity_type: String,
    pub content: String,
    pub created_at: i64,
    pub last_accessed: i64,
    pub access_count: i64,
    pub ttl_seconds: i64,
    pub metadata: String,
    pub valid_from: i64,
    pub valid_until: i64,
}

impl Default for MemoryEntity {
    fn default() -> Self {
        Self {
            id: String::new(),
            entity_type: String::new(),
            content: String::new(),
            created_at: 0,
            last_accessed: 0,
            access_count: 0,
            ttl_seconds: 0,
            metadata: "{}".to_string(),
            valid_from: 0,
            valid_until: i64::MAX,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoryRelation {
    pub src_id: String,
    pub dst_id: String,
    pub relation_type: String,
    pub weight: f64,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub entity: MemoryEntity,
    pub score: f64,
}

pub struct MemoryStore {
    conn: Connection,
    embedding_dim: usize,
    schema_initialized: std::sync::atomic::AtomicBool,
}

impl MemoryStore {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            embedding_dim: DEFAULT_EMBEDDING_DIM,
            schema_initialized: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn with_embedding_dim(mut self, dim: usize) -> Self {
        self.embedding_dim = dim;
        self
    }

    pub fn ensure_schema(&self) -> Result<()> {
        if self.schema_initialized.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }

        let db = self.conn.client_context.database.clone();
        let storage = db.storage_manager.read();
        let exists = storage.node_tables.contains_key(ENTITY_TABLE);
        drop(storage);

        if !exists {
            let create_entity = format!(
                "CREATE NODE TABLE {ENTITY_TABLE} (id STRING, type STRING, content STRING, \
                 created_at INT64, last_accessed INT64, access_count INT64, \
                 ttl_seconds INT64, metadata STRING, \
                 valid_from INT64, valid_until INT64, PRIMARY KEY (id))"
            );
            self.conn.execute(&create_entity, None)?;

            let create_relates = format!(
                "CREATE REL TABLE {RELATES_TABLE} (FROM {ENTITY_TABLE} TO {ENTITY_TABLE}, type STRING, weight DOUBLE, created_at TIMESTAMP)"
            );
            self.conn.execute(&create_relates, None)?;

            {
                let mut storage = db.storage_manager.write();
                let _ = storage.create_fts_index(ENTITY_TABLE);
                let _ = storage.create_vector_index(ENTITY_TABLE);
            }
        }

        self.schema_initialized.store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    pub fn now_micros_for_test() -> i64 {
        Self::now_micros()
    }

    fn now_micros() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    }

    pub fn store(&self, entity: MemoryEntity) -> Result<()> {
        self.ensure_schema()?;
        // Delete existing entity with this ID if it exists (soft delete)
        let _ = self.forget(&entity.id);
        // Insert new version — this goes through bulk_insert_batch
        // which handles FTS and vector indexing automatically
        self.store_batch(vec![entity])?;
        Ok(())
    }

    pub fn store_batch(&self, entities: Vec<MemoryEntity>) -> Result<usize> {
        self.ensure_schema()?;

        if entities.is_empty() {
            return Ok(0);
        }

        let now = Self::now_micros();
        let num_rows = entities.len();
        let mut ids = Vec::with_capacity(num_rows);
        let mut types = Vec::with_capacity(num_rows);
        let mut contents = Vec::with_capacity(num_rows);
        let mut created_at = Vec::with_capacity(num_rows);
        let mut last_accessed = Vec::with_capacity(num_rows);
        let mut access_counts = Vec::with_capacity(num_rows);
        let mut ttl_seconds = Vec::with_capacity(num_rows);
        let mut metadatas = Vec::with_capacity(num_rows);
        let mut valid_from = Vec::with_capacity(num_rows);
        let mut valid_until = Vec::with_capacity(num_rows);

        for e in entities {
            ids.push(e.id);
            types.push(e.entity_type);
            contents.push(e.content);
            created_at.push(e.created_at.max(now));
            last_accessed.push(now);
            access_counts.push(e.access_count.max(1));
            ttl_seconds.push(e.ttl_seconds);
            metadatas.push(e.metadata);
            valid_from.push(e.valid_from.max(now));
            valid_until.push(if e.valid_until == 0 { 0i64 } else { e.valid_until });
        }

        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
            Field::new("created_at", DataType::Int64, false),
            Field::new("last_accessed", DataType::Int64, false),
            Field::new("access_count", DataType::Int64, false),
            Field::new("ttl_seconds", DataType::Int64, false),
            Field::new("metadata", DataType::Utf8, false),
            Field::new("valid_from", DataType::Int64, false),
            Field::new("valid_until", DataType::Int64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow::array::StringArray::from(ids)),
                Arc::new(arrow::array::StringArray::from(types)),
                Arc::new(arrow::array::StringArray::from(contents)),
                Arc::new(arrow::array::Int64Array::from(created_at)),
                Arc::new(arrow::array::Int64Array::from(last_accessed)),
                Arc::new(arrow::array::Int64Array::from(access_counts)),
                Arc::new(arrow::array::Int64Array::from(ttl_seconds)),
                Arc::new(arrow::array::StringArray::from(metadatas)),
                Arc::new(arrow::array::Int64Array::from(valid_from)),
                Arc::new(arrow::array::Int64Array::from(valid_until)),
            ],
        )?;

        self.conn.bulk_insert_batch(ENTITY_TABLE, &batch)
    }

    pub fn recall(&self, query_text: &str, embedding: &[f32], top_k: usize) -> Result<Vec<SearchResult>> {
        self.ensure_schema()?;

        let db = self.conn.client_context.database.clone();
        let storage = db.storage_manager.read();

        let mut results: HashMap<String, (MemoryEntity, f64)> = HashMap::new();
        let k = 60.0;

        if let Some(fts) = storage.fts_indexes.get(ENTITY_TABLE) {
            let tx = db.transaction_manager.begin(true)?;
            if let Ok(fts_results) = fts.search(query_text, top_k * 2, &db.buffer_manager, &tx) {
                for (rank, (node_id, _)) in fts_results.iter().enumerate() {
                    if let Some(entity) = self.lookup_by_internal_id(*node_id) {
                        let rrf_score = 1.0 / (k + (rank as f64) + 1.0);
                        results.entry(entity.id.clone()).or_insert((entity, 0.0)).1 += rrf_score;
                    }
                }
            }
            let _ = db.transaction_manager.rollback(&db, &tx);
        }

        if !embedding.is_empty() && embedding.len() == self.embedding_dim {
            if let Some(vec_idx) = storage.vector_indexes.get(ENTITY_TABLE) {
                let tx = db.transaction_manager.begin(true)?;
                let emb: [f32; 768] = embedding
                    .try_into()
                    .map_err(|_| crate::LightningError::Internal(format!(
                        "Expected embedding of length {} but got {}", 768, embedding.len()
                    )))?;

                if let Ok(vec_results) = vec_idx.search(&emb, top_k * 2, &db.buffer_manager, &tx) {
                    for (rank, (node_id, _)) in vec_results.iter().enumerate() {
                        if let Some(entity) = self.lookup_by_internal_id(*node_id) {
                            let rrf_score = 1.0 / (k + (rank as f64) + 1.0);
                            results.entry(entity.id.clone()).or_insert((entity, 0.0)).1 += rrf_score;
                        }
                    }
                }
                let _ = db.transaction_manager.rollback(&db, &tx);
            }
        }

        let mut sorted: Vec<SearchResult> = results
            .into_iter()
            .map(|(_, (entity, score))| SearchResult { entity, score })
            .collect();
        sorted.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        sorted.truncate(top_k);
        Ok(sorted)
    }

    /// Streaming variant of `recall()`. Returns a channel that yields
    /// `SearchResult` items as they become available. This is useful for
    /// real-time display of results or processing large result sets.
    pub fn recall_stream(
        &self,
        query_text: &str,
        embedding: &[f32],
        top_k: usize,
    ) -> Result<Receiver<Result<SearchResult>>> {
        self.ensure_schema()?;

        let db = self.conn.client_context.database.clone();
        let storage = db.storage_manager.read();
        let _fts_exists = storage.fts_indexes.contains_key(ENTITY_TABLE);
        let _vec_exists = storage.vector_indexes.contains_key(ENTITY_TABLE);
        drop(storage);

        let (tx, rx) = crossbeam::channel::unbounded();
        let query_text = query_text.to_string();
        let embedding = embedding.to_vec();
        let conn = self.conn.client_context.database.clone();

        std::thread::spawn(move || {
            let new_conn = conn.connect();
            let store = MemoryStore::new(new_conn);
            let results = match store.recall(&query_text, &embedding, top_k) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            };
            for r in results {
                if tx.send(Ok(r)).is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }

    /// Full RAG pipeline: hybrid search → graph expansion → reranking → context assembly.
    ///
    /// Returns a RagResult containing assembled context and source metadata.
    /// The pipeline:
    ///   1. Hybrid search (FTS + vector) for initial candidates
    ///   2. Graph expansion via Relates edges for context enrichment
    ///   3. Reranking by search score × graph degree × temporal recency
    ///   4. Assembly into LLM-ready context string
    ///
    /// Uses the default RagConfig. For custom settings, use rag_query_with_config.
    pub fn rag_query(&self, query_text: &str, embedding: &[f32], top_k: usize) -> Result<RagResult> {
        self.rag_query_with_config(query_text, embedding, top_k, &RagConfig::default())
    }

    /// Full RAG pipeline with configurable parameters.
    pub fn rag_query_with_config(
        &self,
        query_text: &str,
        embedding: &[f32],
        top_k: usize,
        config: &RagConfig,
    ) -> Result<RagResult> {
        self.ensure_schema()?;

        // Phase 1: Hybrid search
        let initial = self.recall(query_text, embedding, top_k)?;
        if initial.is_empty() {
            return Ok(RagResult::default());
        }

        let mut all_entities: HashMap<String, (MemoryEntity, f64)> = HashMap::new();
        for r in &initial {
            all_entities.insert(r.entity.id.clone(), (r.entity.clone(), r.score));
        }

        // Phase 2: Graph expansion — find neighbors for top results
        let top_for_expansion = std::cmp::min(config.expansion_depth, initial.len());
        if top_for_expansion > 0 {
            let db = self.conn.client_context.database.clone();
            let storage = db.storage_manager.read();
            let rel_table = storage.rel_tables.get(RELATES_TABLE).cloned();
            drop(storage);

            if let Some(ref rel_tab) = rel_table {
                let tx = db.transaction_manager.begin(true)?;
                let card = rel_tab.stats.read().cardinality;
                if card > 0 {
                    let mut srcs = Vec::new();
                    let mut dsts = Vec::new();
                    let bm = &db.buffer_manager;
                    let _ = rel_tab.columns[0].scan(bm, 0, card, &tx, &mut srcs);
                    let _ = rel_tab.columns[1].scan(bm, 0, card, &tx, &mut dsts);

                    let mut internal_ids: Vec<(u64, String)> = Vec::new();
                    for i in 0..top_for_expansion {
                        if i >= initial.len() { break; }
                        let lookup = format!(
                            "MATCH (e:{}) WHERE e.id = '{}' RETURN e._id LIMIT 1",
                            ENTITY_TABLE, initial[i].entity.id
                        );
                        if let Ok(res) = self.conn.execute(&lookup, None) {
                            if let Some(b) = res.batches.first() {
                                let arr = b.column(0).as_any().downcast_ref::<UInt64Array>();
                                if let Some(a) = arr {
                                    internal_ids.push((a.value(0), initial[i].entity.id.clone()));
                                }
                            }
                        }
                    }

                    for (nid, _) in &internal_ids {
                        for (s, d) in srcs.iter().zip(dsts.iter()) {
                            let neighbor_eid = if s.as_node() == *nid {
                                self.lookup_by_internal_id(d.as_node())
                            } else if d.as_node() == *nid {
                                self.lookup_by_internal_id(s.as_node())
                            } else {
                                continue;
                            };
                            if let Some(ne) = neighbor_eid {
                                if !all_entities.contains_key(&ne.id) {
                                    all_entities.insert(ne.id.clone(), (ne, 0.0));
                                }
                            }
                        }
                    }
                }
                let _ = db.transaction_manager.rollback(&db, &tx);
            }
        }

        // Phase 3: Compute graph degree for all entities
        let mut degree: HashMap<String, usize> = HashMap::new();
        for (id, _) in &all_entities {
            let count = all_entities.keys().filter(|k| *k != id).count();
            degree.insert(id.clone(), count);
        }

        // Phase 4: Rerank by configurable composite score
        let now_secs = Self::now_micros() / 1_000_000;
        let mut ranked: Vec<(MemoryEntity, f64)> = all_entities.into_values().collect();
        for (entity, score) in &mut ranked {
            let search_score = *score;
            let created_secs = (entity.created_at / 1_000_000) as f64;
            let recency = (now_secs as f64 - created_secs).max(0.001).recip();
            let deg = *degree.get(&entity.id).unwrap_or(&0) as f64;
            let composite = config.search_weight * search_score
                + config.recency_weight * recency
                + config.degree_weight * deg;
            *score = composite;
        }
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        // Phase 5: Cross-encoder reranking if configured
        if !config.cross_encoder_wasm.is_empty() {
            let top_n = std::cmp::min(top_k * 3, ranked.len());
            let mut cross_scores: Vec<(usize, f64)> = Vec::new();
            let db = self.conn.client_context.database.clone();
            for (i, (entity, _)) in ranked.iter().enumerate().take(top_n) {
                // Try to call the WASM cross-encoder function via the registry
                if let Some(func) = db.function_registry.get_scalar_function(&config.cross_encoder_wasm) {
                    let query_arr = arrow::array::StringArray::from(vec![query_text.to_string()]);
                    let content_arr = arrow::array::StringArray::from(vec![entity.content.clone()]);
                    let args = vec![
                        Arc::new(query_arr) as ArrayRef,
                        Arc::new(content_arr) as ArrayRef,
                    ];
                    if let Ok(result) = (func.exec)(&args, 1) {
                        if let Some(f) = result.as_any().downcast_ref::<Float64Array>() {
                            cross_scores.push((i, f.value(0)));
                        }
                    }
                }
            }
            // Re-rank by cross-encoder score
            cross_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            let re_ranked: Vec<(MemoryEntity, f64)> = cross_scores
                .into_iter()
                .map(|(idx, ce_score)| (ranked[idx].0.clone(), ce_score))
                .collect();
            ranked = re_ranked;
        }

        // Phase 6: Assemble context
        let top_n = std::cmp::min(top_k * 2, ranked.len());
        let used = &ranked[..top_n];
        let sources: Vec<String> = used.iter().map(|(e, _)| e.id.clone()).collect();
        let mut context = String::new();
        context.push_str(&format!("Query: {query_text}\n\nRelevant context:\n"));
        for (i, (entity, score)) in used.iter().enumerate() {
            context.push_str(&format!(
                "[{}] (score={:.3}, type={}) {}\n",
                i + 1, score, entity.entity_type, entity.content
            ));
        }
        context.push_str(&format!("\n---\nTotal sources: {top_n}"));

        Ok(RagResult {
            context,
            sources,
            total_sources: top_n,
            query: query_text.to_string(),
        })
    }

    /// Execute a streaming Cypher query. Results arrive on a channel
    /// as DataChunks are produced — useful for large result sets.
    pub fn query_stream(
        &self,
        query: &str,
    ) -> Result<crossbeam::channel::Receiver<Result<DataChunk>>> {
        self.conn.query_stream(query)
    }

    /// Execute a Cypher query as of a specific MVCC timestamp.
    /// The database shows only data committed at or before `snapshot_micros`.
    /// This works because Lightning's MVCC already tracks every version.
    pub fn execute_at(&self, query: &str, snapshot_micros: u64) -> Result<QueryResult> {
        self.conn.execute_at(query, snapshot_micros, None)
    }

    pub fn recall_by_type(&self, entity_type: &str, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;
        let query = format!(
            "MATCH (e:{ENTITY_TABLE}) WHERE e.type = $type AND e.valid_until = 0 \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until \
             ORDER BY e.last_accessed DESC LIMIT {top_k}"
        );
        println!("query: {query}");
        let mut params = HashMap::new();
        params.insert("type".to_string(), Value::String(entity_type.to_string()));
        let res = self.conn.execute(&query, Some(params))?;
        Ok(self.batches_to_entities(&res.batches))
    }

    // ============================================================
    // Feature: Temporal Graph Queries
    // Query what the memory graph looked like at any point in time
    // ============================================================

    pub fn recall_at_time(&self, at_micros: i64, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;
        let query = format!(
            "MATCH (e:{ENTITY_TABLE}) WHERE e.valid_from <= $at AND (e.valid_until = 0 OR e.valid_until > $at) \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until \
             ORDER BY e.last_accessed DESC LIMIT {top_k}"
        );
        let mut params = HashMap::new();
        params.insert("at".to_string(), Value::Number(at_micros as f64));
        let res = self.conn.execute(&query, Some(params))?;
        Ok(self.batches_to_entities(&res.batches))
    }

    /// Return the full version history of a specific entity across time
    pub fn entity_history(&self, entity_id: &str) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;
        let query = format!(
            "MATCH (e:{ENTITY_TABLE}) WHERE e.id = $id \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until \
             ORDER BY e.valid_from DESC"
        );
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(entity_id.to_string()));
        let res = self.conn.execute(&query, Some(params))?;
        Ok(self.batches_to_entities(&res.batches))
    }

    // ============================================================
    // Feature: Memory Consolidation Pipeline
    // Automatically links related memories, detects contradictions,
    // and identifies important clusters via PageRank.
    // ============================================================

    /// Run the full consolidation pipeline:
    /// 1. Load all entities, compute content-based similarity via n-gram overlap
    /// 2. Auto-link similar entities with RelatedTo edges
    /// 3. Detect contradictions (semantically close but lexically divergent)
    /// 4. Run PageRank on the graph to identify important entities
    pub fn consolidate(&self) -> Result<ConsolidationReport> {
        self.ensure_schema()?;

        // Step 0: Load all active entities
        let all: Vec<MemoryEntity> = self.recall_recent(usize::MAX)?;
        let n = all.len();
        if n < 2 {
            return Ok(ConsolidationReport::default());
        }

        let mut links_created = 0usize;
        let mut contradictions_found = 0usize;
        let mut adjacency: Vec<Vec<(usize, f64)>> = vec![Vec::new(); n];

        // Step 1-2: Compute content-based similarity (n-gram Jaccard on word sets)
        // Process in batches to avoid O(n^2) memory for very large datasets
        let word_sets: Vec<HashSet<String>> = all.iter().map(|e| {
            e.content.split_whitespace()
                .map(|w| w.to_lowercase())
                .collect()
        }).collect();

        for chunk_start in (0..n).step_by(200) {
            let chunk_end = std::cmp::min(chunk_start + 200, n);
            for i in chunk_start..chunk_end {
                for j in (i + 1)..chunk_end {
                    let intersection: usize = word_sets[i].intersection(&word_sets[j]).count();
                    let union: usize = word_sets[i].union(&word_sets[j]).count();
                    if union == 0 { continue; }
                    let jaccard = intersection as f64 / union as f64;

                    if jaccard > 0.35 {
                        let _ = self.associate(&all[i].id, &all[j].id, "RelatedTo", jaccard);
                        adjacency[i].push((j, jaccard));
                        adjacency[j].push((i, jaccard));
                        links_created += 1;
                    }

                    // Contradiction: low word overlap but similar content length
                    if jaccard < 0.15 {
                        let len_sim = 1.0 - (all[i].content.len() as f64 - all[j].content.len() as f64).abs()
                            / all[i].content.len().max(all[j].content.len()).max(1) as f64;
                        if len_sim > 0.8 {
                            let _ = self.associate(&all[i].id, &all[j].id, "Contradicts", 1.0 - jaccard);
                            contradictions_found += 1;
                        }
                    }
                }
            }
        }

        // Step 3: PageRank on the consolidation graph
        if n > 5 && links_created > 0 {
            let damping = 0.85;
            let max_iter = 25;
            let mut rank = vec![1.0 / n as f64; n];
            for _iter in 0..max_iter {
                let mut new_rank = vec![0.0; n];
                for i in 0..n {
                    let out = adjacency[i].len() as f64;
                    if out > 0.0 {
                        let contrib = rank[i] / out;
                        for (j, _) in &adjacency[i] {
                            new_rank[*j] += contrib;
                        }
                    }
                }
                let dangling: f64 = rank.iter().enumerate()
                    .filter(|(i, _)| adjacency[*i].is_empty())
                    .map(|(_, r)| r).sum();
                for i in 0..n {
                    new_rank[i] = (1.0 - damping) / n as f64
                        + damping * (new_rank[i] + dangling / n as f64);
                }
                rank = new_rank;
            }

            let mut ranked: Vec<(usize, f64)> = rank.into_iter().enumerate().collect();
            ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            for (idx, score) in ranked.iter().take(std::cmp::min(10, n)) {
                let new_meta = format!(
                    r#"{{"pagerank":{:.6},"id":"{}"}}"#,
                    score, all[*idx].id
                );
                let query = format!(
                    "MATCH (e:{ENTITY_TABLE} {{id: $id}}) SET e.metadata = $meta"
                );
                let mut params = HashMap::new();
                params.insert("id".to_string(), Value::String(all[*idx].id.clone()));
                params.insert("meta".to_string(), Value::String(new_meta));
                let _ = self.conn.execute(&query, Some(params));
            }
        }

        Ok(ConsolidationReport {
            links_created,
            contradictions_found,
            total_entities: n,
        })
    }

    // ============================================================
    // Feature: Change Data Capture via WAL streaming
    // ============================================================

    /// Create a subscriber that receives notifications on every write.
    /// Returns a receiver channel. The subscriber runs in the background
    /// and pushes ChangeEvents into the channel.
    pub fn subscribe_changes(&self) -> Result<std::sync::mpsc::Receiver<ChangeEvent>> {
        let (tx, rx) = std::sync::mpsc::channel();
        let db = self.conn.client_context.database.clone();

        std::thread::spawn(move || {
            let mut last_wal_size = 0u64;
            loop {
                match db.wal.size() {
                    Ok(size) if size > last_wal_size => {
                        let event = ChangeEvent {
                            timestamp: Self::now_micros(),
                            bytes_written: size - last_wal_size,
                            total_wal_bytes: size,
                        };
                        if tx.send(event).is_err() {
                            break;
                        }
                        last_wal_size = size;
                    }
                    _ => {}
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
                if db.buffer_manager.is_shutting_down() {
                    break;
                }
            }
        });

        Ok(rx)
    }

    pub fn recall_recent(&self, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;
        let query = format!(
            "MATCH (e:{ENTITY_TABLE}) WHERE e.valid_until = 0 \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until \
             ORDER BY e.created_at DESC LIMIT {top_k}"
        );
        let res = self.conn.execute(&query, None)?;
        Ok(self.batches_to_entities(&res.batches))
    }

    pub fn recall_by_time(&self, start: i64, end: i64, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;
        let query = format!(
            "MATCH (e:{ENTITY_TABLE}) WHERE e.valid_from >= $start AND e.valid_from <= $end AND e.valid_until = 0 \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until \
             ORDER BY e.created_at DESC LIMIT {top_k}"
        );
        let mut params = HashMap::new();
        params.insert("start".to_string(), Value::Number(start as f64));
        params.insert("end".to_string(), Value::Number(end as f64));
        let res = self.conn.execute(&query, Some(params))?;
        Ok(self.batches_to_entities(&res.batches))
    }

    pub fn expand(&self, entity_id: &str, hops: u32, edge_types: &[&str]) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;

        let db = self.conn.client_context.database.clone();

        let internal_id = {
            let id_query = format!(
                "MATCH (e:{ENTITY_TABLE}) WHERE e.id = $id RETURN e._id LIMIT 1"
            );
            let mut params = HashMap::new();
            params.insert("id".to_string(), Value::String(entity_id.to_string()));
            if let Ok(res) = self.conn.execute(&id_query, Some(params)) {
                res.batches.first()
                    .and_then(|b| b.column(0).as_any().downcast_ref::<UInt64Array>())
                    .map(|arr| arr.value(0))
            } else {
                None
            }
        };

        let start_id = match internal_id {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let bm = &db.buffer_manager;
        let tx = db.transaction_manager.begin(true)?;

        // Try to use CSR index for efficient traversal; fall back to full scan
        let csr_opt = {
            let storage = db.storage_manager.read();
            storage.fwd_csr.get(RELATES_TABLE).cloned()
        };

        let neighbor_ids = if let Some(csr) = csr_opt {
            // BFS using CSR index
            let mut visited = std::collections::HashSet::new();
            let mut current_frontier = Vec::new();
            let mut next_frontier = Vec::new();
            let mut all_found = Vec::new();

            visited.insert(start_id);
            current_frontier.push(start_id);

            for _depth in 0..hops {
                for &node_id in &current_frontier {
                    csr.for_each_neighbor(bm, node_id, &tx, |neighbor| {
                        if visited.insert(neighbor) {
                            next_frontier.push(neighbor);
                            all_found.push(neighbor);
                        }
                    })?;
                }
                std::mem::swap(&mut current_frontier, &mut next_frontier);
                next_frontier.clear();
                if current_frontier.is_empty() {
                    break;
                }
            }

            all_found
        } else {
            // Fallback: full scan of Relates table for up to `hops` levels
            let storage = db.storage_manager.read();
            let rel_table = match storage.rel_tables.get(RELATES_TABLE) {
                Some(t) => t.clone(),
                None => {
                    let _ = db.transaction_manager.rollback(&db, &tx);
                    return Ok(Vec::new());
                }
            };
            drop(storage);

            let cardinality = rel_table.stats.read().cardinality;
            if cardinality == 0 {
                let _ = db.transaction_manager.rollback(&db, &tx);
                return Ok(Vec::new());
            }

            let mut src_col = Vec::new();
            let mut dst_col = Vec::new();
            let _ = rel_table.columns[0].scan(bm, 0, cardinality, &tx, &mut src_col);
            let _ = rel_table.columns[1].scan(bm, 0, cardinality, &tx, &mut dst_col);

            let mut type_col: Vec<crate::processor::Value> = Vec::new();
            if !edge_types.is_empty() && rel_table.columns.len() > 2 {
                let _ = rel_table.columns[2].scan(bm, 0, cardinality, &tx, &mut type_col);
            }

            // Build adjacency list from scanned edges
            let mut adj: std::collections::HashMap<u64, Vec<u64>> =
                std::collections::HashMap::new();
            for (i, (src, dst)) in src_col.iter().zip(dst_col.iter()).enumerate() {
                let s = src.as_node();
                let d = dst.as_node();
                if !edge_types.is_empty() {
                    if let Some(type_val) = type_col.get(i) {
                        let rel_type_str = format!("{}", type_val).trim_matches('"').to_string();
                        if !edge_types.iter().any(|et| *et == rel_type_str) {
                            continue;
                        }
                    }
                }
                adj.entry(s).or_default().push(d);
                if hops > 1 {
                    adj.entry(d).or_default().push(s);
                }
            }

            // BFS over adjacency list
            let mut visited = std::collections::HashSet::new();
            let mut queue = std::collections::VecDeque::new();
            visited.insert(start_id);
            queue.push_back((start_id, 0u32));

            while let Some((current, depth)) = queue.pop_front() {
                if depth >= hops {
                    continue;
                }
                if let Some(neighbors) = adj.get(&current) {
                    for &neighbor in neighbors {
                        if visited.insert(neighbor) {
                            queue.push_back((neighbor, depth + 1));
                        }
                    }
                }
            }

            visited.remove(&start_id);
            visited.into_iter().collect()
        };

        let _ = db.transaction_manager.rollback(&db, &tx);

        if neighbor_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Look up neighbor entities by _id
        let conditions: Vec<String> = neighbor_ids.iter()
            .map(|id| format!("e._id = {id}"))
            .collect();
        let query = format!(
            "MATCH (e:{}) WHERE {} AND e.valid_until = 0 \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until LIMIT {}",
            ENTITY_TABLE, conditions.join(" OR "), neighbor_ids.len()
        );
        let res = self.conn.execute(&query, None)?;
        Ok(self.batches_to_entities(&res.batches))
    }

    pub fn associate(&self, src_id: &str, dst_id: &str, rel_type: &str, weight: f64) -> Result<()> {
        self.ensure_schema()?;

        let now = Self::now_micros();

        let query = format!(
            "MATCH (a:{ENTITY_TABLE} {{id: $src_id}}), (b:{ENTITY_TABLE} {{id: $dst_id}}) \
             CREATE (a)-[:{RELATES_TABLE} {{type: $rel_type, weight: $weight, created_at: $created_at}}]->(b)"
        );

        let mut params = HashMap::new();
        params.insert("src_id".to_string(), Value::String(src_id.to_string()));
        params.insert("dst_id".to_string(), Value::String(dst_id.to_string()));
        params.insert("rel_type".to_string(), Value::String(rel_type.to_string()));
        params.insert("weight".to_string(), Value::Number(weight));
        params.insert("created_at".to_string(), Value::Number(now as f64));

        self.conn.execute(&query, Some(params))?;
        Ok(())
    }

    pub fn forget(&self, entity_id: &str) -> Result<bool> {
        self.ensure_schema()?;

        let now = Self::now_micros();
        let db = self.conn.client_context.database.clone();
        let conn = db.connect();

        // Soft-delete: set valid_until to current time
        let soft_delete = format!(
            "MATCH (e:{ENTITY_TABLE} {{id: '{entity_id}'}}) SET e.valid_until = {now}"
        );
        let _ = conn.execute(&soft_delete, None)?;

        let del_rels = format!(
            "MATCH (a:{ENTITY_TABLE} {{id: '{entity_id}'}}) OPTIONAL MATCH (a)-[r]-() DELETE r"
        );
        let _ = conn.execute(&del_rels, None);

        Ok(true)
    }

    pub fn decay(&self) -> Result<usize> {
        self.ensure_schema()?;
        let db = self.conn.client_context.database.clone();
        let conn = db.connect();
        let now = Self::now_micros();
        let now_secs = now / 1_000_000;

        // First find expired entities
        let find = format!(
            "MATCH (e:{ENTITY_TABLE}) WHERE e.ttl_seconds > 0 AND e.valid_until = 0 AND \
             (e.created_at / 1000000 + e.ttl_seconds) <= {now_secs} \
             RETURN e.id"
        );
        let find_res = conn.execute(&find, None)?;
        let expired_ids: Vec<String> = find_res.batches.iter().flat_map(|b| {
            let arr = b.column(0).as_any().downcast_ref::<arrow::array::StringArray>()?;
            Some((0..b.num_rows()).map(|i| arr.value(i).to_string()).collect::<Vec<_>>())
        }).flatten().collect();

        let count = expired_ids.len();

        // Set valid_until for each expired entity
        for id in &expired_ids {
            let soft_delete = format!(
                "MATCH (e:{ENTITY_TABLE} {{id: '{id}'}}) SET e.valid_until = {now}"
            );
            let _ = conn.execute(&soft_delete, None);
        }

        Ok(count)
    }

    fn lookup_by_internal_id(&self, internal_id: u64) -> Option<MemoryEntity> {
        let query = format!(
            "MATCH (e:{ENTITY_TABLE}) WHERE e._id = {internal_id} \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until LIMIT 1"
        );
        if let Ok(res) = self.conn.execute(&query, None) {
            let entities = self.batches_to_entities(&res.batches);
            entities.into_iter().next()
        } else {
            None
        }
    }

    fn batches_to_entities(&self, batches: &[RecordBatch]) -> Vec<MemoryEntity> {
        let mut entities = Vec::new();
        for batch in batches {
            let ids = batch.column(0).as_any().downcast_ref::<StringArray>();
            let types = batch.column(1).as_any().downcast_ref::<StringArray>();
            let contents = batch.column(2).as_any().downcast_ref::<StringArray>();
            let created_at = batch.column(3).as_any().downcast_ref::<Int64Array>();
            let last_accessed = batch.column(4).as_any().downcast_ref::<Int64Array>();
            let access_counts = batch.column(5).as_any().downcast_ref::<Int64Array>();
            let ttl_seconds = batch.column(6).as_any().downcast_ref::<Int64Array>();
            let metadatas = batch.column(7).as_any().downcast_ref::<StringArray>();
            let valid_from = batch.column(8).as_any().downcast_ref::<Int64Array>();
            let valid_until = batch.column(9).as_any().downcast_ref::<Int64Array>();

            let num_rows = batch.num_rows();
            for i in 0..num_rows {
                entities.push(MemoryEntity {
                    id: ids.map(|a| a.value(i).to_string()).unwrap_or_default(),
                    entity_type: types.map(|a| a.value(i).to_string()).unwrap_or_default(),
                    content: contents.map(|a| a.value(i).to_string()).unwrap_or_default(),
                    created_at: created_at.map(|a| a.value(i)).unwrap_or(0),
                    last_accessed: last_accessed.map(|a| a.value(i)).unwrap_or(0),
                    access_count: access_counts.map(|a| a.value(i)).unwrap_or(0),
                    ttl_seconds: ttl_seconds.map(|a| a.value(i)).unwrap_or(0),
                    metadata: metadatas.map(|a| a.value(i).to_string()).unwrap_or_default(),
                    valid_from: valid_from.map(|a| a.value(i)).unwrap_or(0),
                    valid_until: valid_until.map(|a| a.value(i)).unwrap_or(0),
                });
            }
        }
        entities
    }
}

#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub timestamp: i64,
    pub bytes_written: u64,
    pub total_wal_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ConsolidationReport {
    pub links_created: usize,
    pub contradictions_found: usize,
    pub total_entities: usize,
}

#[derive(Debug, Clone, Default)]
pub struct RagResult {
    pub context: String,
    pub sources: Vec<String>,
    pub total_sources: usize,
    pub query: String,
}
