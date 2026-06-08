use crate::processor::{DataChunk, Value};
use crate::Result;
use crate::Connection;
use crate::QueryResult;
use arrow::array::{Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crossbeam::channel::Receiver;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const ENTITY_TABLE: &str = "Entity";
const RELATES_TABLE: &str = "Relates";
pub const DEFAULT_EMBEDDING_DIM: usize = 768;
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
    pub embedding: Vec<f32>,
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
            embedding: Vec::new(),
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
    cdc_senders: parking_lot::Mutex<Vec<crossbeam::channel::Sender<ChangeEvent>>>,
}

impl MemoryStore {
    pub fn new(conn: Connection, embedding_dim: usize) -> Self {
        Self {
            conn,
            embedding_dim,
            schema_initialized: std::sync::atomic::AtomicBool::new(false),
            cdc_senders: parking_lot::Mutex::new(Vec::new()),
        }
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
                if let Err(e) = storage.create_fts_index(ENTITY_TABLE) {
                    tracing::warn!("MemoryStore: failed to create FTS index for {}: {}", ENTITY_TABLE, e);
                }
                if let Err(e) = storage.create_vector_index(ENTITY_TABLE, self.embedding_dim) {
                    tracing::warn!("MemoryStore: failed to create vector index for {}: {}", ENTITY_TABLE, e);
                }
            }
        }

        self.schema_initialized.store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    pub fn now_micros_for_test() -> i64 {
        Self::now_micros()
    }

    pub fn embedding_dim(&self) -> usize {
        self.embedding_dim
    }

    fn now_micros() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    }

    pub fn store(&self, entity: MemoryEntity) -> Result<()> {
        self.ensure_schema()?;
        if let Err(e) = self.forget(&entity.id) {
            tracing::warn!("MemoryStore: failed to forget entity {} before storing: {}", entity.id, e);
        }
        let eid = entity.id.clone();
        self.store_batch(vec![entity])?;
        self.emit_cdc_event(Some(eid), Some("INSERT".to_string()));
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

        for e in &entities {
            ids.push(e.id.clone());
            types.push(e.entity_type.clone());
            contents.push(e.content.clone());
            created_at.push(e.created_at.max(now));
            last_accessed.push(now);
            access_counts.push(e.access_count.max(1));
            ttl_seconds.push(e.ttl_seconds);
            metadatas.push(e.metadata.clone());
            valid_from.push(e.valid_from.max(now));
            valid_until.push(if e.valid_until == 0 { i64::MAX } else { e.valid_until });
        }

        let emb_dim = self.embedding_dim;
        let has_embedding = entities.iter().any(|e| !e.embedding.is_empty());

        let mut fields: Vec<Field> = vec![
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
        ];

        let mut columns: Vec<ArrayRef> = vec![
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
        ];

        if has_embedding {
            let mut emb_values: Vec<f32> = Vec::with_capacity(num_rows * emb_dim);
            for e in &entities {
                if e.embedding.len() == emb_dim {
                    emb_values.extend_from_slice(&e.embedding);
                } else {
                    emb_values.extend(std::iter::repeat(0.0f32).take(emb_dim));
                }
            }
            let emb_values_array = Float32Array::from(emb_values);
            let emb_list = FixedSizeListArray::new(
                Arc::new(Field::new("item", DataType::Float32, true)),
                emb_dim as i32,
                Arc::new(emb_values_array),
                None,
            );
            fields.push(Field::new("embedding", DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                emb_dim as i32,
            ), true));
            columns.push(Arc::new(emb_list));
        }

        let schema = Schema::new(fields);
        let batch = RecordBatch::try_new(Arc::new(schema), columns)?;

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
            if let Err(e) = db.transaction_manager.rollback(&db, &tx) {
                tracing::warn!("MemoryStore: FTS transaction rollback failed: {}", e);
            }
        }

        if !embedding.is_empty() && embedding.len() == self.embedding_dim {
            if let Some(vec_idx) = storage.vector_indexes.get(ENTITY_TABLE) {
                let tx = db.transaction_manager.begin(true)?;
                if let Ok(vec_results) = vec_idx.search(embedding, top_k * 2, &db.buffer_manager, &tx) {
                    for (rank, (node_id, _)) in vec_results.iter().enumerate() {
                        if let Some(entity) = self.lookup_by_internal_id(*node_id) {
                            let rrf_score = 1.0 / (k + (rank as f64) + 1.0);
                            results.entry(entity.id.clone()).or_insert((entity, 0.0)).1 += rrf_score;
                        }
                    }
                }
                if let Err(e) = db.transaction_manager.rollback(&db, &tx) {
                    tracing::warn!("MemoryStore: vector index transaction rollback failed: {}", e);
                }
            }
        }

        let mut sorted: Vec<SearchResult> = results
            .into_iter()
            .map(|(_, (entity, score))| SearchResult { entity, score })
            .collect();
        sorted.sort_by(|a, b| b.score.partial_cmp(&a.score).expect("infallible: scores are finite"));
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
        let embed_dim = self.embedding_dim;

        std::thread::spawn(move || {
            let new_conn = conn.connect();
            let store = MemoryStore::new(new_conn, embed_dim);
            let results = match store.recall(&query_text, &embedding, top_k) {
                Ok(r) => r,
                Err(e) => {
                    if tx.send(Err(e)).is_err() {
                        tracing::warn!("MemoryStore: recall_stream channel closed, dropping error");
                    }
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

        // Phase 3 prep: initialize degree map for all entities
        let mut degree: HashMap<String, usize> = HashMap::new();
        for (id, _) in &all_entities {
            degree.insert(id.clone(), 0);
        }

        // Phase 2: Graph expansion — find neighbors for top results using CSR index
        let top_for_expansion = std::cmp::min(config.expansion_depth, initial.len());
        let db = self.conn.client_context.database.clone();
        for i in 0..top_for_expansion {
            if let Ok(neighbors) = self.expand(&initial[i].entity.id, 1, &[]) {
                for neighbor in &neighbors {
                    if !all_entities.contains_key(&neighbor.id) {
                        all_entities.insert(neighbor.id.clone(), (neighbor.clone(), 0.0));
                    }
                }
            }
        }

        // Compute graph degree for each entity using CSR index
        if let Ok(tx) = db.transaction_manager.begin(true) {
            let storage = db.storage_manager.read();
            if let Some(fwd_csr) = storage.fwd_csr.get(RELATES_TABLE) {
                let bm = &db.buffer_manager;
                for (eid, _) in &all_entities {
                    let lookup = format!(
                        "MATCH (e:{ENTITY_TABLE}) WHERE e.id = $id RETURN e._id LIMIT 1"
                    );
                    let mut params = HashMap::new();
                    params.insert("id".to_string(), Value::String(eid.clone()));
                    if let Ok(res) = self.conn.execute(&lookup, Some(params)) {
                        if let Some(b) = res.batches.first() {
                            if let Some(arr) = b.column(0).as_any().downcast_ref::<UInt64Array>() {
                                let internal_id = arr.value(0);
                                let mut count = 0u64;
                                let _ = fwd_csr.for_each_neighbor(bm, internal_id, &tx, |_| count += 1);
                                *degree.get_mut(eid).unwrap_or(&mut 0) = count as usize;
                            }
                        }
                    }
                }
            }
            let _ = db.transaction_manager.rollback(&db, &tx);
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
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).expect("infallible: scores are finite"));

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
            cross_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).expect("infallible: scores are finite"));
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
            "MATCH (e:{ENTITY_TABLE}) WHERE e.type = $type AND (e.valid_until = 0 OR e.valid_until = 9223372036854775807) \
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
            "MATCH (e:{ENTITY_TABLE}) \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until \
             ORDER BY e.last_accessed DESC LIMIT {top_k}"
        );
        let res = self.conn.execute_at(&query, at_micros as u64, None)?;
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
    pub fn consolidate(&self, config: Option<ConsolidationConfig>) -> Result<ConsolidationReport> {
        self.ensure_schema()?;
        let cfg = config.unwrap_or_default();
        let mut warnings: Vec<String> = Vec::new();

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
        // and embedding-based cosine similarity for contradiction detection.
        // Process in batches to avoid O(n^2) memory for very large datasets.
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

                    if jaccard > cfg.similarity_threshold {
                        if let Err(e) = self.associate(&all[i].id, &all[j].id, "RelatedTo", jaccard) {
                            let msg = format!("MemoryStore: failed to associate RelatedTo link: {e}");
                            tracing::warn!("{msg}");
                            warnings.push(msg);
                        }
                        adjacency[i].push((j, jaccard));
                        adjacency[j].push((i, jaccard));
                        links_created += 1;
                    }

                    // Contradiction detection: embeddings are similar but word sets are different.
                    // This catches cases like "User likes Python" vs "User dislikes Python"
                    // where embeddings are similar (same topic) but words differ (opposite sentiment).
                    if jaccard < cfg.contradiction_jaccard_max
                        && all[i].embedding.len() >= cfg.contradiction_length_sim_min as usize
                        && all[j].embedding.len() >= cfg.contradiction_length_sim_min as usize
                    {
                        let dot: f32 = all[i].embedding.iter()
                            .zip(all[j].embedding.iter())
                            .map(|(a, b)| a * b)
                            .sum();
                        let norm_i: f32 = all[i].embedding.iter().map(|v| v * v).sum::<f32>().sqrt();
                        let norm_j: f32 = all[j].embedding.iter().map(|v| v * v).sum::<f32>().sqrt();
                        let cosine = (dot / (norm_i * norm_j.max(f32::EPSILON))) as f64;

                        if cosine > cfg.contradiction_cosine_min {
                            if let Err(e) = self.associate(&all[i].id, &all[j].id, "Contradicts", 1.0 - jaccard) {
                                let msg = format!("MemoryStore: failed to associate Contradicts link: {e}");
                                tracing::warn!("{msg}");
                                warnings.push(msg);
                            }
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
            ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).expect("infallible: scores are finite"));
            let top_n = std::cmp::min(10, n);
            if top_n > 0 {
                let mut unwind_parts: Vec<String> = Vec::with_capacity(top_n);
                let mut id_params: Vec<String> = Vec::with_capacity(top_n);
                let mut meta_params: Vec<String> = Vec::with_capacity(top_n);
                for (idx, score) in ranked.iter().take(top_n) {
                    let pid = format!("id_{}", idx);
                    let pmid = format!("meta_{}", idx);
                    unwind_parts.push(format!("{{id: ${pid}, meta: ${pmid}}}"));
                    id_params.push(pid);
                    meta_params.push(pmid);
                    let new_meta = format!(
                        r#"{{"pagerank":{:.6},"id":"{}"}}"#,
                        score, all[*idx].id
                    );
                }
                let unwind_expr = unwind_parts.join(", ");
                let batch_query = format!(
                    "UNWIND [{unwind_expr}] AS row MATCH (e:{ENTITY_TABLE} {{id: row.id}}) SET e.metadata = row.meta"
                );
                let mut params = HashMap::new();
                for ((idx, score), (pid, pmid)) in ranked.iter().take(top_n).zip(
                    id_params.iter().zip(meta_params.iter())
                ) {
                    params.insert(pid.clone(), Value::String(all[*idx].id.clone()));
                    params.insert(pmid.clone(), Value::String(format!(
                        r#"{{"pagerank":{:.6},"id":"{}"}}"#,
                        score, all[*idx].id
                    )));
                }
                if let Err(e) = self.conn.execute(&batch_query, Some(params)) {
                    let msg = format!("MemoryStore: failed to batch update PageRank metadata: {e}");
                    tracing::warn!("{msg}");
                    warnings.push(msg);
                }
            }
        }

        Ok(ConsolidationReport {
            links_created,
            contradictions_found,
            total_entities: n,
            warnings,
        })
    }

    // ============================================================
    // Feature: Change Data Capture via WAL streaming
    // ============================================================

    /// Create a subscriber that receives notifications on every write.
    /// Returns a receiver channel. The subscriber runs in the background
    /// and pushes ChangeEvents into the channel.
    pub fn subscribe_changes(&self) -> Result<crossbeam::channel::Receiver<ChangeEvent>> {
        let (tx, rx) = crossbeam::channel::bounded(64);
        self.cdc_senders.lock().push(tx);
        Ok(rx)
    }

    fn emit_cdc_event(&self, entity_id: Option<String>, operation_type: Option<String>) {
        let event = ChangeEvent {
            timestamp: Self::now_micros(),
            bytes_written: 0,
            total_wal_bytes: 0,
            entity_id,
            operation_type,
        };
        let senders = self.cdc_senders.lock();
        for tx in senders.iter() {
            if tx.try_send(event.clone()).is_err() {
                // Channel full (slow consumer) — block until space is available.
                // This applies backpressure instead of silently dropping the event.
                let _ = tx.send(event.clone());
            }
        }
    }

    pub fn recall_recent(&self, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;
        let query = format!(
            "MATCH (e:{ENTITY_TABLE}) WHERE (e.valid_until = 0 OR e.valid_until = 9223372036854775807) \
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
            "MATCH (e:{ENTITY_TABLE}) WHERE e.valid_from >= $start AND e.valid_from <= $end AND (e.valid_until = 0 OR e.valid_until = 9223372036854775807) \
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
                if let Err(e) = db.transaction_manager.rollback(&db, &tx) {
                    tracing::warn!("MemoryStore: rag_query transaction rollback failed: {}", e);
                }
                    return Ok(Vec::new());
                }
            };
            drop(storage);

            let cardinality = rel_table.stats.read().cardinality;
            if cardinality == 0 {
                if let Err(e) = db.transaction_manager.rollback(&db, &tx) {
                    tracing::warn!("MemoryStore: expand transaction rollback failed: {}", e);
                }
                return Ok(Vec::new());
            }

            let mut src_col = Vec::new();
            let mut dst_col = Vec::new();
            if let Err(e) = rel_table.columns[0].scan(bm, 0, cardinality, &tx, &mut src_col) {
                tracing::warn!("MemoryStore: failed to scan src column in expand: {}", e);
            }
            if let Err(e) = rel_table.columns[1].scan(bm, 0, cardinality, &tx, &mut dst_col) {
                tracing::warn!("MemoryStore: failed to scan dst column in expand: {}", e);
            }

            let mut type_col: Vec<crate::processor::Value> = Vec::new();
            if !edge_types.is_empty() && rel_table.columns.len() > 2 {
                if let Err(e) = rel_table.columns[2].scan(bm, 0, cardinality, &tx, &mut type_col) {
                    tracing::warn!("MemoryStore: failed to scan type column in expand: {}", e);
                }
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
            "MATCH (e:{}) WHERE {} AND (e.valid_until = 0 OR e.valid_until = 9223372036854775807) \
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

    pub fn get(&self, entity_id: &str) -> Result<Option<MemoryEntity>> {
        use arrow::array::Array;
        let conn = self.conn.client_context.database.connect();
        let now = Self::now_micros();
        let query = format!(
            "MATCH (e:{ENTITY_TABLE} {{id: $id}}) WHERE e.valid_until > $now RETURN e.id, e.entity_type, e.content, e.metadata"
        );
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(entity_id.to_string()));
        params.insert("now".to_string(), Value::Number(now as f64));
        match conn.execute(&query, Some(params)) {
            Ok(res) => {
                for batch in &res.batches {
                    if batch.num_rows() == 0 { continue; }
                    let ids = batch.column(0).as_any().downcast_ref::<StringArray>();
                    let types = batch.column(1).as_any().downcast_ref::<StringArray>();
                    let contents = batch.column(2).as_any().downcast_ref::<StringArray>();
                    let metadatas = batch.column(3).as_any().downcast_ref::<StringArray>();
                    if let (Some(ids), Some(types), Some(contents), Some(metadatas)) = (ids, types, contents, metadatas) {
                        if !ids.is_null(0) {
                            return Ok(Some(MemoryEntity {
                                id: ids.value(0).to_string(),
                                entity_type: types.value(0).to_string(),
                                content: contents.value(0).to_string(),
                                metadata: metadatas.value(0).to_string(),
                                ..Default::default()
                            }));
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("not found") || msg.contains("exist") || msg.contains("no such table") {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    pub fn forget(&self, entity_id: &str) -> Result<bool> {
        self.ensure_schema()?;

        let now = Self::now_micros();
        let db = self.conn.client_context.database.clone();
        let conn = db.connect();

        let soft_delete = format!(
            "MATCH (e:{ENTITY_TABLE} {{id: $id}}) SET e.valid_until = $now"
        );
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(entity_id.to_string()));
        params.insert("now".to_string(), Value::Number(now as f64));
        let _ = conn.execute(&soft_delete, Some(params))?;

        let del_rels = format!(
            "MATCH (a:{ENTITY_TABLE} {{id: $id}}) OPTIONAL MATCH (a)-[r:{RELATES_TABLE}]-() DELETE r"
        );
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(entity_id.to_string()));
        if let Err(e) = conn.execute(&del_rels, Some(params)) {
            tracing::warn!("MemoryStore: failed to delete relations for entity {}: {}", entity_id, e);
        }

        self.emit_cdc_event(Some(entity_id.to_string()), Some("DELETE".to_string()));
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
            "MATCH (e:{ENTITY_TABLE}) WHERE e.ttl_seconds > 0 AND (e.valid_until = 0 OR e.valid_until = 9223372036854775807) AND \
             (e.created_at / 1000000 + e.ttl_seconds) <= $now \
             RETURN e.id"
        );
        let mut params = HashMap::new();
        params.insert("now".to_string(), Value::Number(now_secs as f64));
        let find_res = conn.execute(&find, Some(params))?;
        let expired_ids: Vec<String> = find_res.batches.iter().flat_map(|b| {
            let arr = b.column(0).as_any().downcast_ref::<arrow::array::StringArray>()?;
            Some((0..b.num_rows()).map(|i| arr.value(i).to_string()).collect::<Vec<_>>())
        }).flatten().collect();

        let count = expired_ids.len();

        // Set valid_until for each expired entity
        for id in &expired_ids {
            let soft_delete = format!(
                "MATCH (e:{ENTITY_TABLE} {{id: $id}}) SET e.valid_until = $now"
            );
            let mut params = HashMap::new();
            params.insert("id".to_string(), Value::String(id.to_string()));
            params.insert("now".to_string(), Value::Number(now as f64));
            if let Err(e) = conn.execute(&soft_delete, Some(params)) {
                tracing::warn!("MemoryStore: failed to soft-delete expired entity {}: {}", id, e);
            }
        }

        Ok(count)
    }

    fn lookup_by_internal_id(&self, internal_id: u64) -> Option<MemoryEntity> {
        let query = format!(
            "MATCH (e:{ENTITY_TABLE}) WHERE e._id = $id \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until LIMIT 1"
        );
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::Number(internal_id as f64));
        if let Ok(res) = self.conn.execute(&query, Some(params)) {
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
            let embeddings: Option<&FixedSizeListArray> = if batch.num_columns() > 10 {
                batch.column(10).as_any().downcast_ref::<FixedSizeListArray>()
            } else {
                None
            };

            let num_rows = batch.num_rows();
            let emb_dim = self.embedding_dim;
            for i in 0..num_rows {
                let embedding = if let Some(emb_arr) = embeddings {
                    emb_arr
                        .values()
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .map(|vals| {
                            let start = i * emb_dim;
                            let end = (i + 1) * emb_dim;
                            vals.values()[start..end].to_vec()
                        })
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
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
                    embedding,
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
    pub entity_id: Option<String>,
    pub operation_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ConsolidationConfig {
    pub similarity_threshold: f64,
    pub contradiction_jaccard_max: f64,
    pub contradiction_cosine_min: f64,
    pub contradiction_length_sim_min: f64,
}

impl Default for ConsolidationConfig {
    fn default() -> Self {
        Self {
            similarity_threshold: 0.35,
            contradiction_jaccard_max: 0.15,
            contradiction_cosine_min: 0.7,
            contradiction_length_sim_min: 0.8,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConsolidationReport {
    pub links_created: usize,
    pub contradictions_found: usize,
    pub total_entities: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RagResult {
    pub context: String,
    pub sources: Vec<String>,
    pub total_sources: usize,
    pub query: String,
}
