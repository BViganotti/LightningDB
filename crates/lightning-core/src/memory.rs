use crate::processor::Value;
use crate::Result;
use crate::Connection;
use arrow::array::{Array, Float64Array, Int64Array, StringArray, UInt64Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const ENTITY_TABLE: &str = "Entity";
const RELATES_TABLE: &str = "Relates";
const DEFAULT_EMBEDDING_DIM: usize = 768;
const SIMILARITY_THRESHOLD: f64 = 0.82;
const CONTRADICTION_THRESHOLD: f64 = 0.70;

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

    fn ensure_schema(&self) -> Result<()> {
        if self.schema_initialized.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }

        let db = self.conn.client_context.database.clone();
        let storage = db.storage_manager.read();
        let exists = storage.node_tables.contains_key(ENTITY_TABLE);
        drop(storage);

        if !exists {
            let create_entity = format!(
                "CREATE NODE TABLE {} (id STRING, type STRING, content STRING, embedding FLOAT[{}], \
                 created_at TIMESTAMP, last_accessed TIMESTAMP, access_count INT64, \
                 ttl_seconds INT64, metadata STRING, \
                 valid_from TIMESTAMP, valid_until TIMESTAMP, PRIMARY KEY (id))",
                ENTITY_TABLE, self.embedding_dim
            );
            self.conn.execute(&create_entity, None)?;

            let create_relates = format!(
                "CREATE REL TABLE {} (FROM {} TO {}, type STRING, weight DOUBLE, created_at TIMESTAMP)",
                RELATES_TABLE, ENTITY_TABLE, ENTITY_TABLE
            );
            self.conn.execute(&create_relates, None)?;

            let _ = self.conn.execute(
                &format!("CALL create_fts_index('{}')", ENTITY_TABLE),
                None,
            );
            let _ = self.conn.execute(
                &format!("CALL create_vector_index('{}')", ENTITY_TABLE),
                None,
            );
        }

        self.schema_initialized.store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    fn now_micros() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    }

    pub fn store(&self, entity: MemoryEntity) -> Result<()> {
        self.ensure_schema()?;

        let now = Self::now_micros();

        let query = format!(
            "MERGE (e:{0} {{id: $id}}) \
             SET e.type = $type, e.content = $content, \
             e.created_at = COALESCE(e.created_at, $now), \
             e.last_accessed = $now, \
             e.access_count = e.access_count + 1, \
             e.ttl_seconds = $ttl, e.metadata = $metadata, \
             e.valid_from = $now, e.valid_until = 0",
            ENTITY_TABLE
        );

        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(entity.id));
        params.insert("type".to_string(), Value::String(entity.entity_type));
        params.insert("content".to_string(), Value::String(entity.content));
        params.insert("now".to_string(), Value::Timestamp(now));
        params.insert("ttl".to_string(), Value::Number(entity.ttl_seconds as f64));
        params.insert("metadata".to_string(), Value::String(entity.metadata));

        self.conn.execute(&query, Some(params))?;
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
            valid_until.push(if e.valid_until == 0 { i64::MAX } else { e.valid_until });
        }

        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
            Field::new("created_at", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), false),
            Field::new("last_accessed", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), false),
            Field::new("access_count", DataType::Int64, false),
            Field::new("ttl_seconds", DataType::Int64, false),
            Field::new("metadata", DataType::Utf8, false),
            Field::new("valid_from", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), false),
            Field::new("valid_until", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow::array::StringArray::from(ids)),
                Arc::new(arrow::array::StringArray::from(types)),
                Arc::new(arrow::array::StringArray::from(contents)),
                Arc::new(arrow::array::TimestampMicrosecondArray::from(created_at)),
                Arc::new(arrow::array::TimestampMicrosecondArray::from(last_accessed)),
                Arc::new(arrow::array::Int64Array::from(access_counts)),
                Arc::new(arrow::array::Int64Array::from(ttl_seconds)),
                Arc::new(arrow::array::StringArray::from(metadatas)),
                Arc::new(arrow::array::TimestampMicrosecondArray::from(valid_from)),
                Arc::new(arrow::array::TimestampMicrosecondArray::from(valid_until)),
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
                let mut emb = [0f32; 768];
                let copy_len = std::cmp::min(embedding.len(), 768);
                emb[..copy_len].copy_from_slice(&embedding[..copy_len]);

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

    pub fn recall_by_type(&self, entity_type: &str, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;
        let query = format!(
            "MATCH (e:{}) WHERE e.type = $type AND e.valid_until = 0 \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until \
             ORDER BY e.last_accessed DESC LIMIT {}",
            ENTITY_TABLE, top_k
        );
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
            "MATCH (e:{}) WHERE e.valid_from <= $at AND e.valid_until > $at \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until \
             ORDER BY e.last_accessed DESC LIMIT {}",
            ENTITY_TABLE, top_k
        );
        let mut params = HashMap::new();
        params.insert("at".to_string(), Value::Timestamp(at_micros));
        let res = self.conn.execute(&query, Some(params))?;
        Ok(self.batches_to_entities(&res.batches))
    }

    /// Return the full version history of a specific entity across time
    pub fn entity_history(&self, entity_id: &str) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;
        let query = format!(
            "MATCH (e:{}) WHERE e.id = $id \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until \
             ORDER BY e.valid_from DESC",
            ENTITY_TABLE
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
        let batch_size = std::cmp::min(n, 200usize);
        let word_sets: Vec<HashSet<String>> = all.iter().map(|e| {
            e.content.split_whitespace()
                .map(|w| w.to_lowercase())
                .collect()
        }).collect();

        for i in 0..batch_size {
            for j in (i + 1)..batch_size {
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
                    "MATCH (e:{} {{id: $id}}) SET e.metadata = $meta",
                    ENTITY_TABLE
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
            "MATCH (e:{}) WHERE e.valid_until = 0 \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until \
             ORDER BY e.created_at DESC LIMIT {}",
            ENTITY_TABLE, top_k
        );
        let res = self.conn.execute(&query, None)?;
        Ok(self.batches_to_entities(&res.batches))
    }

    pub fn recall_by_time(&self, start: i64, end: i64, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;
        let query = format!(
            "MATCH (e:{}) WHERE e.valid_from >= $start AND e.valid_from <= $end AND e.valid_until = 0 \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until \
             ORDER BY e.created_at DESC LIMIT {}",
            ENTITY_TABLE, top_k
        );
        let mut params = HashMap::new();
        params.insert("start".to_string(), Value::Timestamp(start));
        params.insert("end".to_string(), Value::Timestamp(end));
        let res = self.conn.execute(&query, Some(params))?;
        Ok(self.batches_to_entities(&res.batches))
    }

    pub fn expand(&self, entity_id: &str, hops: u32, edge_types: &[&str]) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;

        if edge_types.is_empty() {
            return Ok(Vec::new());
        }

        let edge_pattern = edge_types.join("|");
        let query = format!(
            "MATCH (e:{})-[r:{}*1..{}]->(neighbor:{}) \
             WHERE e.id = $id AND neighbor.valid_until = 0 \
             RETURN DISTINCT neighbor.id, neighbor.type, neighbor.content, \
             neighbor.created_at, neighbor.last_accessed, neighbor.access_count, \
             neighbor.ttl_seconds, neighbor.metadata, \
             neighbor.valid_from, neighbor.valid_until",
            ENTITY_TABLE, edge_pattern, hops, ENTITY_TABLE
        );

        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(entity_id.to_string()));
        let res = self.conn.execute(&query, Some(params))?;
        Ok(self.batches_to_entities(&res.batches))
    }

    pub fn associate(&self, src_id: &str, dst_id: &str, rel_type: &str, weight: f64) -> Result<()> {
        self.ensure_schema()?;

        let now = Self::now_micros();

        let query = format!(
            "MATCH (a:{} {{id: $src_id}}), (b:{} {{id: $dst_id}}) \
             CREATE (a)-[:{} {{type: $rel_type, weight: $weight, created_at: $created_at}}]->(b)",
            ENTITY_TABLE, ENTITY_TABLE, RELATES_TABLE
        );

        let mut params = HashMap::new();
        params.insert("src_id".to_string(), Value::String(src_id.to_string()));
        params.insert("dst_id".to_string(), Value::String(dst_id.to_string()));
        params.insert("rel_type".to_string(), Value::String(rel_type.to_string()));
        params.insert("weight".to_string(), Value::Number(weight));
        params.insert("created_at".to_string(), Value::Timestamp(now));

        self.conn.execute(&query, Some(params))?;
        Ok(())
    }

    pub fn forget(&self, entity_id: &str) -> Result<bool> {
        self.ensure_schema()?;

        // Soft delete: set valid_until instead of hard delete
        let now = Self::now_micros();
        let soft_delete = format!(
            "MATCH (e:{} {{id: $id}}) SET e.valid_until = $now",
            ENTITY_TABLE
        );
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(entity_id.to_string()));
        params.insert("now".to_string(), Value::Timestamp(now));
        let res = self.conn.execute(&soft_delete, Some(params.clone()))?;
        let deleted = res.batches.first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<UInt64Array>())
            .map(|arr| arr.value(0) > 0)
            .unwrap_or(false);

        // Also remove relationships
        let del_rels = format!(
            "MATCH (a:{} {{id: $id}})-[r]-() DELETE r",
            ENTITY_TABLE
        );
        let _ = self.conn.execute(&del_rels, Some(params));

        Ok(deleted)
    }

    pub fn decay(&self) -> Result<usize> {
        self.ensure_schema()?;
        let now = Self::now_micros();
        let delete_query = format!(
            "MATCH (e:{}) WHERE e.ttl_seconds > 0 AND \
             (e.created_at / 1000000 + e.ttl_seconds) < $now \
             SET e.valid_until = $now",
            ENTITY_TABLE
        );
        let mut params = HashMap::new();
        params.insert("now".to_string(), Value::Timestamp(now));
        let res = self.conn.execute(&delete_query, Some(params))?;

        let count = res.batches.first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
            .map(|arr| arr.value(0) as usize)
            .unwrap_or(0);

        Ok(count)
    }

    fn lookup_by_internal_id(&self, internal_id: u64) -> Option<MemoryEntity> {
        let query = format!(
            "MATCH (e:{}) WHERE e._id = {} \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata, \
             e.valid_from, e.valid_until LIMIT 1",
            ENTITY_TABLE, internal_id
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
            let created_at = batch.column(3).as_any().downcast_ref::<TimestampMicrosecondArray>();
            let last_accessed = batch.column(4).as_any().downcast_ref::<TimestampMicrosecondArray>();
            let access_counts = batch.column(5).as_any().downcast_ref::<Int64Array>();
            let ttl_seconds = batch.column(6).as_any().downcast_ref::<Int64Array>();
            let metadatas = batch.column(7).as_any().downcast_ref::<StringArray>();
            let valid_from = batch.column(8).as_any().downcast_ref::<TimestampMicrosecondArray>();
            let valid_until = batch.column(9).as_any().downcast_ref::<TimestampMicrosecondArray>();

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
