use crate::processor::Value;
use crate::Result;
use crate::Connection;
use arrow::array::{Array, Float64Array, Int64Array, StringArray, UInt64Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

const ENTITY_TABLE: &str = "Entity";
const RELATES_TABLE: &str = "Relates";
const DEFAULT_EMBEDDING_DIM: usize = 768;

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
                 ttl_seconds INT64, metadata STRING, PRIMARY KEY (id))",
                ENTITY_TABLE, self.embedding_dim
            );
            self.conn.execute(&create_entity, None)?;

            let create_relates = format!(
                "CREATE REL TABLE {} (FROM {} TO {}, type STRING, weight DOUBLE, created_at TIMESTAMP)",
                RELATES_TABLE, ENTITY_TABLE, ENTITY_TABLE
            );
            self.conn.execute(&create_relates, None)?;

            // Create indexes for common query patterns
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

    pub fn store(&self, entity: MemoryEntity) -> Result<()> {
        self.ensure_schema()?;

        let query = format!(
            "MERGE (e:{0} {{id: $id}}) \
             SET e.type = $type, e.content = $content, \
             e.created_at = $created_at, e.last_accessed = $last_accessed, \
             e.access_count = $access_count, e.ttl_seconds = $ttl_seconds, \
             e.metadata = $metadata",
            ENTITY_TABLE
        );

        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(entity.id));
        params.insert("type".to_string(), Value::String(entity.entity_type));
        params.insert("content".to_string(), Value::String(entity.content));
        params.insert("created_at".to_string(), Value::Timestamp(entity.created_at));
        params.insert("last_accessed".to_string(), Value::Timestamp(entity.last_accessed));
        params.insert("access_count".to_string(), Value::Number(entity.access_count as f64));
        params.insert("ttl_seconds".to_string(), Value::Number(entity.ttl_seconds as f64));
        params.insert("metadata".to_string(), Value::String(entity.metadata));

        self.conn.execute(&query, Some(params))?;
        Ok(())
    }

    pub fn store_batch(&self, entities: Vec<MemoryEntity>) -> Result<usize> {
        self.ensure_schema()?;

        if entities.is_empty() {
            return Ok(0);
        }

        let num_rows = entities.len();
        let mut ids = Vec::with_capacity(num_rows);
        let mut types = Vec::with_capacity(num_rows);
        let mut contents = Vec::with_capacity(num_rows);
        let mut created_at = Vec::with_capacity(num_rows);
        let mut last_accessed = Vec::with_capacity(num_rows);
        let mut access_counts = Vec::with_capacity(num_rows);
        let mut ttl_seconds = Vec::with_capacity(num_rows);
        let mut metadatas = Vec::with_capacity(num_rows);

        for e in entities {
            ids.push(e.id);
            types.push(e.entity_type);
            contents.push(e.content);
            created_at.push(e.created_at);
            last_accessed.push(e.last_accessed);
            access_counts.push(e.access_count);
            ttl_seconds.push(e.ttl_seconds);
            metadatas.push(e.metadata);
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

        // FTS search
        if let Some(fts) = storage.fts_indexes.get(ENTITY_TABLE) {
            let tx = db.transaction_manager.begin(true)?;
            if let Ok(fts_results) = fts.search(query_text, top_k * 2, &db.buffer_manager, &tx) {
                for (rank, (node_id, score)) in fts_results.iter().enumerate() {
                    if let Some(entity) = self.lookup_by_internal_id(*node_id) {
                        let rrf_score = 1.0 / (k + (rank as f64) + 1.0);
                        results.entry(entity.id.clone()).or_insert((entity, 0.0)).1 += rrf_score;
                    }
                }
            }
            let _ = db.transaction_manager.rollback(&db, &tx);
        }

        // Vector search
        if !embedding.is_empty() && embedding.len() == self.embedding_dim {
            if let Some(vec_idx) = storage.vector_indexes.get(ENTITY_TABLE) {
                let tx = db.transaction_manager.begin(true)?;
                let mut emb = [0f32; 768];
                let copy_len = std::cmp::min(embedding.len(), 768);
                emb[..copy_len].copy_from_slice(&embedding[..copy_len]);

                if let Ok(vec_results) = vec_idx.search(&emb, top_k * 2, &db.buffer_manager, &tx) {
                    for (rank, (node_id, score)) in vec_results.iter().enumerate() {
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
            "MATCH (e:{}) WHERE e.type = $type RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata ORDER BY e.last_accessed DESC LIMIT {}",
            ENTITY_TABLE, top_k
        );
        let mut params = HashMap::new();
        params.insert("type".to_string(), Value::String(entity_type.to_string()));
        let res = self.conn.execute(&query, Some(params))?;
        Ok(self.batches_to_entities(&res.batches))
    }

    pub fn recall_recent(&self, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;
        let query = format!(
            "MATCH (e:{}) RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata \
             ORDER BY e.created_at DESC LIMIT {}",
            ENTITY_TABLE, top_k
        );
        let res = self.conn.execute(&query, None)?;
        Ok(self.batches_to_entities(&res.batches))
    }

    pub fn recall_by_time(&self, start: i64, end: i64, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.ensure_schema()?;
        let query = format!(
            "MATCH (e:{}) WHERE e.created_at >= $start AND e.created_at <= $end \
             RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata \
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
             WHERE e.id = $id \
             RETURN DISTINCT neighbor.id, neighbor.type, neighbor.content, \
             neighbor.created_at, neighbor.last_accessed, neighbor.access_count, \
             neighbor.ttl_seconds, neighbor.metadata",
            ENTITY_TABLE, edge_pattern, hops, ENTITY_TABLE
        );

        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(entity_id.to_string()));
        let res = self.conn.execute(&query, Some(params))?;
        Ok(self.batches_to_entities(&res.batches))
    }

    pub fn associate(&self, src_id: &str, dst_id: &str, rel_type: &str, weight: f64) -> Result<()> {
        self.ensure_schema()?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);

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

        // Delete both directions of relationships first
        let del_rels = format!(
            "MATCH (a:{} {{id: $id}})-[r]-() DELETE r",
            ENTITY_TABLE
        );
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::String(entity_id.to_string()));
        let _ = self.conn.execute(&del_rels, Some(params.clone()));

        // Delete the entity itself
        let del_entity = format!(
            "MATCH (e:{} {{id: $id}}) DELETE e",
            ENTITY_TABLE
        );
        let res = self.conn.execute(&del_entity, Some(params))?;

        // Check if any rows were affected
        let deleted = res.batches.first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<UInt64Array>())
            .map(|arr| arr.value(0) > 0)
            .unwrap_or(false);
        Ok(deleted)
    }

    pub fn decay(&self) -> Result<usize> {
        self.ensure_schema()?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);

        // Find expired entities (ttl > 0 and created_at + ttl < now)
        let query = format!(
            "MATCH (e:{}) WHERE e.ttl_seconds > 0 AND \
             (e.created_at / 1000000 + e.ttl_seconds) < $now \
             RETURN count(*) as expired",
            ENTITY_TABLE
        );

        let mut params = HashMap::new();
        params.insert("now".to_string(), Value::Number(now as f64 / 1_000_000.0));
        let res = self.conn.execute(&query, Some(params))?;

        let count = res.batches.first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
            .map(|arr| arr.value(0) as usize)
            .unwrap_or(0);

        if count > 0 {
            let delete_query = format!(
                "MATCH (e:{})-[r]-() WHERE e.ttl_seconds > 0 AND \
                 (e.created_at / 1000000 + e.ttl_seconds) < $now DELETE r, e",
                ENTITY_TABLE
            );
            let mut params = HashMap::new();
            params.insert("now".to_string(), Value::Number(now as f64 / 1_000_000.0));
            let _ = self.conn.execute(&delete_query, Some(params));
        }

        Ok(count)
    }

    fn lookup_by_internal_id(&self, internal_id: u64) -> Option<MemoryEntity> {
        let query = format!(
            "MATCH (e:{}) WHERE e._id = {} RETURN e.id, e.type, e.content, e.created_at, \
             e.last_accessed, e.access_count, e.ttl_seconds, e.metadata LIMIT 1",
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
                });
            }
        }
        entities
    }
}
