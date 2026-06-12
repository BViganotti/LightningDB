use std::collections::HashMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
    pub query: String,
    #[serde(default)]
    pub params: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    pub snapshot_ts: Option<u64>,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

const MAX_TIMEOUT_MS: u64 = 300_000; // 5 minutes max

fn default_timeout() -> u64 {
    30000
}

impl QueryRequest {
    pub fn effective_timeout_ms(&self) -> u64 {
        let requested = if self.timeout_ms > 0 { self.timeout_ms } else { default_timeout() };
        requested.min(MAX_TIMEOUT_MS)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreRequest {
    pub id: String,
    pub content: String,
    #[serde(default = "default_entity_type")]
    pub entity_type: String,
    #[serde(default = "default_metadata")]
    pub metadata: String,
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
    #[serde(default)]
    pub created_at: Option<i64>,
    #[serde(default)]
    pub ttl_seconds: Option<i64>,
    #[serde(default)]
    pub last_accessed: Option<i64>,
    #[serde(default)]
    pub access_count: Option<i64>,
    #[serde(default)]
    pub valid_from: Option<i64>,
    #[serde(default)]
    pub valid_until: Option<i64>,
}

const MAX_ID_LENGTH: usize = 4096;
const MAX_CONTENT_LENGTH: usize = 1_000_000;

impl StoreRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.id.is_empty() {
            return Err("id cannot be empty".into());
        }
        if self.id.len() > MAX_ID_LENGTH {
            return Err(format!("id length ({}) exceeds maximum ({})", self.id.len(), MAX_ID_LENGTH));
        }
        if self.content.len() > MAX_CONTENT_LENGTH {
            return Err(format!("content length ({}) exceeds maximum ({})", self.content.len(), MAX_CONTENT_LENGTH));
        }
        Ok(())
    }
}

fn default_entity_type() -> String {
    "memory".to_string()
}

fn default_metadata() -> String {
    "{}".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreBatchRequest {
    pub entities: Vec<StoreRequest>,
}

fn default_top_k() -> usize {
    10
}

const MAX_TOP_K: usize = 10000;
const MAX_HOPS: u32 = 20;
const MAX_RAG_TOP_K: usize = 1000;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecallRequest {
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

impl RecallRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.top_k == 0 {
            return Err("top_k must be greater than 0".into());
        }
        if self.top_k > MAX_TOP_K {
            return Err(format!("top_k cannot exceed {}", MAX_TOP_K));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecallRecentRequest {
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecallByTypeRequest {
    pub entity_type: String,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForgetRequest {
    pub id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssociateRequest {
    pub src_id: String,
    pub dst_id: String,
    pub rel_type: String,
    #[serde(default = "default_weight")]
    pub weight: f64,
}

fn default_weight() -> f64 {
    1.0
}

fn default_hops() -> u32 {
    1
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpandRequest {
    pub entity_id: String,
    #[serde(default = "default_hops")]
    pub hops: u32,
    #[serde(default)]
    pub edge_types: Option<Vec<String>>,
}

impl ExpandRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.hops == 0 {
            return Err("hops must be greater than 0".into());
        }
        if self.hops > MAX_HOPS {
            return Err(format!("hops cannot exceed {}", MAX_HOPS));
        }
        Ok(())
    }
}

fn default_rag_top_k() -> usize {
    5
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RagRequest {
    pub query: String,
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
    #[serde(default = "default_rag_top_k")]
    pub top_k: usize,
    #[serde(default)]
    pub expansion_depth: Option<usize>,
    #[serde(default)]
    pub search_weight: Option<f64>,
    #[serde(default)]
    pub recency_weight: Option<f64>,
    #[serde(default)]
    pub degree_weight: Option<f64>,
    #[serde(default)]
    pub max_tokens: Option<usize>,
}

impl RagRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.top_k == 0 {
            return Err("top_k must be greater than 0".into());
        }
        if self.top_k > MAX_RAG_TOP_K {
            return Err(format!("top_k cannot exceed {}", MAX_RAG_TOP_K));
        }
        if let Some(depth) = self.expansion_depth {
            if depth > 10 {
                return Err("expansion_depth cannot exceed 10".into());
            }
        }
        if let Some(tokens) = self.max_tokens {
            if tokens > 1_000_000 {
                return Err("max_tokens cannot exceed 1,000,000".into());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityHistoryRequest {
    pub id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsolidateRequest {
    #[serde(default)]
    pub similarity_threshold: Option<f64>,
    #[serde(default)]
    pub contradiction_jaccard_max: Option<f64>,
    #[serde(default)]
    pub contradiction_cosine_min: Option<f64>,
    #[serde(default)]
    pub contradiction_length_sim_min: Option<f64>,
    #[serde(default)]
    pub max_comparisons_per_entity: Option<usize>,
}
