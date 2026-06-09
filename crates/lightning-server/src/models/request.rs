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
    #[allow(dead_code)]
    pub timeout_ms: u64,
}

fn default_timeout() -> u64 {
    30000
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

fn default_top_k() -> usize {
    10
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpandRequest {
    pub entity_id: String,
    #[serde(default = "default_hops")]
    pub hops: u32,
    #[serde(default)]
    pub edge_types: Option<Vec<String>>,
}

fn default_hops() -> u32 {
    1
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

fn default_rag_top_k() -> usize {
    5
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
}
