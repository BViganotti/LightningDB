use serde::Serialize;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiResponse<T: Serialize> {
    pub data: T,
    pub meta: ResponseMeta,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseMeta {
    pub request_id: String,
    pub duration_ms: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    pub num_rows: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResultItem {
    pub id: String,
    pub content: String,
    #[serde(rename = "type")]
    pub entity_type: String,
    pub score: f64,
    pub metadata: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecallResponse {
    pub results: Vec<SearchResultItem>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityItem {
    pub id: String,
    #[serde(rename = "type")]
    pub entity_type: String,
    pub content: String,
    pub metadata: String,
    pub created_at: i64,
    pub last_accessed: i64,
    pub access_count: i64,
    pub ttl_seconds: i64,
    pub valid_from: i64,
    pub valid_until: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntitiesResponse {
    pub entities: Vec<EntityItem>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreBatchResponse {
    pub stored: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ForgetResponse {
    pub deleted: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DecayResponse {
    pub expired: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceRef {
    pub id: String,
    pub score: f64,
    #[serde(rename = "type")]
    pub entity_type: String,
    pub excerpt: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RagResponse {
    pub context: String,
    pub sources: Vec<SourceRef>,
    pub total_sources: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpandResponse {
    pub entities: Vec<EntityItem>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityHistoryResponse {
    pub versions: Vec<EntityItem>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsolidationReportResponse {
    pub links_created: usize,
    pub contradictions_found: usize,
    pub total_entities: usize,
    pub warnings: Vec<String>,
}
