use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use tokio::sync::mpsc;

use crate::circuit_breaker::CircuitBreaker;
use crate::config::{ClientConfig, TlsConfig};
use crate::error::Error;
use crate::retry::compute_backoff;
use crate::transport::{execute_and_unwrap, execute_and_unwrap_blocking};
use crate::types::*;
use crate::validation;

fn build_headers(config: &ClientConfig) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        USER_AGENT,
        HeaderValue::from_str(&config.user_agent).unwrap_or_else(|_| HeaderValue::from_static("lightning-client-rust/0.1.0")),
    );
    if let Some(ref token) = config.auth_token {
        let auth_value = format!("Bearer {}", token);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth_value).expect("valid auth header"),
        );
    }
    headers
}

fn build_reqwest_client(config: &ClientConfig) -> Result<reqwest::Client, Error> {
    let mut builder = reqwest::Client::builder()
        .timeout(config.default_timeout)
        .pool_max_idle_per_host(config.max_keepalive)
        .pool_idle_timeout(config.keepalive_timeout)
        .redirect(reqwest::redirect::Policy::none());

    if config.follow_redirects {
        builder = builder.redirect(reqwest::redirect::Policy::limited(5));
    }

    if let Some(ref tls) = config.tls {
        builder = apply_tls(builder, tls)?;
    }

    builder
        .build()
        .map_err(|e| Error::Config(format!("failed to build HTTP client: {}", e)))
}

fn apply_tls(
    mut builder: reqwest::ClientBuilder,
    tls: &TlsConfig,
) -> Result<reqwest::ClientBuilder, Error> {
    let cert = if let Some(ref ca_path) = tls.ca_bundle_path {
        let pem = std::fs::read(ca_path)
            .map_err(|e| Error::Tls(format!("failed to read CA bundle: {}", e)))?;
        Some(reqwest::Certificate::from_pem(&pem)
            .map_err(|e| Error::Tls(format!("invalid CA bundle: {}", e)))?)
    } else {
        None
    };

    let identity = match (&tls.cert_path, &tls.key_path) {
        (Some(cert_path), Some(key_path)) => {
            let cert_pem = std::fs::read(cert_path)
                .map_err(|e| Error::Tls(format!("failed to read cert: {}", e)))?;
            let key_pem = std::fs::read(key_path)
                .map_err(|e| Error::Tls(format!("failed to read key: {}", e)))?;
            let identity = reqwest::Identity::from_pem(&[&cert_pem[..], &key_pem[..]].concat())
                .map_err(|e| Error::Tls(format!("invalid client cert/key: {}", e)))?;
            Some(identity)
        }
        _ => None,
    };

    if let Some(cert) = cert {
        builder = builder.add_root_certificate(cert);
    }
    if let Some(identity) = identity {
        builder = builder.identity(identity);
    }
    if !tls.verify {
        builder = builder.danger_accept_invalid_certs(true);
    }

    Ok(builder)
}

fn generate_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub struct Client {
    base_url: String,
    config: ClientConfig,
    headers: HeaderMap,
    http_client: reqwest::Client,
    circuit_breaker: Option<CircuitBreaker>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("base_url", &self.base_url)
            .field("config", &self.config)
            .finish()
    }
}

impl Client {
    pub fn new(config: ClientConfig) -> Result<Self, Error> {
        let headers = build_headers(&config);
        let http_client = build_reqwest_client(&config)?;
        let circuit_breaker = config
            .circuit_breaker
            .clone()
            .map(CircuitBreaker::new);

        Ok(Self {
            base_url: config.base_url.trim_end_matches('/').to_string(),
            config,
            headers,
            http_client,
            circuit_breaker,
        })
    }

    fn check_circuit_breaker(&self) -> Result<(), Error> {
        if let Some(ref cb) = self.circuit_breaker {
            if !cb.allow_request() {
                let state = cb.state();
                if let Some(ref hooks) = self.config.telemetry {
                    if let Some(ref cb_hook) = hooks.on_circuit_breaker {
                        cb_hook("denied", &state.to_string());
                    }
                }
                return Err(Error::CircuitBreakerOpen(format!(
                    "circuit breaker is {}",
                    state
                )));
            }
        }
        Ok(())
    }

    fn report_success(&self) {
        if let Some(ref cb) = self.circuit_breaker {
            cb.on_success();
        }
    }

    fn report_failure(&self) {
        if let Some(ref cb) = self.circuit_breaker {
            cb.on_failure();
        }
    }

    async fn execute<T>(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<T, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        self.check_circuit_breaker()?;

        let request_id = generate_request_id();
        let url = format!("{}{}", self.base_url, path);

        let mut last_err = None;
        let max_retries = self.config.retry.max_retries;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                let delay = compute_backoff(attempt - 1, &self.config.retry);
                if let Some(ref hooks) = self.config.telemetry {
                    if let Some(ref cb) = hooks.on_retry {
                        cb(&request_id, method, path, attempt, delay.as_secs_f64() * 1000.0);
                    }
                }
                tokio::time::sleep(delay).await;
            }

            let mut builder = self
                .http_client
                .request(
                    reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
                    &url,
                )
                .headers(self.headers.clone())
                .header("X-Request-Id", &request_id);

            if let Some(ref body_val) = body {
                builder = builder.json(body_val);
            }

            match execute_and_unwrap::<T>(builder, self.config.max_content_bytes).await {
                Ok(result) => {
                    self.report_success();
                    if let Some(ref hooks) = self.config.telemetry {
                        if let Some(ref cb) = hooks.on_request_end {
                            cb(&request_id, method, path, 200, 0.0);
                        }
                    }
                    return Ok(result);
                }
                Err(e) => {
                    let is_retryable = e.is_retryable();
                    if let Some(ref hooks) = self.config.telemetry {
                        if let Some(ref cb) = hooks.on_error {
                            cb(&request_id, method, path, &e);
                        }
                    }

                    if is_retryable && attempt < max_retries {
                        last_err = Some(e);
                        continue;
                    }

                    self.report_failure();
                    return if attempt >= max_retries && is_retryable {
                        Err(Error::MaxRetriesExceeded(
                            max_retries + 1,
                            last_err.map(|e| e.to_string()).unwrap_or_default(),
                        ))
                    } else {
                        Err(e)
                    };
                }
            }
        }

        self.report_failure();
        Err(Error::MaxRetriesExceeded(
            max_retries + 1,
            last_err.map(|e| e.to_string()).unwrap_or_default(),
        ))
    }

    fn execute_blocking<T>(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<T, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        self.check_circuit_breaker()?;

        let request_id = generate_request_id();
        let url = format!("{}{}", self.base_url, path);

        let client = reqwest::blocking::Client::builder()
            .timeout(self.config.default_timeout)
            .build()
            .map_err(|e| Error::Config(format!("failed to build blocking client: {}", e)))?;

        let mut last_err = None;
        let max_retries = self.config.retry.max_retries;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                let delay = compute_backoff(attempt - 1, &self.config.retry);
                std::thread::sleep(delay);
            }

            let mut builder = client
                .request(
                    reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
                    &url,
                )
                .headers(self.headers.clone())
                .header("X-Request-Id", &request_id);

            if let Some(ref body_val) = body {
                builder = builder.json(body_val);
            }

            match execute_and_unwrap_blocking::<T>(builder, self.config.max_content_bytes) {
                Ok(result) => {
                    self.report_success();
                    return Ok(result);
                }
                Err(e) => {
                    let is_retryable = e.is_retryable();
                    if is_retryable && attempt < max_retries {
                        last_err = Some(e);
                        continue;
                    }
                    self.report_failure();
                    return if attempt >= max_retries && is_retryable {
                        Err(Error::MaxRetriesExceeded(
                            max_retries + 1,
                            last_err.map(|e| e.to_string()).unwrap_or_default(),
                        ))
                    } else {
                        Err(e)
                    };
                }
            }
        }

        self.report_failure();
        Err(Error::MaxRetriesExceeded(
            max_retries + 1,
            last_err.map(|e| e.to_string()).unwrap_or_default(),
        ))
    }

    // ── Memory ────────────────────────────────────────────────────────────

    pub async fn store(&self, req: StoreRequest) -> Result<(), Error> {
        let entity_type = if req.entity_type.is_empty() {
            "memory"
        } else {
            &req.entity_type
        };
        validation::validate_id(&req.id, "id")?;
        validation::validate_content(&req.content)?;
        validation::validate_entity_type(entity_type)?;

        let mut body = serde_json::json!({
            "id": req.id,
            "content": req.content,
            "entityType": entity_type,
        });

        if let Some(embedding) = &req.embedding {
            validation::validate_embedding(embedding)?;
            body["embedding"] = serde_json::json!(embedding);
        }

        // Include all optional fields
        if !req.metadata.is_empty() {
            body["metadata"] = serde_json::json!(&req.metadata);
        }
        if let Some(v) = req.ttl_seconds {
            body["ttlSeconds"] = serde_json::json!(v);
        }
        if let Some(v) = req.created_at {
            body["createdAt"] = serde_json::json!(v);
        }
        if let Some(v) = req.last_accessed {
            body["lastAccessed"] = serde_json::json!(v);
        }
        if let Some(v) = req.access_count {
            body["accessCount"] = serde_json::json!(v);
        }
        if let Some(v) = req.valid_from {
            body["validFrom"] = serde_json::json!(v);
        }
        if let Some(v) = req.valid_until {
            body["validUntil"] = serde_json::json!(v);
        }

        self.execute::<serde_json::Value>("POST", "/v1/memory/store", Some(&body))
            .await?;
        Ok(())
    }

    pub async fn store_batch(&self, entities: Vec<StoreRequest>) -> Result<usize, Error> {
        validation::validate_batch_size(entities.len(), self.config.max_batch_entities)?;
        for e in &entities {
            validation::validate_id(&e.id, "id")?;
            validation::validate_content(&e.content)?;
        }

        let body = serde_json::json!({ "entities": entities });
        let result: StoreBatchResponse =
            self.execute("POST", "/v1/memory/store-batch", Some(&body))
                .await?;
        Ok(result.stored)
    }

    pub async fn recall(
        &self,
        query: &str,
        embedding: Option<&[f32]>,
        top_k: usize,
    ) -> Result<Vec<SearchResult>, Error> {
        validation::validate_query_string(query)?;
        validation::validate_top_k(top_k, self.config.max_top_k)?;

        if let Some(emb) = embedding {
            validation::validate_embedding(emb)?;
        }

        let mut body = serde_json::json!({
            "query": query,
            "topK": top_k,
        });
        if let Some(emb) = embedding {
            body["embedding"] = serde_json::json!(emb);
        }

        let result: RecallResponse = self.execute("POST", "/v1/memory/recall", Some(&body)).await?;
        Ok(result.results)
    }

    pub async fn recall_recent(&self, top_k: usize) -> Result<Vec<Entity>, Error> {
        validation::validate_top_k(top_k, self.config.max_top_k)?;

        let body = serde_json::json!({ "topK": top_k });
        let result: RecallRecentResponse =
            self.execute("POST", "/v1/memory/recall-recent", Some(&body))
                .await?;
        Ok(result.entities)
    }

    pub async fn recall_by_type(&self, entity_type: &str, top_k: usize) -> Result<Vec<Entity>, Error> {
        validation::validate_entity_type(entity_type)?;
        validation::validate_top_k(top_k, self.config.max_top_k)?;

        let body = serde_json::json!({
            "entityType": entity_type,
            "topK": top_k,
        });
        let result: RecallByTypeResponse =
            self.execute("POST", "/v1/memory/recall-by-type", Some(&body))
                .await?;
        Ok(result.entities)
    }

    pub async fn forget(&self, id: &str) -> Result<bool, Error> {
        validation::validate_id(id, "id")?;

        let body = serde_json::json!({ "id": id });
        let result: ForgetResponse = self.execute("POST", "/v1/memory/forget", Some(&body)).await?;
        Ok(result.deleted)
    }

    pub async fn decay(&self) -> Result<usize, Error> {
        let body = serde_json::json!({});
        let result: DecayResponse = self.execute("POST", "/v1/memory/decay", Some(&body)).await?;
        Ok(result.expired)
    }

    pub async fn entity_history(&self, id: &str) -> Result<Vec<Entity>, Error> {
        validation::validate_id(id, "id")?;

        let body = serde_json::json!({ "id": id });
        let result: EntityHistoryResponse =
            self.execute("POST", "/v1/memory/entity-history", Some(&body))
                .await?;
        Ok(result.versions)
    }

    pub async fn consolidate(&self, config: ConsolidateRequest) -> Result<ConsolidationReport, Error> {
        let body = serde_json::to_value(&config)
            .map_err(|e| Error::Validation(format!("serialization error: {}", e)))?;
        self.execute("POST", "/v1/memory/consolidate", Some(&body))
            .await
    }

    // ── Graph ─────────────────────────────────────────────────────────────

    pub async fn associate(
        &self,
        src_id: &str,
        dst_id: &str,
        rel_type: &str,
        weight: f64,
    ) -> Result<(), Error> {
        validation::validate_id(src_id, "src_id")?;
        validation::validate_id(dst_id, "dst_id")?;

        let body = serde_json::json!({
            "srcId": src_id,
            "dstId": dst_id,
            "relType": rel_type,
            "weight": weight,
        });
        self.execute::<serde_json::Value>("POST", "/v1/graph/associate", Some(&body))
            .await?;
        Ok(())
    }

    pub async fn expand(
        &self,
        entity_id: &str,
        hops: usize,
        edge_types: Option<&[String]>,
    ) -> Result<Vec<Entity>, Error> {
        validation::validate_id(entity_id, "entity_id")?;
        validation::validate_hops(hops)?;

        let mut body = serde_json::json!({
            "entityId": entity_id,
            "hops": hops,
        });
        if let Some(types) = edge_types {
            body["edgeTypes"] = serde_json::json!(types);
        }

        let result: ExpandResponse = self.execute("POST", "/v1/graph/expand", Some(&body)).await?;
        Ok(result.entities)
    }

    // ── RAG ────────────────────────────────────────────────────────────────

    pub async fn rag_query(
        &self,
        query: &str,
        embedding: Option<&[f32]>,
        top_k: usize,
        rag_config: Option<RagQueryConfig>,
    ) -> Result<RagResult, Error> {
        validation::validate_query_string(query)?;
        validation::validate_top_k(top_k, self.config.max_top_k)?;

        if let Some(emb) = embedding {
            validation::validate_embedding(emb)?;
        }

        let mut body = serde_json::json!({
            "query": query,
            "topK": top_k,
        });
        if let Some(emb) = embedding {
            body["embedding"] = serde_json::json!(emb);
        }
        if let Some(ref cfg) = rag_config {
            if let Some(v) = cfg.expansion_depth {
                body["expansionDepth"] = serde_json::json!(v);
            }
            if let Some(v) = cfg.search_weight {
                body["searchWeight"] = serde_json::json!(v);
            }
            if let Some(v) = cfg.recency_weight {
                body["recencyWeight"] = serde_json::json!(v);
            }
            if let Some(v) = cfg.degree_weight {
                body["degreeWeight"] = serde_json::json!(v);
            }
            if let Some(v) = cfg.max_tokens {
                body["maxTokens"] = serde_json::json!(v);
            }
        }

        self.execute("POST", "/v1/rag/query", Some(&body)).await
    }

    // ── Query ──────────────────────────────────────────────────────────────

    pub async fn query(
        &self,
        query: &str,
        params: Option<&serde_json::Value>,
        snapshot_ts: Option<i64>,
        timeout_ms: u64,
    ) -> Result<QueryResult, Error> {
        validation::validate_query_string(query)?;

        let mut body = serde_json::json!({
            "query": query,
            "timeoutMs": timeout_ms,
        });
        if let Some(p) = params {
            body["params"] = p.clone();
        }
        if let Some(ts) = snapshot_ts {
            body["snapshotTs"] = serde_json::json!(ts);
        }

        self.execute("POST", "/v1/query", Some(&body)).await
    }

    // ── Auth ───────────────────────────────────────────────────────────────

    pub async fn login(&self, username: &str, password: &str) -> Result<LoginResponse, Error> {
        let body = serde_json::json!({
            "username": username,
            "password": password,
        });
        self.execute("POST", "/v1/auth/login", Some(&body)).await
    }

    pub async fn refresh(&self, refresh_token: &str) -> Result<RefreshResponse, Error> {
        let body = serde_json::json!({
            "refreshToken": refresh_token,
        });
        self.execute("POST", "/v1/auth/refresh", Some(&body)).await
    }

    pub async fn logout(&self) -> Result<(), Error> {
        let body = serde_json::json!({});
        self.execute::<serde_json::Value>("POST", "/v1/auth/logout", Some(&body))
            .await?;
        Ok(())
    }

    pub async fn me(&self) -> Result<UserInfo, Error> {
        self.execute("GET", "/v1/auth/me", None).await
    }

    // ── Admin ──────────────────────────────────────────────────────────────

    pub async fn checkpoint(&self) -> Result<(), Error> {
        let body = serde_json::json!({});
        self.execute::<serde_json::Value>("POST", "/v1/admin/checkpoint", Some(&body))
            .await?;
        Ok(())
    }

    pub async fn vacuum(&self) -> Result<(), Error> {
        let body = serde_json::json!({});
        self.execute::<serde_json::Value>("POST", "/v1/admin/vacuum", Some(&body))
            .await?;
        Ok(())
    }

    pub async fn list_users(&self) -> Result<Vec<UserInfo>, Error> {
        let result: UserListResponse = self.execute("GET", "/v1/admin/users", None).await?;
        Ok(result.users)
    }

    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        role: &str,
    ) -> Result<UserInfo, Error> {
        let body = serde_json::json!({
            "username": username,
            "password": password,
            "role": role,
        });
        self.execute("POST", "/v1/admin/users", Some(&body)).await
    }

    pub async fn update_user(
        &self,
        id: &str,
        username: Option<&str>,
        password: Option<&str>,
        role: Option<&str>,
    ) -> Result<UserInfo, Error> {
        let mut body = serde_json::json!({});
        if let Some(u) = username {
            body["username"] = serde_json::json!(u);
        }
        if let Some(p) = password {
            body["password"] = serde_json::json!(p);
        }
        if let Some(r) = role {
            body["role"] = serde_json::json!(r);
        }
        self.execute("POST", &format!("/v1/admin/users/{}", id), Some(&body))
            .await
    }

    pub async fn delete_user(&self, id: &str) -> Result<(), Error> {
        let builder = self
            .http_client
            .delete(format!("{}/v1/admin/users/{}", self.base_url, id))
            .headers(self.headers.clone());
        execute_and_unwrap::<serde_json::Value>(builder, self.config.max_content_bytes).await?;
        Ok(())
    }

    pub async fn reset_password(&self, user_id: &str, new_password: &str) -> Result<(), Error> {
        let body = serde_json::json!({ "newPassword": new_password });
        self.execute::<serde_json::Value>(
            "POST",
            &format!("/v1/admin/users/{}/reset-password", user_id),
            Some(&body),
        )
        .await?;
        Ok(())
    }

    pub async fn list_api_keys(&self, user_id: &str) -> Result<Vec<ApiKey>, Error> {
        let result: ApiKeyListResponse =
            self.execute("GET", &format!("/v1/admin/users/{}/keys", user_id), None)
                .await?;
        Ok(result.keys)
    }

    pub async fn create_api_key(
        &self,
        user_id: &str,
        label: &str,
    ) -> Result<ApiKeyCreateResponse, Error> {
        let body = serde_json::json!({ "label": label });
        self.execute("POST", &format!("/v1/admin/users/{}/keys", user_id), Some(&body))
            .await
    }

    pub async fn delete_api_key(&self, user_id: &str, key_id: &str) -> Result<(), Error> {
        let builder = self
            .http_client
            .delete(format!(
                "{}/v1/admin/users/{}/keys/{}",
                self.base_url, user_id, key_id
            ))
            .headers(self.headers.clone());
        execute_and_unwrap::<serde_json::Value>(builder, self.config.max_content_bytes).await?;
        Ok(())
    }

    // ── Health / Metrics ───────────────────────────────────────────────────

    pub async fn health(&self) -> Result<serde_json::Value, Error> {
        self.execute("GET", "/health", None).await
    }

    pub async fn metrics(&self) -> Result<String, Error> {
        let builder = self
            .http_client
            .get(format!("{}/metrics", self.base_url))
            .headers(self.headers.clone());

        let response = builder.send().await.map_err(Error::Http)?;
        let text = response.text().await.map_err(Error::Http)?;
        Ok(text)
    }

    // ── CDC Subscribe ──────────────────────────────────────────────────────

    pub async fn subscribe(&self) -> Result<mpsc::Receiver<Result<ChangeEvent, Error>>, Error> {
        let builder = self
            .http_client
            .get(format!("{}/v1/subscribe", self.base_url))
            .headers(self.headers.clone());

        let response = builder.send().await.map_err(Error::Http)?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let text = response.text().await.unwrap_or_default();
            return Err(Error::Lightning(crate::error::LightningError {
                error: format!("subscribe failed: HTTP {}", status),
                code: status.to_string(),
                details: Some(serde_json::json!({"body": text})),
                request_id: None,
                status,
            }));
        }

        crate::subscribe::subscribe_sse(response).await
    }

    // ── Blocking API ───────────────────────────────────────────────────────

    pub fn blocking_store(&self, req: StoreRequest) -> Result<(), Error> {
        let entity_type = if req.entity_type.is_empty() {
            "memory"
        } else {
            &req.entity_type
        };
        validation::validate_id(&req.id, "id")?;
        validation::validate_content(&req.content)?;
        validation::validate_entity_type(entity_type)?;

        let mut body = serde_json::json!({
            "id": req.id,
            "content": req.content,
            "entityType": entity_type,
        });

        if let Some(embedding) = &req.embedding {
            validation::validate_embedding(embedding)?;
            body["embedding"] = serde_json::json!(embedding);
        }

        if !req.metadata.is_empty() {
            body["metadata"] = serde_json::json!(req.metadata);
        }
        if let Some(v) = req.ttl_seconds {
            body["ttlSeconds"] = serde_json::json!(v);
        }
        if let Some(v) = req.created_at {
            body["createdAt"] = serde_json::json!(v);
        }
        if let Some(v) = req.last_accessed {
            body["lastAccessed"] = serde_json::json!(v);
        }
        if let Some(v) = req.access_count {
            body["accessCount"] = serde_json::json!(v);
        }
        if let Some(v) = req.valid_from {
            body["validFrom"] = serde_json::json!(v);
        }
        if let Some(v) = req.valid_until {
            body["validUntil"] = serde_json::json!(v);
        }

        self.execute_blocking::<serde_json::Value>("POST", "/v1/memory/store", Some(&body))?;
        Ok(())
    }

    pub fn blocking_store_batch(&self, entities: Vec<StoreRequest>) -> Result<usize, Error> {
        validation::validate_batch_size(entities.len(), self.config.max_batch_entities)?;
        for e in &entities {
            validation::validate_id(&e.id, "id")?;
            validation::validate_content(&e.content)?;
        }

        let body = serde_json::json!({ "entities": entities });
        let result: StoreBatchResponse =
            self.execute_blocking("POST", "/v1/memory/store-batch", Some(&body))?;
        Ok(result.stored)
    }

    pub fn blocking_recall(
        &self,
        query: &str,
        embedding: Option<&[f32]>,
        top_k: usize,
    ) -> Result<Vec<SearchResult>, Error> {
        validation::validate_query_string(query)?;
        validation::validate_top_k(top_k, self.config.max_top_k)?;
        if let Some(emb) = embedding {
            validation::validate_embedding(emb)?;
        }

        let mut body = serde_json::json!({ "query": query, "topK": top_k });
        if let Some(emb) = embedding {
            body["embedding"] = serde_json::json!(emb);
        }

        let result: RecallResponse =
            self.execute_blocking("POST", "/v1/memory/recall", Some(&body))?;
        Ok(result.results)
    }

    pub fn blocking_recall_recent(&self, top_k: usize) -> Result<Vec<Entity>, Error> {
        validation::validate_top_k(top_k, self.config.max_top_k)?;
        let body = serde_json::json!({ "topK": top_k });
        let result: RecallRecentResponse =
            self.execute_blocking("POST", "/v1/memory/recall-recent", Some(&body))?;
        Ok(result.entities)
    }

    pub fn blocking_recall_by_type(
        &self,
        entity_type: &str,
        top_k: usize,
    ) -> Result<Vec<Entity>, Error> {
        validation::validate_entity_type(entity_type)?;
        validation::validate_top_k(top_k, self.config.max_top_k)?;
        let body = serde_json::json!({ "entityType": entity_type, "topK": top_k });
        let result: RecallByTypeResponse =
            self.execute_blocking("POST", "/v1/memory/recall-by-type", Some(&body))?;
        Ok(result.entities)
    }

    pub fn blocking_forget(&self, id: &str) -> Result<bool, Error> {
        validation::validate_id(id, "id")?;
        let body = serde_json::json!({ "id": id });
        let result: ForgetResponse =
            self.execute_blocking("POST", "/v1/memory/forget", Some(&body))?;
        Ok(result.deleted)
    }

    pub fn blocking_decay(&self) -> Result<usize, Error> {
        let body = serde_json::json!({});
        let result: DecayResponse =
            self.execute_blocking("POST", "/v1/memory/decay", Some(&body))?;
        Ok(result.expired)
    }

    pub fn blocking_entity_history(&self, id: &str) -> Result<Vec<Entity>, Error> {
        validation::validate_id(id, "id")?;
        let body = serde_json::json!({ "id": id });
        let result: EntityHistoryResponse =
            self.execute_blocking("POST", "/v1/memory/entity-history", Some(&body))?;
        Ok(result.versions)
    }

    pub fn blocking_consolidate(
        &self,
        config: ConsolidateRequest,
    ) -> Result<ConsolidationReport, Error> {
        let body = serde_json::to_value(&config)
            .map_err(|e| Error::Validation(format!("serialization error: {}", e)))?;
        self.execute_blocking("POST", "/v1/memory/consolidate", Some(&body))
    }

    pub fn blocking_associate(
        &self,
        src_id: &str,
        dst_id: &str,
        rel_type: &str,
        weight: f64,
    ) -> Result<(), Error> {
        validation::validate_id(src_id, "src_id")?;
        validation::validate_id(dst_id, "dst_id")?;
        let body = serde_json::json!({
            "srcId": src_id,
            "dstId": dst_id,
            "relType": rel_type,
            "weight": weight,
        });
        self.execute_blocking::<serde_json::Value>(
            "POST",
            "/v1/graph/associate",
            Some(&body),
        )?;
        Ok(())
    }

    pub fn blocking_expand(
        &self,
        entity_id: &str,
        hops: usize,
        edge_types: Option<&[String]>,
    ) -> Result<Vec<Entity>, Error> {
        validation::validate_id(entity_id, "entity_id")?;
        validation::validate_hops(hops)?;
        let mut body = serde_json::json!({ "entityId": entity_id, "hops": hops });
        if let Some(types) = edge_types {
            body["edgeTypes"] = serde_json::json!(types);
        }
        let result: ExpandResponse =
            self.execute_blocking("POST", "/v1/graph/expand", Some(&body))?;
        Ok(result.entities)
    }

    pub fn blocking_rag_query(
        &self,
        query: &str,
        embedding: Option<&[f32]>,
        top_k: usize,
        rag_config: Option<RagQueryConfig>,
    ) -> Result<RagResult, Error> {
        validation::validate_query_string(query)?;
        validation::validate_top_k(top_k, self.config.max_top_k)?;
        if let Some(emb) = embedding {
            validation::validate_embedding(emb)?;
        }

        let mut body = serde_json::json!({ "query": query, "topK": top_k });
        if let Some(emb) = embedding {
            body["embedding"] = serde_json::json!(emb);
        }
        if let Some(ref cfg) = rag_config {
            if let Some(v) = cfg.expansion_depth {
                body["expansionDepth"] = serde_json::json!(v);
            }
            if let Some(v) = cfg.search_weight {
                body["searchWeight"] = serde_json::json!(v);
            }
            if let Some(v) = cfg.recency_weight {
                body["recencyWeight"] = serde_json::json!(v);
            }
            if let Some(v) = cfg.degree_weight {
                body["degreeWeight"] = serde_json::json!(v);
            }
            if let Some(v) = cfg.max_tokens {
                body["maxTokens"] = serde_json::json!(v);
            }
        }

        self.execute_blocking("POST", "/v1/rag/query", Some(&body))
    }

    pub fn blocking_query(
        &self,
        query: &str,
        params: Option<&serde_json::Value>,
        snapshot_ts: Option<i64>,
        timeout_ms: u64,
    ) -> Result<QueryResult, Error> {
        validation::validate_query_string(query)?;
        let mut body = serde_json::json!({ "query": query, "timeoutMs": timeout_ms });
        if let Some(p) = params {
            body["params"] = p.clone();
        }
        if let Some(ts) = snapshot_ts {
            body["snapshotTs"] = serde_json::json!(ts);
        }
        self.execute_blocking("POST", "/v1/query", Some(&body))
    }

    pub fn blocking_health(&self) -> Result<serde_json::Value, Error> {
        self.execute_blocking("GET", "/health", None)
    }

    pub fn blocking_metrics(&self) -> Result<String, Error> {
        let client = reqwest::blocking::Client::builder()
            .timeout(self.config.default_timeout)
            .build()
            .map_err(|e| Error::Config(format!("failed to build blocking client: {}", e)))?;

        let response = client
            .get(format!("{}/metrics", self.base_url))
            .headers(self.headers.clone())
            .send()
            .map_err(Error::Http)?;

        response.text().map_err(Error::Http)
    }

    pub fn blocking_login(&self, username: &str, password: &str) -> Result<LoginResponse, Error> {
        let body = serde_json::json!({ "username": username, "password": password });
        self.execute_blocking("POST", "/v1/auth/login", Some(&body))
    }

    pub fn blocking_me(&self) -> Result<UserInfo, Error> {
        self.execute_blocking("GET", "/v1/auth/me", None)
    }
}

#[derive(Debug, Clone)]
pub struct RagQueryConfig {
    pub expansion_depth: Option<usize>,
    pub search_weight: Option<f64>,
    pub recency_weight: Option<f64>,
    pub degree_weight: Option<f64>,
    pub max_tokens: Option<usize>,
}
