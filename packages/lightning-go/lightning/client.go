package lightning

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Client is a hardened HTTP client for LightningDB server.
type Client struct {
	baseURL        string
	authToken      string
	authProvider   func() string
	defaultTimeout time.Duration
	retry          RetryConfig
	circuitBreaker *CircuitBreaker
	tele           *TelemetryHooks
	followRedirect bool
	maxContent     int64
	maxBatch       int
	maxTopK        int
	userAgent      string

	httpClient *http.Client
	mu         sync.Mutex
}

// New creates a new hardened LightningDB HTTP client.
func New(cfg ClientConfig) (*Client, error) {
	transport, err := buildHTTPTransport(cfg)
	if err != nil {
		return nil, fmt.Errorf("build transport: %w", err)
	}

	var cb *CircuitBreaker
	if cfg.CircuitBreaker != nil {
		cb = NewCircuitBreaker(*cfg.CircuitBreaker, cfg.Telemetry)
	}

	httpClient := &http.Client{
		Timeout:   cfg.DefaultTimeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if !cfg.FollowRedirects {
				return http.ErrUseLastResponse
			}
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	if cfg.TLS != nil && !strings.HasPrefix(baseURL, "https://") {
		if strings.HasPrefix(baseURL, "http://") {
			baseURL = strings.Replace(baseURL, "http://", "https://", 1)
		} else {
			return nil, fmt.Errorf("TLS requires https:// scheme, got %s", cfg.BaseURL)
		}
	}

	return &Client{
		baseURL:        baseURL,
		authToken:      cfg.AuthToken,
		authProvider:   cfg.AuthTokenProvider,
		defaultTimeout: cfg.DefaultTimeout,
		retry:          cfg.Retry,
		circuitBreaker: cb,
		tele:           cfg.Telemetry,
		followRedirect: cfg.FollowRedirects,
		maxContent:     cfg.MaxContentBytes,
		maxBatch:       cfg.MaxBatchEntities,
		maxTopK:        cfg.MaxTopK,
		userAgent:      cfg.UserAgent,
		httpClient:     httpClient,
	}, nil
}

func (c *Client) resolveAuth() string {
	if c.authProvider != nil {
		return c.authProvider()
	}
	return c.authToken
}

func (c *Client) generateRequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (c *Client) headers(requestID string) http.Header {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("User-Agent", c.userAgent)
	h.Set("X-Request-Id", requestID)
	if token := c.resolveAuth(); token != "" {
		h.Set("Authorization", "Bearer "+token)
	}
	return h
}

func (c *Client) checkCircuitBreaker() error {
	if c.circuitBreaker == nil {
		return nil
	}
	if !c.circuitBreaker.AllowRequest() {
		state := c.circuitBreaker.State()
		if c.tele != nil && c.tele.OnCircuitBreaker != nil {
			c.tele.OnCircuitBreaker("denied", state.String())
		}
		return ErrCircuitBreakerOpen
	}
	return nil
}

// apiResponse wraps the server's standard JSON envelope.
type apiResponse struct {
	Data json.RawMessage `json:"data"`
	Meta *struct {
		RequestID  string `json:"requestId"`
		DurationMs uint64 `json:"durationMs"`
	} `json:"meta"`
}

func (c *Client) do(method, path string, body interface{}, timeout time.Duration) (*http.Response, error) {
	if err := c.checkCircuitBreaker(); err != nil {
		return nil, err
	}

	requestID := c.generateRequestID()
	h := c.headers(requestID)

	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, fmt.Errorf("encode body: %w", err)
		}
	}

	start := time.Now()
	if c.tele != nil && c.tele.OnRequestStart != nil {
		c.tele.OnRequestStart(requestID, method, path)
	}

	var lastErr error
	for attempt := 0; attempt <= c.retry.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := computeBackoff(attempt-1, c.retry)
			if c.tele != nil && c.tele.OnRetry != nil {
				c.tele.OnRetry(requestID, method, path, attempt, delay.Seconds()*1000)
			}
			time.Sleep(delay)
		}

		req, err := http.NewRequest(method, c.baseURL+path, bytes.NewReader(buf.Bytes()))
		if err != nil {
			lastErr = fmt.Errorf("create request: %w", err)
			continue
		}
		req.Header = h
		var cancel context.CancelFunc
		if timeout > 0 {
			var reqCtx context.Context
			reqCtx, cancel = context.WithTimeout(req.Context(), timeout)
			req = req.WithContext(reqCtx)
		}

		resp, err := c.httpClient.Do(req)
		if cancel != nil {
			cancel()
		}
		if err != nil {
			lastErr = err
			if c.tele != nil && c.tele.OnError != nil {
				c.tele.OnError(requestID, method, path, err)
			}
			if attempt < c.retry.MaxRetries {
				continue
			}
			c.reportFailure()
			return nil, fmt.Errorf("http do: %w", err)
		}

		if resp.StatusCode >= 400 {
			status := resp.StatusCode
			bodyBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, c.maxContent))
			resp.Body.Close()
			if readErr != nil && c.tele != nil && c.tele.OnError != nil {
				c.tele.OnError(requestID, method, path, readErr)
			}

			if IsRetryable(status, c.retry) && attempt < c.retry.MaxRetries {
				continue
			}

			c.reportFailure()
			var le LightningError
			if json.Unmarshal(bodyBytes, &le) == nil && le.Message != "" {
				le.Status = status
				return nil, &le
			}
			return nil, &LightningError{
				Message:   fmt.Sprintf("http %d: %s", status, string(bodyBytes)),
				Status:    status,
				RequestID: requestID,
			}
		}

		c.reportSuccess()
		duration := time.Since(start)
		if c.tele != nil && c.tele.OnRequestEnd != nil {
			c.tele.OnRequestEnd(requestID, method, path, resp.StatusCode, duration.Seconds()*1000)
		}
		return resp, nil
	}

	c.reportFailure()
	return nil, fmt.Errorf("%w after %d attempts: %v", ErrMaxRetriesExceeded, c.retry.MaxRetries+1, lastErr)
}

func (c *Client) reportSuccess() {
	if c.circuitBreaker != nil {
		c.circuitBreaker.OnSuccess()
	}
}

func (c *Client) reportFailure() {
	if c.circuitBreaker != nil {
		c.circuitBreaker.OnFailure()
	}
}

func (c *Client) post(path string, body, into interface{}, timeout time.Duration) error {
	resp, err := c.do("POST", path, body, timeout)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return c.decodeResponse(resp, into)
}

func (c *Client) get(path string, into interface{}, timeout time.Duration) error {
	resp, err := c.do("GET", path, nil, timeout)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return c.decodeResponse(resp, into)
}

func (c *Client) decodeResponse(resp *http.Response, into interface{}) error {
	var reader io.Reader = resp.Body
	if c.maxContent > 0 {
		reader = io.LimitReader(resp.Body, c.maxContent)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	ct := resp.Header.Get("Content-Type")
	if strings.Contains(ct, "text/plain") {
		if s, ok := into.(*string); ok {
			*s = string(raw)
			return nil
		}
	}

	var wrapped apiResponse
	if err := json.Unmarshal(raw, &wrapped); err != nil {
		// Direct unwrap
		return json.Unmarshal(raw, into)
	}
	if into != nil {
		return json.Unmarshal(wrapped.Data, into)
	}
	return nil
}

// ── Memory ────────────────────────────────────────────────────────────

// StoreRequest describes an entity to store.
type StoreRequest struct {
	ID           string    `json:"id"`
	Content      string    `json:"content"`
	EntityType   string    `json:"entityType"`
	Metadata     string    `json:"metadata"`
	Embedding    []float32 `json:"embedding,omitempty"`
	TTLSeconds   *int64    `json:"ttlSeconds,omitempty"`
	CreatedAt    *int64    `json:"createdAt,omitempty"`
	LastAccessed *int64    `json:"lastAccessed,omitempty"`
	AccessCount  *int64    `json:"accessCount,omitempty"`
	ValidFrom    *int64    `json:"validFrom,omitempty"`
	ValidUntil   *int64    `json:"validUntil,omitempty"`

	timeout time.Duration
}

// Store stores a single entity.
func (c *Client) Store(req StoreRequest) error {
	if req.EntityType == "" {
		req.EntityType = "memory"
	}
	meta, err := validateMetadata(req.Metadata)
	if err != nil {
		return err
	}
	req.Metadata = meta
	if err := validateStoreParams(req.ID, req.Content, req.EntityType, req.Metadata, req.Embedding); err != nil {
		return err
	}
	return c.post("/v1/memory/store", req, nil, req.timeout)
}

// StoreBatch stores multiple entities in one call.
func (c *Client) StoreBatch(entities []StoreRequest) (int, error) {
	if len(entities) == 0 {
		return 0, NewValidationError("entities must not be empty")
	}
	if len(entities) > c.maxBatch {
		return 0, NewValidationError(fmt.Sprintf("batch size %d exceeds max %d", len(entities), c.maxBatch))
	}
	var result struct {
		Stored int `json:"stored"`
	}
	if err := c.post("/v1/memory/store-batch", map[string]any{"entities": entities}, &result, 0); err != nil {
		return 0, err
	}
	return result.Stored, nil
}

// Recall performs hybrid search (FTS + vector).
func (c *Client) Recall(query string, embedding []float32, topK int) ([]SearchResult, error) {
	if err := validateTopK(topK, c.maxTopK); err != nil {
		return nil, err
	}
	if err := validateEmbedding(embedding); err != nil {
		return nil, err
	}
	body := map[string]any{"query": query, "topK": topK}
	if embedding != nil {
		body["embedding"] = embedding
	}
	var result struct {
		Results []SearchResult `json:"results"`
	}
	if err := c.post("/v1/memory/recall", body, &result, 0); err != nil {
		return nil, err
	}
	return result.Results, nil
}

// RecallRecent returns the most recently stored entities.
func (c *Client) RecallRecent(topK int) ([]Entity, error) {
	if err := validateTopK(topK, c.maxTopK); err != nil {
		return nil, err
	}
	var result struct {
		Entities []Entity `json:"entities"`
	}
	if err := c.post("/v1/memory/recall-recent", map[string]any{"topK": topK}, &result, 0); err != nil {
		return nil, err
	}
	return result.Entities, nil
}

// RecallByType returns entities of a specific type.
func (c *Client) RecallByType(entityType string, topK int) ([]Entity, error) {
	if err := validateEntityType(entityType); err != nil {
		return nil, err
	}
	if err := validateTopK(topK, c.maxTopK); err != nil {
		return nil, err
	}
	var result struct {
		Entities []Entity `json:"entities"`
	}
	if err := c.post("/v1/memory/recall-by-type", map[string]any{"entityType": entityType, "topK": topK}, &result, 0); err != nil {
		return nil, err
	}
	return result.Entities, nil
}

// Forget soft-deletes an entity by id.
func (c *Client) Forget(id string) (bool, error) {
	if err := validateID(id, "id"); err != nil {
		return false, err
	}
	var result struct {
		Deleted bool `json:"deleted"`
	}
	if err := c.post("/v1/memory/forget", map[string]any{"id": id}, &result, 0); err != nil {
		return false, err
	}
	return result.Deleted, nil
}

// Decay prunes expired TTL entities.
func (c *Client) Decay() (int, error) {
	var result struct {
		Expired int `json:"expired"`
	}
	if err := c.post("/v1/memory/decay", map[string]any{}, &result, 0); err != nil {
		return 0, err
	}
	return result.Expired, nil
}

// EntityHistory returns the full version history of an entity.
func (c *Client) EntityHistory(id string) ([]Entity, error) {
	if err := validateID(id, "id"); err != nil {
		return nil, err
	}
	var result struct {
		Versions []Entity `json:"versions"`
	}
	if err := c.post("/v1/memory/entity-history", map[string]any{"id": id}, &result, 0); err != nil {
		return nil, err
	}
	return result.Versions, nil
}

// ConsolidateConfig configures consolidation behavior.
type ConsolidateConfig struct {
	SimilarityThreshold       *float64
	ContradictionJaccardMax   *float64
	ContradictionCosineMin    *float64
	ContradictionLengthSimMin *float64
	MaxComparisonsPerEntity   *int
}

// Consolidate runs auto-linking and contradiction detection.
func (c *Client) Consolidate(config *ConsolidateConfig) (*ConsolidationReport, error) {
	body := map[string]any{}
	if config != nil {
		if config.SimilarityThreshold != nil {
			body["similarityThreshold"] = *config.SimilarityThreshold
		}
		if config.ContradictionJaccardMax != nil {
			body["contradictionJaccardMax"] = *config.ContradictionJaccardMax
		}
		if config.ContradictionCosineMin != nil {
			body["contradictionCosineMin"] = *config.ContradictionCosineMin
		}
		if config.ContradictionLengthSimMin != nil {
			body["contradictionLengthSimMin"] = *config.ContradictionLengthSimMin
		}
		if config.MaxComparisonsPerEntity != nil {
			body["maxComparisonsPerEntity"] = *config.MaxComparisonsPerEntity
		}
	}
	var result ConsolidationReport
	if err := c.post("/v1/memory/consolidate", body, &result, 0); err != nil {
		return nil, err
	}
	return &result, nil
}

// ── Graph ─────────────────────────────────────────────────────────────

// Associate creates a relationship between two entities.
func (c *Client) Associate(srcID, dstID, relType string, weight float64) error {
	if err := validateID(srcID, "src_id"); err != nil {
		return err
	}
	if err := validateID(dstID, "dst_id"); err != nil {
		return err
	}
	return c.post("/v1/graph/associate", map[string]any{
		"srcId":   srcID,
		"dstId":   dstID,
		"relType": relType,
		"weight":  weight,
	}, nil, 0)
}

// Expand traverses the graph from an entity.
func (c *Client) Expand(entityID string, hops int, edgeTypes []string) ([]Entity, error) {
	if err := validateID(entityID, "entity_id"); err != nil {
		return nil, err
	}
	if err := validateHops(hops); err != nil {
		return nil, err
	}
	body := map[string]any{"entityId": entityID, "hops": hops}
	if edgeTypes != nil {
		body["edgeTypes"] = edgeTypes
	}
	var result struct {
		Entities []Entity `json:"entities"`
	}
	if err := c.post("/v1/graph/expand", body, &result, 0); err != nil {
		return nil, err
	}
	return result.Entities, nil
}

// ── RAG ───────────────────────────────────────────────────────────────

// RagConfig configures RAG query behavior.
type RagConfig struct {
	ExpansionDepth *int
	SearchWeight   *float64
	RecencyWeight  *float64
	DegreeWeight   *float64
	MaxTokens      *int
}

// RagQuery performs a full RAG pipeline query.
func (c *Client) RagQuery(query string, embedding []float32, topK int, config *RagConfig) (*RagResult, error) {
	if err := validateQueryString(query); err != nil {
		return nil, err
	}
	if err := validateTopK(topK, c.maxTopK); err != nil {
		return nil, err
	}
	if err := validateEmbedding(embedding); err != nil {
		return nil, err
	}
	body := map[string]any{"query": query, "topK": topK}
	if embedding != nil {
		body["embedding"] = embedding
	}
	if config != nil {
		if config.ExpansionDepth != nil {
			body["expansionDepth"] = *config.ExpansionDepth
		}
		if config.SearchWeight != nil {
			body["searchWeight"] = *config.SearchWeight
		}
		if config.RecencyWeight != nil {
			body["recencyWeight"] = *config.RecencyWeight
		}
		if config.DegreeWeight != nil {
			body["degreeWeight"] = *config.DegreeWeight
		}
		if config.MaxTokens != nil {
			body["maxTokens"] = *config.MaxTokens
		}
	}
	var result RagResult
	if err := c.post("/v1/rag/query", body, &result, 0); err != nil {
		return nil, err
	}
	return &result, nil
}

// ── Query ─────────────────────────────────────────────────────────────

// Query executes a raw Cypher query.
func (c *Client) Query(query string, params map[string]any, snapshotTs *int64, timeoutMs int) (*QueryResult, error) {
	if err := validateQueryString(query); err != nil {
		return nil, err
	}
	body := map[string]any{"query": query, "timeoutMs": timeoutMs}
	if params != nil {
		body["params"] = params
	}
	if snapshotTs != nil {
		body["snapshotTs"] = *snapshotTs
	}
	var result QueryResult
	if err := c.post("/v1/query", body, &result, 0); err != nil {
		return nil, err
	}
	return &result, nil
}

// ── Admin ─────────────────────────────────────────────────────────────

// Checkpoint forces a WAL checkpoint.
func (c *Client) Checkpoint() error {
	return c.post("/v1/admin/checkpoint", map[string]any{}, nil, 0)
}

// Vacuum forces database vacuum.
func (c *Client) Vacuum() error {
	return c.post("/v1/admin/vacuum", map[string]any{}, nil, 0)
}

// ── Health / Metrics ──────────────────────────────────────────────────

// Health returns the health check response.
func (c *Client) Health() (map[string]any, error) {
	var result map[string]any
	if err := c.get("/health", &result, 0); err != nil {
		return nil, err
	}
	return result, nil
}

// Metrics returns Prometheus-format metrics as a string.
func (c *Client) Metrics() (string, error) {
	var result string
	if err := c.get("/metrics", &result, 0); err != nil {
		return "", err
	}
	return result, nil
}

// ── CDC (Server-Sent Events) ──────────────────────────────────────────

// Subscribe returns a channel of WAL change events.
// The context is used for cancellation.
func (c *Client) Subscribe(ctx context.Context) (<-chan ChangeEvent, <-chan error) {
	events := make(chan ChangeEvent)
	errs := make(chan error, 1)

	go func() {
		defer close(events)
		defer close(errs)

		requestID := c.generateRequestID()
		req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/v1/subscribe", nil)
		if err != nil {
			errs <- fmt.Errorf("subscribe request: %w", err)
			return
		}
		req.Header = c.headers(requestID)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			errs <- fmt.Errorf("subscribe: %w", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			errs <- fmt.Errorf("subscribe: http %d", resp.StatusCode)
			return
		}

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				var ev ChangeEvent
				if err := json.Unmarshal([]byte(line[6:]), &ev); err != nil {
					errs <- fmt.Errorf("decode event: %w", err)
					continue
				}
				select {
				case events <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
		if err := scanner.Err(); err != nil {
			errs <- fmt.Errorf("subscribe read: %w", err)
		}
	}()

	return events, errs
}

// Close closes the underlying HTTP client connections.
func (c *Client) Close() {
	c.httpClient.CloseIdleConnections()
}
