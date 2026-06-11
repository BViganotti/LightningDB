package lightning

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"time"
)

// SearchResult from a recall query.
type SearchResult struct {
	ID         string  `json:"id"`
	Content    string  `json:"content"`
	EntityType string  `json:"type"`
	Score      float64 `json:"score"`
	Metadata   string  `json:"metadata"`
}

// Entity represents a stored memory entity.
type Entity struct {
	ID           string `json:"id"`
	EntityType   string `json:"type"`
	Content      string `json:"content"`
	Metadata     string `json:"metadata"`
	CreatedAt    int64  `json:"createdAt"`
	LastAccessed int64  `json:"lastAccessed"`
	AccessCount  int64  `json:"accessCount"`
	TTLSeconds   int64  `json:"ttlSeconds"`
	ValidFrom    int64  `json:"validFrom"`
	ValidUntil   int64  `json:"validUntil"`
}

// QueryResult from a raw Cypher query.
type QueryResult struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	NumRows int              `json:"numRows"`
}

// RagResult from a RAG query.
type RagResult struct {
	Context      string     `json:"context"`
	Sources      []SourceRef `json:"sources"`
	TotalSources int        `json:"totalSources"`
	Warnings     []string   `json:"warnings"`
}

// SourceRef is a cited source in a RAG result.
type SourceRef struct {
	ID         string  `json:"id"`
	Score      float64 `json:"score"`
	EntityType string  `json:"type"`
	Excerpt    string  `json:"excerpt"`
}

// ConsolidationReport from a consolidation run.
type ConsolidationReport struct {
	LinksCreated       int      `json:"linksCreated"`
	ContradictionsFound int     `json:"contradictionsFound"`
	TotalEntities      int      `json:"totalEntities"`
	Warnings           []string `json:"warnings"`
}

// ChangeEvent from the CDC subscribe stream.
type ChangeEvent struct {
	Timestamp     int64  `json:"timestamp"`
	BytesWritten  int64  `json:"bytesWritten"`
	TotalWalBytes int64  `json:"totalWalBytes"`
	EntityID      string `json:"entityId,omitempty"`
	OperationType string `json:"operationType"`
}

// ClientConfig configures the LightningDB HTTP client.
type ClientConfig struct {
	BaseURL              string
	AuthToken            string
	AuthTokenProvider    func() string
	DefaultTimeout       time.Duration
	Retry                RetryConfig
	CircuitBreaker       *CircuitBreakerConfig
	TLS                  *TLSConfig
	Telemetry            *TelemetryHooks
	MaxConnections       int
	MaxKeepalive         int
	KeepaliveTimeout     time.Duration
	FollowRedirects      bool
	MaxContentBytes      int64
	MaxBatchEntities     int
	MaxTopK              int
	UserAgent            string
}

// RetryConfig configures retry with exponential backoff.
type RetryConfig struct {
	MaxRetries    int
	BaseDelay     time.Duration
	MaxDelay      time.Duration
	JitterFactor  float64
	RetryStatuses map[int]bool
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:   3,
		BaseDelay:    100 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		JitterFactor: 0.1,
		RetryStatuses: map[int]bool{
			429: true,
			502: true,
			503: true,
			504: true,
		},
	}
}

// CircuitBreakerConfig configures circuit breaker behavior.
type CircuitBreakerConfig struct {
	FailureThreshold    int
	RecoveryTimeout     time.Duration
	HalfOpenMaxRequests int
	SuccessThreshold    int
}

// TLSConfig configures TLS/mTLS settings.
type TLSConfig struct {
	Verify             bool
	CABundlePath       string
	CertPath           string
	KeyPath            string
	ServerNameOverride string
}

// TelemetryHooks provides observability callbacks.
type TelemetryHooks struct {
	OnRequestStart   func(requestID, method, path string)
	OnRequestEnd     func(requestID, method, path string, status int, durationMs float64)
	OnError          func(requestID, method, path string, err error)
	OnRetry          func(requestID, method, path string, attempt int, delayMs float64)
	OnCircuitBreaker func(newState, previousState string)
}

// DefaultClientConfig returns a production-ready default configuration.
func DefaultClientConfig(baseURL string) ClientConfig {
	return ClientConfig{
		BaseURL:          baseURL,
		DefaultTimeout:   30 * time.Second,
		Retry:            DefaultRetryConfig(),
		MaxConnections:   10,
		MaxKeepalive:     5,
		KeepaliveTimeout: 60 * time.Second,
		FollowRedirects:  false,
		MaxContentBytes:  10 * 1024 * 1024,
		MaxBatchEntities: 1000,
		MaxTopK:          1000,
		UserAgent:        "lightning-client-go/0.1.0",
	}
}

// buildHTTPTransport creates an http.Transport with connection pooling and TLS.
func buildHTTPTransport(cfg ClientConfig) (*http.Transport, error) {
	t := &http.Transport{
		MaxIdleConns:        cfg.MaxConnections,
		MaxIdleConnsPerHost: cfg.MaxKeepalive,
		IdleConnTimeout:     cfg.KeepaliveTimeout,
	}

	if cfg.TLS != nil {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: !cfg.TLS.Verify,
		}
		if cfg.TLS.ServerNameOverride != "" {
			tlsCfg.ServerName = cfg.TLS.ServerNameOverride
		}
		if cfg.TLS.CABundlePath != "" {
			caCert, err := os.ReadFile(cfg.TLS.CABundlePath)
			if err != nil {
				return nil, err
			}
			caPool := x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(caCert) {
				return nil, ErrInvalidCABundle
			}
			tlsCfg.RootCAs = caPool
		}
		if cfg.TLS.CertPath != "" && cfg.TLS.KeyPath != "" {
			cert, err := tls.LoadX509KeyPair(cfg.TLS.CertPath, cfg.TLS.KeyPath)
			if err != nil {
				return nil, err
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		t.TLSClientConfig = tlsCfg
	}

	return t, nil
}
