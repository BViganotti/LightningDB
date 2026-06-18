package lightning

import (
	"testing"
	"time"
)

// ── Validation Tests ────────────────────────────────────────────────

func TestValidateID_Empty(t *testing.T) {
	if err := validateID("", "id"); err == nil {
		t.Fatal("expected error for empty id")
	}
}

func TestValidateID_TooLong(t *testing.T) {
	buf := make([]byte, maxIDLength+1)
	for i := range buf {
		buf[i] = 'a'
	}
	if err := validateID(string(buf), "id"); err == nil {
		t.Fatal("expected error for too-long id")
	}
}

func TestValidateContent_TooLong(t *testing.T) {
	buf := make([]byte, maxContentLength+1)
	for i := range buf {
		buf[i] = 'a'
	}
	if err := validateContent(string(buf)); err == nil {
		t.Fatal("expected error for too-long content")
	}
}

func TestValidateEntityType_Empty(t *testing.T) {
	if err := validateEntityType(""); err == nil {
		t.Fatal("expected error for empty entity type")
	}
}

func TestValidateEmbedding_Empty(t *testing.T) {
	if err := validateEmbedding([]float32{}); err == nil {
		t.Fatal("expected error for empty embedding")
	}
}

func TestValidateEmbedding_TooLarge(t *testing.T) {
	e := make([]float32, maxEmbeddingDim+1)
	if err := validateEmbedding(e); err == nil {
		t.Fatal("expected error for oversized embedding")
	}
}

func TestValidateTopK_Zero(t *testing.T) {
	if err := validateTopK(0, 1000); err == nil {
		t.Fatal("expected error for topK=0")
	}
}

func TestValidateTopK_ExceedsMax(t *testing.T) {
	if err := validateTopK(100, 50); err == nil {
		t.Fatal("expected error for topK > max")
	}
}

func TestValidateHops_Zero(t *testing.T) {
	if err := validateHops(0); err == nil {
		t.Fatal("expected error for hops=0")
	}
}

func TestValidateHops_TooLarge(t *testing.T) {
	if err := validateHops(11); err == nil {
		t.Fatal("expected error for hops > 10")
	}
}

func TestValidateMetadata_JSONString(t *testing.T) {
	s, err := validateMetadata(`{"key":"val"}`)
	if err != nil {
		t.Fatal(err)
	}
	if s != `{"key":"val"}` {
		t.Fatalf("expected original JSON, got %s", s)
	}
}

func TestValidateMetadata_Map(t *testing.T) {
	s, err := validateMetadata(map[string]any{"key": "val"})
	if err != nil {
		t.Fatal(err)
	}
	if s != `{"key":"val"}` {
		t.Fatalf("expected serialized JSON, got %s", s)
	}
}

func TestValidateMetadata_InvalidJSON(t *testing.T) {
	_, err := validateMetadata("{invalid}")
	if err == nil {
		t.Fatal("expected error for invalid JSON string")
	}
}

// ── Circuit Breaker Tests ──────────────────────────────────────────

func TestCircuitBreaker_InitialState(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold:    3,
		RecoveryTimeout:     30 * time.Second,
		HalfOpenMaxRequests: 2,
		SuccessThreshold:    1,
	}, nil)
	if cb.State() != CircuitClosed {
		t.Fatalf("expected CLOSED, got %s", cb.State())
	}
	if !cb.AllowRequest() {
		t.Fatal("expected allow")
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold:    2,
		RecoveryTimeout:     30 * time.Second,
		HalfOpenMaxRequests: 2,
		SuccessThreshold:    1,
	}, nil)
	cb.OnFailure()
	cb.OnFailure()
	if cb.State() != CircuitOpen {
		t.Fatalf("expected OPEN after 2 failures, got %s", cb.State())
	}
	if cb.AllowRequest() {
		t.Fatal("expected deny when OPEN")
	}
}

func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold:    1,
		RecoveryTimeout:     50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
		SuccessThreshold:    1,
	}, nil)
	cb.OnFailure()
	if cb.State() != CircuitOpen {
		t.Fatalf("expected OPEN, got %s", cb.State())
	}
	time.Sleep(60 * time.Millisecond)
	if !cb.AllowRequest() {
		t.Fatal("expected allow after recovery timeout")
	}
	if cb.State() != CircuitHalfOpen {
		t.Fatalf("expected HALF_OPEN, got %s", cb.State())
	}
}

func TestCircuitBreaker_ClosesAfterSuccessInHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold:    1,
		RecoveryTimeout:     50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
		SuccessThreshold:    1,
	}, nil)
	cb.OnFailure()
	time.Sleep(60 * time.Millisecond)
	cb.AllowRequest()
	cb.OnSuccess()
	if cb.State() != CircuitClosed {
		t.Fatalf("expected CLOSED after success, got %s", cb.State())
	}
}

func TestCircuitBreaker_LimitsHalfOpenRequests(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold:    1,
		RecoveryTimeout:     50 * time.Millisecond,
		HalfOpenMaxRequests: 1,
		SuccessThreshold:    1,
	}, nil)
	cb.OnFailure()
	time.Sleep(60 * time.Millisecond)
	if !cb.AllowRequest() {
		t.Fatal("expected first half-open request to be allowed")
	}
	if cb.AllowRequest() {
		t.Fatal("expected second half-open request to be denied")
	}
}

// ── Retry Logic Tests ──────────────────────────────────────────────

func TestComputeBackoff_Increases(t *testing.T) {
	cfg := DefaultRetryConfig()
	d0 := computeBackoff(0, cfg)
	d1 := computeBackoff(1, cfg)
	d2 := computeBackoff(2, cfg)
	if d1 < d0 {
		t.Fatal("backoff should increase")
	}
	if d2 < d1 {
		t.Fatal("backoff should increase")
	}
}

func TestComputeBackoff_RespectsMax(t *testing.T) {
	cfg := RetryConfig{
		BaseDelay: 10 * time.Second,
		MaxDelay:  12 * time.Second,
	}
	d := computeBackoff(10, cfg)
	if d > 12*time.Second {
		t.Fatalf("backoff %v exceeds max 12s", d)
	}
}

func TestComputeBackoff_NonNegative(t *testing.T) {
	cfg := RetryConfig{
		BaseDelay:    100 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		JitterFactor: 0.5,
	}
	for i := 0; i < 10; i++ {
		d := computeBackoff(i, cfg)
		if d < 0 {
			t.Fatalf("negative backoff: %v", d)
		}
	}
}

func TestIsRetryable(t *testing.T) {
	cfg := DefaultRetryConfig()
	if !IsRetryable(429, cfg) {
		t.Fatal("429 should be retryable")
	}
	if !IsRetryable(503, cfg) {
		t.Fatal("503 should be retryable")
	}
	if IsRetryable(400, cfg) {
		t.Fatal("400 should NOT be retryable")
	}
	if IsRetryable(401, cfg) {
		t.Fatal("401 should NOT be retryable")
	}
}

// ── Client Config Tests ────────────────────────────────────────────

func TestDefaultClientConfig(t *testing.T) {
	cfg := DefaultClientConfig("http://localhost:8080")
	if cfg.BaseURL != "http://localhost:8080" {
		t.Fatal("base URL not set")
	}
	if cfg.DefaultTimeout != 30*time.Second {
		t.Fatal("default timeout wrong")
	}
	if cfg.Retry.MaxRetries != 3 {
		t.Fatal("max retries should be 3")
	}
	if cfg.FollowRedirects {
		t.Fatal("redirects should be disabled by default")
	}
}

// ── StoreRequest Validation ────────────────────────────────────────

func TestValidateStoreParams(t *testing.T) {
	err := validateStoreParams("id", "content", "memory", map[string]any{}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidateStoreParams_EmptyID(t *testing.T) {
	err := validateStoreParams("", "content", "memory", nil, nil)
	if err == nil {
		t.Fatal("expected error for empty id")
	}
}

func TestValidateStoreParams_EmptyEntityType(t *testing.T) {
	err := validateStoreParams("id", "content", "", nil, nil)
	if err == nil {
		t.Fatal("expected error for empty entity_type")
	}
}
