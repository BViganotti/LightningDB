package lightning

import (
	"fmt"
)

// Error sentinel values.
var (
	ErrInvalidCABundle    = fmt.Errorf("invalid CA bundle")
	ErrCircuitBreakerOpen = fmt.Errorf("circuit breaker is open")
	ErrMaxRetriesExceeded = fmt.Errorf("max retries exceeded")
	ErrValidation         = func(msg string) error { return &ValidationError{msg: msg} }
)

// LightningError is a structured error from the Lightning server.
type LightningError struct {
	Message   string `json:"error"`
	Code      string `json:"code,omitempty"`
	RequestID string `json:"requestId,omitempty"`
	Status    int
}

func (e *LightningError) Error() string {
	return e.Message
}

// ValidationError indicates invalid input parameters.
type ValidationError struct {
	msg string
}

func (e *ValidationError) Error() string {
	return e.msg
}

func IsRetryable(status int, cfg RetryConfig) bool {
	return cfg.RetryStatuses[status]
}
