package lightning

import (
	"errors"
)

// Sentinel errors.
var (
	ErrInvalidCABundle    = errors.New("invalid CA bundle")
	ErrCircuitBreakerOpen = errors.New("circuit breaker is open")
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
)

// NewValidationError creates a new ValidationError.
func NewValidationError(msg string) error {
	return &ValidationError{msg: msg}
}

// IsErrValidation reports whether err is a ValidationError.
func IsErrValidation(err error) bool {
	var ve *ValidationError
	return errors.As(err, &ve)
}

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

func (e *LightningError) Unwrap() error {
	return nil
}

// ValidationError indicates invalid input parameters.
type ValidationError struct {
	msg string
}

func (e *ValidationError) Error() string {
	return e.msg
}

func (e *ValidationError) Unwrap() error {
	return nil
}

func IsRetryable(status int, cfg RetryConfig) bool {
	return cfg.RetryStatuses[status]
}
