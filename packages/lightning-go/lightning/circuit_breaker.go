package lightning

import (
	"sync"
	"time"
)

// CircuitState represents the state of the circuit breaker.
type CircuitState int

const (
	CircuitClosed   CircuitState = iota
	CircuitOpen     CircuitState = iota
	CircuitHalfOpen CircuitState = iota
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// CircuitBreaker implements the circuit breaker pattern.
type CircuitBreaker struct {
	mu sync.Mutex

	state            CircuitState
	failureCount     int
	successCount     int
	lastFailureTime  time.Time
	halfOpenPermits  int

	cfg    CircuitBreakerConfig
	tele   *TelemetryHooks
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(cfg CircuitBreakerConfig, tele *TelemetryHooks) *CircuitBreaker {
	return &CircuitBreaker{
		state: CircuitClosed,
		cfg:   cfg,
		tele:  tele,
	}
}

// State returns the current state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// AllowRequest returns true if the request should proceed.
func (cb *CircuitBreaker) AllowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailureTime) >= cb.cfg.RecoveryTimeout {
			cb.transitionToHalfOpen()
			return true
		}
		return false
	case CircuitHalfOpen:
		if cb.halfOpenPermits < cb.cfg.HalfOpenMaxRequests {
			cb.halfOpenPermits++
			return true
		}
		return false
	default:
		return false
	}
}

// OnSuccess reports a successful request.
func (cb *CircuitBreaker) OnSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == CircuitHalfOpen {
		cb.successCount++
		if cb.successCount >= cb.cfg.SuccessThreshold {
			cb.transitionToClosed()
		}
	} else if cb.state == CircuitClosed {
		cb.failureCount = 0
	}
}

// OnFailure reports a failed request.
func (cb *CircuitBreaker) OnFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailureTime = time.Now()
	if cb.state == CircuitHalfOpen {
		cb.transitionToOpen()
		return
	}
	if cb.state == CircuitClosed {
		cb.failureCount++
		if cb.failureCount >= cb.cfg.FailureThreshold {
			cb.transitionToOpen()
		}
	}
}

func (cb *CircuitBreaker) transitionToOpen() {
	prev := cb.state
	cb.state = CircuitOpen
	cb.halfOpenPermits = 0
	cb.successCount = 0
	if cb.tele != nil && cb.tele.OnCircuitBreaker != nil {
		cb.tele.OnCircuitBreaker("open", prev.String())
	}
}

func (cb *CircuitBreaker) transitionToHalfOpen() {
	prev := cb.state
	cb.state = CircuitHalfOpen
	cb.halfOpenPermits = 0
	cb.successCount = 0
	if cb.tele != nil && cb.tele.OnCircuitBreaker != nil {
		cb.tele.OnCircuitBreaker("half_open", prev.String())
	}
}

func (cb *CircuitBreaker) transitionToClosed() {
	prev := cb.state
	cb.state = CircuitClosed
	cb.failureCount = 0
	cb.successCount = 0
	cb.halfOpenPermits = 0
	if cb.tele != nil && cb.tele.OnCircuitBreaker != nil {
		cb.tele.OnCircuitBreaker("closed", prev.String())
	}
}
