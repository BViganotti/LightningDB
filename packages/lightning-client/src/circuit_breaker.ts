import type { CircuitBreakerConfig, TelemetryHooks } from './types'

export enum CircuitState {
  CLOSED = 'closed',
  OPEN = 'open',
  HALF_OPEN = 'half_open',
}

export class CircuitBreaker {
  private state: CircuitState = CircuitState.CLOSED
  private failureCount = 0
  private successCount = 0
  private lastFailureTime = 0
  private halfOpenPermits = 0

  constructor(
    private config: Required<CircuitBreakerConfig>,
    private telemetry?: TelemetryHooks,
  ) {}

  getState(): CircuitState {
    return this.state
  }

  allowRequest(): boolean {
    if (this.state === CircuitState.CLOSED) return true

    if (this.state === CircuitState.OPEN) {
      const elapsed = Date.now() - this.lastFailureTime
      if (elapsed >= this.config.recoveryTimeout * 1000) {
        this.transitionToHalfOpen()
        return true
      }
      return false
    }

    if (this.state === CircuitState.HALF_OPEN) {
      if (this.halfOpenPermits < this.config.halfOpenMaxRequests) {
        this.halfOpenPermits++
        return true
      }
      return false
    }

    return false
  }

  onSuccess(): void {
    if (this.state === CircuitState.HALF_OPEN) {
      this.successCount++
      if (this.successCount >= this.config.successThreshold) {
        this.transitionToClosed()
      }
    } else if (this.state === CircuitState.CLOSED) {
      this.failureCount = 0
    }
  }

  onFailure(): void {
    this.lastFailureTime = Date.now()
    if (this.state === CircuitState.HALF_OPEN) {
      this.transitionToOpen()
      return
    }
    if (this.state === CircuitState.CLOSED) {
      this.failureCount++
      if (this.failureCount >= this.config.failureThreshold) {
        this.transitionToOpen()
      }
    }
  }

  private transitionToOpen(): void {
    const prev = this.state
    this.state = CircuitState.OPEN
    this.halfOpenPermits = 0
    this.successCount = 0
    this.telemetry?.onCircuitBreaker?.('open', prev)
  }

  private transitionToHalfOpen(): void {
    const prev = this.state
    this.state = CircuitState.HALF_OPEN
    this.halfOpenPermits = 0
    this.successCount = 0
    this.telemetry?.onCircuitBreaker?.('half_open', prev)
  }

  private transitionToClosed(): void {
    const prev = this.state
    this.state = CircuitState.CLOSED
    this.failureCount = 0
    this.successCount = 0
    this.halfOpenPermits = 0
    this.telemetry?.onCircuitBreaker?.('closed', prev)
  }
}
