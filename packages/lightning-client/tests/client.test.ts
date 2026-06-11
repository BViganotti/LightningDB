import { describe, it, expect } from 'vitest'
import { LightningClient } from '../src/client'
import { CircuitBreaker, CircuitState } from '../src/circuit_breaker'
import { computeBackoff, shouldRetry, DEFAULT_RETRY_CONFIG } from '../src/retry'

// ── Validation Tests ────────────────────────────────────────────────

describe('validation', () => {
  const client = new LightningClient({ baseUrl: 'http://localhost:9999' })

  it('rejects empty id on store', async () => {
    await expect(client.store('', 'content')).rejects.toThrow('must not be empty')
  })

  it('rejects missing entity_type', async () => {
    await expect(client.store('id', 'content', '')).rejects.toThrow('must not be empty')
  })

  it('rejects invalid top_k', async () => {
    await expect(client.recall('query', undefined, 0)).rejects.toThrow('>= 1')
    await expect(client.recall('query', undefined, 99999)).rejects.toThrow('exceeds max')
  })

  it('rejects invalid hops', async () => {
    await expect(client.expand('id', 0)).rejects.toThrow('>= 1')
    await expect(client.expand('id', 20)).rejects.toThrow('not exceed 10')
  })

  it('rejects invalid embedding', async () => {
    await expect(client.store('id', 'content', 'memory', {}, { embedding: [] as number[] })).rejects.toThrow(
      'must not be empty',
    )
  })

  it('rejects empty batch', async () => {
    await expect(client.storeBatch([])).rejects.toThrow('must not be empty')
  })

  it('rejects invalid id on forget', async () => {
    await expect(client.forget('')).rejects.toThrow('must not be empty')
  })
})

// ── Circuit Breaker Tests ───────────────────────────────────────────

describe('CircuitBreaker', () => {
  it('starts closed', () => {
    const cb = new CircuitBreaker({
      failureThreshold: 3,
      recoveryTimeout: 30,
      halfOpenMaxRequests: 2,
      successThreshold: 1,
    })
    expect(cb.getState()).toBe(CircuitState.CLOSED)
    expect(cb.allowRequest()).toBe(true)
  })

  it('opens after threshold failures', () => {
    const cb = new CircuitBreaker({
      failureThreshold: 2,
      recoveryTimeout: 30,
      halfOpenMaxRequests: 2,
      successThreshold: 1,
    })
    cb.onFailure()
    expect(cb.getState()).toBe(CircuitState.CLOSED)
    cb.onFailure()
    expect(cb.getState()).toBe(CircuitState.OPEN)
    expect(cb.allowRequest()).toBe(false)
  })

  it('transitions to half-open after recovery timeout', async () => {
    const cb = new CircuitBreaker({
      failureThreshold: 1,
      recoveryTimeout: 0.05,
      halfOpenMaxRequests: 2,
      successThreshold: 1,
    })
    cb.onFailure()
    expect(cb.getState()).toBe(CircuitState.OPEN)
    await new Promise((r) => setTimeout(r, 60))
    expect(cb.allowRequest()).toBe(true)
    expect(cb.getState()).toBe(CircuitState.HALF_OPEN)
  })

  it('closes after success in half-open', () => {
    const cb = new CircuitBreaker({
      failureThreshold: 1,
      recoveryTimeout: 0.05,
      halfOpenMaxRequests: 2,
      successThreshold: 1,
    })
    cb.onFailure()
    // Force half-open
    cb['transitionToHalfOpen']()
    cb.onSuccess()
    expect(cb.getState()).toBe(CircuitState.CLOSED)
  })

  it('limits requests in half-open state', () => {
    const cb = new CircuitBreaker({
      failureThreshold: 1,
      recoveryTimeout: 0.05,
      halfOpenMaxRequests: 1,
      successThreshold: 1,
    })
    cb['transitionToHalfOpen']()
    expect(cb.allowRequest()).toBe(true)
    expect(cb.allowRequest()).toBe(false)
  })
})

// ── Retry Logic Tests ──────────────────────────────────────────────

describe('retry', () => {
  it('computeBackoff increases with attempts', () => {
    const delays = [0, 1, 2, 3].map((i) => computeBackoff(i, DEFAULT_RETRY_CONFIG))
    for (let i = 1; i < delays.length; i++) {
      expect(delays[i]).toBeGreaterThanOrEqual(delays[i - 1])
    }
  })

  it('computeBackoff respects max delay', () => {
    const config = { ...DEFAULT_RETRY_CONFIG, baseDelay: 10, maxDelay: 15 }
    const d = computeBackoff(10, config)
    expect(d).toBeLessThanOrEqual(15)
  })

  it('shouldRetry on 429', () => {
    expect(shouldRetry(429, 0, DEFAULT_RETRY_CONFIG)).toBe(true)
    expect(shouldRetry(429, 3, DEFAULT_RETRY_CONFIG)).toBe(false)
  })

  it('should not retry on 400', () => {
    expect(shouldRetry(400, 0, DEFAULT_RETRY_CONFIG)).toBe(false)
  })

  it('should not retry with 0 max retries', () => {
    const config = { ...DEFAULT_RETRY_CONFIG, maxRetries: 0 }
    expect(shouldRetry(429, 0, config)).toBe(false)
  })
})
