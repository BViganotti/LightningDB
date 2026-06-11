export interface RetryConfig {
  maxRetries: number
  baseDelay: number
  maxDelay: number
  jitterFactor: number
  retryableStatuses: ReadonlySet<number>
}

export const DEFAULT_RETRY_CONFIG: RetryConfig = {
  maxRetries: 3,
  baseDelay: 0.1,
  maxDelay: 10.0,
  jitterFactor: 0.1,
  retryableStatuses: new Set([429, 502, 503, 504]),
}

export function computeBackoff(attempt: number, config: RetryConfig): number {
  const delay = Math.min(config.baseDelay * Math.pow(2, attempt), config.maxDelay)
  const jitter = (Math.random() * 2 - 1) * config.jitterFactor * delay
  return Math.max(0, delay + jitter)
}

export function shouldRetry(statusCode: number, attempt: number, config: RetryConfig): boolean {
  if (attempt >= config.maxRetries) return false
  return config.retryableStatuses.has(statusCode)
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms * 1000))
}
