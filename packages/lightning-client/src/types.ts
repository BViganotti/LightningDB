export interface ClientOptions {
  baseUrl?: string
  authToken?: string
  authTokenProvider?: () => string | undefined
  defaultTimeout?: number
  retry?: RetryConfig
  circuitBreaker?: CircuitBreakerConfig
  tls?: TlsConfig
  telemetry?: TelemetryHooks
  maxConnections?: number
  maxKeepaliveConnections?: number
  followRedirects?: boolean
  maxContentBytes?: number
  maxBatchEntities?: number
  maxTopK?: number
  userAgent?: string
}

export interface RetryConfig {
  maxRetries?: number
  baseDelay?: number
  maxDelay?: number
  jitterFactor?: number
  retryableStatuses?: ReadonlySet<number>
}

export interface CircuitBreakerConfig {
  failureThreshold?: number
  recoveryTimeout?: number
  halfOpenMaxRequests?: number
  successThreshold?: number
}

export interface TlsConfig {
  verify?: boolean
  caBundlePath?: string
  certPath?: string
  keyPath?: string
  serverNameOverride?: string
}

export interface TelemetryHooks {
  onRequestStart?: (requestId: string, method: string, path: string) => void
  onRequestEnd?: (requestId: string, method: string, path: string, status: number, durationMs: number) => void
  onError?: (requestId: string, method: string, path: string, error: Error) => void
  onRetry?: (requestId: string, method: string, path: string, attempt: number, delayMs: number) => void
  onCircuitBreaker?: (newState: string, previousState: string) => void
}

export interface SearchResult {
  id: string
  content: string
  entityType: string
  score: number
  metadata: string
}

export interface Entity {
  id: string
  entityType: string
  content: string
  metadata: string
  createdAt: number
  lastAccessed: number
  accessCount: number
  ttlSeconds: number
  validFrom: number
  validUntil: number
}

export interface QueryResult {
  columns: string[]
  rows: Record<string, unknown>[]
  numRows: number
}

export interface SourceRef {
  id: string
  score: number
  type: string
  excerpt: string
}

export interface RagResult {
  context: string
  sources: SourceRef[]
  totalSources: number
  warnings: string[]
}

export interface ConsolidationReport {
  linksCreated: number
  contradictionsFound: number
  totalEntities: number
  warnings: string[]
}

export interface ChangeEvent {
  timestamp: number
  bytesWritten: number
  totalWalBytes: number
  entityId: string | null
  operationType: string
}
