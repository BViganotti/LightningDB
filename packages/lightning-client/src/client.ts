import { v4 as uuidv4 } from 'uuid'
import { CircuitBreaker, CircuitState } from './circuit_breaker'
import { computeBackoff, DEFAULT_RETRY_CONFIG, shouldRetry, type RetryConfig } from './retry'
import type {
  ChangeEvent,
  CircuitBreakerConfig,
  ClientOptions,
  ConsolidationDetail,
  ConsolidationReport,
  Entity,
  QueryResult,
  RagResult,
  SearchResult,
  SnapshotSelector,
  TelemetryHooks,
  TlsConfig,
} from './types'
import {
  validateBatchEntities,
  validateContent,
  validateEmbedding,
  validateEntityType,
  validateHops,
  validateId,
  validateMetadata,
  validateQueryString,
  validateTopK,
  type ValidationError,
} from './validation'

export class LightningError extends Error {
  constructor(
    message: string,
    public statusCode?: number,
    public code?: string,
    public requestId?: string,
  ) {
    super(message)
    this.name = 'LightningError'
  }
}

interface Defaults {
  retry: RetryConfig
  circuitBreaker: Required<CircuitBreakerConfig>
  telemetry: Required<TelemetryHooks>
}

function buildDefaults(opts: ClientOptions): Defaults {
  const retry: RetryConfig = {
    ...DEFAULT_RETRY_CONFIG,
    ...opts.retry,
  }
  const circuitBreaker: Required<CircuitBreakerConfig> = {
    failureThreshold: 5,
    recoveryTimeout: 30,
    halfOpenMaxRequests: 3,
    successThreshold: 2,
    ...opts.circuitBreaker,
  }
  const telemetry: Required<TelemetryHooks> = {
    onRequestStart: () => {},
    onRequestEnd: () => {},
    onError: () => {},
    onRetry: () => {},
    onCircuitBreaker: () => {},
    ...opts.telemetry,
  }
  return { retry, circuitBreaker, telemetry }
}

export class LightningClient {
  private baseUrl: string
  private authToken?: string
  private authTokenProvider?: () => string | undefined
  private accessToken?: string
  private refreshToken?: string
  private tls?: TlsConfig
  private defaultTimeout: number
  private retry: RetryConfig
  private circuitBreaker: CircuitBreaker | undefined
  private telemetry: Required<TelemetryHooks>
  private followRedirects: boolean
  private maxContentBytes: number
  private maxBatchEntities: number
  private maxTopK: number
  private userAgent: string
  private tlsAgent: unknown
  private _tlsAgentInit: Promise<unknown> | undefined
  private abortController?: AbortController

  constructor(options: ClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? 'http://127.0.0.1:8080').replace(/\/+$/, '')
    this.authToken = options.authToken
    this.authTokenProvider = options.authTokenProvider
    this.tls = options.tls

    if (options.tls) {
      this.baseUrl = this.baseUrl.replace(/^http:/, 'https:')
    }

    this.defaultTimeout = options.defaultTimeout ?? 30_000
    this.followRedirects = options.followRedirects ?? false
    this.maxContentBytes = options.maxContentBytes ?? 10 * 1024 * 1024
    this.maxBatchEntities = options.maxBatchEntities ?? 1000
    this.maxTopK = options.maxTopK ?? 1000
    this.userAgent = options.userAgent ?? 'lightning-client-ts/0.1.0'

    const defaults = buildDefaults(options)
    this.retry = defaults.retry
    this.telemetry = defaults.telemetry

    if (options.circuitBreaker) {
      this.circuitBreaker = new CircuitBreaker(defaults.circuitBreaker, options.telemetry)
    }
  }

  async login(username: string, password: string): Promise<void> {
    const r = await fetch(`${this.baseUrl}/v1/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    })
    if (!r.ok) {
      const body = await r.json().catch(() => ({}))
      const data = (body as Record<string, unknown>)?.data as Record<string, unknown> | undefined
      throw new LightningError(
        (data?.error as string) ?? 'login failed',
        r.status,
      )
    }
    const body = await r.json()
    const data = (body as Record<string, unknown>)?.data as Record<string, unknown> ?? body
    this.accessToken = data.accessToken as string
    this.refreshToken = data.refreshToken as string
    this.authTokenProvider = () => this.accessToken
  }

  async loginWithApiKey(apiKey: string): Promise<void> {
    this.authToken = apiKey
  }

  private async ensureTlsAgent(): Promise<unknown> {
    if (this._tlsAgentInit) return this._tlsAgentInit
    this._tlsAgentInit = this.buildTlsAgent()
    this.tlsAgent = await this._tlsAgentInit
    return this.tlsAgent
  }

  private async buildTlsAgent(): Promise<unknown> {
    if (!this.tls) return undefined
    if (typeof process === 'undefined' || !process.versions?.node) return undefined

    let ca: Buffer | undefined
    let cert: Buffer | undefined
    let key: Buffer | undefined

    const fs: typeof import('fs') = await import('node:fs')
    if (this.tls.caBundlePath) {
      ca = fs.readFileSync(this.tls.caBundlePath)
    }
    if (this.tls.certPath) {
      cert = fs.readFileSync(this.tls.certPath)
    }
    if (this.tls.keyPath) {
      key = fs.readFileSync(this.tls.keyPath)
    }

    const https: typeof import('https') = await import('node:https')
    return new https.Agent({
      rejectUnauthorized: this.tls.verify !== false,
      ca,
      cert,
      key,
      servername: this.tls.serverNameOverride,
    })
  }

  private resolveAuth(): string | undefined {
    if (this.authTokenProvider) {
      return this.authTokenProvider()
    }
    return this.authToken
  }

  private headers(requestId: string): Record<string, string> {
    const h: Record<string, string> = {
      'Content-Type': 'application/json',
      'User-Agent': this.userAgent,
      'X-Request-Id': requestId,
    }
    const token = this.resolveAuth()
    if (token) {
      h['Authorization'] = `Bearer ${token}`
    }
    return h
  }

  private checkCircuitBreaker(path: string): void {
    if (!this.circuitBreaker) return
    if (!this.circuitBreaker.allowRequest()) {
      const state = this.circuitBreaker.getState()
      this.telemetry.onCircuitBreaker('denied', state)
      throw new LightningError(`circuit breaker is ${state}, request denied`)
    }
  }

  private reportSuccess(): void {
    this.circuitBreaker?.onSuccess()
  }

  private reportFailure(): void {
    this.circuitBreaker?.onFailure()
  }

  private async request<T>(
    method: string,
    path: string,
    body?: Record<string, unknown>,
    timeoutOverride?: number,
  ): Promise<T> {
    this.checkCircuitBreaker(path)
    const requestId = uuidv4()
    const authToken = this.resolveAuth()
    const headers = this.headers(requestId)
    const timeout = timeoutOverride ?? this.defaultTimeout
    const start = performance.now()
    const agent = await this.ensureTlsAgent()

    this.telemetry.onRequestStart(requestId, method, path)

    const attempt = async (retryCount: number): Promise<T> => {
      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), timeout)

      try {
        const r = await fetch(`${this.baseUrl}${path}`, {
          method,
          headers,
          body: body ? JSON.stringify(body) : undefined,
          signal: controller.signal,
          redirect: this.followRedirects ? 'follow' : 'error',
          ...(agent ? { agent } : {}),
        })

        clearTimeout(timeoutId)
        const contentType = r.headers.get('content-type') ?? ''
        const isPlain = contentType.includes('text/plain')

        let raw: unknown
        try {
          raw = isPlain ? await r.text() : await r.json()
        } catch {
          raw = await r.text()
        }

        if (!r.ok) {
          const errBody = (typeof raw === 'object' && raw !== null ? (raw as Record<string, unknown>) : {}) as Record<string, unknown>
          const errorMsg = (errBody.error as string) ?? r.statusText
          const code = errBody.code as string | undefined
          const reqId = errBody.requestId as string | undefined

          // Token refresh on 401
          if (r.status === 401 && this.refreshToken) {
            try {
              const refreshRes = await fetch(`${this.baseUrl}/v1/auth/refresh`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ refreshToken: this.refreshToken }),
              })
              if (refreshRes.ok) {
                const refreshBody = await refreshRes.json()
                const refreshData = (refreshBody as Record<string, unknown>)?.data as Record<string, unknown> ?? refreshBody
                this.accessToken = refreshData.accessToken as string
                this.refreshToken = refreshData.refreshToken as string
                return attempt(retryCount)
              }
            } catch {
              // refresh failed, fall through to normal error handling
            }
          }

          if (shouldRetry(r.status, retryCount, this.retry)) {
            this.telemetry.onRetry(requestId, method, path, retryCount + 1, 0)
            const delay = computeBackoff(retryCount, this.retry)
            await new Promise((r) => setTimeout(r, delay * 1000))
            return attempt(retryCount + 1)
          }

          this.reportFailure()
          throw new LightningError(errorMsg, r.status, code, reqId)
        }

        this.reportSuccess()
        const duration = performance.now() - start
        this.telemetry.onRequestEnd(requestId, method, path, r.status, duration)

        if (isPlain) return raw as T
        const wrapper = raw as { data?: T }
        return (wrapper.data ?? raw) as T
      } catch (e) {
        clearTimeout(timeoutId)
        if (e instanceof LightningError) throw e

        if (retryCount < this.retry.maxRetries) {
          this.telemetry.onRetry(requestId, method, path, retryCount + 1, 0)
          const delay = computeBackoff(retryCount, this.retry)
          await new Promise((r) => setTimeout(r, delay * 1000))
          return attempt(retryCount + 1)
        }

        this.reportFailure()
        this.telemetry.onError(requestId, method, path, e as Error)
        throw new LightningError(
          (e as Error).message ?? 'unknown error',
          undefined,
          undefined,
          requestId,
        )
      }
    }

    return attempt(0)
  }

  private async post<T>(path: string, body: Record<string, unknown>, timeout?: number): Promise<T> {
    return this.request<T>('POST', path, body, timeout)
  }

  private async get<T>(path: string, timeout?: number): Promise<T> {
    return this.request<T>('GET', path, undefined, timeout)
  }

  // ── Memory ─────────────────────────────────────────────────────

  async store(
    id: string,
    content: string,
    entityType = 'memory',
    metadata: Record<string, unknown> | string = '{}',
    options?: {
      embedding?: number[]
      ttlSeconds?: number
      createdAt?: number
      lastAccessed?: number
      accessCount?: number
      validFrom?: number
      validUntil?: number
    },
    timeout?: number,
  ): Promise<void> {
    validateId(id)
    validateContent(content)
    validateEntityType(entityType)
    const metadataStr = validateMetadata(metadata)
    validateEmbedding(options?.embedding)

    const body: Record<string, unknown> = {
      id,
      content,
      entityType,
      metadata: metadataStr,
    }
    if (options?.embedding) body.embedding = options.embedding
    if (options?.ttlSeconds !== undefined) body.ttlSeconds = options.ttlSeconds
    if (options?.createdAt !== undefined) body.createdAt = options.createdAt
    if (options?.lastAccessed !== undefined) body.lastAccessed = options.lastAccessed
    if (options?.accessCount !== undefined) body.accessCount = options.accessCount
    if (options?.validFrom !== undefined) body.validFrom = options.validFrom
    if (options?.validUntil !== undefined) body.validUntil = options.validUntil

    await this.post('/v1/memory/store', body, timeout)
  }

  async storeBatch(
    entities: Record<string, unknown>[],
    timeout?: number,
  ): Promise<number> {
    validateBatchEntities(entities, this.maxBatchEntities)
    const r = await this.post<{ stored: number }>('/v1/memory/store-batch', { entities }, timeout)
    return r.stored
  }

  async recall(
    query = '',
    embedding?: number[],
    topK = 10,
    timeout?: number,
  ): Promise<SearchResult[]> {
    validateTopK(topK, this.maxTopK)
    validateEmbedding(embedding)
    const body: Record<string, unknown> = { query, topK }
    if (embedding) body.embedding = embedding
    const r = await this.post<{ results: SearchResult[] }>('/v1/memory/recall', body, timeout)
    return r.results
  }

  async recallRecent(topK = 10, timeout?: number): Promise<Entity[]> {
    validateTopK(topK, this.maxTopK)
    const r = await this.post<{ entities: Entity[] }>('/v1/memory/recall-recent', { topK }, timeout)
    return r.entities
  }

  async recallByType(entityType: string, topK = 10, timeout?: number): Promise<Entity[]> {
    validateEntityType(entityType)
    validateTopK(topK, this.maxTopK)
    const r = await this.post<{ entities: Entity[] }>(
      '/v1/memory/recall-by-type',
      { entityType, topK },
      timeout,
    )
    return r.entities
  }

  async forget(id: string, timeout?: number): Promise<boolean> {
    validateId(id)
    const r = await this.post<{ deleted: boolean }>('/v1/memory/forget', { id }, timeout)
    return r.deleted
  }

  async decay(timeout?: number): Promise<number> {
    const r = await this.post<{ expired: number }>('/v1/memory/decay', {}, timeout)
    return r.expired
  }

  async entityHistory(id: string, timeout?: number): Promise<Entity[]> {
    validateId(id)
    const r = await this.post<{ versions: Entity[] }>('/v1/memory/entity-history', { id }, timeout)
    return r.versions
  }

  async consolidate(
    config?: {
      similarityThreshold?: number
      contradictionJaccardMax?: number
      contradictionCosineMin?: number
      contradictionLengthSimMin?: number
      maxComparisonsPerEntity?: number
      includeDetails?: boolean
    },
    timeout?: number,
  ): Promise<ConsolidationReport> {
    return this.post('/v1/memory/consolidate', config ?? {}, timeout)
  }

  // ── Graph ──────────────────────────────────────────────────────

  async associate(
    srcId: string,
    dstId: string,
    relType: string,
    weight = 1.0,
    timeout?: number,
  ): Promise<void> {
    validateId(srcId, 'src_id')
    validateId(dstId, 'dst_id')
    await this.post('/v1/graph/associate', { srcId, dstId, relType, weight }, timeout)
  }

  async expand(
    entityId: string,
    hops = 1,
    edgeTypes?: string[],
    timeout?: number,
  ): Promise<Entity[]> {
    validateId(entityId, 'entity_id')
    validateHops(hops)
    const body: Record<string, unknown> = { entityId, hops }
    if (edgeTypes) body.edgeTypes = edgeTypes
    const r = await this.post<{ entities: Entity[] }>('/v1/graph/expand', body, timeout)
    return r.entities
  }

  // ── RAG ────────────────────────────────────────────────────────

  async ragQuery(
    query: string,
    embedding?: number[],
    topK = 5,
    config?: {
      expansionDepth?: number
      searchWeight?: number
      recencyWeight?: number
      degreeWeight?: number
      maxTokens?: number
    },
    timeout?: number,
  ): Promise<RagResult> {
    validateQueryString(query)
    validateTopK(topK, this.maxTopK)
    validateEmbedding(embedding)
    const body: Record<string, unknown> = { query, topK }
    if (embedding) body.embedding = embedding
    if (config) {
      if (config.expansionDepth !== undefined) body.expansionDepth = config.expansionDepth
      if (config.searchWeight !== undefined) body.searchWeight = config.searchWeight
      if (config.recencyWeight !== undefined) body.recencyWeight = config.recencyWeight
      if (config.degreeWeight !== undefined) body.degreeWeight = config.degreeWeight
      if (config.maxTokens !== undefined) body.maxTokens = config.maxTokens
    }
    return this.post('/v1/rag/query', body, timeout)
  }

  // ── Query ──────────────────────────────────────────────────────

  async query(
    query: string,
    params?: Record<string, unknown>,
    snapshotTsOrSelector?: number | SnapshotSelector,
    timeoutMs = 30000,
    timeout?: number,
  ): Promise<QueryResult> {
    validateQueryString(query)
    const body: Record<string, unknown> = { query, timeoutMs }
    if (params) body.params = params
    if (snapshotTsOrSelector !== undefined) {
      if (typeof snapshotTsOrSelector === 'number') {
        body.snapshotTs = snapshotTsOrSelector
      } else {
        body.snapshot = snapshotTsOrSelector
      }
    }
    return this.post('/v1/query', body, timeout)
  }

  // ── Admin ──────────────────────────────────────────────────────

  async checkpoint(timeout?: number): Promise<void> {
    await this.post('/v1/admin/checkpoint', {}, timeout)
  }

  async vacuum(timeout?: number): Promise<void> {
    await this.post('/v1/admin/vacuum', {}, timeout)
  }

  // ── Health / Metrics ───────────────────────────────────────────

  async health(timeout?: number): Promise<Record<string, unknown>> {
    return this.get('/health', timeout)
  }

  async metrics(timeout?: number): Promise<string> {
    const requestId = uuidv4()
    const agent = await this.ensureTlsAgent()
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), timeout ?? this.defaultTimeout)

    try {
      const r = await fetch(`${this.baseUrl}/metrics`, {
        headers: this.headers(requestId),
        signal: controller.signal,
        redirect: this.followRedirects ? 'follow' : 'error',
        ...(agent ? { agent } : {}),
      })
      clearTimeout(timeoutId)
      if (!r.ok) throw new LightningError(await r.text(), r.status)
      return r.text()
    } catch (e) {
      clearTimeout(timeoutId)
      this.telemetry.onError(requestId, 'GET', '/metrics', e as Error)
      throw e instanceof LightningError ? e : new LightningError((e as Error).message)
    }
  }

  // ── CDC ────────────────────────────────────────────────────────

  async *subscribe(): AsyncGenerator<ChangeEvent> {
    const requestId = uuidv4()
    const agent = await this.ensureTlsAgent()
    const r = await fetch(`${this.baseUrl}/v1/subscribe`, {
      headers: this.headers(requestId),
      redirect: this.followRedirects ? 'follow' : 'error',
      ...(agent ? { agent } : {}),
    })
    if (!r.ok) throw new LightningError(`subscribe failed: ${r.statusText}`, r.status)

    const reader = r.body?.getReader()
    if (!reader) throw new LightningError('no response body')

    const decoder = new TextDecoder()
    let buf = ''

    try {
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buf += decoder.decode(value, { stream: true })
        const lines = buf.split('\n')
        buf = lines.pop() ?? ''
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            yield JSON.parse(line.slice(6)) as ChangeEvent
          }
        }
      }
    } finally {
      reader.releaseLock()
    }
  }
}
