export class ValidationError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ValidationError'
  }
}

const MAX_ID_LENGTH = 1024
const MAX_CONTENT_LENGTH = 10 * 1024 * 1024
const MAX_ENTITY_TYPE_LENGTH = 256
const MAX_EMBEDDING_DIM = 16384
const MAX_QUERY_LENGTH = 100_000

export function validateId(id: unknown, fieldName = 'id'): asserts id is string {
  if (typeof id !== 'string') {
    throw new ValidationError(`${fieldName} must be a string, got ${typeof id}`)
  }
  if (id.length === 0) {
    throw new ValidationError(`${fieldName} must not be empty`)
  }
  if (id.length > MAX_ID_LENGTH) {
    throw new ValidationError(`${fieldName} exceeds max length of ${MAX_ID_LENGTH}`)
  }
}

export function validateContent(content: unknown, fieldName = 'content'): asserts content is string {
  if (typeof content !== 'string') {
    throw new ValidationError(`${fieldName} must be a string, got ${typeof content}`)
  }
  if (content.length > MAX_CONTENT_LENGTH) {
    throw new ValidationError(`${fieldName} exceeds max ${MAX_CONTENT_LENGTH} bytes`)
  }
}

export function validateEntityType(entityType: unknown): asserts entityType is string {
  if (typeof entityType !== 'string') {
    throw new ValidationError(`entity_type must be a string, got ${typeof entityType}`)
  }
  if (entityType.length === 0) {
    throw new ValidationError('entity_type must not be empty')
  }
  if (entityType.length > MAX_ENTITY_TYPE_LENGTH) {
    throw new ValidationError(`entity_type exceeds max length of ${MAX_ENTITY_TYPE_LENGTH}`)
  }
}

export function validateEmbedding(embedding: unknown): embedding is number[] | undefined {
  if (embedding === undefined || embedding === null) return true
  if (!Array.isArray(embedding)) {
    throw new ValidationError(`embedding must be an array of floats, got ${typeof embedding}`)
  }
  if (embedding.length === 0) {
    throw new ValidationError('embedding must not be empty if provided')
  }
  if (embedding.length > MAX_EMBEDDING_DIM) {
    throw new ValidationError(`embedding dimension ${embedding.length} exceeds max ${MAX_EMBEDDING_DIM}`)
  }
  for (let i = 0; i < embedding.length; i++) {
    if (typeof embedding[i] !== 'number' || !Number.isFinite(embedding[i])) {
      throw new ValidationError(`embedding[${i}] must be a finite number`)
    }
  }
  return true
}

export function validateTopK(topK: unknown, maxAllowed = 1000): asserts topK is number {
  if (typeof topK !== 'number' || !Number.isInteger(topK)) {
    throw new ValidationError(`top_k must be an integer, got ${typeof topK}`)
  }
  if (topK < 1) {
    throw new ValidationError('top_k must be >= 1')
  }
  if (topK > maxAllowed) {
    throw new ValidationError(`top_k ${topK} exceeds max allowed ${maxAllowed}`)
  }
}

export function validateQueryString(query: unknown): asserts query is string {
  if (typeof query !== 'string') {
    throw new ValidationError(`query must be a string, got ${typeof query}`)
  }
  if (query.length > MAX_QUERY_LENGTH) {
    throw new ValidationError(`query exceeds max length of ${MAX_QUERY_LENGTH}`)
  }
}

export function validateHops(hops: unknown): asserts hops is number {
  if (typeof hops !== 'number' || !Number.isInteger(hops)) {
    throw new ValidationError(`hops must be an integer, got ${typeof hops}`)
  }
  if (hops < 1) {
    throw new ValidationError('hops must be >= 1')
  }
  if (hops > 10) {
    throw new ValidationError('hops must not exceed 10 (exponential traversal guard)')
  }
}

export function validateBatchEntities(entities: unknown, maxBatch = 1000): asserts entities is Record<string, unknown>[] {
  if (!Array.isArray(entities)) {
    throw new ValidationError(`entities must be an array, got ${typeof entities}`)
  }
  if (entities.length === 0) {
    throw new ValidationError('entities list must not be empty')
  }
  if (entities.length > maxBatch) {
    throw new ValidationError(`batch size ${entities.length} exceeds max ${maxBatch}`)
  }
  for (let i = 0; i < entities.length; i++) {
    const e = entities[i]
    if (typeof e !== 'object' || e === null) {
      throw new ValidationError(`entities[${i}] must be an object`)
    }
    if (!('id' in e) || typeof (e as Record<string, unknown>).id !== 'string') {
      throw new ValidationError(`entities[${i}] is missing required 'id' field`)
    }
    if (!('content' in e) || typeof (e as Record<string, unknown>).content !== 'string') {
      throw new ValidationError(`entities[${i}] is missing required 'content' field`)
    }
  }
}

export function validateMetadata(metadata: unknown): string {
  if (typeof metadata === 'string') {
    try {
      JSON.parse(metadata)
      return metadata
    } catch {
      throw new ValidationError('metadata is not valid JSON')
    }
  }
  return JSON.stringify(metadata ?? {})
}
