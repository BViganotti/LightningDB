from __future__ import annotations

import json
from typing import Any, Optional


class ValidationError(ValueError):
    pass


MAX_ID_LENGTH = 1024
MAX_CONTENT_LENGTH = 10 * 1024 * 1024  # 10MB
MAX_ENTITY_TYPE_LENGTH = 256
MAX_METADATA_SIZE = 1024 * 1024  # 1MB
MAX_EMBEDDING_DIM = 16384
MAX_QUERY_LENGTH = 100_000


def validate_id(id: str, field_name: str = "id") -> None:
    if not isinstance(id, str):
        raise ValidationError(f"{field_name} must be a string, got {type(id).__name__}")
    if not id:
        raise ValidationError(f"{field_name} must not be empty")
    if len(id) > MAX_ID_LENGTH:
        raise ValidationError(f"{field_name} exceeds max length of {MAX_ID_LENGTH}")


def validate_content(content: str, field_name: str = "content") -> None:
    if not isinstance(content, str):
        raise ValidationError(f"{field_name} must be a string, got {type(content).__name__}")
    if len(content) > MAX_CONTENT_LENGTH:
        raise ValidationError(f"{field_name} exceeds max {MAX_CONTENT_LENGTH} bytes")


def validate_entity_type(entity_type: str) -> None:
    if not isinstance(entity_type, str):
        raise ValidationError(f"entity_type must be a string, got {type(entity_type).__name__}")
    if not entity_type:
        raise ValidationError("entity_type must not be empty")
    if len(entity_type) > MAX_ENTITY_TYPE_LENGTH:
        raise ValidationError(f"entity_type exceeds max length of {MAX_ENTITY_TYPE_LENGTH}")


def validate_metadata(metadata: Any) -> str:
    if isinstance(metadata, str):
        if len(metadata) > MAX_METADATA_SIZE:
            raise ValidationError(f"metadata exceeds max {MAX_METADATA_SIZE} bytes")
        try:
            json.loads(metadata)
            return metadata
        except json.JSONDecodeError as e:
            raise ValidationError(f"metadata is not valid JSON: {e}")
    serialized = json.dumps(metadata, default=str)
    if len(serialized) > MAX_METADATA_SIZE:
        raise ValidationError(f"metadata exceeds max {MAX_METADATA_SIZE} bytes after serialization")
    return serialized


def validate_embedding(embedding: Optional[list[float]]) -> None:
    if embedding is None:
        return
    if not isinstance(embedding, list):
        raise ValidationError(f"embedding must be a list of floats, got {type(embedding).__name__}")
    if not embedding:
        raise ValidationError("embedding must not be empty if provided")
    if len(embedding) > MAX_EMBEDDING_DIM:
        raise ValidationError(f"embedding dimension {len(embedding)} exceeds max {MAX_EMBEDDING_DIM}")
    for i, v in enumerate(embedding):
        if not isinstance(v, (int, float)):
            raise ValidationError(f"embedding[{i}] must be numeric, got {type(v).__name__}")


def validate_top_k(top_k: int, max_allowed: int = 1000) -> None:
    if not isinstance(top_k, int):
        raise ValidationError(f"top_k must be an integer, got {type(top_k).__name__}")
    if top_k < 1:
        raise ValidationError("top_k must be >= 1")
    if top_k > max_allowed:
        raise ValidationError(f"top_k {top_k} exceeds max allowed {max_allowed}")


def validate_query_string(query: str) -> None:
    if not isinstance(query, str):
        raise ValidationError(f"query must be a string, got {type(query).__name__}")
    if len(query) > MAX_QUERY_LENGTH:
        raise ValidationError(f"query exceeds max length of {MAX_QUERY_LENGTH}")


def validate_batch_entities(entities: list[dict], max_batch: int = 1000) -> None:
    if not isinstance(entities, list):
        raise ValidationError(f"entities must be a list, got {type(entities).__name__}")
    if not entities:
        raise ValidationError("entities list must not be empty")
    if len(entities) > max_batch:
        raise ValidationError(f"batch size {len(entities)} exceeds max {max_batch}")
    for i, e in enumerate(entities):
        if not isinstance(e, dict):
            raise ValidationError(f"entities[{i}] must be a dict, got {type(e).__name__}")
        if "id" not in e:
            raise ValidationError(f"entities[{i}] is missing required 'id' field")
        if "content" not in e:
            raise ValidationError(f"entities[{i}] is missing required 'content' field")
        validate_id(e["id"], f"entities[{i}].id")
        validate_content(e["content"], f"entities[{i}].content")


def validate_hops(hops: int) -> None:
    if not isinstance(hops, int):
        raise ValidationError(f"hops must be an integer, got {type(hops).__name__}")
    if hops < 1:
        raise ValidationError("hops must be >= 1")
    if hops > 10:
        raise ValidationError("hops must not exceed 10 (exponential traversal guard)")


def validate_store_params(
    id: str,
    content: str,
    entity_type: str,
    metadata: Any,
    embedding: Optional[list[float]],
) -> None:
    validate_id(id)
    validate_content(content)
    validate_entity_type(entity_type)
    validate_metadata(metadata)
    validate_embedding(embedding)
