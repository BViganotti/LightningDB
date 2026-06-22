use crate::error::Error;

const MAX_ID_LENGTH: usize = 512;
const MAX_CONTENT_LENGTH: usize = 1_000_000;
const MAX_METADATA_LENGTH: usize = 1_000_000;
const MAX_ENTITY_TYPE_LENGTH: usize = 128;
const MAX_QUERY_LENGTH: usize = 100_000;
const MAX_HOPS: usize = 10;
const MAX_EMBEDDING_DIM: usize = 8192;

pub fn validate_id(id: &str, label: &str) -> Result<(), Error> {
    if id.is_empty() {
        return Err(Error::Validation(format!("{} must not be empty", label)));
    }
    if id.len() > MAX_ID_LENGTH {
        return Err(Error::Validation(format!(
            "{} length {} exceeds max {}",
            label,
            id.len(),
            MAX_ID_LENGTH
        )));
    }
    Ok(())
}

pub fn validate_content(content: &str) -> Result<(), Error> {
    if content.is_empty() {
        return Err(Error::Validation("content must not be empty".into()));
    }
    if content.len() > MAX_CONTENT_LENGTH {
        return Err(Error::Validation(format!(
            "content length {} exceeds max {}",
            content.len(),
            MAX_CONTENT_LENGTH
        )));
    }
    Ok(())
}

pub fn validate_metadata(metadata: &str) -> Result<String, Error> {
    if metadata.is_empty() {
        return Ok("{}".to_string());
    }
    if metadata.len() > MAX_METADATA_LENGTH {
        return Err(Error::Validation(format!(
            "metadata length {} exceeds max {}",
            metadata.len(),
            MAX_METADATA_LENGTH
        )));
    }
    let v: serde_json::Value =
        serde_json::from_str(metadata).map_err(|e| Error::Validation(format!("invalid metadata JSON: {}", e)))?;
    if !v.is_object() {
        return Err(Error::Validation("metadata must be a JSON object".into()));
    }
    Ok(metadata.to_string())
}

pub fn validate_entity_type(entity_type: &str) -> Result<(), Error> {
    if entity_type.is_empty() {
        return Err(Error::Validation("entityType must not be empty".into()));
    }
    if entity_type.len() > MAX_ENTITY_TYPE_LENGTH {
        return Err(Error::Validation(format!(
            "entityType length {} exceeds max {}",
            entity_type.len(),
            MAX_ENTITY_TYPE_LENGTH
        )));
    }
    Ok(())
}

pub fn validate_top_k(top_k: usize, max_top_k: usize) -> Result<(), Error> {
    if top_k == 0 {
        return Err(Error::Validation("topK must be > 0".into()));
    }
    if top_k > max_top_k {
        return Err(Error::Validation(format!(
            "topK {} exceeds max {}",
            top_k, max_top_k
        )));
    }
    Ok(())
}

pub fn validate_batch_size(size: usize, max_batch: usize) -> Result<(), Error> {
    if size == 0 {
        return Err(Error::Validation("batch must not be empty".into()));
    }
    if size > max_batch {
        return Err(Error::Validation(format!(
            "batch size {} exceeds max {}",
            size, max_batch
        )));
    }
    Ok(())
}

pub fn validate_embedding(embedding: &[f32]) -> Result<(), Error> {
    if embedding.is_empty() {
        return Err(Error::Validation("embedding must not be empty".into()));
    }
    if embedding.len() > MAX_EMBEDDING_DIM {
        return Err(Error::Validation(format!(
            "embedding dimension {} exceeds max {}",
            embedding.len(),
            MAX_EMBEDDING_DIM
        )));
    }
    Ok(())
}

pub fn validate_query_string(query: &str) -> Result<(), Error> {
    if query.is_empty() {
        return Err(Error::Validation("query must not be empty".into()));
    }
    if query.len() > MAX_QUERY_LENGTH {
        return Err(Error::Validation(format!(
            "query length {} exceeds max {}",
            query.len(),
            MAX_QUERY_LENGTH
        )));
    }
    Ok(())
}

pub fn validate_hops(hops: usize) -> Result<(), Error> {
    if hops == 0 {
        return Err(Error::Validation("hops must be > 0".into()));
    }
    if hops > MAX_HOPS {
        return Err(Error::Validation(format!(
            "hops {} exceeds max {}",
            hops, MAX_HOPS
        )));
    }
    Ok(())
}
