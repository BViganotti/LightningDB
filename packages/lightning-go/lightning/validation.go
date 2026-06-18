package lightning

import (
	"encoding/json"
	"fmt"
)

const (
	maxIDLength        = 1024
	maxContentLength   = 10 * 1024 * 1024
	maxEntityTypeLen   = 256
	maxEmbeddingDim    = 16384
	maxQueryLength     = 100_000
	maxMetadataLength  = 1 * 1024 * 1024
)

func validateID(id string, fieldName string) error {
	if id == "" {
		return NewValidationError(fmt.Sprintf("%s must not be empty", fieldName))
	}
	if len(id) > maxIDLength {
		return NewValidationError(fmt.Sprintf("%s exceeds max length of %d", fieldName, maxIDLength))
	}
	return nil
}

func validateContent(content string) error {
	if len(content) > maxContentLength {
		return NewValidationError(fmt.Sprintf("content exceeds max %d bytes", maxContentLength))
	}
	return nil
}

func validateEntityType(entityType string) error {
	if entityType == "" {
		return NewValidationError("entity_type must not be empty")
	}
	if len(entityType) > maxEntityTypeLen {
		return NewValidationError(fmt.Sprintf("entity_type exceeds max length of %d", maxEntityTypeLen))
	}
	return nil
}

func validateEmbedding(embedding []float32) error {
	if embedding == nil {
		return nil
	}
	if len(embedding) == 0 {
		return NewValidationError("embedding must not be empty if provided")
	}
	if len(embedding) > maxEmbeddingDim {
		return NewValidationError(fmt.Sprintf("embedding dimension %d exceeds max %d", len(embedding), maxEmbeddingDim))
	}
	return nil
}

func validateTopK(topK int, maxAllowed int) error {
	if topK < 1 {
		return NewValidationError("top_k must be >= 1")
	}
	if topK > maxAllowed {
		return NewValidationError(fmt.Sprintf("top_k %d exceeds max allowed %d", topK, maxAllowed))
	}
	return nil
}

func validateHops(hops int) error {
	if hops < 1 {
		return NewValidationError("hops must be >= 1")
	}
	if hops > 10 {
		return NewValidationError("hops must not exceed 10 (exponential traversal guard)")
	}
	return nil
}

func validateQueryString(query string) error {
	if len(query) > maxQueryLength {
		return NewValidationError(fmt.Sprintf("query exceeds max length of %d", maxQueryLength))
	}
	return nil
}

func validateMetadata(metadata interface{}) (string, error) {
	if metadata == nil {
		return "{}", nil
	}
	switch m := metadata.(type) {
	case string:
		if len(m) > maxMetadataLength {
			return "", NewValidationError(fmt.Sprintf("metadata exceeds max length of %d", maxMetadataLength))
		}
		var dummy interface{}
		if err := json.Unmarshal([]byte(m), &dummy); err != nil {
			return "", NewValidationError("metadata is not valid JSON: " + err.Error())
		}
		return m, nil
	default:
		b, err := json.Marshal(metadata)
		if err != nil {
			return "", NewValidationError("metadata serialization failed: " + err.Error())
		}
		if len(b) > maxMetadataLength {
			return "", NewValidationError(fmt.Sprintf("metadata exceeds max length of %d", maxMetadataLength))
		}
		return string(b), nil
	}
}

func validateStoreParams(id, content, entityType string, metadata interface{}, embedding []float32) error {
	if err := validateID(id, "id"); err != nil {
		return err
	}
	if err := validateContent(content); err != nil {
		return err
	}
	if err := validateEntityType(entityType); err != nil {
		return err
	}
	if _, err := validateMetadata(metadata); err != nil {
		return err
	}
	if err := validateEmbedding(embedding); err != nil {
		return err
	}
	return nil
}
