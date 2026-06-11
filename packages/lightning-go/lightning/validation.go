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
)

func validateID(id string, fieldName string) error {
	if id == "" {
		return ErrValidation(fmt.Sprintf("%s must not be empty", fieldName))
	}
	if len(id) > maxIDLength {
		return ErrValidation(fmt.Sprintf("%s exceeds max length of %d", fieldName, maxIDLength))
	}
	return nil
}

func validateContent(content string) error {
	if len(content) > maxContentLength {
		return ErrValidation(fmt.Sprintf("content exceeds max %d bytes", maxContentLength))
	}
	return nil
}

func validateEntityType(entityType string) error {
	if entityType == "" {
		return ErrValidation("entity_type must not be empty")
	}
	if len(entityType) > maxEntityTypeLen {
		return ErrValidation(fmt.Sprintf("entity_type exceeds max length of %d", maxEntityTypeLen))
	}
	return nil
}

func validateEmbedding(embedding []float32) error {
	if embedding == nil {
		return nil
	}
	if len(embedding) == 0 {
		return ErrValidation("embedding must not be empty if provided")
	}
	if len(embedding) > maxEmbeddingDim {
		return ErrValidation(fmt.Sprintf("embedding dimension %d exceeds max %d", len(embedding), maxEmbeddingDim))
	}
	return nil
}

func validateTopK(topK int, maxAllowed int) error {
	if topK < 1 {
		return ErrValidation("top_k must be >= 1")
	}
	if topK > maxAllowed {
		return ErrValidation(fmt.Sprintf("top_k %d exceeds max allowed %d", topK, maxAllowed))
	}
	return nil
}

func validateHops(hops int) error {
	if hops < 1 {
		return ErrValidation("hops must be >= 1")
	}
	if hops > 10 {
		return ErrValidation("hops must not exceed 10 (exponential traversal guard)")
	}
	return nil
}

func validateQueryString(query string) error {
	if len(query) > maxQueryLength {
		return ErrValidation(fmt.Sprintf("query exceeds max length of %d", maxQueryLength))
	}
	return nil
}

func validateMetadata(metadata interface{}) (string, error) {
	if metadata == nil {
		return "{}", nil
	}
	switch m := metadata.(type) {
	case string:
		var dummy interface{}
		if err := json.Unmarshal([]byte(m), &dummy); err != nil {
			return "", ErrValidation("metadata is not valid JSON: " + err.Error())
		}
		return m, nil
	default:
		b, err := json.Marshal(metadata)
		if err != nil {
			return "", ErrValidation("metadata serialization failed: " + err.Error())
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
	if err := validateEmbedding(embedding); err != nil {
		return err
	}
	return nil
}
