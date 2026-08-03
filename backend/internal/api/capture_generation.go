package api

import (
	"crypto/sha256"
	"fmt"

	"github.com/google/uuid"
)

// captureGenerationFingerprint identifies one capture generation without
// exposing its lease token, which is an active worker bearer credential.
func captureGenerationFingerprint(token *uuid.UUID) *string {
	if token == nil {
		return nil
	}
	sum := sha256.Sum256([]byte(token.String()))
	value := fmt.Sprintf("sha256:%x", sum[:])
	return &value
}
