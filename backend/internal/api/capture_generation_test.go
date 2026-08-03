package api

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestCaptureGenerationFingerprintDoesNotExposeLeaseToken(t *testing.T) {
	token := uuid.MustParse("11111111-2222-4333-8444-555555555555")
	got := captureGenerationFingerprint(&token)
	want := "sha256:cf4c4732fd3b8f8a55b60871950a2f22c893ea7afd75d2146826534e3f67cc49"
	if got == nil || *got != want {
		t.Fatalf("fingerprint=%v", got)
	}
	if strings.Contains(*got, token.String()) {
		t.Fatal("fingerprint exposed raw lease token")
	}
	if captureGenerationFingerprint(nil) != nil {
		t.Fatal("nil legacy generation did not remain nil")
	}
}
