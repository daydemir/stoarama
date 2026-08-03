package api

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestCaptureGenerationFingerprintDoesNotExposeLeaseToken(t *testing.T) {
	token := uuid.MustParse("11111111-2222-4333-8444-555555555555")
	got := captureGenerationFingerprint(&token)
	if got == nil || !strings.HasPrefix(*got, "sha256:") || len(*got) != len("sha256:")+64 {
		t.Fatalf("fingerprint=%v", got)
	}
	if strings.Contains(*got, token.String()) {
		t.Fatal("fingerprint exposed raw lease token")
	}
	if captureGenerationFingerprint(nil) != nil {
		t.Fatal("nil legacy generation did not remain nil")
	}
}
