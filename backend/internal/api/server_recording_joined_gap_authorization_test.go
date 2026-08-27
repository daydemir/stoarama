package api

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

func TestJoinedGapOnlyFrozenAuthorizationRequestValidation(t *testing.T) {
	valid := joinedGapOnlyFrozenAuthorizationRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		BatchID: "tier1-generation-1", ArtifactID: 90123, IncidentID: "operator-review-2026-08-27",
		HourID:    "tier1-generation-1__recording-1__date-2026-08-01__hour-01__generation-1",
		ObjectKey: "joined/tier1-generation-1/coverage/hours/hour.json", ExpectedSizeBytes: 100,
		ExpectedSHA256: strings.Repeat("a", 64), ReviewEvidenceSHA256: strings.Repeat("b", 64), Apply: true}
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*joinedGapOnlyFrozenAuthorizationRequest){
		"protocol": func(r *joinedGapOnlyFrozenAuthorizationRequest) { r.ProtocolVersion++ },
		"batch":    func(r *joinedGapOnlyFrozenAuthorizationRequest) { r.BatchID = " bad" },
		"artifact": func(r *joinedGapOnlyFrozenAuthorizationRequest) { r.ArtifactID = 0 },
		"incident": func(r *joinedGapOnlyFrozenAuthorizationRequest) { r.IncidentID = "Bad incident" },
		"hour":     func(r *joinedGapOnlyFrozenAuthorizationRequest) { r.HourID += " " },
		"object":   func(r *joinedGapOnlyFrozenAuthorizationRequest) { r.ObjectKey = "" },
		"size":     func(r *joinedGapOnlyFrozenAuthorizationRequest) { r.ExpectedSizeBytes = 0 },
		"sha":      func(r *joinedGapOnlyFrozenAuthorizationRequest) { r.ExpectedSHA256 = strings.Repeat("A", 64) },
		"review":   func(r *joinedGapOnlyFrozenAuthorizationRequest) { r.ReviewEvidenceSHA256 = strings.Repeat("b", 63) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if err := candidate.validate(); err == nil {
				t.Fatal("invalid authorization request accepted")
			}
		})
	}
}

func TestVerifyJoinedGapAuthorizationStorage(t *testing.T) {
	body := []byte(`{"status":"gap_only"}`)
	sha := "1a5f3ed7f0b7cc8d9f7f28f50f94a2e33ec8c5d45441b0ec8868889b3dc08225"
	store := joinedOutputStoreStub{body: body}
	if digest, err := verifyJoinedGapAuthorizationStorage(context.Background(), store, 90123, "published", "joined/hour.json",
		int64(len(body)), sha, "etag", "version", body); err != nil || !lowerHex64(digest) {
		t.Fatalf("exact published verification digest=%q err=%v", digest, err)
	}
	store.body = append([]byte(nil), body...)
	store.body[len(store.body)-2] = 'X'
	if _, err := verifyJoinedGapAuthorizationStorage(context.Background(), store, 90123, "published", "joined/hour.json",
		int64(len(body)), sha, "etag", "version", body); err == nil {
		t.Fatal("changed published object passed verification")
	}
	absent := joinedOutputStoreStub{head: r2.ObjectHead{}, openErr: errors.New("must not open")}
	absent.headErr = &smithy.GenericAPIError{Code: "NoSuchKey", Message: "missing"}
	if digest, err := verifyJoinedGapAuthorizationStorage(context.Background(), absent, 90124, "sealed", "joined/hour.json",
		int64(len(body)), sha, "", "", body); err != nil || !lowerHex64(digest) {
		t.Fatalf("sealed absence digest=%q err=%v", digest, err)
	}
}
