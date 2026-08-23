package api

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestJoinedStreamDayHeadPreflightStepLimitIsBounded(t *testing.T) {
	id := uuid.NewString()
	for _, test := range []struct {
		name  string
		limit int
		valid bool
	}{
		{name: "default", limit: 0, valid: true},
		{name: "one", limit: 1, valid: true},
		{name: "maximum", limit: joinedStreamDayHeadPreflightMaxBatch, valid: true},
		{name: "negative", limit: -1},
		{name: "too large", limit: joinedStreamDayHeadPreflightMaxBatch + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, got, err := (joinedStreamDayHeadPreflightStepRequest{RunID: id, Limit: test.limit}).validate()
			if (err == nil) != test.valid {
				t.Fatalf("limit=%d error=%v valid=%v", test.limit, err, test.valid)
			}
			if test.valid && test.limit == 0 && got != joinedStreamDayHeadPreflightMaxBatch {
				t.Fatalf("default limit=%d want=%d", got, joinedStreamDayHeadPreflightMaxBatch)
			}
		})
	}
	if _, _, err := (joinedStreamDayHeadPreflightStepRequest{RunID: "not-a-uuid", Limit: 1}).validate(); err == nil {
		t.Fatal("malformed run id accepted")
	}
}

func TestJoinedStreamDayHeadPreflightStartUsesExactSealRequestValidation(t *testing.T) {
	valid := joinedStreamDayHeadPreflightStartRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		BatchID: "tier1-2026-08-generation-1", RecordingID: 377, LocalDate: "2026-08-01"}
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*joinedStreamDayHeadPreflightStartRequest){
		"protocol":  func(r *joinedStreamDayHeadPreflightStartRequest) { r.ProtocolVersion++ },
		"batch":     func(r *joinedStreamDayHeadPreflightStartRequest) { r.BatchID = "bad_batch" },
		"recording": func(r *joinedStreamDayHeadPreflightStartRequest) { r.RecordingID = 0 },
		"date":      func(r *joinedStreamDayHeadPreflightStartRequest) { r.LocalDate = "2026-8-1" },
	} {
		t.Run(name, func(t *testing.T) {
			request := valid
			mutate(&request)
			if err := request.validate(); err == nil {
				t.Fatal("invalid start request accepted")
			}
		})
	}
	if !strings.Contains(valid.sealRequest().LocalDate, "-") {
		t.Fatal("start request did not preserve exact date")
	}
}
