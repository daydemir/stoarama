package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestJoinedExactRetryGrantCLI(t *testing.T) {
	const operator = "retry-grant-operator-token-32-bytes"
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var req joinedrecording.ExactRetryGrantRequest
		if r.Method != http.MethodPut || r.URL.Path != "/api/v1/recording/joined/retry-grant" || r.Header.Get("Authorization") != "Bearer "+operator || json.NewDecoder(r.Body).Decode(&req) != nil || req.Validate() != nil || req.HourID != "exact-hour" || req.ExpectedAttemptCount != 2 {
			http.Error(w, "unexpected request", 400)
			return
		}
		writeJoinedTestJSON(t, w, joinedrecording.ExactRetryGrant{BatchID: req.BatchID, HourID: req.HourID, ExpectedAttemptCount: req.ExpectedAttemptCount, FailureID: 42, Reason: req.Reason, GrantedAt: time.Now()})
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "distinct-bootstrap-token-32-bytes", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	factory := func(context.Context, config.Config) (joinedOperatorService, error) {
		return &remoteJoinedOperatorService{api: api, operatorToken: operator}, nil
	}
	args := []string{"retry-grant", "--batch-id", "batch", "--hour-id", "exact-hour", "--expected-attempt-count", "2", "--reason", "Reviewed preseal failure"}
	if _, err := runRecordingJoinedWith(context.Background(), config.Config{}, args, factory); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("requests=%d", calls)
	}
	for _, bad := range [][]string{{"retry-grant"}, {"retry-grant", "--batch-id", "batch", "--hour-id", "exact-hour", "--reason", "reviewed"}, append(append([]string{}, args...), "extra")} {
		if _, err := runRecordingJoinedWith(context.Background(), config.Config{}, bad, factory); err == nil {
			t.Fatal("accepted incomplete or extra arguments")
		}
	}
	if calls != 1 {
		t.Fatalf("invalid arguments caused requests=%d", calls)
	}
}
