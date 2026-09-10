package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestJoinedPreflightSourceCapabilityRetries520(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	request := joinedrecording.SourceCapabilityRequest{ProtocolVersion: 1, HourID: "hour-test", ClipID: 7, Operation: "get"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got joinedrecording.SourceCapabilityRequest
		if r.URL.Path != joinedSourceCapabilityPath || r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer operation-token" || json.NewDecoder(r.Body).Decode(&got) != nil || got != request {
			t.Error("retry changed immutable source request or authorization")
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(520)
			return
		}
		writeJoinedTestJSON(t, w, joinedrecording.SourceReadCapability{ProtocolVersion: 1, ObjectKey: "raw/immutable-source"})
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "bootstrap-token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	cfg := validJoinedWorkerConfig()
	cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	cfg.JoinedRecordingCanaryHourIDs = ""
	service := &remoteJoinedOperatorService{cfg: cfg, api: api}
	got, err := service.preflightSourceCapability(context.Background(), "operation-token", request)
	if err != nil || got.ObjectKey != "raw/immutable-source" || calls.Load() != 2 {
		t.Fatalf("response=%+v err=%v calls=%d", got, err, calls.Load())
	}
}

func TestJoinedPreflightSourceCapabilityStopsAndBounds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                   string
		status                 int
		body                   string
		canary, direct, cancel bool
		wantCalls              int32
	}{
		{name: "auth", status: 401, wantCalls: 1},
		{name: "forbidden", status: 403, wantCalls: 1},
		{name: "identity conflict", status: 409, wantCalls: 1},
		{name: "schema", status: 200, body: "{invalid", wantCalls: 1},
		{name: "canary", status: 520, canary: true, wantCalls: 1},
		{name: "postseal unchanged", status: 520, direct: true, wantCalls: 1},
		{name: "exhausted", status: 520, wantCalls: joinedSourceCapabilityAttempts},
		{name: "canceled wait", status: 520, cancel: true, wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			api, err := newJoinedAPIClient(server.URL, "bootstrap-token", server.Client())
			if err != nil {
				t.Fatal(err)
			}
			cfg := validJoinedWorkerConfig()
			if !tc.canary {
				cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
				cfg.JoinedRecordingCanaryHourIDs = ""
			}
			service := &remoteJoinedOperatorService{cfg: cfg, api: api}
			ctx := context.Background()
			if tc.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			}
			request := joinedrecording.SourceCapabilityRequest{ProtocolVersion: 1, HourID: "hour-test", ClipID: 7, Operation: "get"}
			if tc.direct {
				_, err = api.sourceCapability(ctx, "operation-token", request)
			} else {
				_, err = service.preflightSourceCapability(ctx, "operation-token", request)
			}
			if err == nil || calls.Load() != tc.wantCalls {
				t.Fatalf("err=%v calls=%d want=%d", err, calls.Load(), tc.wantCalls)
			}
			if tc.cancel && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost parent cancellation: %v", err)
			}
		})
	}
}

func TestJoinedSourceCapabilityRetryClassification(t *testing.T) {
	t.Parallel()
	transient := &joinedAPIResponseError{path: joinedSourceCapabilityPath, status: 520}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"520", transient, true},
		{"429", &joinedAPIResponseError{path: joinedSourceCapabilityPath, status: 429}, true},
		{"transport", &joinedAPITransportError{path: joinedSourceCapabilityPath, cause: io.EOF}, true},
		{"mixed transport", &joinedAPITransportError{path: joinedSourceCapabilityPath, cause: errors.Join(io.EOF, errors.New("unknown"))}, false},
		{"auth", &joinedAPIResponseError{path: joinedSourceCapabilityPath, status: 401}, false},
		{"seal", &joinedAPIResponseError{path: "/api/v1/recording/joined/hour/seal", status: 520}, false},
		{"foreign route", &joinedAPIResponseError{path: joinedSourceCapabilityPath + "?foreign=1", status: 520}, false},
		{"mixed", errors.Join(transient, errors.New("identity mismatch")), false},
		{"canceled", context.Canceled, false},
		{"unknown", errors.New("unknown"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := joinedSourceCapabilityRetryable(tc.err); got != tc.want {
				t.Fatalf("retryable=%v want=%v", got, tc.want)
			}
		})
	}
}
