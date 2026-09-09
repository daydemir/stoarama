package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestJoinedFrozenWorkerStartupRetriesTransientStatusThenSucceeds(t *testing.T) {
	t.Parallel()
	cfg := validJoinedWorkerConfig()
	cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	cfg.JoinedRecordingCanaryHourIDs = ""
	const token = "startup-bootstrap-token-kept-secret"
	var statusCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/recording/joined/status" || r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if statusCalls.Add(1) == 1 {
			http.Error(w, "brief deploy", http.StatusServiceUnavailable)
			return
		}
		writeJoinedTestJSON(t, w, joinedWorkerStatus{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
			Enabled: true, BatchID: cfg.JoinedRecordingBatchID, WorkScope: config.JoinedWorkScopeFrozenBatch,
			CanaryHourIDs: []string{}, Hours: map[string]int64{}})
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, token, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{cfg: cfg, api: api, idlePoll: time.Millisecond}
	req := joinedWorkerRequest{BatchID: cfg.JoinedRecordingBatchID, WorkerID: "worker-1", ScratchRoot: root}
	err = checkJoinedWorkerRemoteStartupWithRetry(context.Background(), cfg, service.idlePoll, func(ctx context.Context) error {
		return service.checkJoinedWorkerBackendStartup(ctx, req)
	})
	if err != nil || statusCalls.Load() != 2 {
		t.Fatalf("startup err=%v status_calls=%d", err, statusCalls.Load())
	}
}

func TestJoinedWorkerStartupRetryFailsClosed(t *testing.T) {
	t.Parallel()
	frozen := validJoinedWorkerConfig()
	frozen.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	frozen.JoinedRecordingCanaryHourIDs = ""
	canary := validJoinedWorkerConfig()
	for _, tc := range []struct {
		name string
		cfg  config.Config
		err  error
	}{
		{"auth", frozen, &joinedAPIResponseError{path: "/api/v1/recording/joined/status?batch_id=x", status: http.StatusUnauthorized}},
		{"schema", frozen, errors.New("joined backend status schema differs")},
		{"canary transient", canary, &joinedAPIResponseError{path: "/api/v1/recording/joined/status?batch_id=x", status: http.StatusServiceUnavailable}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			err := checkJoinedWorkerRemoteStartupWithRetry(context.Background(), tc.cfg, time.Millisecond, func(context.Context) error {
				calls.Add(1)
				return tc.err
			})
			if err != tc.err || calls.Load() != 1 {
				t.Fatalf("startup err=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestJoinedFrozenWorkerStartupDoesNotRetryAuthOrSchema(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"auth", http.StatusUnauthorized, ""},
		{"schema", http.StatusOK, `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validJoinedWorkerConfig()
			cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
			cfg.JoinedRecordingCanaryHourIDs = ""
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			api, err := newJoinedAPIClient(server.URL, "startup-bootstrap-token-kept-secret", server.Client())
			if err != nil {
				t.Fatal(err)
			}
			service := &remoteJoinedOperatorService{cfg: cfg, api: api, idlePoll: time.Millisecond}
			req := joinedWorkerRequest{BatchID: cfg.JoinedRecordingBatchID, WorkerID: "worker-1", ScratchRoot: t.TempDir()}
			err = checkJoinedWorkerRemoteStartupWithRetry(context.Background(), cfg, service.idlePoll, func(ctx context.Context) error {
				return service.checkJoinedWorkerBackendStartup(ctx, req)
			})
			if err == nil || calls.Load() != 1 {
				t.Fatalf("startup err=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestJoinedWorkerStartupRetryBudgetAndCancellation(t *testing.T) {
	t.Parallel()
	statusErr := &joinedAPIResponseError{path: "/api/v1/recording/joined/status?batch_id=x", status: http.StatusServiceUnavailable}
	state := joinedAdmissionRetryState{}
	now := time.Now()
	for attempt := 1; attempt <= joinedAdmissionRetryMaxAttempts; attempt++ {
		if _, ok := state.next(statusErr, time.Second, now.Add(time.Duration(attempt)*time.Second)); !ok {
			t.Fatalf("startup retry %d rejected", attempt)
		}
	}
	if delay, ok := state.next(statusErr, time.Second, now); ok || delay != 0 {
		t.Fatalf("startup retry budget exceeded delay=%s ok=%v", delay, ok)
	}

	frozen := validJoinedWorkerConfig()
	frozen.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	frozen.JoinedRecordingCanaryHourIDs = ""
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	err := checkJoinedWorkerRemoteStartupWithRetry(ctx, frozen, time.Hour, func(context.Context) error {
		calls.Add(1)
		cancel()
		return statusErr
	})
	if !errors.Is(err, context.Canceled) || calls.Load() != 1 {
		t.Fatalf("canceled startup err=%v calls=%d", err, calls.Load())
	}
}
