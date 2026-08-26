package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestJoinedAPIClientRejectsMalformedBaseURL(t *testing.T) {
	if _, err := newJoinedAPIClient("https://%", "", nil); err == nil {
		t.Fatal("malformed APP_BASE_URL was accepted")
	}
}

func TestJoinedClaimAdmissionOperatorContract(t *testing.T) {
	const (
		batch = "tier1-generation-1"
		op    = "joined-tier1-operator-token-at-least-32-bytes"
	)
	updated := time.Now().UTC().Truncate(time.Second)
	requests := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+op {
			t.Fatalf("operator authorization differs")
		}
		requests <- r.Method + " " + r.URL.RequestURI()
		paused := false
		if r.Method == http.MethodPut {
			var req joinedrecording.ClaimAdmissionRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.BatchID != batch || !req.ClaimsPaused {
				t.Fatalf("bad admission request: %+v err=%v", req, err)
			}
			paused = true
		}
		_ = json.NewEncoder(w).Encode(joinedrecording.ClaimAdmissionStatus{ProtocolVersion: 1, BatchID: batch,
			ClaimsPaused: paused, ActiveHourLeases: 1, ActiveLeaseCount: 1, UpdatedAt: updated})
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "joined-worker-bootstrap-token-at-least-32-bytes", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{api: api, operatorToken: op}
	if _, err := service.ClaimAdmissionStatus(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SetClaimAdmission(context.Background(), joinedrecording.ClaimAdmissionRequest{
		ProtocolVersion: 1, BatchID: batch, ClaimsPaused: true,
	}); err != nil {
		t.Fatal(err)
	}
	if got := <-requests; got != "GET /api/v1/recording/joined/admission?batch_id="+batch {
		t.Fatal(got)
	}
	if got := <-requests; got != "PUT /api/v1/recording/joined/admission" {
		t.Fatal(got)
	}
}

func TestJoinedWorkerClaimsPublicationBeforePreflight(t *testing.T) {
	t.Parallel()
	const (
		batchID   = "tier1-2026-08"
		workerID  = "worker-1"
		bootstrap = "bootstrap-token-kept-secret"
		claim     = "claim-token-kept-secret-value"
	)
	paths := make(chan string, 5)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		if got := r.Header.Get("Authorization"); got != "Bearer "+map[string]string{
			"/api/v1/recording/joined/token":             bootstrap,
			"/api/v1/recording/joined/publication/claim": claim,
			"/api/v1/recording/joined/claim":             claim,
		}[r.URL.Path] {
			t.Errorf("authorization on %s = %q", r.URL.Path, got)
		}
		switch r.URL.Path {
		case "/api/v1/recording/joined/token":
			var request joinedrecording.WorkerBootstrapRequest
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.BatchID != batchID || request.Validate() != nil {
				t.Errorf("unexpected bootstrap request: %+v", request)
			}
			writeJoinedTestJSON(t, w, joinedrecording.WorkerBootstrapResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
				BatchID: batchID, ClaimToken: claim, ExpiresAt: time.Now().Add(time.Hour), WorkScopeIdentity: request.WorkScopeIdentity})
		case "/api/v1/recording/joined/publication/claim", "/api/v1/recording/joined/claim":
			var request joinedrecording.WorkClaimRequest
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.BatchID != batchID || request.WorkerID != workerID {
				t.Errorf("unexpected claim request: %+v", request)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	api, err := newJoinedAPIClient(server.URL, bootstrap, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{cfg: validJoinedWorkerConfig(), api: api}
	worked, err := service.runWorkerOnce(context.Background(), joinedWorkerRequest{BatchID: batchID, WorkerID: workerID, ScratchRoot: t.TempDir()})
	if err != nil || worked {
		t.Fatalf("run once: worked=%v err=%v", worked, err)
	}
	for _, want := range []string{"/api/v1/recording/joined/token", "/api/v1/recording/joined/publication/claim", "/api/v1/recording/joined/claim"} {
		select {
		case got := <-paths:
			if got != want {
				t.Fatalf("claim order: got %q want %q", got, want)
			}
		default:
			t.Fatalf("missing request %q", want)
		}
	}
}

func TestJoinedFrozenBatchClaimReportsSafeScratchBudget(t *testing.T) {
	t.Parallel()
	cfg := validJoinedWorkerConfig()
	cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	cfg.JoinedRecordingCanaryHourIDs = ""
	root := filepath.Join(t.TempDir(), "scratch")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	const claimToken = "claim-token-kept-secret-value"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/recording/joined/token":
			var request joinedrecording.WorkerBootstrapRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			writeJoinedTestJSON(t, w, joinedrecording.WorkerBootstrapResponse{ProtocolVersion: 1,
				BatchID: cfg.JoinedRecordingBatchID, ClaimToken: claimToken, ExpiresAt: time.Now().Add(time.Hour), WorkScopeIdentity: request.WorkScopeIdentity})
		case "/api/v1/recording/joined/publication/claim", "/api/v1/recording/joined/claim":
			var request joinedrecording.WorkClaimRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Validate() != nil ||
				request.ScratchAvailableBytes <= 0 || request.TaskBudgetBytes != request.ScratchAvailableBytes {
				t.Errorf("unsafe broad claim request: %+v err=%v", request, err)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "bootstrap-token-kept-secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{cfg: cfg, api: api}
	worked, err := service.runWorkerOnce(context.Background(), joinedWorkerRequest{
		BatchID: cfg.JoinedRecordingBatchID, WorkerID: "worker-1", ScratchRoot: root})
	if err != nil || worked {
		t.Fatalf("broad claim worked=%v err=%v", worked, err)
	}
}

func TestJoinedFailureReportingUsesStableClassAndFencedEndpoint(t *testing.T) {
	t.Parallel()
	if class, reason := joinedFailureClassification(syscall.ENOSPC); class != "resource" || reason != "scratch_resource_exhausted" {
		t.Fatalf("disk failure class=%q reason=%q", class, reason)
	}
	if class, reason := joinedFailureClassification(context.DeadlineExceeded); class != "transient" || reason != "worker_task_deadline" {
		t.Fatalf("deadline class=%q reason=%q", class, reason)
	}
	const token = "operation-token-kept-secret-value"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/recording/joined/failure" || r.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("failure request path=%q authorization=%q", r.URL.Path, r.Header.Get("Authorization"))
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		var request joinedrecording.WorkFailureRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Validate() != nil || request.ReasonCode != "worker_task_failed" {
			t.Errorf("failure request=%+v", request)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		next := time.Now().UTC().Add(time.Minute)
		writeJoinedTestJSON(t, w, joinedrecording.WorkFailureResponse{ProtocolVersion: 1, State: "retry", AttemptCount: 1, NextAttemptAt: &next})
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "bootstrap-token-kept-secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{api: api}
	err = service.reportJoinedTaskFailure(context.Background(), token, "hour", "hour-1", errors.New("decoder failed"))
	if err != nil {
		t.Fatalf("recorded task failure stopped worker: %v", err)
	}
}

func TestJoinedOperationTrackerRejectsForeignOrRegressedHeartbeat(t *testing.T) {
	now := time.Now().UTC()
	tracker := newJoinedOperationTracker(strings.Repeat("a", 43), "valid-current-token-value", now.Add(5*time.Minute))
	tracker.accept(joinedrecording.OperationCredentials{LeaseID: strings.Repeat("b", 43), OperationToken: "foreign-token-value", ExpiresAt: now.Add(10 * time.Minute)})
	if got := tracker.get(); got != "valid-current-token-value" {
		t.Fatalf("foreign lease poisoned failure fence: %q", got)
	}
	tracker.accept(joinedrecording.OperationCredentials{LeaseID: strings.Repeat("a", 43), OperationToken: "regressed-token-value", ExpiresAt: now.Add(time.Minute)})
	if got := tracker.get(); got != "valid-current-token-value" {
		t.Fatalf("regressed expiry poisoned failure fence: %q", got)
	}
	tracker.accept(joinedrecording.OperationCredentials{LeaseID: strings.Repeat("a", 43), OperationToken: "renewed-token-value", ExpiresAt: now.Add(10 * time.Minute)})
	if got := tracker.get(); got != "renewed-token-value" {
		t.Fatalf("valid renewal was not tracked: %q", got)
	}
}

func TestJoinedWorkerIdleCancellationStopsCleanly(t *testing.T) {
	t.Parallel()
	requested := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/recording/joined/token" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		requested <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "bootstrap-token-kept-secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{cfg: validJoinedWorkerConfig(), api: api, idlePoll: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- service.RunWorker(ctx, joinedWorkerRequest{BatchID: "tier1-2026-08", WorkerID: "worker-1"})
	}()
	select {
	case <-requested:
	case <-time.After(time.Second):
		t.Fatal("worker did not poll")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("cancelled idle worker: %v", err)
	}
}

func TestJoinedWorkerDrainStopsClaimsAndLetsAdmittedTaskFinish(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	var calls atomic.Int32
	go func() {
		done <- runJoinedWorkerLoop(ctx, time.Hour, func(admissionCtx, taskCtx context.Context) (bool, error) {
			if calls.Add(1) != 1 {
				return false, errors.New("draining worker admitted another claim")
			}
			if admissionCtx != ctx {
				return false, errors.New("claim admission did not use the signal context")
			}
			close(started)
			select {
			case <-taskCtx.Done():
				return false, errors.New("admitted task was canceled during drain")
			case <-release:
				return true, nil
			}
		})
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		t.Fatalf("worker stopped before admitted task completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("drained worker: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("claim calls=%d want 1", got)
	}
}

func TestJoinedWorkerDrainPreservesAdmittedTaskFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	want := errors.New("admitted task failed")
	err := runJoinedWorkerLoop(ctx, time.Hour, func(context.Context, context.Context) (bool, error) {
		cancel()
		return true, want
	})
	if !errors.Is(err, want) {
		t.Fatalf("drain hid admitted task failure: %v", err)
	}
}

func TestJoinedWorkerScratchCleanupUsesBootstrapScopedLeaseProof(t *testing.T) {
	t.Parallel()
	cfg := validJoinedWorkerConfig()
	inactive := strings.Repeat("I", 43)
	active := strings.Repeat("A", 43)
	root := filepath.Join(t.TempDir(), "scratch")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, leaseID := range []string{inactive, active} {
		if err := os.Mkdir(filepath.Join(root, leaseID), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	const bootstrapToken = "scratch-proof-bootstrap-token-kept-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/recording/joined/leases/status":
			if r.Header.Get("Authorization") != "Bearer "+bootstrapToken {
				t.Errorf("lease proof auth=%q", r.Header.Get("Authorization"))
			}
			var request joinedrecording.LeaseStatusRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Validate() != nil || !slices.Equal(request.LeaseIDs, []string{active, inactive}) {
				t.Errorf("invalid lease proof request: %+v err=%v", request, err)
			}
			expires := time.Now().Add(time.Hour)
			writeJoinedTestJSON(t, w, joinedrecording.LeaseStatusResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
				Leases: []joinedrecording.LeaseStatus{{LeaseID: active, Active: true, ExpiresAt: &expires}, {LeaseID: inactive}}})
		default:
			t.Errorf("unexpected scratch cleanup path %q", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, bootstrapToken, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{cfg: cfg, api: api}
	removed, err := service.cleanupInactiveScratch(context.Background(), joinedWorkerRequest{
		BatchID: cfg.JoinedRecordingBatchID, WorkerID: "worker-1", ScratchRoot: root})
	if err != nil || !slices.Equal(removed, []string{inactive}) {
		t.Fatalf("scratch cleanup removed=%v err=%v", removed, err)
	}
	if _, err := os.Stat(filepath.Join(root, active)); err != nil {
		t.Fatalf("active scratch was removed: %v", err)
	}
}

func TestJoinedAPIRejectsRedirectAndDoesNotExposeResponseBody(t *testing.T) {
	t.Parallel()
	const secret = "response-body-secret-must-not-leak"
	for _, tc := range []struct {
		name   string
		status int
	}{
		{name: "redirect", status: http.StatusTemporaryRedirect},
		{name: "failure", status: http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "/redirect-target")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(secret))
			}))
			defer server.Close()
			api, err := newJoinedAPIClient(server.URL, "bootstrap-token-kept-secret", server.Client())
			if err != nil {
				t.Fatal(err)
			}
			scope, scopeErr := joinedrecording.NewWorkScopeIdentity("tier1-2026-08", joinedrecording.WorkScopeFrozenBatch, nil)
			if scopeErr != nil {
				t.Fatal(scopeErr)
			}
			_, _, err = api.bootstrap(context.Background(), joinedrecording.WorkerBootstrapRequest{
				ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: "tier1-2026-08", WorkScopeIdentity: scope})
			if err == nil || strings.Contains(err.Error(), secret) {
				t.Fatalf("unsafe API error: %v", err)
			}
		})
	}
}

func TestJoinedWorkerRejectsBootstrapScopeDriftBeforeClaim(t *testing.T) {
	t.Parallel()
	cfg := validJoinedWorkerConfig()
	var claimCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/recording/joined/token" {
			claimCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var request joinedrecording.WorkerBootstrapRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Validate() != nil {
			t.Errorf("invalid bootstrap request: %+v err=%v", request, err)
		}
		drifted, err := joinedrecording.NewWorkScopeIdentity(cfg.JoinedRecordingBatchID, joinedrecording.WorkScopeFrozenBatch, nil)
		if err != nil {
			t.Fatal(err)
		}
		writeJoinedTestJSON(t, w, joinedrecording.WorkerBootstrapResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
			BatchID: cfg.JoinedRecordingBatchID, ClaimToken: "claim-token-kept-secret-value", ExpiresAt: time.Now().Add(time.Hour),
			WorkScopeIdentity: drifted})
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "bootstrap-token-kept-secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{cfg: cfg, api: api}
	worked, err := service.runWorkerOnce(context.Background(), joinedWorkerRequest{BatchID: cfg.JoinedRecordingBatchID,
		WorkerID: "worker-1", ScratchRoot: t.TempDir()})
	if err == nil || worked || claimCalls.Load() != 0 {
		t.Fatalf("scope drift crossed bootstrap fence: worked=%v claim_calls=%d err=%v", worked, claimCalls.Load(), err)
	}
}

func TestJoinedOperatorUsesOnlyDedicatedAdminToken(t *testing.T) {
	t.Parallel()
	const (
		operatorToken = "joined-tier1-operator-token-at-least-32-bytes"
		batchID       = "tier1-2026-08-generation-1"
	)
	paths := make(chan string, 5)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+operatorToken {
			t.Errorf("operator request method=%s auth=%q", r.Method, r.Header.Get("Authorization"))
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["protocol_version"] != float64(1) ||
			(strings.HasSuffix(r.URL.Path, "/freeze-tier1") || strings.HasSuffix(r.URL.Path, "/stream-days/seal") || strings.HasSuffix(r.URL.Path, "/batches/final-freeze") || strings.HasSuffix(r.URL.Path, "/batches/index/seal")) && body["batch_id"] != batchID {
			t.Errorf("operator request body=%v err=%v", body, err)
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/recording/joined/stream-days/seal" {
			writeJoinedTestJSON(t, w, joinedSealReceipt(batchID, 377, "2026-08-01"))
			return
		}
		if r.URL.Path == "/api/v1/recording/joined/batches/final-freeze/validation/start" {
			_, _ = w.Write([]byte(`{"run_id":"11111111-1111-4111-8111-111111111111","batch_id":"tier1-2026-08-generation-1","state":"ready","completed_scopes":462,"expected_scopes":462}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{api: api, operatorToken: operatorToken}
	endpoint := "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com"
	if _, err := service.FreezeTier1(context.Background(), joinedFreezeTier1Request{ConnectionID: 44, BatchID: batchID,
		Generation: 1, SourceEndpoint: endpoint, QualificationRunID: 7}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SealStreamDay(context.Background(), joinedSealStreamDayRequest{BatchID: batchID,
		RecordingID: 377, LocalDate: "2026-08-01", Apply: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalValidation(context.Background(), joinedFinalFreezeRequest{BatchID: batchID,
		ExpectedFrozenDenominatorSHA256: strings.Repeat("a", 64), Apply: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.FinalFreeze(context.Background(), joinedFinalFreezeRequest{BatchID: batchID,
		ExpectedFrozenDenominatorSHA256: strings.Repeat("a", 64), Apply: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SealBatchIndex(context.Background(), joinedSealBatchIndexRequest{BatchID: batchID,
		ExpectedSHA256: strings.Repeat("b", 64), Apply: true}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"/api/v1/recording/joined/freeze-tier1",
		"/api/v1/recording/joined/stream-days/seal",
		"/api/v1/recording/joined/batches/final-freeze/validation/start",
		"/api/v1/recording/joined/batches/final-freeze",
		"/api/v1/recording/joined/batches/index/seal",
	} {
		if got := <-paths; got != want {
			t.Fatalf("operator path=%q want=%q", got, want)
		}
	}
}

func TestJoinedCheckpointedFreezeResumesFromAuthoritativeStatusWithoutApplying(t *testing.T) {
	t.Parallel()
	const (
		operatorToken = "joined-tier1-operator-token-at-least-32-bytes"
		batchID       = "tier1-2026-08-generation-1"
		runID         = "11111111-1111-4111-8111-111111111111"
	)
	endpoint := "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com"
	requestCount := 0
	completed := 0
	ready := false
	firstStepFailed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.Header.Get("Authorization") != "Bearer "+operatorToken {
			t.Errorf("checkpointed auth=%q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		progress := func() joinedTier1CheckpointedProgress {
			p := joinedTier1CheckpointedProgress{RunID: runID, State: "building", CompletedRecordings: completed, ExpectedRecordings: 33}
			if ready {
				p.State = "ready"
				sha := strings.Repeat("a", 64)
				p.RequestSHA256 = &sha
			} else {
				next := completed + 1
				p.NextPriorityOrdinal = &next
			}
			return p
		}
		switch r.URL.Path {
		case "/api/v1/recording/joined/freeze-tier1/dry-run/start":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["apply"] != false {
				t.Errorf("unsafe checkpointed start body=%v err=%v", body, err)
			}
			writeJoinedTestJSON(t, w, progress())
		case "/api/v1/recording/joined/freeze-tier1/dry-run/step":
			var body struct {
				RunID           string `json:"run_id"`
				PriorityOrdinal int    `json:"priority_ordinal"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RunID != runID || body.PriorityOrdinal != completed+1 {
				t.Errorf("checkpointed step body=%+v err=%v completed=%d", body, err, completed)
			}
			completed = body.PriorityOrdinal
			if completed == 33 {
				ready = true
			}
			if completed == 1 && !firstStepFailed {
				firstStepFailed = true
				w.WriteHeader(http.StatusGatewayTimeout)
				return
			}
			writeJoinedTestJSON(t, w, progress())
		case "/api/v1/recording/joined/freeze-tier1/dry-run/status":
			if r.URL.Query().Get("run_id") != runID {
				t.Errorf("status run_id=%q", r.URL.Query().Get("run_id"))
			}
			writeJoinedTestJSON(t, w, progress())
		default:
			t.Errorf("unexpected checkpointed path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "joined-worker-bootstrap-token-at-least-32-bytes", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{api: api, operatorToken: operatorToken}
	result, err := service.FreezeTier1Checkpointed(context.Background(), joinedFreezeTier1Request{
		ConnectionID: 44, BatchID: batchID, Generation: 1, SourceEndpoint: endpoint, QualificationRunID: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	progress, ok := result.(joinedTier1CheckpointedProgress)
	if !ok || progress.State != "ready" || progress.CompletedRecordings != 33 || progress.RequestSHA256 == nil {
		t.Fatalf("checkpointed result=%+v", result)
	}
	if requestCount != 67 {
		t.Fatalf("checkpointed request count=%d want 67 (start + 33 steps + 33 status)", requestCount)
	}
}

func TestJoinedCheckpointedFreezeStopsOnDeterministicStepRejection(t *testing.T) {
	const (
		operatorToken = "joined-tier1-operator-token-at-least-32-bytes"
		batchID       = "tier1-2026-08-generation-1"
		runID         = "11111111-1111-4111-8111-111111111111"
	)
	endpoint := "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com"
	stepCalls := 0
	statusCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/recording/joined/freeze-tier1/dry-run/start":
			writeJoinedTestJSON(t, w, joinedTier1CheckpointedProgress{RunID: runID, State: "building",
				CompletedRecordings: 0, ExpectedRecordings: 33, NextPriorityOrdinal: intPointer(1)})
		case "/api/v1/recording/joined/freeze-tier1/dry-run/step":
			stepCalls++
			w.WriteHeader(http.StatusConflict)
		case "/api/v1/recording/joined/freeze-tier1/dry-run/status":
			statusCalls++
			writeJoinedTestJSON(t, w, joinedTier1CheckpointedProgress{RunID: runID, State: "building",
				CompletedRecordings: 0, ExpectedRecordings: 33, NextPriorityOrdinal: intPointer(1)})
		default:
			t.Errorf("unexpected checkpointed path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "joined-worker-bootstrap-token-at-least-32-bytes", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{api: api, operatorToken: operatorToken}
	_, err = service.FreezeTier1Checkpointed(context.Background(), joinedFreezeTier1Request{
		ConnectionID: 44, BatchID: batchID, Generation: 1, SourceEndpoint: endpoint, QualificationRunID: 7,
	})
	if err == nil || !strings.Contains(err.Error(), "did not advance") {
		t.Fatalf("deterministic rejection error=%v", err)
	}
	if stepCalls != 1 || statusCalls != 1 {
		t.Fatalf("deterministic rejection spun: step_calls=%d status_calls=%d", stepCalls, statusCalls)
	}
}

func intPointer(value int) *int { return &value }

func TestJoinedOperatorRejectsWorkerTokenAliasBeforeRequest(t *testing.T) {
	t.Parallel()
	api, err := newJoinedAPIClient("https://example.test", "same-token-at-least-32-bytes-long", nil)
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{api: api, operatorToken: api.bootstrapToken}
	if _, err := service.SealStreamDay(context.Background(), joinedSealStreamDayRequest{}); err == nil {
		t.Fatal("worker token alias reached operator API")
	}
}

func TestJoinedSealStreamDayRejectsUnboundSuccessReceipt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{api: api, operatorToken: "joined-tier1-operator-token-at-least-32-bytes"}
	if _, err := service.SealStreamDay(context.Background(), joinedSealStreamDayRequest{BatchID: "tier1-2026-08",
		RecordingID: 377, LocalDate: "2026-08-01", Apply: true}); err == nil {
		t.Fatal("unbound 2xx stream-day response was accepted")
	}
}

func TestJoinedRemainingDaysProvesCanaryAndSealsSerially(t *testing.T) {
	t.Parallel()
	const (
		operatorToken = "joined-tier1-operator-token-at-least-32-bytes"
		batchID       = "tier1-2026-08-generation-1"
	)
	canarySHA := strings.Repeat("a", 64)
	sealedSHA := strings.Repeat("b", 64)
	streamDays := make([]joinedAdminBatchStatusStreamDay, 0, len(joinedrecording.Tier1RecordingIDs)*14)
	for _, recordingID := range joinedrecording.Tier1RecordingIDs {
		for day := 1; day <= 14; day++ {
			streamDays = append(streamDays, joinedAdminBatchStatusStreamDay{RecordingID: recordingID,
				LocalDate: fmt.Sprintf("2026-08-%02d", day), State: "sealed", SourceCount: 1,
				SourceBytes: 1, SealRequestSHA256: sealedSHA})
		}
	}
	streamDays[0].SealRequestSHA256 = canarySHA
	pending := len(streamDays) - 1
	streamDays[pending].State, streamDays[pending].SealRequestSHA256 = "pending", ""
	requests := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Method + " " + r.URL.RequestURI()
		if r.Header.Get("Authorization") != "Bearer "+operatorToken {
			t.Errorf("remaining auth=%q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			writeJoinedTestJSON(t, w, joinedAdminBatchStatus{ProtocolVersion: 1, BatchID: batchID, State: "building",
				FrozenDenominatorSHA256: strings.Repeat("c", 64), ExpectedStreamDays: len(streamDays),
				ExpectedScheduledHours: len(streamDays) * 12, StreamDays: streamDays})
			return
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil ||
			request["recording_id"] != float64(streamDays[pending].RecordingID) || request["local_date"] != streamDays[pending].LocalDate {
			t.Errorf("pending request=%v err=%v", request, err)
		}
		writeJoinedTestJSON(t, w, joinedSealReceipt(batchID, streamDays[pending].RecordingID, streamDays[pending].LocalDate))
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{api: api, operatorToken: operatorToken}
	result, err := service.SealRemainingDays(context.Background(), joinedSealRemainingDaysRequest{BatchID: batchID,
		CanaryRecordingID: streamDays[0].RecordingID, CanaryLocalDate: streamDays[0].LocalDate,
		ExpectedCanarySealRequestSHA256: canarySHA, Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.(map[string]any)["sealed_now"] != 1 {
		t.Fatalf("result=%v", result)
	}
	if got := <-requests; got != "GET /api/v1/recording/joined/batches/status?batch_id="+batchID {
		t.Fatalf("status request=%q", got)
	}
	if got := <-requests; got != "POST /api/v1/recording/joined/stream-days/seal" {
		t.Fatalf("seal request=%q", got)
	}
}

func TestJoinedRemainingDaysStopsAtFirstFailure(t *testing.T) {
	t.Parallel()
	const (
		operatorToken = "joined-tier1-operator-token-at-least-32-bytes"
		batchID       = "tier1-2026-08-generation-1"
	)
	canarySHA := strings.Repeat("a", 64)
	days := make([]joinedAdminBatchStatusStreamDay, 0, len(joinedrecording.Tier1RecordingIDs)*14)
	for _, recordingID := range joinedrecording.Tier1RecordingIDs {
		for day := 1; day <= 14; day++ {
			days = append(days, joinedAdminBatchStatusStreamDay{RecordingID: recordingID,
				LocalDate: fmt.Sprintf("2026-08-%02d", day), State: "sealed", SourceCount: 1,
				SourceBytes: 1, SealRequestSHA256: strings.Repeat("b", 64)})
		}
	}
	days[0].SealRequestSHA256 = canarySHA
	days[len(days)-2].State, days[len(days)-2].SealRequestSHA256 = "pending", ""
	days[len(days)-1].State, days[len(days)-1].SealRequestSHA256 = "pending", ""
	requests := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Method
		if r.Method == http.MethodGet {
			writeJoinedTestJSON(t, w, joinedAdminBatchStatus{ProtocolVersion: 1, BatchID: batchID, State: "building",
				FrozenDenominatorSHA256: strings.Repeat("c", 64), ExpectedStreamDays: len(days),
				ExpectedScheduledHours: len(days) * 12, StreamDays: days})
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{api: api, operatorToken: operatorToken}
	_, err = service.SealRemainingDays(context.Background(), joinedSealRemainingDaysRequest{BatchID: batchID,
		CanaryRecordingID: days[0].RecordingID, CanaryLocalDate: days[0].LocalDate,
		ExpectedCanarySealRequestSHA256: canarySHA, Apply: true})
	if err == nil {
		t.Fatal("remaining-day failure was ignored")
	}
	if len(requests) != 2 || <-requests != http.MethodGet || <-requests != http.MethodPost {
		t.Fatalf("remaining-day request count/order differs")
	}
}

func TestJoinedFinalizeAcceptsExactNoContent(t *testing.T) {
	t.Parallel()
	const token = "operation-token-kept-secret"
	paths := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("unexpected finalize request: %s auth=%q", r.Method, r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "bootstrap-token-kept-secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	if err := api.finalizeLedger(context.Background(), token, joinedrecording.FinalizeLedgerRequest{ProtocolVersion: 1, Published: joinedrecording.PublishedLedger{ArtifactID: 1, ObjectKey: "joined/ledger.json", ETag: "etag", SizeBytes: 1, SHA256: sha}}); err != nil {
		t.Fatal(err)
	}
	if err := api.finalizeHour(context.Background(), token, joinedrecording.FinalizeHourRequest{ProtocolVersion: 1, Published: joinedrecording.PublishedHour{HourID: "hour", RecordingID: 1, LocalHour: 1, HourManifestObjectKey: "joined/hour.json", HourManifestETag: "etag", HourManifestSizeBytes: 1, HourManifestSHA256: sha}}); err != nil {
		t.Fatal(err)
	}
	if err := api.finalizeBatchIndex(context.Background(), token, joinedrecording.FinalizeBatchIndexRequest{ProtocolVersion: 1, Published: joinedrecording.PublishedBatchIndex{ArtifactID: 2, ObjectKey: "joined/index.json", ETag: "etag", SizeBytes: 1, SHA256: sha}}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"/api/v1/recording/joined/publication/ledger/finalize",
		"/api/v1/recording/joined/publication/hour/finalize",
		"/api/v1/recording/joined/publication/index/finalize",
	} {
		if got := <-paths; got != want {
			t.Fatalf("finalize path=%q want=%q", got, want)
		}
	}
}

func TestJoinedWorkerRejectsMalformedReclaimedHourBeforeStorage(t *testing.T) {
	t.Parallel()
	service := &remoteJoinedOperatorService{}
	err := service.publishClaim(context.Background(), joinedrecording.PublicationClaimResponse{Kind: "hour", Hour: &joinedrecording.WorkerClaim{HourID: "sealed-hour"}}, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "authority is incomplete") {
		t.Fatalf("malformed reclaimed hour did not fail before storage: %v", err)
	}
}

func TestJoinedWorkerTaskHasHardDeadline(t *testing.T) {
	started := make(chan struct{})
	err := runJoinedWorkerTask(context.Background(), time.Millisecond, "preflight_and_publish", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return errors.New("media parser raced the deadline")
	})
	<-started
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "joined worker task deadline exceeded stage=preflight_and_publish") {
		t.Fatalf("task deadline err=%v", err)
	}
}

func TestBoundedMediaDeadlineUsesWorkerTaskClassification(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "ffmpeg-blocked")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FFMPEG_BIN", fake)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err := joinedrecording.InspectMediaToolEvidence(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded media deadline err=%v", err)
	}
	if class, reason := joinedFailureClassification(err); class != "transient" || reason != "worker_task_deadline" {
		t.Fatalf("classification=%s/%s err=%v", class, reason, err)
	}
}

func TestJoinedWorkerTaskPreservesCompletedSuccessAtDeadline(t *testing.T) {
	finalized := false
	err := runJoinedWorkerTask(context.Background(), time.Millisecond, "publish_claim", func(ctx context.Context) error {
		// The worker contract permits nil only after its finalize call commits.
		finalized = true
		<-ctx.Done()
		return nil
	})
	if err != nil || !finalized {
		t.Fatalf("completed task was changed into a deadline failure: %v", err)
	}
}

func TestJoinedStageTimingLogHasOnlyBoundedFields(t *testing.T) {
	got := joinedStageTimingLog("batch__recording-413__hour-02", joinedrecording.StageTimingEvent{Stage: "download", ElapsedMS: 1234, Outcome: "ok"})
	want := "joined worker stage timing hour_id=batch__recording-413__hour-02 stage=download elapsed_ms=1234 outcome=ok"
	if got != want {
		t.Fatalf("timing log=%q want=%q", got, want)
	}
	for _, forbidden := range []string{"https://", "X-Amz-", "token", "object_key"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("timing log contains forbidden field %q: %q", forbidden, got)
		}
	}
}

func TestJoinedWorkerStatusBindsExactBatchAndCanaryScope(t *testing.T) {
	cfg := validJoinedWorkerConfig()
	hours, err := cfg.JoinedCanaryHourIDs()
	if err != nil {
		t.Fatal(err)
	}
	status := joinedWorkerStatus{ProtocolVersion: joinedrecording.JoinedProtocolVersion, Enabled: true,
		BatchID: cfg.JoinedRecordingBatchID, WorkScope: config.JoinedWorkScopeCanary, CanaryHourIDs: hours}
	if err := validateJoinedWorkerStatus(cfg, cfg.JoinedRecordingBatchID, status); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*joinedWorkerStatus){
		"disabled": func(s *joinedWorkerStatus) { s.Enabled = false },
		"batch":    func(s *joinedWorkerStatus) { s.BatchID = "other-batch" },
		"scope": func(s *joinedWorkerStatus) {
			s.CanaryHourIDs[0], s.CanaryHourIDs[1] = s.CanaryHourIDs[1], s.CanaryHourIDs[0]
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := status
			bad.CanaryHourIDs = append([]string(nil), status.CanaryHourIDs...)
			mutate(&bad)
			if validateJoinedWorkerStatus(cfg, cfg.JoinedRecordingBatchID, bad) == nil {
				t.Fatal("mismatched worker status accepted")
			}
		})
	}
}

func TestJoinedWorkerStatusBindsExactSingleCanaryScope(t *testing.T) {
	cfg := validJoinedWorkerConfig()
	cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeSingleCanary
	cfg.JoinedRecordingCanaryHourIDs = strings.Split(cfg.JoinedRecordingCanaryHourIDs, ",")[0]
	hours, err := cfg.JoinedCanaryHourIDs()
	if err != nil {
		t.Fatal(err)
	}
	status := joinedWorkerStatus{ProtocolVersion: joinedrecording.JoinedProtocolVersion, Enabled: true,
		BatchID: cfg.JoinedRecordingBatchID, WorkScope: config.JoinedWorkScopeSingleCanary, CanaryHourIDs: hours}
	if err := validateJoinedWorkerStatus(cfg, cfg.JoinedRecordingBatchID, status); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*joinedWorkerStatus){
		"scope":      func(s *joinedWorkerStatus) { s.WorkScope = config.JoinedWorkScopeCanary },
		"extra hour": func(s *joinedWorkerStatus) { s.CanaryHourIDs = append(s.CanaryHourIDs, "extra") },
		"wrong hour": func(s *joinedWorkerStatus) {
			s.CanaryHourIDs[0] = strings.Replace(s.CanaryHourIDs[0], "hour-01", "hour-02", 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := status
			bad.CanaryHourIDs = append([]string(nil), status.CanaryHourIDs...)
			mutate(&bad)
			if validateJoinedWorkerStatus(cfg, cfg.JoinedRecordingBatchID, bad) == nil {
				t.Fatal("mismatched single-canary worker status accepted")
			}
		})
	}
}

func TestJoinedWorkerStatusBindsFrozenBatchScopeWithoutCanaryHours(t *testing.T) {
	cfg := validJoinedWorkerConfig()
	cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	cfg.JoinedRecordingCanaryHourIDs = ""
	status := joinedWorkerStatus{ProtocolVersion: joinedrecording.JoinedProtocolVersion, Enabled: true,
		BatchID: cfg.JoinedRecordingBatchID, WorkScope: config.JoinedWorkScopeFrozenBatch, CanaryHourIDs: []string{}}
	if err := validateJoinedWorkerStatus(cfg, cfg.JoinedRecordingBatchID, status); err != nil {
		t.Fatal(err)
	}
	status.CanaryHourIDs = []string{"unexpected"}
	if err := validateJoinedWorkerStatus(cfg, cfg.JoinedRecordingBatchID, status); err == nil {
		t.Fatal("frozen-batch status accepted a canary allowlist")
	}
}

func writeJoinedTestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}

func joinedSealReceipt(batchID string, recordingID int64, localDate string) joinedSealStreamDayReceipt {
	return joinedSealStreamDayReceipt{ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: batchID,
		RecordingID: recordingID, LocalDate: localDate, SourceSnapshotSHA: strings.Repeat("a", 64),
		HeadManifestSHA: strings.Repeat("b", 64), LedgerSHA: strings.Repeat("c", 64),
		LedgerArtifactSHA: strings.Repeat("d", 64), SealRequestSHA: strings.Repeat("e", 64),
		LedgerArtifactID: 1, SourceCount: 1, SourceBytes: 1}
}
