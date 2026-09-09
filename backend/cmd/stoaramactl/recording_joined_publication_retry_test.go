package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestJoinedPublicationFailureRetryabilityFailsClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"typed transport", &joinedAPITransportError{cause: errors.New("connection reset")}, true},
		{"storage transport", &joinedrecording.StorageCapabilityError{Operation: "put", Reason: "transport", Cause: context.DeadlineExceeded}, true},
		{"request timeout", &joinedrecording.StorageCapabilityError{Operation: "put", Reason: "status", StatusCode: http.StatusRequestTimeout}, true},
		{"too early", &joinedAPIResponseError{path: "/finalize", status: http.StatusTooEarly}, true},
		{"rate limited", &joinedAPIResponseError{path: "/finalize", status: http.StatusTooManyRequests}, true},
		{"server error", &joinedrecording.StorageCapabilityError{Operation: "reread", Reason: "status", StatusCode: http.StatusServiceUnavailable}, true},
		{"worker timeout", errJoinedWorkerTaskDeadline, true},
		{"runner timeout", errors.Join(errJoinedWorkerTaskDeadline, context.DeadlineExceeded), true},
		{"timeout plus unknown invariant", errors.Join(errJoinedWorkerTaskDeadline, context.DeadlineExceeded, errors.New("identity invariant")), false},
		{"timeout plus IO error", errors.Join(errJoinedWorkerTaskDeadline, context.DeadlineExceeded, syscall.EIO), false},
		{"conflict", &joinedAPIResponseError{path: "/finalize", status: http.StatusConflict}, false},
		{"identity", &joinedrecording.StorageCapabilityError{Operation: "reread", Reason: "identity", StatusCode: http.StatusOK}, false},
		{"hash", &joinedrecording.StorageCapabilityError{Operation: "reread", Reason: "hash", StatusCode: http.StatusOK}, false},
		{"capability schema", &joinedrecording.StorageCapabilityError{Operation: "create_capability", Reason: "capability"}, false},
		{"heartbeat", errors.Join(joinedrecording.ErrWorkerHeartbeatFailed, &joinedAPITransportError{cause: errors.New("reset")}), false},
		{"mixed transport and identity", errors.Join(&joinedAPITransportError{cause: errors.New("reset")}, &joinedrecording.StorageCapabilityError{Operation: "reread", Reason: "identity"}), false},
		{"caller canceled", errors.Join(context.Canceled, &joinedAPITransportError{cause: context.Canceled}), false},
		{"ambiguous deadline", context.DeadlineExceeded, false},
		{"ordinary process failure", errors.New("decoder failed"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := joinedPublicationFailureRetryable(tc.err); got != tc.want {
				t.Fatalf("retryable=%v want %v err=%v", got, tc.want, tc.err)
			}
		})
	}
}

func TestJoinedPublicationTransientFailureReclaimsSameImmutableArtifact(t *testing.T) {
	t.Parallel()
	const (
		bootstrapToken = "bootstrap-token-kept-secret"
		claimToken     = "claim-token-kept-secret-value"
		batchID        = "goodplus-20260821-generation-1"
		artifactID     = int64(77)
	)
	ledgerJSON, err := os.ReadFile(filepath.Join("..", "..", "internal", "joinedrecording", "testdata", "allocation_ledger_v1.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var ledger joinedrecording.StreamDayAllocation
	if err := json.Unmarshal(ledgerJSON, &ledger); err != nil {
		t.Fatal(err)
	}
	canonical, artifactSHA, err := joinedrecording.CanonicalAllocationLedgerArtifact(ledger)
	if err != nil {
		t.Fatal(err)
	}
	scopeID, err := joinedrecording.CanonicalLedgerID(batchID, ledger.RecordingID, ledger.LocalDate, ledger.Generation)
	if err != nil {
		t.Fatal(err)
	}
	var reports atomic.Int32
	var claims atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/recording/joined/token":
			var request joinedrecording.WorkerBootstrapRequest
			if r.Header.Get("Authorization") != "Bearer "+bootstrapToken || json.NewDecoder(r.Body).Decode(&request) != nil || request.Validate() != nil {
				http.Error(w, "invalid bootstrap", http.StatusBadRequest)
				return
			}
			writeJoinedTestJSON(t, w, joinedrecording.WorkerBootstrapResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
				BatchID: batchID, ClaimToken: claimToken, ExpiresAt: time.Now().Add(time.Hour), WorkScopeIdentity: request.WorkScopeIdentity})
		case "/api/v1/recording/joined/publication/claim":
			attempt := claims.Add(1)
			operationToken := strings.Repeat(string(rune('a'+attempt-1)), 32)
			leaseID := strings.Repeat(string(rune('L'+attempt-1)), 43)
			claim := joinedrecording.LedgerPublicationClaim{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
				ArtifactID: artifactID, ScopeID: scopeID, LeaseID: leaseID, OperationToken: operationToken,
				LeaseExpires: time.Now().Add(time.Hour), StorageAuthority: "example.r2.cloudflarestorage.com",
				StorageBucket: "recordings", BatchID: batchID, Ledger: ledger,
				ExpectedSize: int64(len(canonical)), ExpectedSHA256: artifactSHA}
			writeJoinedTestJSON(t, w, joinedrecording.PublicationClaimResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
				Kind: "ledger", Ledger: &claim})
		case "/api/v1/recording/joined/failure":
			var request joinedrecording.WorkFailureRequest
			if r.Header.Get("Authorization") != "Bearer "+strings.Repeat("a", 32) || json.NewDecoder(r.Body).Decode(&request) != nil ||
				request.Validate() != nil || request.ScopeKind != "ledger" || request.ScopeID != scopeID {
				http.Error(w, "invalid failure", http.StatusBadRequest)
				return
			}
			reports.Add(1)
			next := time.Now().Add(-time.Millisecond)
			writeJoinedTestJSON(t, w, joinedrecording.WorkFailureResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
				State: "retry", AttemptCount: 1, NextAttemptAt: &next})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, bootstrapToken, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := validJoinedWorkerConfig()
	cfg.JoinedRecordingBatchID = batchID
	cfg.JoinedRecordingWorkScope = "frozen_batch"
	cfg.JoinedRecordingCanaryHourIDs = ""
	service := &remoteJoinedOperatorService{cfg: cfg, api: api, idlePoll: time.Hour}
	var processed atomic.Int32
	service.processClaim = func(_ context.Context, got joinedrecording.PublicationClaimResponse, _ string) error {
		if got.Ledger == nil || got.Ledger.ArtifactID != artifactID || got.Ledger.ExpectedSHA256 != artifactSHA || got.Ledger.ExpectedSize != int64(len(canonical)) {
			t.Fatalf("reclaimed immutable artifact identity changed: %+v", got.Ledger)
		}
		switch processed.Add(1) {
		case 1:
			return &joinedrecording.StorageCapabilityError{Operation: "put", Reason: "status",
				StatusCode: http.StatusServiceUnavailable, ArtifactID: 77}
		case 2:
			if got.Ledger.OperationToken != strings.Repeat("b", 32) || got.Ledger.LeaseID != strings.Repeat("M", 43) {
				t.Fatalf("reclaim did not receive a new fence: %+v", got.Ledger)
			}
			cancel()
			return nil
		default:
			t.Fatalf("unexpected publication attempt %d", processed.Load())
			return nil
		}
	}
	root := filepath.Join(t.TempDir(), "scratch")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	err = service.RunWorker(ctx, joinedWorkerRequest{BatchID: batchID, WorkerID: "worker-1", ScratchRoot: root})
	if err != nil || claims.Load() != 2 || processed.Load() != 2 || reports.Load() != 1 {
		t.Fatalf("worker err=%v claims=%d processed=%d reports=%d", err, claims.Load(), processed.Load(), reports.Load())
	}
}

func TestJoinedPublicationRetryRequiresBoundedAcknowledgement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		state string
		next  *time.Time
	}{
		{name: "terminal", state: "terminal"},
		{name: "unbounded", state: "retry", next: func() *time.Time { v := time.Now().Add(joinedPublicationRetryMaxWait + time.Minute); return &v }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJoinedTestJSON(t, w, joinedrecording.WorkFailureResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
					State: tc.state, AttemptCount: 1, NextAttemptAt: tc.next})
			}))
			defer server.Close()
			api, err := newJoinedAPIClient(server.URL, "bootstrap-token-kept-secret", server.Client())
			if err != nil {
				t.Fatal(err)
			}
			service := &remoteJoinedOperatorService{api: api}
			err = service.reportJoinedPublicationFailure(context.Background(), context.Background(), "operation-token-kept-secret", "hour", "hour-1",
				&joinedAPIResponseError{path: "/finalize", status: http.StatusServiceUnavailable})
			if tc.name == "terminal" {
				if !errors.Is(err, errJoinedTaskFailureReported) || errors.Is(err, errJoinedPublicationRetryAcknowledged) {
					t.Fatalf("terminal acknowledgement err=%v", err)
				}
			} else if err == nil || errors.Is(err, errJoinedPublicationRetryAcknowledged) {
				t.Fatalf("unbounded acknowledgement err=%v", err)
			}
		})
	}
}

func TestJoinedPublicationRetryStopsOnAmbiguousFailureReport(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "bootstrap-token-kept-secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	taskErr := &joinedAPIResponseError{path: "/finalize", status: http.StatusServiceUnavailable}
	err = (&remoteJoinedOperatorService{api: api}).reportJoinedPublicationFailure(context.Background(), context.Background(),
		"operation-token-kept-secret", "hour", "hour-1", taskErr)
	if err == nil || !errors.Is(err, taskErr) || errors.Is(err, errJoinedPublicationRetryAcknowledged) || errors.Is(err, errJoinedTaskFailureReported) {
		t.Fatalf("ambiguous failure report err=%v", err)
	}
}
