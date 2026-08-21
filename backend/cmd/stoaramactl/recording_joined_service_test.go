package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestJoinedWorkerClaimsPublicationBeforePreflight(t *testing.T) {
	t.Parallel()
	const (
		batchID   = "tier1-2026-08"
		workerID  = "worker-1"
		bootstrap = "bootstrap-token-kept-secret"
		claim     = "claim-token-kept-secret-value"
	)
	paths := make(chan string, 3)
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
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.BatchID != batchID {
				t.Errorf("unexpected bootstrap request: %+v", request)
			}
			writeJoinedTestJSON(t, w, joinedrecording.WorkerBootstrapResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: batchID, ClaimToken: claim, ExpiresAt: time.Now().Add(time.Hour)})
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
	service := &remoteJoinedOperatorService{cfg: config.Config{}, api: api}
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
	service := &remoteJoinedOperatorService{api: api, idlePoll: time.Hour}
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
			_, _, err = api.bootstrap(context.Background(), "tier1-2026-08")
			if err == nil || strings.Contains(err.Error(), secret) {
				t.Fatalf("unsafe API error: %v", err)
			}
		})
	}
}

func TestJoinedOperatorUsesOnlyDedicatedAdminToken(t *testing.T) {
	t.Parallel()
	const (
		operatorToken = "joined-tier1-operator-token-at-least-32-bytes"
		batchID       = "tier1-2026-08-generation-1"
	)
	paths := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+operatorToken {
			t.Errorf("operator request method=%s auth=%q", r.Method, r.Header.Get("Authorization"))
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["protocol_version"] != float64(1) || body["batch_id"] != batchID {
			t.Errorf("operator request body=%v err=%v", body, err)
		}
		w.Header().Set("Content-Type", "application/json")
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
	if _, err := service.FinalFreeze(context.Background(), joinedFinalFreezeRequest{BatchID: batchID,
		ExpectedFrozenDenominatorSHA256: strings.Repeat("a", 64), Apply: true}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"/api/v1/recording/joined/freeze-tier1",
		"/api/v1/recording/joined/stream-days/seal",
		"/api/v1/recording/joined/batches/final-freeze",
	} {
		if got := <-paths; got != want {
			t.Fatalf("operator path=%q want=%q", got, want)
		}
	}
}

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
		_, _ = w.Write([]byte(`{"ok":true}`))
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

func TestJoinedWorkerFailsClosedForReclaimedHour(t *testing.T) {
	t.Parallel()
	service := &remoteJoinedOperatorService{}
	err := service.publishClaim(context.Background(), joinedrecording.PublicationClaimResponse{Kind: "hour", Hour: &joinedrecording.WorkerClaim{HourID: "sealed-hour"}}, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "cannot yet be rebuilt") {
		t.Fatalf("reclaimed hour did not fail closed: %v", err)
	}
}

func writeJoinedTestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}
