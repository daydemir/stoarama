package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
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

func TestJoinedAPIRequestPreservesCanonicalRawEvidence(t *testing.T) {
	for name, facts := range map[string]json.RawMessage{
		"html placeholders":    json.RawMessage(`{"category":"invalid_media_data","normalized_fact":"<address>&<scratch-file>"}`),
		"producer field order": json.RawMessage(`{"status":"failed","packet_payload_order_status":"passed","decoded_frame_totals_status":"failed","source_fingerprint":{"duration_seconds":1},"output_fingerprint":{"duration_seconds":2}}`),
	} {
		t.Run(name, func(t *testing.T) {
			payload := struct {
				Facts json.RawMessage `json:"facts"`
			}{Facts: facts}
			ordinary, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			body, err := marshalJoinedAPIRequest(payload)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.HasSuffix(body, []byte("\n")) {
				t.Fatal("joined request retained encoder newline")
			}
			if name == "html placeholders" && (!bytes.Contains(ordinary, []byte(`\u003caddress\u003e`)) || !bytes.Contains(body, []byte(`<address>&<scratch-file>`))) {
				t.Fatalf("fixture did not isolate HTML escaping: ordinary=%s joined=%s", ordinary, body)
			}
			var received struct {
				Facts json.RawMessage `json:"facts"`
			}
			if err := json.Unmarshal(body, &received); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(received.Facts, facts) {
				t.Fatalf("canonical evidence changed across joined request: got=%s want=%s", received.Facts, facts)
			}
		})
	}
}

func TestJoinedAPIClientIncludesBoundedStructuredServerError(t *testing.T) {
	const operationToken = "operation-token-secret-sentinel"
	const requestSecret = "request-body-secret-sentinel"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+operationToken {
			t.Error("operation authorization differs")
			return
		}
		var payload struct {
			Secret string `json:"secret"`
		}
		if json.NewDecoder(r.Body).Decode(&payload) != nil || payload.Secret != requestSecret {
			t.Error("request payload differs")
			return
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"worker media partition differs from canonical plan"}`))
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "joined-worker-bootstrap-token-at-least-32-bytes", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	err = api.postJSON(context.Background(), "/seal", operationToken, struct {
		Secret string `json:"secret"`
	}{requestSecret}, &struct{}{})
	want := `joined API /seal returned status 409 error="worker media partition differs from canonical plan"`
	if err == nil || err.Error() != want || strings.Contains(err.Error(), operationToken) || strings.Contains(err.Error(), requestSecret) {
		t.Fatalf("structured server error was not surfaced safely: %v", err)
	}
}

func TestJoinedAPIClientOmitsMalformedOrOversizedServerError(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusBadGateway} {
		for _, body := range []string{"", `not-json`, `{"error":"` + strings.Repeat("x", joinedAPIErrorLimit) + `"}`} {
			err := joinedAPIStatusError("/seal", status, strings.NewReader(body))
			var responseErr *joinedAPIResponseError
			want := fmt.Sprintf("joined API /seal returned status %d", status)
			if err == nil || err.Error() != want || !errors.As(err, &responseErr) || responseErr.status != status {
				t.Fatalf("untrusted response was included: %v", err)
			}
		}
	}
}

func TestJoinedAPIClientAcceptsBoundedLargeHourResponses(t *testing.T) {
	claim := joinedrecording.WorkerClaim{
		ProtocolVersion:  joinedrecording.JoinedProtocolVersion,
		MediaArtifactIDs: make([]int64, 30),
	}
	claim.Plan.Outputs = make([]joinedrecording.OutputPlan, 30)
	for i := range claim.MediaArtifactIDs {
		claim.MediaArtifactIDs[i] = int64(i + 1)
		// Source identities and verification evidence dominate real multi-part
		// claims. Keep the fixture in the production schema while crossing 1 MiB.
		claim.Plan.Outputs[i].Sources = []joinedrecording.SourceClip{{Endpoint: strings.Repeat("a", 40<<10)}}
	}
	sealBody, err := json.Marshal(claim)
	if err != nil {
		t.Fatal(err)
	}
	publicationBody, err := json.Marshal(joinedrecording.PublicationClaimResponse{
		ProtocolVersion: joinedrecording.JoinedProtocolVersion, Kind: "hour", Hour: &claim,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sealBody) <= joinedAPIResponseLimit || len(publicationBody) <= joinedAPIResponseLimit {
		t.Fatalf("fixtures do not exceed default response limit: seal=%d publication=%d", len(sealBody), len(publicationBody))
	}

	for _, tc := range []struct {
		path string
		body []byte
		new  func() any
		ids  func(any) []int64
	}{
		{"/api/v1/recording/joined/hour/seal", sealBody, func() any { return &joinedrecording.WorkerClaim{} }, func(v any) []int64 { return v.(*joinedrecording.WorkerClaim).MediaArtifactIDs }},
		{"/api/v1/recording/joined/publication/claim", publicationBody, func() any { return &joinedrecording.PublicationClaimResponse{} }, func(v any) []int64 { return v.(*joinedrecording.PublicationClaimResponse).Hour.MediaArtifactIDs }},
	} {
		t.Run(tc.path, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(tc.body)
			}))
			defer server.Close()
			api, err := newJoinedAPIClient(server.URL, "joined-worker-bootstrap-token-at-least-32-bytes", server.Client())
			if err != nil {
				t.Fatal(err)
			}
			got := tc.new()
			if err := api.postJSON(context.Background(), tc.path, "operation-token", struct{}{}, got); err != nil {
				t.Fatal(err)
			}
			ids := tc.ids(got)
			if len(ids) != 30 || ids[29] != 30 {
				t.Fatalf("decoded media artifact IDs differ: %+v", ids)
			}
		})
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(sealBody) }))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "joined-worker-bootstrap-token-at-least-32-bytes", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	var ordinary joinedrecording.WorkerClaim
	if err := api.postJSON(context.Background(), "/api/v1/recording/joined/heartbeat", "operation-token", struct{}{}, &ordinary); err == nil || !strings.Contains(err.Error(), "response exceeds limit") {
		t.Fatalf("ordinary endpoint lost default response limit: %v", err)
	}
}

func TestJoinedAPIClientRejectsOversizedHourResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"proof":"%s"}`, strings.Repeat("a", joinedLargeHourAPIResponseLimit))
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "joined-worker-bootstrap-token-at-least-32-bytes", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Proof string `json:"proof"`
	}
	for _, path := range []string{"/api/v1/recording/joined/hour/seal", "/api/v1/recording/joined/publication/claim"} {
		err = api.postJSON(context.Background(), path, "operation-token", struct{}{}, &got)
		if err == nil || !strings.Contains(err.Error(), "response exceeds limit") {
			t.Fatalf("oversized %s response was accepted: %v", path, err)
		}
	}
}

func TestJoinedTaskFailureDiagnosticSelectsOnlyStructuredAPIErrors(t *testing.T) {
	want := `joined API /seal returned status 409 error="canonical plan differs"`
	structured := joinedAPIStatusError("/seal", http.StatusConflict, strings.NewReader(`{"error":"canonical plan differs"}`))
	if got := joinedTaskFailureDiagnostic(fmt.Errorf("preflight: %w", structured)); got != want {
		t.Fatalf("structured diagnostic differs: %q", got)
	}
	if got := joinedTaskFailureDiagnostic(errors.New("request-body-secret-sentinel")); got != "" {
		t.Fatalf("arbitrary task error was exposed: %q", got)
	}
}

func TestJoinedTaskFailureDiagnosticDistinguishesSafeUploadSteps(t *testing.T) {
	put := &joinedrecording.StorageCapabilityError{Operation: "put", Reason: "status", StatusCode: http.StatusTooManyRequests,
		ArtifactID: 646, Ordinal: 29,
		RequestID:         joinedrecording.StorageRequestIDEvidence{SHA256: strings.Repeat("a", 64), Length: 10},
		ExtendedRequestID: joinedrecording.StorageRequestIDEvidence{SHA256: strings.Repeat("b", 64), Length: 11},
		RayID:             joinedrecording.StorageRequestIDEvidence{SHA256: strings.Repeat("c", 64), Length: 12},
		Cause:             errors.New("url-query-header-checksum-token-secret")}
	wantPut := "storage capability operation=put reason=status status=429 artifact_id=646 ordinal=29 request_id_sha256=" + strings.Repeat("a", 64) +
		" request_id_length=10 extended_request_id_sha256=" + strings.Repeat("b", 64) +
		" extended_request_id_length=11 ray_id_sha256=" + strings.Repeat("c", 64) + " ray_id_length=12"
	if got := joinedTaskFailureDiagnostic(put); got != wantPut {
		t.Fatalf("put diagnostic=%q want=%q", got, wantPut)
	}
	put.Operation = "put-token-secret"
	put.Reason = "reason-token-secret"
	put.StatusCode = 900
	put.RequestID = joinedrecording.StorageRequestIDEvidence{SHA256: "request-header-secret", Length: 99}
	if got := joinedTaskFailureDiagnostic(put); got != "storage capability operation=unknown reason=unknown status=0 artifact_id=646 ordinal=29 extended_request_id_sha256="+strings.Repeat("b", 64)+" extended_request_id_length=11 ray_id_sha256="+strings.Repeat("c", 64)+" ray_id_length=12" {
		t.Fatalf("unsafe typed fields reached diagnostic: %q", got)
	}
	conflict := joinedAPIStatusError("/api/v1/recording/joined/capabilities/artifact", http.StatusConflict,
		strings.NewReader(`{"error":"response-body-secret-sentinel"}`))
	reread := &joinedrecording.StorageCapabilityError{Operation: "reread_capability", Reason: "capability", ArtifactID: 646, Ordinal: 29, Cause: conflict}
	wantReread := "storage capability operation=reread_capability reason=capability status=0 artifact_id=646 ordinal=29 api_status=409"
	if got := joinedTaskFailureDiagnostic(reread); got != wantReread || strings.Contains(got, "secret") {
		t.Fatalf("reread diagnostic=%q want=%q", got, wantReread)
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

func TestJoinedDeliveryStatusUsesOperatorReadContract(t *testing.T) {
	const (
		batch = "tier1-generation-1"
		op    = "joined-tier1-operator-token-at-least-32-bytes"
	)
	observed := time.Now().UTC().Truncate(time.Second)
	attempted := observed.Add(-time.Minute)
	retry := observed.Add(time.Minute)
	headHour := "older-hour-1"
	lastAttemptID := int64(401)
	blockerSHA := strings.Repeat("b", 64)
	lastSuccess := observed.Add(-2 * time.Minute)
	batchCompleted := observed.Add(-3 * time.Minute)
	oldestPending := observed.Add(-4 * time.Minute)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.RequestURI() != "/api/v1/recording/joined/delivery-status?batch_id="+batch+"&artifact_id=429" {
			t.Fatalf("request=%s %s", r.Method, r.URL.RequestURI())
		}
		if r.Header.Get("Authorization") != "Bearer "+op {
			t.Fatal("operator authorization differs")
		}
		writeJoinedTestJSON(t, w, joinedDeliveryStatus{BatchID: batch, ArtifactID: 429,
			ArtifactKind: "media", HourID: "hour-429", RelativePath: "joined/hour-429.mp4",
			ExpectedSizeBytes: 12, ExpectedSHA256: strings.Repeat("a", 64), PublicationState: "published",
			Acknowledged: true, IdentityMatches: true, ConnectionID: 47, ConnectionProtocol: 1,
			ObservedAt: observed, FeedHead: &joinedFeedHeadStatus{ArtifactID: 401, BatchID: "older-generation-1",
				HourID: &headHour, Kind: "media", Ordinal: 2, ExpectedSizeBytes: 99, ExpectedSHA256: strings.Repeat("c", 64)},
			LastAttemptArtifactID: &lastAttemptID, LastAttemptBlockerClass: "present",
			LastAttemptBlockerSHA256: blockerSHA, LastAttemptAt: &attempted, RetryAt: &retry, TelemetryMatchesHead: true,
			RawDelivery: joinedRawDeliveryStatus{LastCursorID: 100, ClipsPulled: 90, BytesPulled: 9000,
				ClientLastSuccessAt: &lastSuccess, NASBatchCompletedAt: &batchCompleted, NASBatchClips: 4,
				NASBatchBytes: 400, PendingClips: 3, PendingBytes: 300, OldestPendingAt: &oldestPending,
				JoinedFilesPulled: 7, JoinedBytesPulled: 700}})
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "joined-worker-bootstrap-token-at-least-32-bytes", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{api: api, operatorToken: op}
	result, err := service.DeliveryStatus(context.Background(), joinedDeliveryStatusRequest{BatchID: batch, ArtifactID: 429})
	if err != nil {
		t.Fatal(err)
	}
	got := result.(joinedDeliveryStatus)
	if !got.Acknowledged || !got.ObservedAt.Equal(observed) || got.FeedHead == nil || got.FeedHead.ArtifactID != 401 ||
		got.FeedHead.BatchID != "older-generation-1" || got.FeedHead.HourID == nil || *got.FeedHead.HourID != headHour ||
		got.LastAttemptArtifactID == nil || *got.LastAttemptArtifactID != lastAttemptID ||
		got.LastAttemptBlockerClass != "present" || got.LastAttemptBlockerSHA256 != blockerSHA ||
		got.LastAttemptAt == nil || !got.LastAttemptAt.Equal(attempted) || got.RetryAt == nil || !got.RetryAt.Equal(retry) ||
		!got.TelemetryMatchesHead || got.RawDelivery.LastCursorID != 100 || got.RawDelivery.ClipsPulled != 90 ||
		got.RawDelivery.PendingClips != 3 || got.RawDelivery.PendingBytes != 300 ||
		got.RawDelivery.ClientLastSuccessAt == nil || !got.RawDelivery.ClientLastSuccessAt.Equal(lastSuccess) ||
		got.RawDelivery.OldestPendingAt == nil || !got.RawDelivery.OldestPendingAt.Equal(oldestPending) ||
		got.RawDelivery.JoinedFilesPulled != 7 || got.RawDelivery.JoinedBytesPulled != 700 {
		t.Fatalf("result=%+v", got)
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
	root := filepath.Join(t.TempDir(), "scratch")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{cfg: validJoinedWorkerConfig(), api: api}
	worked, err := service.runWorkerOnce(context.Background(), joinedWorkerRequest{BatchID: batchID, WorkerID: workerID, ScratchRoot: root})
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
	t.Setenv("JOINED_LOSSLESS_NORMALIZATION_ENABLED", "true")
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
				request.ScratchAvailableBytes <= 0 || request.TaskBudgetBytes <= 0 || request.TaskBudgetBytes > request.ScratchAvailableBytes {
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
	if class, reason := joinedFailureClassification(joinedrecording.ErrPreflightSealRequestInvalid); class != "transient" || reason != "preflight_seal_request_invalid" {
		t.Fatalf("seal validation class=%q reason=%q", class, reason)
	}
	if class, reason := joinedFailureClassification(joinedrecording.ErrPreflightLeaseEndedBeforeSeal); class != "transient" || reason != "preflight_lease_ended_before_seal" {
		t.Fatalf("pre-seal lease class=%q reason=%q", class, reason)
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

func TestJoinedWorkerDrainIsBoundedByAdmittedTaskDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runJoinedWorkerLoop(ctx, time.Hour, func(_ context.Context, admittedCtx context.Context) (bool, error) {
			err := runJoinedWorkerTask(admittedCtx, 20*time.Millisecond, "preflight_and_publish", func(taskCtx context.Context) error {
				close(started)
				<-taskCtx.Done()
				return taskCtx.Err()
			})
			return true, err
		})
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("bounded drain error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("draining worker outlived admitted task deadline")
	}
}

func TestJoinedWorkerIdleCleansOnlyInactiveScratchAfterClaims(t *testing.T) {
	t.Parallel()
	cfg := validJoinedWorkerConfig()
	inactive := strings.Repeat("I", 43)
	active := strings.Repeat("A", 43)
	unknown := "operator-notes"
	root := filepath.Join(t.TempDir(), "scratch")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, leaseID := range []string{inactive, active, unknown} {
		if err := os.Mkdir(filepath.Join(root, leaseID), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	const (
		bootstrapToken = "scratch-proof-bootstrap-token-kept-secret"
		claimToken     = "claim-token-kept-secret-value"
	)
	var claims atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/recording/joined/token":
			var request joinedrecording.WorkerBootstrapRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Validate() != nil {
				t.Errorf("invalid bootstrap request: %+v err=%v", request, err)
			}
			writeJoinedTestJSON(t, w, joinedrecording.WorkerBootstrapResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
				BatchID: cfg.JoinedRecordingBatchID, ClaimToken: claimToken, ExpiresAt: time.Now().Add(time.Hour), WorkScopeIdentity: request.WorkScopeIdentity})
		case "/api/v1/recording/joined/publication/claim", "/api/v1/recording/joined/claim":
			claims.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case "/api/v1/recording/joined/leases/status":
			if claims.Load() != 2 {
				t.Errorf("scratch cleanup started after %d claims", claims.Load())
			}
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
	worked, err := service.runWorkerOnce(context.Background(), joinedWorkerRequest{
		BatchID: cfg.JoinedRecordingBatchID, WorkerID: "worker-1", ScratchRoot: root})
	if err != nil || worked || claims.Load() != 2 {
		t.Fatalf("idle cleanup worked=%v claims=%d err=%v", worked, claims.Load(), err)
	}
	if _, err := os.Stat(filepath.Join(root, inactive)); !os.IsNotExist(err) {
		t.Fatalf("inactive scratch remains: %v", err)
	}
	for _, name := range []string{active, unknown} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("retained scratch %s: %v", name, err)
		}
	}
}

func TestJoinedWorkerIdleScratchCleanupFailsClosed(t *testing.T) {
	t.Parallel()
	cfg := validJoinedWorkerConfig()
	leaseID := strings.Repeat("L", 43)
	root := filepath.Join(t.TempDir(), "scratch")
	if err := os.MkdirAll(filepath.Join(root, leaseID), 0o700); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/recording/joined/token":
			var request joinedrecording.WorkerBootstrapRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			writeJoinedTestJSON(t, w, joinedrecording.WorkerBootstrapResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
				BatchID: cfg.JoinedRecordingBatchID, ClaimToken: "claim-token-kept-secret-value", ExpiresAt: time.Now().Add(time.Hour), WorkScopeIdentity: request.WorkScopeIdentity})
		case "/api/v1/recording/joined/publication/claim", "/api/v1/recording/joined/claim":
			w.WriteHeader(http.StatusNoContent)
		case "/api/v1/recording/joined/leases/status":
			http.Error(w, "proof unavailable", http.StatusServiceUnavailable)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "scratch-proof-bootstrap-token-kept-secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{cfg: cfg, api: api}
	worked, err := service.runWorkerOnce(context.Background(), joinedWorkerRequest{
		BatchID: cfg.JoinedRecordingBatchID, WorkerID: "worker-1", ScratchRoot: root})
	if err == nil || worked || !strings.Contains(err.Error(), "cleanup inactive joined scratch while idle") {
		t.Fatalf("idle cleanup failure worked=%v err=%v", worked, err)
	}
	if _, err := os.Stat(filepath.Join(root, leaseID)); err != nil {
		t.Fatalf("failed cleanup changed scratch: %v", err)
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

func TestJoinedWorkerTaskBudgetCoversMeasuredStrictHourRuntime(t *testing.T) {
	// Production hour 865 made healthy, monotonic progress for the full
	// two-hour task budget and was canceled after producing 50 of 60 strict
	// singleton outputs. Four hours covers the measured remaining work plus
	// seal and create-only publication without removing the hard stop.
	const minimumStrictHourBudget = 4 * time.Hour
	if joinedWorkerTaskLimit < minimumStrictHourBudget {
		t.Fatalf("joined worker task limit=%s is below measured strict-hour budget=%s", joinedWorkerTaskLimit, minimumStrictHourBudget)
	}
}

func TestBoundedMediaDeadlineUsesWorkerTaskClassification(t *testing.T) {
	ffprobe := strings.TrimSpace(os.Getenv("FFPROBE_BIN"))
	if ffprobe == "" {
		ffprobe = "ffprobe"
	}
	if _, err := exec.LookPath(ffprobe); err != nil {
		t.Skip("ffprobe unavailable")
	}
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

func TestJoinedStageTimingLogIncludesOnlyBoundedUploadVerifyDiagnostic(t *testing.T) {
	event := joinedrecording.StageTimingEvent{Stage: "upload_verify", ElapsedMS: 1234, Outcome: "error",
		FailureStage: joinedrecording.UploadVerifyFailurePartUpload, ArtifactID: 646, ArtifactOrdinal: 29}
	got := joinedStageTimingLog("batch__recording-413__hour-02", event)
	want := "joined worker stage timing hour_id=batch__recording-413__hour-02 stage=upload_verify elapsed_ms=1234 outcome=error failure_stage=part_upload artifact_id=646 artifact_ordinal=29"
	if got != want {
		t.Fatalf("diagnostic log=%q want=%q", got, want)
	}
}

func TestJoinedCreateCapabilityErrorClassificationIsClosedAndSafe(t *testing.T) {
	tests := []struct {
		name, reason string
		status       int
		err          error
	}{
		{"transport", "transport", 0, &joinedAPITransportError{cause: errors.New("signed URL must not escape")}},
		{"context timeout", "transport", 0, context.DeadlineExceeded},
		{"caller cancellation", "capability", 0, context.Canceled},
		{"retryable status", "status", http.StatusServiceUnavailable, &joinedAPIResponseError{path: "/secret", status: http.StatusServiceUnavailable, message: "raw body"}},
		{"deterministic status", "status", http.StatusBadRequest, &joinedAPIResponseError{path: "/secret", status: http.StatusBadRequest, message: "raw body"}},
		{"unknown without status", "transport", 0, errors.New("raw capability error")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got *joinedrecording.StorageCapabilityError
			if !errors.As(joinedCreateCapabilityError(tt.err, 646), &got) || got.Operation != "create_capability" ||
				got.Reason != tt.reason || got.StatusCode != tt.status || got.ArtifactID != 646 {
				t.Fatalf("classification=%+v", got)
			}
			rendered := got.Error()
			if strings.Contains(rendered, "secret") || strings.Contains(rendered, "raw body") || strings.Contains(rendered, "raw capability") {
				t.Fatalf("diagnostic leaked cause: %q", rendered)
			}
		})
	}
}

func TestJoinedReadCapabilityErrorClassificationIsClosedAndSafe(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		err          error
	}{
		{"wrapped transport", "transport", &joinedAPITransportError{cause: errors.New("https://signed.example/?token=secret")}},
		{"context timeout", "transport", context.DeadlineExceeded},
		{"unknown without status", "transport", errors.New("secret raw read error")},
		{"caller cancellation", "capability", context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var diagnostic *joinedrecording.StorageCapabilityError
			err := joinedReadCapabilityError(tc.err, 646)
			if !errors.As(err, &diagnostic) || diagnostic.Operation != "reread_capability" || diagnostic.Reason != tc.reason ||
				diagnostic.ArtifactID != 646 || strings.Contains(diagnostic.Error(), "signed.example") ||
				strings.Contains(diagnostic.Error(), "secret") {
				t.Fatalf("unsafe read capability diagnostic: %+v rendered=%q", diagnostic, diagnostic.Error())
			}
		})
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
