package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// TestJoinedExactFiftyScopeClaimsOnlyListedWork proves the two-worker rollout
// against the real claim SQL. The fixture places an expired, higher-priority
// excluded hour and manifest ahead of the allowlist, then races the two claim
// classes against their shared cap.
func TestJoinedExactFiftyScopeClaimsOnlyListedWork(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-exact-fifty@example.test")
	defer fixture.cleanup()
	ctx := context.Background()
	s, pool := fixture.s, fixture.pool
	s.cfg.JoinedRecordingControlPlaneEnabled = true
	s.cfg.JoinedRecordingProtocolVersion = 1
	s.cfg.JoinedRecordingMaxActiveTasks = 2
	s.cfg.JoinedWorkerBootstrapToken = "joined-bootstrap-credential-32bytes"
	s.cfg.JoinedWorkerSigningKey = "joined-signing-credential-32-bytes"
	s.cfg.R2Endpoint, s.cfg.R2Bucket, s.cfg.R2Region = "https://output.example.test", "joined-output", "auto"
	s.cfg.R2AccessKeyID, s.cfg.R2SecretAccessKey = "output-key", "output-secret"

	req := fixture.req
	req.Apply, req.ExpectedRequestSHA256 = true, fixture.plan.RequestSHA256
	if response, _ := fixture.call(req); response.Code != http.StatusOK {
		t.Fatalf("apply status=%d body=%s", response.Code, response.Body.String())
	}
	finishJoinedTier1Fixture(t, fixture, req)
	var batchRecordID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM recording_joined_batches WHERE batch_id=$1`, req.BatchID).Scan(&batchRecordID); err != nil {
		t.Fatal(err)
	}
	_, _, _, insertFinalChild := materializeJoinedCanonicalBatch(t, fixture, batchRecordID)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertFinalChild(tx); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	validation, err := s.startJoinedFinalValidation(ctx, joinedFinalValidationStartRequest{ProtocolVersion: 1,
		BatchID: req.BatchID, ExpectedFrozenDenominatorSHA256: fixture.plan.FrozenDenominatorSHA256})
	if err != nil {
		t.Fatal(err)
	}
	for validation.State != "ready" {
		if validation.NextOrdinal == nil {
			t.Fatalf("validation stalled: %+v", validation)
		}
		validation, err = s.stepJoinedFinalValidation(ctx, joinedFinalValidationStepRequest{ProtocolVersion: 1,
			RunID: validation.RunID, Ordinal: *validation.NextOrdinal})
		if err != nil {
			t.Fatal(err)
		}
	}
	freezeTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := freezeTx.Exec(ctx, `UPDATE recording_joined_batches SET freeze_started_at=clock_timestamp() WHERE id=$1`, batchRecordID); err != nil {
		_ = freezeTx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := freezeTx.Exec(ctx, `UPDATE recording_joined_batches SET state='frozen',frozen_at=clock_timestamp() WHERE id=$1`, batchRecordID); err != nil {
		_ = freezeTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := freezeTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	s.cfg.JoinedRecordingConnectionID = int(fixture.connectionID)
	s.cfg.JoinedRecordingBatchID = req.BatchID
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=FALSE,updated_at=clock_timestamp()
		WHERE batch_record_id=$1`, batchRecordID); err != nil {
		t.Fatal(err)
	}

	var sourceHour string
	var sourceRecordingID int64
	if err := pool.QueryRow(ctx, `SELECT hour_id,recording_id FROM recording_joined_hours
		WHERE batch_record_id=$1 AND source_clip_count>0 ORDER BY priority_ordinal,id LIMIT 1`, batchRecordID).
		Scan(&sourceHour, &sourceRecordingID); err != nil {
		t.Fatal(err)
	}
	rows, err := pool.Query(ctx, `SELECT hour_id FROM recording_joined_hours WHERE batch_record_id=$1 AND hour_id<>$2
		ORDER BY priority_ordinal DESC,id DESC LIMIT 49`, batchRecordID, sourceHour)
	if err != nil {
		t.Fatal(err)
	}
	var selected []string
	for rows.Next() {
		var hourID string
		if err := rows.Scan(&hourID); err != nil {
			t.Fatal(err)
		}
		selected = append(selected, hourID)
	}
	rows.Close()
	if rows.Err() != nil || len(selected) != 49 {
		t.Fatalf("source-hour candidates=%d err=%v", len(selected), rows.Err())
	}
	allowlist := append([]string{sourceHour}, selected...)
	sort.Strings(allowlist)
	var excludedHour string
	if err := pool.QueryRow(ctx, `SELECT hour_id FROM recording_joined_hours WHERE batch_record_id=$1 AND source_clip_count=0
		AND recording_id<>$3 AND NOT (hour_id=ANY($2::text[])) ORDER BY priority_ordinal,id LIMIT 1`, batchRecordID,
		allowlist, sourceRecordingID).Scan(&excludedHour); err != nil {
		t.Fatal(err)
	}

	// Publish the excluded hour's dependency and create its manifest through
	// the normal APIs. Once expired, both rows would outrank the allowlisted
	// work if any claim query ignored the exact scope.
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeSingleCanary
	s.cfg.JoinedRecordingCanaryHourIDs = excludedHour
	excludedToken := joinedGapBootstrapToken(t, s, req.BatchID)
	excludedLedger := joinedFiftyPublicationClaim(t, s, req.BatchID, excludedToken)
	if excludedLedger.Ledger == nil {
		t.Fatalf("excluded dependency claim=%+v", excludedLedger)
	}
	joinedFiftyFinalizeLedger(t, s, excludedLedger)
	excludedManifest := joinedFiftyPublicationClaim(t, s, req.BatchID, excludedToken)
	if excludedManifest.Hour == nil || excludedManifest.Hour.HourID != excludedHour {
		t.Fatalf("excluded manifest claim=%+v want=%q", excludedManifest, excludedHour)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts DISABLE TRIGGER recording_joined_artifact_update_guard`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_lease_expires_at=now()-interval '1 minute'
		WHERE hour_record_id=(SELECT id FROM recording_joined_hours WHERE hour_id=$1) AND artifact_kind='hour_manifest'`, excludedHour); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts ENABLE TRIGGER recording_joined_artifact_update_guard`); err != nil {
		t.Fatal(err)
	}

	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeAllowlist50
	s.cfg.JoinedRecordingCanaryHourIDs = strings.Join(allowlist, ",")
	if err := s.cfg.ValidateJoined(); err != nil {
		t.Fatal(err)
	}
	claimToken := joinedGapBootstrapToken(t, s, req.BatchID)
	if len(claimToken) >= 1024 {
		t.Fatalf("claim token length=%d", len(claimToken))
	}
	firstLedger := joinedFiftyPublicationClaim(t, s, req.BatchID, claimToken)
	if firstLedger.Ledger == nil || !joinedLedgerTouchesAllowlist(firstLedger.Ledger.Ledger, allowlist) {
		t.Fatalf("first allowlist ledger escaped scope: %+v", firstLedger)
	}
	joinedFiftyFinalizeLedger(t, s, firstLedger)

	type result struct {
		kind string
		code int
		body []byte
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, kind := range []string{"preflight", "publication"} {
		kind := kind
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			body, _ := json.Marshal(joinedrecording.WorkClaimRequest{ProtocolVersion: 1, BatchID: req.BatchID,
				WorkerID: kind + "-worker", ScratchAvailableBytes: 10 << 30, TaskBudgetBytes: 5 << 30})
			path := "/api/v1/recording/joined/claim"
			handler := s.handleJoinedClaim
			if kind == "publication" {
				path = "/api/v1/recording/joined/publication/claim"
				handler = s.handleJoinedPublicationClaim
			}
			httpReq := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
			httpReq.Header.Set("Authorization", "Bearer "+claimToken)
			rec := httptest.NewRecorder()
			s.requireJoinedWorkerAuth(http.HandlerFunc(handler)).ServeHTTP(rec, httpReq)
			results <- result{kind: kind, code: rec.Code, body: rec.Body.Bytes()}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var activePublication joinedrecording.PublicationClaimResponse
	for got := range results {
		if got.code != http.StatusOK {
			t.Fatalf("concurrent %s status=%d body=%s", got.kind, got.code, got.body)
		}
		if got.kind == "preflight" {
			var claim joinedrecording.PreflightHourClaim
			if json.Unmarshal(got.body, &claim) != nil || !containsString(allowlist, claim.HourID) || claim.HourID == excludedHour {
				t.Fatalf("preflight escaped allowlist: %s", got.body)
			}
		} else {
			var claim joinedrecording.PublicationClaimResponse
			if json.Unmarshal(got.body, &claim) != nil || claim.Ledger == nil ||
				!joinedLedgerTouchesAllowlist(claim.Ledger.Ledger, allowlist) {
				t.Fatalf("publication escaped allowlist: %s", got.body)
			}
			activePublication = claim
		}
	}
	joinedFiftyPreflightClaim(t, s, req.BatchID, claimToken, "third-worker", http.StatusNoContent)
	if claim := joinedFiftyPublicationClaimStatus(t, s, req.BatchID, claimToken, http.StatusNoContent); claim.Kind != "" {
		t.Fatalf("third publication claim=%+v", claim)
	}

	// Arm a permit against both active identities, then finish one of them.
	// The next claim transaction must observe the changed identity set, consume
	// no work, and atomically return admission to paused.
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=TRUE,
		one_shot_expected_active_claims_sha256=NULL,one_shot_claims_remaining=0,updated_at=clock_timestamp()
		WHERE batch_record_id=$1`, batchRecordID); err != nil {
		t.Fatal(err)
	}
	statusTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := s.joinedAdmissionStatus(ctx, statusTx, req.BatchID)
	_ = statusTx.Rollback(ctx)
	if err != nil || status.ActiveLeaseCount != 2 || status.ActiveClaimsSHA256 == "" {
		t.Fatalf("active identity status=%+v err=%v", status, err)
	}
	oneShotBody, _ := json.Marshal(joinedrecording.ClaimAdmissionRequest{ProtocolVersion: 1, BatchID: req.BatchID,
		ClaimsPaused: true, ExpectedActiveClaimsSHA256: status.ActiveClaimsSHA256, MaxNewClaims: 1})
	oneShotReq := httptest.NewRequest(http.MethodPut, "/api/v1/recording/joined/admission", bytes.NewReader(oneShotBody))
	oneShotRec := httptest.NewRecorder()
	s.handleJoinedAdmissionSet(oneShotRec, oneShotReq)
	if oneShotRec.Code != http.StatusOK {
		t.Fatalf("arm one-shot status=%d body=%s", oneShotRec.Code, oneShotRec.Body.String())
	}
	var legacyPaused bool
	if err := pool.QueryRow(ctx, `SELECT claims_paused FROM recording_joined_admission_controls
		WHERE batch_record_id=$1`, batchRecordID).Scan(&legacyPaused); err != nil || !legacyPaused {
		t.Fatalf("legacy admission bit did not remain paused: paused=%v err=%v", legacyPaused, err)
	}
	joinedFiftyFinalizeLedger(t, s, activePublication)
	joinedFiftyPreflightClaim(t, s, req.BatchID, claimToken, "raced-worker", http.StatusNoContent)
	statusTx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err = s.joinedAdmissionStatus(ctx, statusTx, req.BatchID)
	_ = statusTx.Rollback(ctx)
	if err != nil || !status.ClaimsPaused || status.OneShotClaimsRemaining != 0 || status.ActiveLeaseCount != 1 {
		t.Fatalf("raced terminal fence status=%+v err=%v", status, err)
	}

	oneShotBody, _ = json.Marshal(joinedrecording.ClaimAdmissionRequest{ProtocolVersion: 1, BatchID: req.BatchID,
		ClaimsPaused: true, ExpectedActiveClaimsSHA256: status.ActiveClaimsSHA256, MaxNewClaims: 1})
	oneShotReq = httptest.NewRequest(http.MethodPut, "/api/v1/recording/joined/admission", bytes.NewReader(oneShotBody))
	oneShotRec = httptest.NewRecorder()
	s.handleJoinedAdmissionSet(oneShotRec, oneShotReq)
	if oneShotRec.Code != http.StatusOK {
		t.Fatalf("rearm one-shot status=%d body=%s", oneShotRec.Code, oneShotRec.Body.String())
	}
	if claim := joinedFiftyPublicationClaimStatus(t, s, req.BatchID, claimToken, http.StatusOK); claim.Kind == "" {
		t.Fatal("one-shot publication claim was empty")
	}
	statusTx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err = s.joinedAdmissionStatus(ctx, statusTx, req.BatchID)
	_ = statusTx.Rollback(ctx)
	if err != nil || !status.ClaimsPaused || status.OneShotClaimsRemaining != 0 || status.ActiveLeaseCount != 2 {
		t.Fatalf("one-shot consume status=%+v err=%v", status, err)
	}
	var excludedHourAttempt, excludedManifestAttempt int
	if err := pool.QueryRow(ctx, `SELECT h.attempt_count,a.publication_attempt_count FROM recording_joined_hours h
		JOIN recording_joined_artifacts a ON a.hour_record_id=h.id AND a.artifact_kind='hour_manifest' WHERE h.hour_id=$1`,
		excludedHour).Scan(&excludedHourAttempt, &excludedManifestAttempt); err != nil {
		t.Fatal(err)
	}
	if excludedHourAttempt != 0 || excludedManifestAttempt != 1 {
		t.Fatalf("excluded stale work was reclaimed hour_attempt=%d manifest_attempt=%d", excludedHourAttempt, excludedManifestAttempt)
	}
}

func joinedFiftyPreflightClaim(t *testing.T, s *Server, batchID, token, worker string, want int) joinedrecording.PreflightHourClaim {
	t.Helper()
	body, _ := json.Marshal(joinedrecording.WorkClaimRequest{ProtocolVersion: 1, BatchID: batchID, WorkerID: worker,
		ScratchAvailableBytes: 10 << 30, TaskBudgetBytes: 5 << 30})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/claim", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedClaim)).ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("preflight status=%d want=%d body=%s", rec.Code, want, rec.Body.String())
	}
	var claim joinedrecording.PreflightHourClaim
	if rec.Code == http.StatusOK && json.Unmarshal(rec.Body.Bytes(), &claim) != nil {
		t.Fatal("decode preflight claim")
	}
	return claim
}

func joinedFiftyPublicationClaim(t *testing.T, s *Server, batchID, token string) joinedrecording.PublicationClaimResponse {
	t.Helper()
	return joinedFiftyPublicationClaimStatus(t, s, batchID, token, http.StatusOK)
}

func joinedFiftyPublicationClaimStatus(t *testing.T, s *Server, batchID, token string, want int) joinedrecording.PublicationClaimResponse {
	t.Helper()
	body, _ := json.Marshal(joinedrecording.PublicationClaimRequest{ProtocolVersion: 1, BatchID: batchID,
		WorkerID: "publication-worker", ScratchAvailableBytes: 10 << 30, TaskBudgetBytes: 5 << 30})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/claim", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedPublicationClaim)).ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("publication status=%d want=%d body=%s", rec.Code, want, rec.Body.String())
	}
	var claim joinedrecording.PublicationClaimResponse
	if rec.Code == http.StatusOK && json.Unmarshal(rec.Body.Bytes(), &claim) != nil {
		t.Fatal("decode publication claim")
	}
	return claim
}

func joinedFiftyFinalizeLedger(t *testing.T, s *Server, claim joinedrecording.PublicationClaimResponse) {
	t.Helper()
	ledger := claim.Ledger
	_, objectKey, err := joinedrecording.CanonicalAllocationLedgerPaths(ledger.BatchID, ledger.Ledger.RecordingID,
		ledger.Ledger.LocalDate)
	if err != nil {
		t.Fatal(err)
	}
	s.joinedOutputStorage = joinedOutputStoreStub{head: r2.ObjectHead{ETag: "allowlist-ledger-etag", SizeBytes: ledger.ExpectedSize}}
	body, _ := json.Marshal(joinedrecording.FinalizeLedgerRequest{ProtocolVersion: 1, Published: joinedrecording.PublishedLedger{
		ArtifactID: ledger.ArtifactID, ObjectKey: objectKey, ETag: "allowlist-ledger-etag", SizeBytes: ledger.ExpectedSize,
		SHA256: ledger.ExpectedSHA256}})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/ledger/finalize", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+ledger.OperationToken)
	rec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeLedger)).ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("ledger finalize status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func joinedLedgerTouchesAllowlist(ledger joinedrecording.StreamDayAllocation, allowlist []string) bool {
	for _, hour := range ledger.Hours {
		hourID := fmt.Sprintf("%s__recording-%d__date-%s__hour-%02d__generation-%d", ledger.BatchID,
			ledger.RecordingID, ledger.LocalDate, hour.DeliveryHour, ledger.Generation)
		if containsString(allowlist, hourID) {
			return true
		}
	}
	return false
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}
