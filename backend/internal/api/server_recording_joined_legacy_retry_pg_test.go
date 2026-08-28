package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/config"
)

// TestJoinedFrozenBatchLegacyRetryFence exercises the real PostgreSQL claim
// lifecycle. The production row numbers name the four legacy shapes that
// prompted this fence; claim eligibility depends only on their persisted
// lifecycle state, so one canonical source hour is reset to each exact shape.
func TestJoinedFrozenBatchLegacyRetryFence(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-legacy-retry-fence@example.test")
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
		t.Fatal(err)
	}
	if _, err := freezeTx.Exec(ctx, `UPDATE recording_joined_batches SET state='frozen',frozen_at=clock_timestamp() WHERE id=$1`, batchRecordID); err != nil {
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
	var hourID string
	if err := pool.QueryRow(ctx, `SELECT hour_id FROM recording_joined_hours WHERE batch_record_id=$1
		AND source_clip_count>0 ORDER BY priority_ordinal,id LIMIT 1`, batchRecordID).Scan(&hourID); err != nil {
		t.Fatal(err)
	}

	// Publish the source hour's allocation ledger through the worker API.
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeSingleCanary
	s.cfg.JoinedRecordingCanaryHourIDs = hourID
	canaryToken := joinedGapBootstrapToken(t, s, req.BatchID)
	ledger := joinedFiftyPublicationClaim(t, s, req.BatchID, canaryToken)
	if ledger.Ledger == nil {
		t.Fatalf("source ledger claim=%+v", ledger)
	}
	joinedFiftyFinalizeLedger(t, s, ledger)

	setHour := func(t *testing.T, state string, attempts int) {
		t.Helper()
		if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_hours DISABLE TRIGGER recording_joined_hour_update_guard`); err != nil {
			t.Fatal(err)
		}
		_, updateErr := pool.Exec(ctx, `UPDATE recording_joined_hours SET state=$2,attempt_count=$3,
			claim_token=CASE WHEN $2='leased' THEN '00000000-0000-0000-0000-000000000099'::uuid END,
			claimed_by=CASE WHEN $2='leased' THEN 'expired-legacy-worker' END,
			lease_expires_at=CASE WHEN $2='leased' THEN now()-interval '1 minute' ELSE NULL::timestamptz END,
			heartbeat_at=CASE WHEN $2='leased' THEN now()-interval '2 minutes' END,
			next_attempt_at=now()-interval '1 minute' WHERE hour_id=$1`, hourID, state, attempts)
		_, enableErr := pool.Exec(ctx, `ALTER TABLE recording_joined_hours ENABLE TRIGGER recording_joined_hour_update_guard`)
		if updateErr != nil {
			t.Fatal(updateErr)
		}
		if enableErr != nil {
			t.Fatal(enableErr)
		}
	}
	claimRace := func(t *testing.T, token string) []int {
		t.Helper()
		start := make(chan struct{})
		codes := make(chan int, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				<-start
				body := []byte(`{"protocol_version":1,"batch_id":"` + req.BatchID + `","worker_id":"legacy-race-worker-` +
					string(rune('a'+worker)) + `","scratch_available_bytes":10737418240,"task_budget_bytes":5368709120}`)
				httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/claim", bytes.NewReader(body))
				httpReq.Header.Set("Authorization", "Bearer "+token)
				recorder := httptest.NewRecorder()
				s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedClaim)).ServeHTTP(recorder, httpReq)
				codes <- recorder.Code
			}(i)
		}
		close(start)
		wg.Wait()
		close(codes)
		out := make([]int, 0, 2)
		for code := range codes {
			out = append(out, code)
		}
		return out
	}

	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	s.cfg.JoinedRecordingCanaryHourIDs = ""
	frozenToken := joinedGapBootstrapToken(t, s, req.BatchID)
	for _, legacy := range []struct {
		name     string
		state    string
		attempts int
	}{
		{name: "row2774_pending_attempt1", state: "pending", attempts: 1},
		{name: "row4549_expired_attempt1", state: "leased", attempts: 1},
		{name: "row5111_expired_attempt7", state: "leased", attempts: 7},
		{name: "row4729_exhausted_attempt8", state: "leased", attempts: 8},
	} {
		t.Run(legacy.name, func(t *testing.T) {
			setHour(t, legacy.state, legacy.attempts)
			codes := claimRace(t, frozenToken)
			if codes[0] != http.StatusNoContent || codes[1] != http.StatusNoContent {
				t.Fatalf("frozen legacy row was claimed: %v", codes)
			}
			var gotState string
			var gotAttempts int
			if err := pool.QueryRow(ctx, `SELECT state,attempt_count FROM recording_joined_hours WHERE hour_id=$1`, hourID).
				Scan(&gotState, &gotAttempts); err != nil || gotState != legacy.state || gotAttempts != legacy.attempts {
				t.Fatalf("legacy accounting changed state=%s attempts=%d err=%v", gotState, gotAttempts, err)
			}
		})
	}

	setHour(t, "pending", 0)
	cleanCodes := claimRace(t, frozenToken)
	sort.Ints(cleanCodes)
	if cleanCodes[0] != http.StatusOK || cleanCodes[1] != http.StatusNoContent {
		t.Fatalf("concurrent clean frozen claims=%v want one claim", cleanCodes)
	}

	// An operator can deliberately remediate one exact legacy row by changing
	// to canary_single and minting a scope-bound token. This path is unchanged.
	setHour(t, "leased", 1)
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeSingleCanary
	s.cfg.JoinedRecordingCanaryHourIDs = hourID
	remediationToken := joinedGapBootstrapToken(t, s, req.BatchID)
	remediated := joinedFiftyPreflightClaim(t, s, req.BatchID, remediationToken, "exact-remediation-worker", http.StatusOK)
	if remediated.HourID != hourID {
		t.Fatalf("exact remediation claim=%q want=%q", remediated.HourID, hourID)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT attempt_count FROM recording_joined_hours WHERE hour_id=$1`, hourID).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("remediation attempt_count=%d want=2 err=%v", attempts, err)
	}
}
