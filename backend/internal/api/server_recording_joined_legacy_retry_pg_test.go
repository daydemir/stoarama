package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

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
	s.cfg.JoinedRecordingFrozenExcludedPublicationArtifactIDs = joinedFrozenPublicationDenyForTest
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
	ledgers, sourceLedger, _, insertFinalChild := materializeJoinedCanonicalBatch(t, fixture, batchRecordID)
	if sourceLedger == nil {
		t.Fatal("source ledger fixture is required")
	}
	legacyArtifactIDs := []int64{468, 469, 470, 471}
	legacyLedgerIndexes := make([]int, 0, len(legacyArtifactIDs))
	for i := range ledgers {
		if ledgers[i].streamDayID != sourceLedger.streamDayID {
			legacyLedgerIndexes = append(legacyLedgerIndexes, i)
			if len(legacyLedgerIndexes) == len(legacyArtifactIDs) {
				break
			}
		}
	}
	if len(legacyLedgerIndexes) != len(legacyArtifactIDs) {
		t.Fatal("legacy publication fixtures are required")
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts DISABLE TRIGGER recording_joined_artifact_update_guard`); err != nil {
		t.Fatal(err)
	}
	for i, ledgerIndex := range legacyLedgerIndexes {
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET id=$2 WHERE id=$1`,
			ledgers[ledgerIndex].artifactID, legacyArtifactIDs[i]); err != nil {
			t.Fatal(err)
		}
		ledgers[ledgerIndex].artifactID = legacyArtifactIDs[i]
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts ENABLE TRIGGER recording_joined_artifact_update_guard`); err != nil {
		t.Fatal(err)
	}
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

	// Leave only the three legacy artifacts and one clean artifact immediately
	// due under frozen_batch. The source ledger is held for the hour test below.
	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts DISABLE TRIGGER recording_joined_artifact_update_guard`); err != nil {
		t.Fatal(err)
	}
	_, publishErr := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='published',
		finalized_token='00000000-0000-0000-0000-000000000077',etag='fixture-published',version_id='',published_at=now()
		WHERE batch_record_id=$1 AND artifact_kind='allocation_ledger' AND NOT (id=ANY($2::bigint[]))
		  AND stream_day_id<>$3`, batchRecordID, legacyArtifactIDs, sourceLedger.streamDayID)
	_, sourceDelayErr := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_next_attempt_at=now()+interval '1 hour'
		WHERE batch_record_id=$1 AND artifact_kind IN ('allocation_ledger','batch_index')
		  AND (stream_day_id=$2 OR artifact_kind='batch_index' OR id=471)`, batchRecordID, sourceLedger.streamDayID)
	_, enableArtifactErr := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts ENABLE TRIGGER recording_joined_artifact_update_guard`)
	if publishErr != nil {
		t.Fatal(publishErr)
	}
	if sourceDelayErr != nil {
		t.Fatal(sourceDelayErr)
	}
	if enableArtifactErr != nil {
		t.Fatal(enableArtifactErr)
	}
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	s.cfg.JoinedRecordingCanaryHourIDs = ""
	joinedGapBootstrapNoWork(t, s, req.BatchID)
	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts DISABLE TRIGGER recording_joined_artifact_update_guard`); err != nil {
		t.Fatal(err)
	}
	_, cleanDueErr := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_next_attempt_at=now()-interval '1 minute'
		WHERE id=471`)
	_, liveFenceErr := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='publishing',
		publication_attempt_count=1,publication_token='00000000-0000-0000-0000-000000000070',
		publication_claimed_by='live-fenced-worker',publication_lease_expires_at=now()+interval '5 minutes',
		publication_heartbeat_at=now() WHERE id=470`)
	_, enableArtifactErr = pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts ENABLE TRIGGER recording_joined_artifact_update_guard`)
	if cleanDueErr != nil {
		t.Fatal(cleanDueErr)
	}
	if liveFenceErr != nil {
		t.Fatal(liveFenceErr)
	}
	if enableArtifactErr != nil {
		t.Fatal(enableArtifactErr)
	}
	publicationToken := joinedGapBootstrapToken(t, s, req.BatchID)
	s.cfg.JoinedRecordingMaxActiveTasks = 1
	if claim := joinedFiftyPublicationClaimStatus(t, s, req.BatchID, publicationToken, http.StatusNoContent); claim.Kind != "" {
		t.Fatalf("live fenced lease escaped global cap: %+v", claim)
	}
	s.cfg.JoinedRecordingMaxActiveTasks = 2
	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts DISABLE TRIGGER recording_joined_artifact_update_guard`); err != nil {
		t.Fatal(err)
	}
	_, resetLiveErr := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='sealed',
		publication_attempt_count=0,publication_token=NULL,publication_claimed_by=NULL,
		publication_lease_expires_at=NULL,publication_heartbeat_at=NULL WHERE id=470`)
	_, enableArtifactErr = pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts ENABLE TRIGGER recording_joined_artifact_update_guard`)
	if resetLiveErr != nil {
		t.Fatal(resetLiveErr)
	}
	if enableArtifactErr != nil {
		t.Fatal(enableArtifactErr)
	}
	cleanPublication := joinedFiftyPublicationClaim(t, s, req.BatchID, publicationToken)
	if cleanPublication.Ledger == nil || cleanPublication.Ledger.ArtifactID != 471 {
		t.Fatalf("frozen publication claim artifact=%+v want clean artifact 471", cleanPublication)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts DISABLE TRIGGER recording_joined_artifact_update_guard`); err != nil {
		t.Fatal(err)
	}
	_, cleanPublishedErr := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='published',
		finalized_token=publication_token,publication_token=NULL,publication_claimed_by=NULL,
		publication_lease_expires_at=NULL,publication_heartbeat_at=NULL,etag='fixture-published',version_id='',published_at=now()
		WHERE id=471`)
	_, expiredFenceErr := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='publishing',
		publication_attempt_count=8,publication_token='00000000-0000-0000-0000-000000000069',
		publication_claimed_by='expired-fenced-worker',publication_lease_expires_at=now()-interval '1 minute',
		publication_heartbeat_at=now()-interval '2 minutes' WHERE id=469`)
	_, enableArtifactErr = pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts ENABLE TRIGGER recording_joined_artifact_update_guard`)
	if cleanPublishedErr != nil {
		t.Fatal(cleanPublishedErr)
	}
	if expiredFenceErr != nil {
		t.Fatal(expiredFenceErr)
	}
	if enableArtifactErr != nil {
		t.Fatal(enableArtifactErr)
	}
	if hours, artifacts, err := s.recordJoinedExpiredAttemptEvidence(ctx, req.BatchID, nil, true); err != nil || hours != 0 || artifacts != 0 {
		t.Fatalf("frozen expired maintenance hours=%d artifacts=%d err=%v", hours, artifacts, err)
	}
	var expiredState string
	var expiredAttempts, expiredFailures int
	if err := pool.QueryRow(ctx, `SELECT publication_state,publication_attempt_count,
		(SELECT count(*) FROM recording_joined_worker_failures WHERE artifact_id=469)
		FROM recording_joined_artifacts WHERE id=469`).Scan(&expiredState, &expiredAttempts, &expiredFailures); err != nil ||
		expiredState != "publishing" || expiredAttempts != 8 || expiredFailures != 0 {
		t.Fatalf("expired fenced artifact mutated state=%s attempts=%d failures=%d err=%v",
			expiredState, expiredAttempts, expiredFailures, err)
	}
	if claim := joinedFiftyPublicationClaimStatus(t, s, req.BatchID, publicationToken, http.StatusNoContent); claim.Kind != "" {
		t.Fatalf("frozen publication escaped legacy fence: %+v", claim)
	}
	var legacyUnchanged, legacyAttempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE publication_state='sealed'),
		COALESCE(sum(publication_attempt_count),0) FROM recording_joined_artifacts WHERE id=ANY($1::bigint[])`,
		[]int64{468, 470}).Scan(&legacyUnchanged, &legacyAttempts); err != nil || legacyUnchanged != 2 || legacyAttempts != 0 {
		t.Fatalf("legacy publication artifacts mutated sealed=%d attempts=%d err=%v", legacyUnchanged, legacyAttempts, err)
	}

	// Exact canary scope remains the explicit remediation authority.
	var legacyHourID string
	if err := pool.QueryRow(ctx, `SELECT h.hour_id FROM recording_joined_artifacts a
		JOIN recording_joined_hours h ON h.stream_day_id=a.stream_day_id WHERE a.id=468
		ORDER BY h.delivery_hour LIMIT 1`).Scan(&legacyHourID); err != nil {
		t.Fatal(err)
	}
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeSingleCanary
	s.cfg.JoinedRecordingCanaryHourIDs = legacyHourID
	legacyToken := joinedGapBootstrapToken(t, s, req.BatchID)
	legacyClaim := joinedFiftyPublicationClaim(t, s, req.BatchID, legacyToken)
	if legacyClaim.Ledger == nil || legacyClaim.Ledger.ArtifactID != 468 {
		t.Fatalf("canary remediation artifact=%+v want 468", legacyClaim)
	}
	joinedFiftyFinalizeLedger(t, s, legacyClaim)

	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts DISABLE TRIGGER recording_joined_artifact_update_guard`); err != nil {
		t.Fatal(err)
	}
	_, sourceDueErr := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_next_attempt_at=now()-interval '1 minute'
		WHERE batch_record_id=$1 AND artifact_kind='allocation_ledger' AND stream_day_id=$2`, batchRecordID, sourceLedger.streamDayID)
	_, enableArtifactErr = pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts ENABLE TRIGGER recording_joined_artifact_update_guard`)
	if sourceDueErr != nil {
		t.Fatal(sourceDueErr)
	}
	if enableArtifactErr != nil {
		t.Fatal(enableArtifactErr)
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
	var claimedBy, claimToken string
	var leaseExpires time.Time
	if err := pool.QueryRow(ctx, `SELECT attempt_count,claimed_by,claim_token::text,lease_expires_at
		FROM recording_joined_hours WHERE hour_id=$1`, hourID).Scan(&attempts, &claimedBy, &claimToken, &leaseExpires); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || claimedBy != "exact-remediation-worker" ||
		claimToken == "00000000-0000-0000-0000-000000000099" || claimToken == "" || !leaseExpires.After(time.Now()) {
		t.Fatalf("remediation lease attempts=%d claimed_by=%q token_replaced=%v future_expiry=%v",
			attempts, claimedBy, claimToken != "00000000-0000-0000-0000-000000000099", leaseExpires.After(time.Now()))
	}
}
