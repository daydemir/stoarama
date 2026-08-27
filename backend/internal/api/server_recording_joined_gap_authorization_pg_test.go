package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestJoinedLegacyGapOnlyAuthorizationFence exercises the production HTTP and
// PostgreSQL seams which made pre-scope-fence gap manifests dangerous. The
// fixture deliberately creates a canonical sealed and published gap manifest
// without an authorization row, matching the shape left by the old server.
func TestJoinedLegacyGapOnlyAuthorizationFence(t *testing.T) {
	const (
		sealedLegacyArtifactID    int64 = 9_000_101
		publishedLegacyArtifactID int64 = 9_000_102
	)
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-legacy-gap-fence@example.test")
	defer fixture.cleanup()
	ctx := context.Background()
	s, pool := fixture.s, fixture.pool
	s.cfg.JoinedRecordingControlPlaneEnabled = true
	s.cfg.JoinedRecordingMaxActiveTasks = 4
	s.cfg.JoinedWorkerBootstrapToken = "joined-bootstrap-credential-32bytes"
	s.cfg.JoinedWorkerSigningKey = "joined-signing-credential-32-bytes"
	s.cfg.JoinedOperatorToken = "joined-operator-credential-32-bytes"
	s.cfg.R2Endpoint = "https://output.example.test"
	s.cfg.R2Bucket = "joined-output"
	s.cfg.R2Region = "auto"
	s.cfg.R2AccessKeyID = "output-key"
	s.cfg.R2SecretAccessKey = "output-secret"

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
	ledgers, _, _, insertFinalChild := materializeJoinedCanonicalBatch(t, fixture, batchRecordID)
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
	validation, err := s.startJoinedFinalValidation(ctx, joinedFinalValidationStartRequest{
		ProtocolVersion: 1, BatchID: req.BatchID, ExpectedFrozenDenominatorSHA256: fixture.plan.FrozenDenominatorSHA256,
	})
	if err != nil {
		t.Fatal(err)
	}
	for validation.State != "ready" {
		if validation.NextOrdinal == nil {
			t.Fatalf("validation stalled: %+v", validation)
		}
		validation, err = s.stepJoinedFinalValidation(ctx, joinedFinalValidationStepRequest{
			ProtocolVersion: 1, RunID: validation.RunID, Ordinal: *validation.NextOrdinal,
		})
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
	if command, err := freezeTx.Exec(ctx, `UPDATE recording_joined_batches SET state='frozen',frozen_at=clock_timestamp() WHERE id=$1`, batchRecordID); err != nil || command.RowsAffected() != 1 {
		_ = freezeTx.Rollback(ctx)
		t.Fatalf("freeze rows=%d err=%v", command.RowsAffected(), err)
	}
	if err := freezeTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	s.cfg.JoinedRecordingProtocolVersion = 1
	s.cfg.JoinedRecordingConnectionID = int(fixture.connectionID)
	s.cfg.JoinedRecordingBatchID = req.BatchID
	var streamDayID int64
	var sourceHourID, gapHourID string
	if err := pool.QueryRow(ctx, `SELECT d.id,
		(SELECT hour_id FROM recording_joined_hours WHERE stream_day_id=d.id AND source_clip_count>0 ORDER BY delivery_hour LIMIT 1),
		(SELECT hour_id FROM recording_joined_hours WHERE stream_day_id=d.id AND source_clip_count=0 ORDER BY delivery_hour LIMIT 1)
		FROM recording_joined_stream_days d WHERE EXISTS(SELECT 1 FROM recording_joined_hours h WHERE h.stream_day_id=d.id AND h.source_clip_count>0)
		AND EXISTS(SELECT 1 FROM recording_joined_hours h WHERE h.stream_day_id=d.id AND h.source_clip_count=0)
		ORDER BY d.id LIMIT 1`).Scan(&streamDayID, &sourceHourID, &gapHourID); err != nil {
		t.Fatal(err)
	}
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeSingleCanary
	s.cfg.JoinedRecordingCanaryHourIDs = sourceHourID
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=FALSE,updated_at=clock_timestamp() WHERE batch_record_id=$1`, batchRecordID); err != nil {
		t.Fatal(err)
	}
	claimToken := joinedGapBootstrapToken(t, s, req.BatchID)
	publication := joinedGapPublicationClaim(t, s, req.BatchID, claimToken, http.StatusOK)
	if publication.Ledger == nil {
		t.Fatalf("claimed dependency ledger=%+v stream_day=%d", publication.Ledger, streamDayID)
	}
	_, ledgerObjectKey, err := joinedrecording.CanonicalAllocationLedgerPaths(req.BatchID,
		publication.Ledger.Ledger.RecordingID, publication.Ledger.Ledger.LocalDate)
	if err != nil {
		t.Fatal(err)
	}
	s.joinedOutputStorage = joinedOutputStoreStub{head: r2.ObjectHead{ETag: "ledger-etag", SizeBytes: publication.Ledger.ExpectedSize}}
	finalizeLedgerBody, _ := json.Marshal(joinedrecording.FinalizeLedgerRequest{ProtocolVersion: 1,
		Published: joinedrecording.PublishedLedger{ArtifactID: publication.Ledger.ArtifactID, ObjectKey: ledgerObjectKey,
			ETag: "ledger-etag", SizeBytes: publication.Ledger.ExpectedSize, SHA256: publication.Ledger.ExpectedSHA256}})
	finalizeLedgerReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/ledger/finalize", bytes.NewReader(finalizeLedgerBody))
	finalizeLedgerReq.Header.Set("Authorization", "Bearer "+publication.Ledger.OperationToken)
	finalizeLedgerRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeLedger)).ServeHTTP(finalizeLedgerRec, finalizeLedgerReq)
	if finalizeLedgerRec.Code != http.StatusNoContent {
		t.Fatalf("ledger finalize status=%d body=%s", finalizeLedgerRec.Code, finalizeLedgerRec.Body.String())
	}

	legacy := insertLegacyGapManifest(t, pool, sealedLegacyArtifactID, gapHourID, false)
	var publishedGapHourID string
	if err := pool.QueryRow(ctx, `SELECT hour_id FROM recording_joined_hours WHERE stream_day_id=$1
		AND source_clip_count=0 AND state='pending' AND hour_id<>$2 ORDER BY delivery_hour LIMIT 1`, streamDayID, gapHourID).
		Scan(&publishedGapHourID); err != nil {
		t.Fatal(err)
	}
	publishedLegacy := insertLegacyGapManifest(t, pool, publishedLegacyArtifactID, publishedGapHourID, true)
	publishLegacyGapManifest(t, pool, publishedLegacy)
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=TRUE,updated_at=clock_timestamp() WHERE batch_record_id=$1`, batchRecordID); err != nil {
		t.Fatal(err)
	}

	t.Run("scope promotion does not authorize legacy gap", func(t *testing.T) {
		s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeSingleCanary
		s.cfg.JoinedRecordingCanaryHourIDs = gapHourID
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=FALSE,updated_at=clock_timestamp() WHERE batch_record_id=$1`, batchRecordID); err != nil {
			t.Fatal(err)
		}
		joinedGapBootstrapNoWork(t, s, req.BatchID)
		joinedGapPublicationClaim(t, s, req.BatchID, mintJoinedClaimForTest(t, s, req.BatchID), http.StatusNoContent)
		s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
		s.cfg.JoinedRecordingCanaryHourIDs = ""
		frozenSHA, err := joinedFrozenScopeSHA(req.BatchID)
		if err != nil {
			t.Fatal(err)
		}
		var frozenAuthorizations int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_joined_gap_only_scope_authorizations
			WHERE artifact_id IN ($1,$2) AND work_scope_identity_sha256=$3`, legacy.artifactID, publishedLegacy.artifactID,
			frozenSHA).Scan(&frozenAuthorizations); err != nil || frozenAuthorizations != 0 {
			t.Fatalf("scope promotion granted frozen authorization count=%d err=%v", frozenAuthorizations, err)
		}
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=TRUE,updated_at=clock_timestamp() WHERE batch_record_id=$1`, batchRecordID); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("operator route rejects foreign credentials and unsafe gates", func(t *testing.T) {
		s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
		authReq := joinedGapAuthorizationRequest(legacy, req.BatchID, gapHourID)
		s.joinedOutputStorage = joinedOutputStoreStub{headErr: &smithy.GenericAPIError{Code: "NoSuchKey", Message: "missing"}}
		for name, token := range map[string]string{
			"missing": "", "bootstrap": s.cfg.JoinedWorkerBootstrapToken, "signing": s.cfg.JoinedWorkerSigningKey,
			"service": "generic-service-key",
		} {
			t.Run(name, func(t *testing.T) {
				rec := joinedGapAuthorizationHTTP(s, authReq, token)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
				}
			})
		}
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=FALSE,updated_at=clock_timestamp() WHERE batch_record_id=$1`, batchRecordID); err != nil {
			t.Fatal(err)
		}
		if rec := joinedGapAuthorizationHTTP(s, authReq, s.cfg.JoinedOperatorToken); rec.Code != http.StatusConflict {
			t.Fatalf("unpaused authorization status=%d body=%s", rec.Code, rec.Body.String())
		}
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=TRUE,updated_at=clock_timestamp() WHERE batch_record_id=$1`, batchRecordID); err != nil {
			t.Fatal(err)
		}
		bad := authReq
		bad.ExpectedSHA256 = strings.Repeat("f", 64)
		wrongArtifact := authReq
		wrongArtifact.ArtifactID = 9_000_199
		wrongHour := authReq
		wrongHour.HourID += "-foreign"
		wrongKey := authReq
		wrongKey.ObjectKey += ".foreign"
		wrongSize := authReq
		wrongSize.ExpectedSizeBytes++
		for name, candidate := range map[string]joinedGapOnlyFrozenAuthorizationRequest{
			"sha": bad, "artifact": wrongArtifact, "hour": wrongHour, "key": wrongKey, "size": wrongSize,
		} {
			t.Run("wrong "+name, func(t *testing.T) {
				if rec := joinedGapAuthorizationHTTP(s, candidate, s.cfg.JoinedOperatorToken); rec.Code != http.StatusConflict {
					t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
				}
			})
		}
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_joined_gap_only_scope_authorizations WHERE artifact_id=$1`, legacy.artifactID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("failed gates left authorization count=%d err=%v", count, err)
		}
	})

	t.Run("published legacy gap is fenced from index and NAS", func(t *testing.T) {
		frozenSHA, err := joinedFrozenScopeSHA(req.BatchID)
		if err != nil {
			t.Fatal(err)
		}
		readTx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := loadJoinedHourReferences(ctx, readTx, batchRecordID, 1, frozenSHA); err == nil {
			_ = readTx.Rollback(ctx)
			t.Fatal("batch index accepted unauthorized legacy gap")
		}
		_ = readTx.Rollback(ctx)
		if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_artifact_acks(artifact_id,connection_id,relative_path,size_bytes,sha256,verified_at)
			SELECT id,connection_id,relative_path,expected_size_bytes,expected_sha256,now() FROM recording_joined_artifacts
			WHERE stream_day_id=$1 AND artifact_kind='allocation_ledger' ON CONFLICT DO NOTHING`, streamDayID); err != nil {
			t.Fatal(err)
		}
		principal := accountPrincipal{AccountID: fixture.accountID, APIKeyID: &fixture.apiKeyID, KeyScopes: []string{accountScopePull}}
		feedReq := httptest.NewRequest(http.MethodGet, "/api/v1/account/joined", nil)
		feedReq = feedReq.WithContext(context.WithValue(feedReq.Context(), accountPrincipalContextKey, principal))
		feedRec := httptest.NewRecorder()
		s.handleAccountJoined(feedRec, feedReq)
		if feedRec.Code != http.StatusOK || strings.Contains(feedRec.Body.String(), fmt.Sprintf(`"artifact_id":%d`, publishedLegacy.artifactID)) {
			t.Fatalf("legacy gap escaped feed status=%d body=%s", feedRec.Code, feedRec.Body.String())
		}
		downloadReq := httptest.NewRequest(http.MethodGet, "/api/v1/account/joined/output/download", nil)
		route := chi.NewRouteContext()
		route.URLParams.Add("joinedId", fmt.Sprint(publishedLegacy.artifactID))
		downloadReq = downloadReq.WithContext(context.WithValue(context.WithValue(downloadReq.Context(), accountPrincipalContextKey, principal), chi.RouteCtxKey, route))
		downloadRec := httptest.NewRecorder()
		s.handleAccountJoinedDownload(downloadRec, downloadReq)
		if downloadRec.Code != http.StatusNotFound {
			t.Fatalf("legacy gap download status=%d body=%s", downloadRec.Code, downloadRec.Body.String())
		}
		ackBody, _ := json.Marshal(joinedAckRequest{ArtifactID: publishedLegacy.artifactID, RelativePath: publishedLegacy.relativePath,
			SizeBytes: publishedLegacy.size, SHA256: publishedLegacy.sha})
		ackReq := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", bytes.NewReader(ackBody))
		ackReq = ackReq.WithContext(context.WithValue(ackReq.Context(), accountPrincipalContextKey, principal))
		ackRec := httptest.NewRecorder()
		s.handleAccountJoinedAck(ackRec, ackReq)
		if ackRec.Code != http.StatusNotFound {
			t.Fatalf("legacy gap ack status=%d body=%s", ackRec.Code, ackRec.Body.String())
		}
	})

	t.Run("published legacy gap cannot finalize under widened scope", func(t *testing.T) {
		var canonical []byte
		if err := pool.QueryRow(ctx, `SELECT canonical_bytes FROM recording_joined_artifacts WHERE id=$1`,
			publishedLegacy.artifactID).Scan(&canonical); err != nil {
			t.Fatal(err)
		}
		var manifest joinedrecording.HourManifest
		if err := json.Unmarshal(canonical, &manifest); err != nil {
			t.Fatal(err)
		}
		operation, err := joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, req.BatchID, joinedauth.SubjectHour,
			publishedGapHourID, publishedLegacy.lease, joinedauth.OperationPublish, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		published := joinedrecording.PublishedHour{HourID: manifest.HourID, RecordingID: manifest.RecordingID,
			LocalDate: manifest.LocalDate, LocalHour: manifest.DeliveryHour, Outputs: []joinedrecording.PublishedOutput{},
			HourManifestObjectKey: publishedLegacy.objectKey, HourManifestETag: "legacy-etag",
			HourManifestVersionID: "legacy-version", HourManifestSizeBytes: publishedLegacy.size,
			HourManifestSHA256: publishedLegacy.sha}
		body, _ := json.Marshal(joinedrecording.FinalizeHourRequest{ProtocolVersion: 1, Published: published})
		httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/hour/finalize", bytes.NewReader(body))
		httpReq.Header.Set("Authorization", "Bearer "+operation)
		headKeys := []string{}
		s.joinedOutputStorage = joinedOutputStoreStub{headKeys: &headKeys}
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeHour)).ServeHTTP(rec, httpReq)
		if rec.Code != http.StatusUnauthorized || len(headKeys) != 0 {
			t.Fatalf("unauthorized legacy finalize status=%d heads=%v body=%s", rec.Code, headKeys, rec.Body.String())
		}
	})

	testLegacyPublishingBatchIndex := func(t *testing.T) {
		const indexArtifactID int64 = 9_000_201
		index := insertLegacyPublishingBatchIndex(t, pool, batchRecordID, indexArtifactID, publishedLegacy.artifactID)
		operation, err := joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, req.BatchID,
			joinedauth.SubjectBatchIndex, req.BatchID, index.lease, joinedauth.OperationPublish, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		var beforeState, beforeToken string
		var beforeHeartbeat time.Time
		var beforeAttempts, beforeFailures int
		if err := pool.QueryRow(ctx, `SELECT publication_state,publication_token::text,publication_heartbeat_at,
			publication_attempt_count,(SELECT count(*) FROM recording_joined_worker_failures WHERE batch_record_id=$2
			  AND scope_kind='batch_index' AND scope_id=$3)
			FROM recording_joined_artifacts WHERE id=$1`, index.artifactID, batchRecordID, req.BatchID).
			Scan(&beforeState, &beforeToken, &beforeHeartbeat, &beforeAttempts, &beforeFailures); err != nil {
			t.Fatal(err)
		}
		headKeys := []string{}
		s.joinedOutputStorage = joinedOutputStoreStub{headKeys: &headKeys}
		published := joinedrecording.PublishedBatchIndex{ArtifactID: index.artifactID, ObjectKey: index.objectKey,
			ETag: "stale-index-etag", SizeBytes: index.size, SHA256: index.sha}
		requests := []struct {
			name, path string
			body       any
			handler    http.HandlerFunc
		}{
			{"operation", "/api/v1/recording/joined/test-operation", map[string]any{}, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }},
			{"heartbeat", "/api/v1/recording/joined/heartbeat", joinedrecording.HeartbeatRequest{ProtocolVersion: 1, ScopeKind: "batch_index", ScopeID: req.BatchID}, s.handleJoinedHeartbeat},
			{"failure", "/api/v1/recording/joined/failure", joinedrecording.WorkFailureRequest{ProtocolVersion: 1, ScopeKind: "batch_index", ScopeID: req.BatchID, FailureClass: "transient", ReasonCode: "worker_task_deadline"}, s.handleJoinedFailure},
			{"capability", "/api/v1/recording/joined/capabilities/artifact", joinedrecording.ArtifactCapabilityRequest{ProtocolVersion: 1, ScopeKind: "batch_index", ScopeID: req.BatchID, ArtifactID: index.artifactID, Operation: "put"}, s.handleJoinedArtifactCapability},
			{"finalize", "/api/v1/recording/joined/publication/index/finalize", joinedrecording.FinalizeBatchIndexRequest{ProtocolVersion: 1, Published: published}, s.handleJoinedFinalizeBatchIndex},
		}
		for _, tc := range requests {
			t.Run(tc.name, func(t *testing.T) {
				body, _ := json.Marshal(tc.body)
				httpReq := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(body))
				httpReq.Header.Set("Authorization", "Bearer "+operation)
				rec := httptest.NewRecorder()
				s.requireJoinedWorkerAuth(tc.handler).ServeHTTP(rec, httpReq)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
				}
			})
		}
		var afterState, afterToken string
		var afterHeartbeat time.Time
		var afterAttempts, afterFailures, refs int
		if err := pool.QueryRow(ctx, `SELECT publication_state,publication_token::text,publication_heartbeat_at,
			publication_attempt_count,(SELECT count(*) FROM recording_joined_worker_failures WHERE batch_record_id=$2
			  AND scope_kind='batch_index' AND scope_id=$3),
			(SELECT count(*) FROM recording_joined_batch_index_refs WHERE index_artifact_id=$1)
			FROM recording_joined_artifacts WHERE id=$1`, index.artifactID, batchRecordID, req.BatchID).
			Scan(&afterState, &afterToken, &afterHeartbeat, &afterAttempts, &afterFailures, &refs); err != nil {
			t.Fatal(err)
		}
		if afterState != beforeState || afterToken != beforeToken || !afterHeartbeat.Equal(beforeHeartbeat) ||
			afterAttempts != beforeAttempts || afterFailures != beforeFailures || refs != 1 || len(headKeys) != 0 {
			t.Fatalf("stale index mutated state=%s token_same=%t heartbeat_same=%t attempts=%d failures=%d refs=%d heads=%v",
				afterState, afterToken == beforeToken, afterHeartbeat.Equal(beforeHeartbeat), afterAttempts, afterFailures, refs, headKeys)
		}
	}

	t.Run("automatic gap authorization failure rolls back seal", func(t *testing.T) {
		var rollbackHourID string
		if err := pool.QueryRow(ctx, `SELECT hour_id FROM recording_joined_hours WHERE stream_day_id=$1
			AND source_clip_count=0 AND state='pending' ORDER BY delivery_hour LIMIT 1`, streamDayID).Scan(&rollbackHourID); err != nil {
			t.Fatal(err)
		}
		var ledgerArtifactID int64
		if err := pool.QueryRow(ctx, `SELECT id FROM recording_joined_artifacts WHERE stream_day_id=$1
			AND artifact_kind='allocation_ledger' AND publication_state='published'`, streamDayID).Scan(&ledgerArtifactID); err != nil {
			t.Fatal(err)
		}
		s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeSingleCanary
		s.cfg.JoinedRecordingCanaryHourIDs = rollbackHourID
		scope, err := s.joinedWorkScopeIdentity()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `CREATE FUNCTION joined_test_reject_gap_authorization() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN RAISE EXCEPTION 'forced gap authorization failure'; END $$;
			CREATE TRIGGER joined_test_reject_gap_authorization BEFORE INSERT ON recording_joined_gap_only_scope_authorizations
			FOR EACH ROW EXECUTE FUNCTION joined_test_reject_gap_authorization()`); err != nil {
			t.Fatal(err)
		}
		sealTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := sealJoinedGapOnlyHoursTx(ctx, sealTx, ledgerArtifactID, scope, int(fixture.connectionID)); err == nil {
			_ = sealTx.Rollback(ctx)
			t.Fatal("forced authorization failure allowed gap seal")
		}
		_ = sealTx.Rollback(ctx)
		if _, err := pool.Exec(ctx, `DROP TRIGGER joined_test_reject_gap_authorization ON recording_joined_gap_only_scope_authorizations;
			DROP FUNCTION joined_test_reject_gap_authorization()`); err != nil {
			t.Fatal(err)
		}
		var state string
		var artifacts, authorizations int
		if err := pool.QueryRow(ctx, `SELECT h.state,
			(SELECT count(*) FROM recording_joined_artifacts a WHERE a.hour_record_id=h.id),
			(SELECT count(*) FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.hour_record_id=h.id)
			FROM recording_joined_hours h WHERE h.hour_id=$1`, rollbackHourID).Scan(&state, &artifacts, &authorizations); err != nil ||
			state != "pending" || artifacts != 0 || authorizations != 0 {
			t.Fatalf("failed seal residue state=%s artifacts=%d authorizations=%d err=%v", state, artifacts, authorizations, err)
		}
		s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
		s.cfg.JoinedRecordingCanaryHourIDs = ""
	})

	t.Run("exact frozen authorization admits only target", func(t *testing.T) {
		preview := joinedGapAuthorizationRequest(legacy, req.BatchID, gapHourID)
		s.joinedOutputStorage = joinedOutputStoreStub{headErr: &smithy.GenericAPIError{Code: "NoSuchKey", Message: "missing"}}
		previewRec := joinedGapAuthorizationHTTP(s, preview, s.cfg.JoinedOperatorToken)
		var previewResponse joinedGapOnlyFrozenAuthorizationResponse
		if previewRec.Code != http.StatusOK || json.Unmarshal(previewRec.Body.Bytes(), &previewResponse) != nil || previewResponse.Applied {
			t.Fatalf("preview authorization status=%d body=%s", previewRec.Code, previewRec.Body.String())
		}
		apply := preview
		apply.Apply = true
		apply.ReviewEvidenceSHA256 = previewResponse.ReviewEvidenceSHA256
		if rec := joinedGapAuthorizationHTTP(s, apply, s.cfg.JoinedOperatorToken); rec.Code != http.StatusOK {
			t.Fatalf("exact authorization status=%d body=%s", rec.Code, rec.Body.String())
		}
		frozenSHA, err := joinedFrozenScopeSHA(req.BatchID)
		if err != nil {
			t.Fatal(err)
		}
		var targetEligible, siblingEligible bool
		if err := pool.QueryRow(ctx, `SELECT
			EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations WHERE artifact_id=$1 AND work_scope_identity_sha256=$3),
			EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations WHERE artifact_id=$2 AND work_scope_identity_sha256=$3)`,
			legacy.artifactID, publishedLegacy.artifactID, frozenSHA).Scan(&targetEligible, &siblingEligible); err != nil ||
			!targetEligible || siblingEligible {
			t.Fatalf("frozen exact eligibility target=%v sibling=%v err=%v", targetEligible, siblingEligible, err)
		}
	})
	t.Run("active worker lease blocks operator preview", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=FALSE,updated_at=clock_timestamp()
			WHERE batch_record_id=$1`, batchRecordID); err != nil {
			t.Fatal(err)
		}
		claimToken := joinedGapBootstrapToken(t, s, req.BatchID)
		body, _ := json.Marshal(joinedrecording.WorkClaimRequest{ProtocolVersion: 1, BatchID: req.BatchID,
			WorkerID: "active-gate-worker", ScratchAvailableBytes: 10 << 30, TaskBudgetBytes: 5 << 30})
		httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/claim", bytes.NewReader(body))
		httpReq.Header.Set("Authorization", "Bearer "+claimToken)
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedClaim)).ServeHTTP(rec, httpReq)
		if rec.Code != http.StatusOK {
			t.Fatalf("active lease claim status=%d body=%s", rec.Code, rec.Body.String())
		}
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=TRUE,updated_at=clock_timestamp()
			WHERE batch_record_id=$1`, batchRecordID); err != nil {
			t.Fatal(err)
		}
		var canonical []byte
		if err := pool.QueryRow(ctx, `SELECT canonical_bytes FROM recording_joined_artifacts WHERE id=$1`,
			publishedLegacy.artifactID).Scan(&canonical); err != nil {
			t.Fatal(err)
		}
		s.joinedOutputStorage = joinedOutputStoreStub{body: canonical}
		preview := joinedGapAuthorizationRequest(publishedLegacy, req.BatchID, publishedGapHourID)
		if out := joinedGapAuthorizationHTTP(s, preview, s.cfg.JoinedOperatorToken); out.Code != http.StatusConflict {
			t.Fatalf("active lease authorization status=%d body=%s", out.Code, out.Body.String())
		}
	})
	t.Run("preexisting publishing batch index cannot reuse stale operation authority", testLegacyPublishingBatchIndex)

	_ = ledgers
}

type joinedLegacyGap struct {
	artifactID                   int64
	relativePath, objectKey, sha string
	size                         int64
	lease                        uuid.UUID
}

func insertLegacyGapManifest(t *testing.T, pool *pgxpool.Pool, artifactID int64, hourID string, publishing bool) joinedLegacyGap {
	t.Helper()
	ctx := context.Background()
	var hourRecordID int64
	var batchID, timezone, folder, localDate string
	var generation, recordingID int64
	var localHour int
	var metadataJSON, qualificationJSON, mediaToolJSON, ledgerJSON []byte
	var ledgerArtifactID int64
	var ledgerSHA, sourceSHA string
	if err := pool.QueryRow(ctx, `SELECT h.id,h.batch_id,b.generation,h.recording_id,br.timezone,br.folder_name,
		h.local_date::text,h.delivery_hour,br.naming_metadata,br.qualification,b.media_tool,d.ledger_bytes,d.ledger_sha256,
		h.source_claim_sha256,ledger.id FROM recording_joined_hours h JOIN recording_joined_batches b ON b.id=h.batch_record_id
		JOIN recording_joined_stream_days d ON d.id=h.stream_day_id JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
		JOIN recording_joined_artifacts ledger ON ledger.stream_day_id=d.id AND ledger.artifact_kind='allocation_ledger'
		WHERE h.hour_id=$1 AND h.state='pending' AND h.source_clip_count=0`, hourID).Scan(&hourRecordID, &batchID, &generation,
		&recordingID, &timezone, &folder, &localDate, &localHour, &metadataJSON, &qualificationJSON, &mediaToolJSON,
		&ledgerJSON, &ledgerSHA, &sourceSHA, &ledgerArtifactID); err != nil {
		t.Fatal(err)
	}
	var metadata recordingnaming.Metadata
	var qualification joinedrecording.QualificationWindow
	var mediaTool joinedrecording.MediaToolEvidence
	var ledger joinedrecording.StreamDayAllocation
	if json.Unmarshal(metadataJSON, &metadata) != nil || json.Unmarshal(qualificationJSON, &qualification) != nil ||
		json.Unmarshal(mediaToolJSON, &mediaTool) != nil || json.Unmarshal(ledgerJSON, &ledger) != nil {
		t.Fatal("decode legacy gap fixture")
	}
	plan, err := joinedrecording.BuildGapOnlyHourPlan(joinedrecording.PlanRequest{BatchID: batchID, Generation: int(generation),
		RecordingID: recordingID, Timezone: timezone, FolderName: folder, Metadata: metadata, Qualification: qualification,
		AllocationLedgerSHA: ledgerSHA, MediaTool: mediaTool}, localDate, localHour, "scheduled_source_gap")
	if err != nil || plan.HourID != hourID || plan.SourceClaimSHA256 != sourceSHA {
		t.Fatalf("build legacy plan hour=%s err=%v", plan.HourID, err)
	}
	allocation, err := joinedrecording.BuildHourManifestAllocation(ledgerArtifactID, plan, ledger)
	if err != nil {
		t.Fatal(err)
	}
	_, manifestBytes, manifestSHA, err := joinedrecording.BuildHourManifest(joinedrecording.HourManifestInput{Plan: plan,
		Allocation: allocation, AllocationLedger: ledger})
	if err != nil {
		t.Fatal(err)
	}
	planJSON, _ := json.Marshal(plan)
	sealTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sealTx.Exec(ctx, `UPDATE recording_joined_hours SET state='sealed',source_only_sha256=$2,canonical_plan=$3,
		manifest_bytes=$4,manifest_sha256=$5,sealed_at=now() WHERE id=$1 AND state='pending'`, hourRecordID, plan.SourceClaimSHA256,
		planJSON, manifestBytes, manifestSHA); err != nil {
		_ = sealTx.Rollback(ctx)
		t.Fatal(err)
	}
	relative := "coverage/hours/" + hourID + ".json"
	legacy := joinedLegacyGap{artifactID: artifactID, relativePath: relative, objectKey: "joined/" + batchID + "/" + relative,
		sha: manifestSHA, size: int64(len(manifestBytes)), lease: uuid.New()}
	state := "sealed"
	if publishing {
		state = "publishing"
	}
	if _, err := sealTx.Exec(ctx, `INSERT INTO recording_joined_artifacts(id,batch_record_id,account_id,connection_id,batch_id,
		scope_kind,scope_id,stream_day_id,hour_record_id,artifact_kind,ordinal,relative_path,object_key,content_type,
		expected_size_bytes,expected_sha256,canonical_bytes,publication_state,publication_attempt_count,publication_token,
		publication_claimed_by,publication_lease_expires_at,publication_heartbeat_at)
		SELECT $9,batch_record_id,account_id,connection_id,batch_id,'hour',hour_id,stream_day_id,id,'hour_manifest',1,$2,$3,
		'application/json',$4,$5,$6,$7,CASE WHEN $7='publishing' THEN 1 ELSE 0 END,
		CASE WHEN $7='publishing' THEN $8::uuid ELSE NULL::uuid END,CASE WHEN $7='publishing' THEN 'legacy-worker' ELSE NULL END,
		CASE WHEN $7='publishing' THEN now()+interval '5 minutes' ELSE NULL END,CASE WHEN $7='publishing' THEN now() ELSE NULL END
		FROM recording_joined_hours WHERE id=$1`, hourRecordID, relative, legacy.objectKey, legacy.size, legacy.sha,
		manifestBytes, state, legacy.lease, artifactID); err != nil {
		_ = sealTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := sealTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return legacy
}

func publishLegacyGapManifest(t *testing.T, pool *pgxpool.Pool, legacy joinedLegacyGap) {
	t.Helper()
	ctx := context.Background()
	// Reproduce a row which reached published before migration 0146 existed.
	// Disable only the new 0146 trigger inside this disposable test schema;
	// the original artifact transition guard still validates the old legal
	// publishing-to-published transition.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `ALTER TABLE recording_joined_artifacts
		DISABLE TRIGGER recording_joined_gap_only_publication_transition_guard`); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='published',
		publication_token=NULL,publication_claimed_by=NULL,publication_lease_expires_at=NULL,publication_heartbeat_at=NULL,
		finalized_token=$2,etag='legacy-etag',version_id='legacy-version',published_at=now()
		WHERE id=$1`, legacy.artifactID, legacy.lease); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `ALTER TABLE recording_joined_artifacts
		ENABLE TRIGGER recording_joined_gap_only_publication_transition_guard`); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func insertLegacyPublishingBatchIndex(t *testing.T, pool *pgxpool.Pool, batchRecordID, artifactID,
	referencedArtifactID int64) joinedLegacyGap {
	t.Helper()
	ctx := context.Background()
	canonical := []byte(`{"legacy_pre_scope_index":true}`)
	index := joinedLegacyGap{artifactID: artifactID, relativePath: "coverage/batch.json",
		sha: sha256Bytes(canonical), size: int64(len(canonical)), lease: uuid.New()}
	var batchID string
	if err := pool.QueryRow(ctx, `SELECT batch_id FROM recording_joined_batches WHERE id=$1`, batchRecordID).Scan(&batchID); err != nil {
		t.Fatal(err)
	}
	index.objectKey = "joined/" + batchID + "/" + index.relativePath
	// Reproduce an index and reference that existed before migration 0146. The
	// disposable schema disables only the two insertion fences that did not
	// exist then, and restores them after committing the fixture. PostgreSQL
	// refuses ALTER TABLE while the insert transaction has deferred events.
	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts DISABLE TRIGGER recording_joined_artifact_insert_guard;
		ALTER TABLE recording_joined_batch_index_refs DISABLE TRIGGER recording_joined_gap_only_batch_index_ref_guard`); err != nil {
		t.Fatal(err)
	}
	triggersDisabled := true
	defer func() {
		if triggersDisabled {
			_, _ = pool.Exec(ctx, `ALTER TABLE recording_joined_batch_index_refs ENABLE TRIGGER recording_joined_gap_only_batch_index_ref_guard;
				ALTER TABLE recording_joined_artifacts ENABLE TRIGGER recording_joined_artifact_insert_guard`)
		}
	}()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_artifacts(id,batch_record_id,account_id,connection_id,batch_id,
		scope_kind,scope_id,artifact_kind,ordinal,relative_path,object_key,content_type,expected_size_bytes,expected_sha256,
		canonical_bytes,publication_state,publication_attempt_count,publication_token,publication_claimed_by,
		publication_lease_expires_at,publication_heartbeat_at)
		SELECT $2,id,account_id,connection_id,batch_id,'batch_index',batch_id,'batch_index',1,$3,$4,'application/json',
		$5,$6,$7,'publishing',1,$8,'legacy-index-worker',now()+interval '5 minutes',now()
		FROM recording_joined_batches WHERE id=$1`, batchRecordID, artifactID, index.relativePath, index.objectKey,
		index.size, index.sha, canonical, index.lease); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_batch_index_refs(batch_record_id,index_artifact_id,
		referenced_artifact_id,reference_kind,ordinal) VALUES($1,$2,$3,'hour_manifest',1)`,
		batchRecordID, artifactID, referencedArtifactID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_batch_index_refs ENABLE TRIGGER recording_joined_gap_only_batch_index_ref_guard;
		ALTER TABLE recording_joined_artifacts ENABLE TRIGGER recording_joined_artifact_insert_guard`); err != nil {
		t.Fatal(err)
	}
	triggersDisabled = false
	return index
}

func joinedGapAuthorizationRequest(legacy joinedLegacyGap, batchID, hourID string) joinedGapOnlyFrozenAuthorizationRequest {
	return joinedGapOnlyFrozenAuthorizationRequest{ProtocolVersion: 1, BatchID: batchID, ArtifactID: legacy.artifactID,
		HourID: hourID, ObjectKey: legacy.objectKey, ExpectedSizeBytes: legacy.size, ExpectedSHA256: legacy.sha,
		IncidentID: "pg-fixture-review-2026-08-27"}
}

func joinedGapAuthorizationHTTP(s *Server, request joinedGapOnlyFrozenAuthorizationRequest, token string) *httptest.ResponseRecorder {
	body, _ := json.Marshal(request)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/recording/joined/gap-only/frozen-authorization", bytes.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.requireJoinedOperatorAuth(http.HandlerFunc(s.handleJoinedGapOnlyFrozenAuthorization)).ServeHTTP(rec, req)
	return rec
}

func joinedGapBootstrapToken(t *testing.T, s *Server, batchID string) string {
	t.Helper()
	scope, err := s.joinedWorkScopeIdentity()
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(joinedrecording.WorkerBootstrapRequest{ProtocolVersion: 1, BatchID: batchID, WorkScopeIdentity: scope})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/token", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+s.cfg.JoinedWorkerBootstrapToken)
	rec := httptest.NewRecorder()
	s.requireJoinedWorkerBootstrapAuth(http.HandlerFunc(s.handleJoinedToken)).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("bootstrap status=%d body=%s", rec.Code, rec.Body.String())
	}
	var response joinedrecording.WorkerBootstrapResponse
	if json.Unmarshal(rec.Body.Bytes(), &response) != nil {
		t.Fatal("decode bootstrap")
	}
	return response.ClaimToken
}

func joinedGapBootstrapNoWork(t *testing.T, s *Server, batchID string) {
	t.Helper()
	scope, err := s.joinedWorkScopeIdentity()
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(joinedrecording.WorkerBootstrapRequest{ProtocolVersion: 1, BatchID: batchID, WorkScopeIdentity: scope})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/token", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+s.cfg.JoinedWorkerBootstrapToken)
	rec := httptest.NewRecorder()
	s.requireJoinedWorkerBootstrapAuth(http.HandlerFunc(s.handleJoinedToken)).ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("bootstrap escaped legacy gap status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func joinedGapPublicationClaim(t *testing.T, s *Server, batchID, token string, want int) joinedrecording.PublicationClaimResponse {
	t.Helper()
	body, _ := json.Marshal(joinedrecording.PublicationClaimRequest{ProtocolVersion: 1, BatchID: batchID, WorkerID: "gap-test-worker",
		ScratchAvailableBytes: 10 << 30, TaskBudgetBytes: 5 << 30})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/claim", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedPublicationClaim)).ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("publication claim status=%d want=%d body=%s", rec.Code, want, rec.Body.String())
	}
	var response joinedrecording.PublicationClaimResponse
	if rec.Code == http.StatusOK && json.Unmarshal(rec.Body.Bytes(), &response) != nil {
		t.Fatal("decode publication claim")
	}
	return response
}
