package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const joinedTestSourceEndpoint = "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com"

// This uses the real migration in a disposable PostgreSQL schema when the
// standard repository test URL is configured.
type joinedCanonicalLedgerFixture struct {
	artifactID, streamDayID, batchRecordingID int64
	recordingID                               int64
	scopeID, relativePath, objectKey          string
	bytes                                     []byte
	sha                                       string
}

type joinedCanonicalSnapshot struct {
	id, streamDayID, storageDestinationID int64
	dayOrdinal                            int
	source                                joinedrecording.SourceClip
	clipCreatedAt                         time.Time
}

func TestJoinedFinalFreezeRecomputesFrozenDenominatorAndIsAdminOnly(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-final-freeze@example.test")
	defer fixture.cleanup()
	ctx := context.Background()
	req := fixture.req
	req.Apply, req.ExpectedRequestSHA256 = true, fixture.plan.RequestSHA256
	if response, _ := fixture.call(req); response.Code != http.StatusOK {
		t.Fatalf("apply status=%d body=%s", response.Code, response.Body.String())
	}
	var batchRecordID int64
	if err := fixture.pool.QueryRow(ctx, `SELECT id FROM recording_joined_batches WHERE batch_id=$1`, req.BatchID).
		Scan(&batchRecordID); err != nil {
		t.Fatal(err)
	}
	ledgers, _, _, insertFinalChild := materializeJoinedCanonicalBatch(t, fixture, batchRecordID)
	if len(ledgers) != 462 || insertFinalChild == nil {
		t.Fatalf("materialized ledgers=%d final=%v", len(ledgers), insertFinalChild != nil)
	}
	childTx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertFinalChild(childTx); err != nil {
		_ = childTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := childTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	fixture.s.cfg.ServiceToken = "generic-service-credential-32-bytes"
	freezeRequest := joinedFinalFreezeRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		BatchID: req.BatchID, ExpectedFrozenDenominatorSHA256: fixture.plan.FrozenDenominatorSHA256}
	call := func(request joinedFinalFreezeRequest, cookie bool, token string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(request)
		httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/recording/joined/batches/final-freeze", bytes.NewReader(body))
		if cookie {
			httpReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: fixture.sessionToken})
		}
		if token != "" {
			httpReq.Header.Set("Authorization", "Bearer "+token)
		}
		recorder := httptest.NewRecorder()
		fixture.s.requireAdminAuth(http.HandlerFunc(fixture.s.handleAdminJoinedFinalFreeze)).ServeHTTP(recorder, httpReq)
		return recorder
	}
	claim := mintJoinedClaimForTest(t, fixture.s, req.BatchID)
	operation, err := joinedauth.MintOperation(fixture.s.cfg.JoinedWorkerSigningKey, req.BatchID, joinedauth.SubjectHour,
		"foreign-hour", uuid.New(), joinedauth.OperationPreflight, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for name, token := range map[string]string{"missing": "", "generic service": fixture.s.cfg.ServiceToken,
		"joined bootstrap": fixture.s.cfg.JoinedWorkerBootstrapToken, "joined claim": claim, "joined operation": operation} {
		t.Run("rejects "+name, func(t *testing.T) {
			if response := call(freezeRequest, false, token); response.Code != http.StatusUnauthorized {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	wrong := freezeRequest
	wrong.ExpectedFrozenDenominatorSHA256 = strings.Repeat("f", 64)
	if response := call(wrong, true, ""); response.Code != http.StatusConflict {
		t.Fatalf("wrong denominator status=%d body=%s", response.Code, response.Body.String())
	}
	var state string
	var freezeStarted bool
	if err := fixture.pool.QueryRow(ctx, `SELECT state,freeze_started_at IS NOT NULL FROM recording_joined_batches
		WHERE id=$1`, batchRecordID).Scan(&state, &freezeStarted); err != nil || state != "building" || freezeStarted {
		t.Fatalf("failed final freeze leaked state=%s started=%v err=%v", state, freezeStarted, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=0 WHERE id=$1`, fixture.connectionID); err != nil {
		t.Fatal(err)
	}
	if response := call(freezeRequest, true, ""); response.Code != http.StatusConflict {
		t.Fatalf("protocol-0 final freeze status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=1 WHERE id=$1`, fixture.connectionID); err != nil {
		t.Fatal(err)
	}
	lockTx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.Exec(ctx, `SELECT id FROM recording_joined_batches WHERE id=$1 FOR UPDATE`, batchRecordID); err != nil {
		_ = lockTx.Rollback(ctx)
		t.Fatal(err)
	}
	lockedResult := make(chan *httptest.ResponseRecorder, 1)
	go func() { lockedResult <- call(freezeRequest, true, "") }()
	select {
	case response := <-lockedResult:
		if response.Code != http.StatusConflict {
			_ = lockTx.Rollback(ctx)
			t.Fatalf("locked final freeze status=%d body=%s", response.Code, response.Body.String())
		}
	case <-time.After(joinedFinalFreezeLockTimeout + 2*time.Second):
		_ = lockTx.Rollback(ctx)
		t.Fatal("locked final freeze exceeded its server-owned lock bound")
	}
	if err := fixture.pool.QueryRow(ctx, `SELECT state,freeze_started_at IS NOT NULL FROM recording_joined_batches
		WHERE id=$1`, batchRecordID).Scan(&state, &freezeStarted); err != nil || state != "building" || freezeStarted {
		_ = lockTx.Rollback(ctx)
		t.Fatalf("locked final freeze leaked state=%s started=%v err=%v", state, freezeStarted, err)
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `CREATE FUNCTION joined_test_delay_final_freeze() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF OLD.state='building' AND NEW.state='frozen' THEN PERFORM pg_sleep(0.5); END IF; RETURN NEW; END $$;
		CREATE TRIGGER zzz_joined_test_delay_final_freeze BEFORE UPDATE OF state ON recording_joined_batches
		FOR EACH ROW EXECUTE FUNCTION joined_test_delay_final_freeze()`); err != nil {
		t.Fatal(err)
	}

	results := make(chan *httptest.ResponseRecorder, 2)
	go func() { results <- call(freezeRequest, true, "") }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var validating bool
		if err := fixture.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
			WHERE datname=current_database() AND state='active'
			AND query LIKE 'UPDATE recording_joined_batches SET state=''frozen''%')`).Scan(&validating); err != nil {
			t.Fatal(err)
		}
		if validating {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("final freeze never entered bounded validation")
		}
		time.Sleep(10 * time.Millisecond)
	}
	type heartbeatResult struct {
		elapsed time.Duration
		err     error
	}
	heartbeat := make(chan heartbeatResult, 1)
	go func() {
		started := time.Now()
		_, err := fixture.pool.Exec(ctx, `UPDATE connections SET last_seen_at=now() WHERE id=$1`, fixture.connectionID)
		heartbeat <- heartbeatResult{elapsed: time.Since(started), err: err}
	}()
	go func() { results <- call(freezeRequest, true, "") }()
	var frozen joinedFinalFreezeResponse
	alreadyFrozen := 0
	for i := 0; i < 2; i++ {
		response := <-results
		var candidate joinedFinalFreezeResponse
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &candidate) != nil ||
			candidate.State != "frozen" || candidate.FrozenAt.IsZero() ||
			candidate.FrozenDenominatorSHA256 != fixture.plan.FrozenDenominatorSHA256 ||
			candidate.RecordingCount != 33 || candidate.StreamDayCount != 462 || candidate.ScheduledHourCount != 5544 {
			t.Fatalf("concurrent final freeze status=%d body=%s", response.Code, response.Body.String())
		}
		if candidate.AlreadyFrozen {
			alreadyFrozen++
		} else {
			frozen = candidate
		}
	}
	if alreadyFrozen != 1 || frozen.FrozenAt.IsZero() {
		t.Fatalf("concurrent final freeze replays=%d frozen_at=%v", alreadyFrozen, frozen.FrozenAt)
	}
	heartbeatOutcome := <-heartbeat
	if heartbeatOutcome.err != nil || heartbeatOutcome.elapsed < 100*time.Millisecond ||
		heartbeatOutcome.elapsed > joinedFinalFreezeStatementTimeout+time.Second {
		t.Fatalf("raw heartbeat final-freeze wait=%s err=%v", heartbeatOutcome.elapsed, heartbeatOutcome.err)
	}
	replay := call(freezeRequest, true, "")
	var replayed joinedFinalFreezeResponse
	if replay.Code != http.StatusOK || json.Unmarshal(replay.Body.Bytes(), &replayed) != nil ||
		!replayed.AlreadyFrozen || !replayed.FrozenAt.Equal(frozen.FrozenAt) {
		t.Fatalf("final freeze replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	t.Log("JOINED_FINAL_FREEZE_EXECUTED")
}

type joinedCanonicalDay struct {
	id, batchRecordingID, accountID, connectionID, recordingID int64
	batchID, localDate, timezone                               string
	dateOrdinal, generation, priorityOrdinal                   int
	qualification                                              joinedrecording.QualificationWindow
	sources                                                    []joinedCanonicalSnapshot
	ledger                                                     joinedrecording.StreamDayAllocation
}

func materializeJoinedCanonicalBatch(t *testing.T, fixture joinedHistoricalTier1Fixture, batchRecordID int64) (
	[]joinedCanonicalLedgerFixture, *joinedCanonicalLedgerFixture, *joinedCanonicalLedgerFixture, func(pgx.Tx) error) {
	t.Helper()
	ctx, pool := context.Background(), fixture.pool
	rows, err := pool.Query(ctx, `SELECT d.id,d.batch_recording_id,d.account_id,d.connection_id,d.batch_id,d.recording_id,
		d.local_date::text,d.date_ordinal,b.generation,br.priority_ordinal,br.timezone,br.qualification
		FROM recording_joined_stream_days d JOIN recording_joined_batches b ON b.id=d.batch_record_id
		JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
		WHERE d.batch_record_id=$1 AND d.state='pending' ORDER BY br.priority_ordinal,d.date_ordinal`, batchRecordID)
	if err != nil {
		t.Fatal(err)
	}
	days := make([]joinedCanonicalDay, 0, 462)
	for rows.Next() {
		var day joinedCanonicalDay
		var qualificationJSON []byte
		if err := rows.Scan(&day.id, &day.batchRecordingID, &day.accountID, &day.connectionID, &day.batchID,
			&day.recordingID, &day.localDate, &day.dateOrdinal, &day.generation, &day.priorityOrdinal, &day.timezone,
			&qualificationJSON); err != nil || json.Unmarshal(qualificationJSON, &day.qualification) != nil {
			rows.Close()
			t.Fatalf("load canonical stream day: %v", err)
		}
		days = append(days, day)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if len(days) != 462 {
		t.Fatalf("applied canonical days=%d", len(days))
	}
	dayIndex := make(map[int64]int, len(days))
	for i := range days {
		dayIndex[days[i].id] = i
	}
	snapshotRows, err := pool.Query(ctx, `SELECT id,stream_day_id,clip_id,recording_id,recording_job_id,
		storage_destination_id,day_ordinal,provider,endpoint,region,bucket,object_key,ingest_etag,size_bytes,sha256,
		start_at,end_at,clip_created_at,released_at FROM recording_joined_source_snapshots
		WHERE batch_record_id=$1 ORDER BY recording_id,recording_job_id,day_ordinal`, batchRecordID)
	if err != nil {
		t.Fatal(err)
	}
	for snapshotRows.Next() {
		var snapshot joinedCanonicalSnapshot
		if err := snapshotRows.Scan(&snapshot.id, &snapshot.streamDayID, &snapshot.source.ClipID,
			&snapshot.source.RecordingID, &snapshot.source.RecordingJobID, &snapshot.storageDestinationID,
			&snapshot.dayOrdinal, &snapshot.source.Provider, &snapshot.source.Endpoint, &snapshot.source.Region,
			&snapshot.source.Bucket, &snapshot.source.Object.Key, &snapshot.source.Object.ETag,
			&snapshot.source.Object.SizeBytes, &snapshot.source.Object.SHA256, &snapshot.source.StartUTC,
			&snapshot.source.EndUTC, &snapshot.clipCreatedAt, &snapshot.source.ReleasedAt); err != nil {
			snapshotRows.Close()
			t.Fatal(err)
		}
		snapshot.source.StorageDestinationID = snapshot.storageDestinationID
		snapshot.source.StartUTC, snapshot.source.EndUTC = snapshot.source.StartUTC.UTC(), snapshot.source.EndUTC.UTC()
		if snapshot.source.ReleasedAt != nil {
			released := snapshot.source.ReleasedAt.UTC()
			snapshot.source.ReleasedAt = &released
		}
		index, ok := dayIndex[snapshot.streamDayID]
		if !ok {
			snapshotRows.Close()
			t.Fatalf("snapshot %d has foreign stream day %d", snapshot.id, snapshot.streamDayID)
		}
		days[index].sources = append(days[index].sources, snapshot)
	}
	if err := snapshotRows.Err(); err != nil {
		t.Fatal(err)
	}
	snapshotRows.Close()
	byRecordingOrdinal := make(map[[2]int64]*joinedCanonicalDay, len(days))
	for i := range days {
		byRecordingOrdinal[[2]int64{days[i].recordingID, int64(days[i].dateOrdinal)}] = &days[i]
	}
	for i := range days {
		day := &days[i]
		sources := make([]joinedrecording.SourceClip, len(day.sources))
		for j := range day.sources {
			sources[j] = day.sources[j].source
		}
		request := joinedrecording.PlanRequest{BatchID: day.batchID, Generation: day.generation,
			RecordingID: day.recordingID, Timezone: day.timezone, LocalDate: day.localDate,
			Qualification: day.qualification, Sources: sources}
		if previous := byRecordingOrdinal[[2]int64{day.recordingID, int64(day.dateOrdinal - 1)}]; previous != nil && len(previous.sources) > 0 {
			value := previous.sources[len(previous.sources)-1].source
			request.PreviousDayLast = &value
		}
		if next := byRecordingOrdinal[[2]int64{day.recordingID, int64(day.dateOrdinal + 1)}]; next != nil && len(next.sources) > 0 {
			value := next.sources[0].source
			request.NextDayFirst = &value
		}
		var draft joinedrecording.StreamDayDraft
		if len(sources) == 0 {
			draft, err = joinedrecording.BuildGapOnlyStreamDay(request, day.localDate)
		} else {
			draft, err = joinedrecording.AllocateStreamDay(request)
		}
		if err != nil {
			t.Fatalf("allocate recording=%d day=%s: %v", day.recordingID, day.localDate, err)
		}
		day.ledger, err = joinedrecording.SealStreamDayAllocation(draft)
		if err != nil {
			t.Fatalf("seal recording=%d day=%s: %v", day.recordingID, day.localDate, err)
		}
	}
	// Put one unrelated gap-only ledger first for publication/feed assertions,
	// followed by the source-bearing ledger used by the worker lifecycle.
	order := make([]int, 0, len(days))
	order = append(order, 14, 0)
	for i := range days {
		if i != 14 && i != 0 && i != len(days)-1 {
			order = append(order, i)
		}
	}
	order = append(order, len(days)-1)
	ledgers := make([]joinedCanonicalLedgerFixture, 0, len(days))
	var sourceLedger, gapAtomicLedger *joinedCanonicalLedgerFixture
	var insertFinalChild func(pgx.Tx) error
	for position, dayIndex := range order {
		day := &days[dayIndex]
		ledgerBytes, ledgerSHA, err := joinedrecording.CanonicalAllocationLedgerArtifact(day.ledger)
		if err != nil {
			t.Fatal(err)
		}
		relativePath, objectKey, err := joinedrecording.CanonicalAllocationLedgerPaths(day.batchID, day.recordingID, day.localDate)
		if err != nil {
			t.Fatal(err)
		}
		ledgers = append(ledgers, joinedCanonicalLedgerFixture{streamDayID: day.id,
			batchRecordingID: day.batchRecordingID, recordingID: day.recordingID,
			scopeID: fmt.Sprintf("%s__recording-%d__date-%s__generation-%d", day.batchID, day.recordingID,
				day.localDate, day.generation), relativePath: relativePath, objectKey: objectKey, bytes: ledgerBytes, sha: ledgerSHA})
		ledgerFixture := &ledgers[len(ledgers)-1]
		deferFinalChild := position == len(order)-1
		hours := prepareJoinedCanonicalDay(t, fixture, batchRecordID, day, deferFinalChild)
		if len(day.sources) > 0 {
			assertJoinedCanonicalSourceMutations(t, fixture, batchRecordID, day, ledgerFixture, hours)
		}
		insertArtifact := func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,connection_id,
				batch_id,scope_kind,scope_id,stream_day_id,artifact_kind,ordinal,relative_path,object_key,content_type,
				expected_size_bytes,expected_sha256,canonical_bytes,publication_state)
				VALUES($1,$2,$3,$4,'ledger',$5,$6,'allocation_ledger',1,$7,$8,'application/json',$9,$10,$11,'sealed') RETURNING id`,
				batchRecordID, day.accountID, day.connectionID, day.batchID, ledgerFixture.scopeID, day.id,
				ledgerFixture.relativePath, ledgerFixture.objectKey, len(ledgerFixture.bytes), ledgerFixture.sha,
				ledgerFixture.bytes).Scan(&ledgerFixture.artifactID)
		}
		completeDay := func(tx pgx.Tx) error {
			if err := insertJoinedCanonicalSources(ctx, tx, batchRecordID, day, hours, false, false); err != nil {
				return err
			}
			return sealJoinedCanonicalDay(ctx, tx, day, ledgerFixture)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if deferFinalChild {
			_ = tx.Rollback(ctx)
			insertFinalChild = func(tx pgx.Tx) error {
				if err := insertJoinedCanonicalCrossDayBoundary(ctx, tx, day.id, 2,
					day.ledger.CrossDayBoundaries[1]); err != nil {
					return err
				}
				if err := completeDay(tx); err != nil {
					return err
				}
				return insertArtifact(tx)
			}
		} else {
			if err := completeDay(tx); err != nil {
				_ = tx.Rollback(ctx)
				t.Fatalf("complete recording=%d day=%s: %v", day.recordingID, day.localDate, err)
			}
			if err := insertArtifact(tx); err != nil {
				_ = tx.Rollback(ctx)
				t.Fatalf("insert ledger recording=%d day=%s: %v", day.recordingID, day.localDate, err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("commit recording=%d day=%s: %v", day.recordingID, day.localDate, err)
			}
		}
		if len(day.sources) > 0 {
			sourceLedger = ledgerFixture
		}
		if position == 2 {
			gapAtomicLedger = ledgerFixture
		}
	}
	return ledgers, sourceLedger, gapAtomicLedger, insertFinalChild
}

func prepareJoinedCanonicalDay(t *testing.T, fixture joinedHistoricalTier1Fixture, batchRecordID int64,
	day *joinedCanonicalDay, deferFinalBoundary bool) map[int]int64 {
	t.Helper()
	ctx := context.Background()
	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	hours := make(map[int]int64, 12)
	sourceByID := make(map[int64]joinedrecording.SourceClip, len(day.ledger.Sources))
	for _, source := range day.ledger.Sources {
		sourceByID[source.ClipID] = source
	}
	for i, hour := range day.ledger.Hours {
		var sourceBytes int64
		for _, clipID := range hour.SourceClipIDs {
			sourceBytes += sourceByID[clipID].Object.SizeBytes
		}
		hourID := fmt.Sprintf("%s__recording-%d__date-%s__hour-%02d__generation-%d", day.batchID,
			day.recordingID, day.localDate, hour.DeliveryHour, day.generation)
		scheduledStart := day.qualification.Days[day.dateOrdinal-1].WindowStart.Add(time.Duration(i) * time.Hour)
		var hourRecordID int64
		if err := tx.QueryRow(ctx, `INSERT INTO recording_joined_hours(batch_record_id,stream_day_id,account_id,
			connection_id,batch_id,recording_id,hour_id,local_date,delivery_hour,clock_hour,scheduled_start_at,
			scheduled_end_at,priority_ordinal,source_clip_count,source_bytes,source_claim_sha256)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING id`, batchRecordID,
			day.id, day.accountID, day.connectionID, day.batchID, day.recordingID, hourID, day.localDate,
			hour.DeliveryHour, hour.ClockHour, scheduledStart, scheduledStart.Add(time.Hour),
			int64((day.priorityOrdinal-1)*168+(day.dateOrdinal-1)*12+hour.DeliveryHour), len(hour.SourceClipIDs),
			sourceBytes, day.ledger.HourSourceSHA256[i]).Scan(&hourRecordID); err != nil {
			t.Fatal(err)
		}
		hours[hour.DeliveryHour] = hourRecordID
	}
	for i, boundary := range day.ledger.Boundaries {
		if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_day_boundaries(stream_day_id,boundary_kind,ordinal,
			previous_delivery_hour,next_delivery_hour,previous_clip_id,next_clip_id,previous_presentation_end_at,
			next_presentation_start_at,signed_gap_nanoseconds,scheduled_at,actual_seam_at,boundary_skew_nanoseconds,
			allocation_decision,verdict,reason) VALUES($1,'cross_hour',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
			day.id, i+1, boundary.PreviousHour, boundary.NextHour, boundary.PreviousClipID, boundary.NextClipID,
			boundary.PreviousPresentationEndUTC, boundary.NextPresentationStartUTC, boundary.SignedGapNanoseconds,
			boundary.ScheduledUTC, boundary.ActualSeamUTC, boundary.BoundarySkewNanoseconds, boundary.AllocationDecision,
			boundary.Verdict, boundary.Reason); err != nil {
			t.Fatal(err)
		}
	}
	for i, boundary := range day.ledger.CrossDayBoundaries {
		if deferFinalBoundary && i == 1 {
			continue
		}
		if err := insertJoinedCanonicalCrossDayBoundary(ctx, tx, day.id, i+1, boundary); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return hours
}

func insertJoinedCanonicalCrossDayBoundary(ctx context.Context, tx pgx.Tx, streamDayID int64, ordinal int,
	boundary joinedrecording.CrossDayBoundary) error {
	_, err := tx.Exec(ctx, `INSERT INTO recording_joined_day_boundaries(stream_day_id,boundary_kind,ordinal,
		previous_clip_id,next_clip_id,previous_presentation_end_at,next_presentation_start_at,signed_gap_nanoseconds,
		scheduled_previous_end_at,scheduled_next_start_at,boundary_skew_nanoseconds,allocation_decision,verdict,reason)
		VALUES($1,'cross_day',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, streamDayID, ordinal,
		boundary.PreviousClipID, boundary.NextClipID, boundary.PreviousPresentationEndUTC,
		boundary.NextPresentationStartUTC, boundary.SignedGapNanoseconds, boundary.ScheduledPreviousEndUTC,
		boundary.ScheduledNextStartUTC, boundary.BoundarySkewNanoseconds, boundary.AllocationDecision,
		boundary.Verdict, boundary.Reason)
	return err
}

func insertJoinedCanonicalSources(ctx context.Context, tx pgx.Tx, batchRecordID int64, day *joinedCanonicalDay,
	hours map[int]int64, wrongHour, fabricatedSeam bool) error {
	snapshots := make(map[int64]joinedCanonicalSnapshot, len(day.sources))
	for _, snapshot := range day.sources {
		snapshots[snapshot.source.ClipID] = snapshot
	}
	dayOrdinal := 0
	for hourIndex, hour := range day.ledger.Hours {
		for hourOrdinal, source := range hour.SourceClipIDs {
			dayOrdinal++
			snapshot := snapshots[source]
			ledgerSource := day.ledger.Sources[dayOrdinal-1]
			seam, _ := json.Marshal(ledgerSource.SeamToPrevious)
			if fabricatedSeam {
				seam = []byte(`{"verdict":"fabricated","reason":"fabricated","signed_gap_nanoseconds":0}`)
			}
			deliveryHour := hourIndex + 1
			if wrongHour {
				deliveryHour = deliveryHour%12 + 1
			}
			if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_sources(source_snapshot_id,batch_record_id,
				stream_day_id,hour_record_id,account_id,connection_id,recording_id,recording_job_id,clip_id,
				storage_destination_id,day_ordinal,hour_ordinal,provider,endpoint,region,bucket,object_key,version_id,
				etag,size_bytes,sha256,start_at,end_at,seam_to_previous,clip_created_at,released_at,observed_at)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,'',$18,$19,$20,$21,$22,$23,$24,$25,clock_timestamp())`,
				snapshot.id, batchRecordID, day.id, hours[deliveryHour], day.accountID, day.connectionID,
				ledgerSource.RecordingID, ledgerSource.RecordingJobID, ledgerSource.ClipID,
				ledgerSource.StorageDestinationID, dayOrdinal, hourOrdinal+1, ledgerSource.Provider,
				ledgerSource.Endpoint, ledgerSource.Region, ledgerSource.Bucket, ledgerSource.Object.Key,
				ledgerSource.Object.ETag, ledgerSource.Object.SizeBytes, ledgerSource.Object.SHA256,
				ledgerSource.StartUTC, ledgerSource.EndUTC, seam, snapshot.clipCreatedAt, ledgerSource.ReleasedAt); err != nil {
				return err
			}
		}
	}
	return nil
}

func sealJoinedCanonicalDay(ctx context.Context, tx pgx.Tx, day *joinedCanonicalDay,
	ledgerFixture *joinedCanonicalLedgerFixture) error {
	var ledger joinedrecording.StreamDayAllocation
	if err := json.Unmarshal(ledgerFixture.bytes, &ledger); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE recording_joined_stream_days SET state='sealed',source_manifest_sha256=$2,
		head_manifest_sha256=$3,seal_request_sha256=$4,ledger_sha256=$5,ledger_bytes=$6,
		ledger_artifact_sha256=$7,sealed_at=clock_timestamp() WHERE id=$1 AND state='pending'`, day.id,
		ledger.SourceClaimSHA256, sha256Bytes([]byte("canonical-head:"+ledger.LedgerSHA256)),
		sha256Bytes([]byte("canonical-seal:"+ledger.LedgerSHA256)), ledger.LedgerSHA256, ledgerFixture.bytes,
		ledgerFixture.sha)
	return err
}

func assertJoinedCanonicalSourceMutations(t *testing.T, fixture joinedHistoricalTier1Fixture, batchRecordID int64,
	day *joinedCanonicalDay, original *joinedCanonicalLedgerFixture, hours map[int]int64) {
	t.Helper()
	ctx := context.Background()
	var originalLedger joinedrecording.StreamDayAllocation
	if err := json.Unmarshal(original.bytes, &originalLedger); err != nil {
		t.Fatal(err)
	}
	rejectRoot := func(name string, body []byte, artifactSHA, ledgerSHA string) {
		t.Run(name, func(t *testing.T) {
			tx, err := fixture.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if err := insertJoinedCanonicalSources(ctx, tx, batchRecordID, day, hours, false, false); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, `UPDATE recording_joined_stream_days SET state='sealed',source_manifest_sha256=$2,
				head_manifest_sha256=$3,seal_request_sha256=$4,ledger_sha256=$5,ledger_bytes=$6,
				ledger_artifact_sha256=$7,sealed_at=clock_timestamp() WHERE id=$1 AND state='pending'`, day.id,
				originalLedger.SourceClaimSHA256, sha256Bytes([]byte("canonical-head:"+ledgerSHA)),
				sha256Bytes([]byte("canonical-seal:"+ledgerSHA)), ledgerSHA, body, artifactSHA); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, `SELECT validate_recording_joined_stream_day($1)`, day.id); err == nil {
				t.Fatal("accepted invalid frozen source root")
			}
		})
	}
	changedRoot := originalLedger
	changedRoot.FrozenSourceSHA256 = strings.Repeat("9", 64)
	changedRoot.LedgerSHA256 = ""
	changedRoot.LedgerSHA256, _, _ = stitchcert.CanonicalSHA(changedRoot)
	changedArtifactSHA, changedBody, err := stitchcert.CanonicalSHA(changedRoot)
	if err != nil {
		t.Fatal(err)
	}
	rejectRoot("rebuilt ledger rejects changed frozen_source_sha256", changedBody, changedArtifactSHA,
		changedRoot.LedgerSHA256)
	nullBody := bytes.Replace(original.bytes,
		[]byte(`"frozen_source_sha256":"`+originalLedger.FrozenSourceSHA256+`"`),
		[]byte(`"frozen_source_sha256":null`), 1)
	if bytes.Equal(nullBody, original.bytes) {
		t.Fatal("frozen source root fixture was not replaced")
	}
	rejectRoot("ledger rejects null frozen_source_sha256", nullBody, sha256Bytes(nullBody), originalLedger.LedgerSHA256)
	for name, mutate := range map[string]func(*joinedrecording.SourceClip){
		"storage_destination_id": func(source *joinedrecording.SourceClip) { source.StorageDestinationID++ },
		"released_at": func(source *joinedrecording.SourceClip) {
			changed := source.EndUTC.Add(time.Second)
			source.ReleasedAt = &changed
		},
	} {
		t.Run("rebuilt ledger rejects changed "+name, func(t *testing.T) {
			changed := day.sources[0].source
			mutate(&changed)
			draft, err := joinedrecording.AllocateStreamDay(joinedrecording.PlanRequest{BatchID: day.batchID,
				Generation: day.generation, RecordingID: day.recordingID, Timezone: day.timezone,
				LocalDate: day.localDate, Qualification: day.qualification, Sources: []joinedrecording.SourceClip{changed}})
			if err != nil {
				t.Fatal(err)
			}
			ledger, err := joinedrecording.SealStreamDayAllocation(draft)
			if err != nil {
				t.Fatal(err)
			}
			body, artifactSHA, err := joinedrecording.CanonicalAllocationLedgerArtifact(ledger)
			if err != nil {
				t.Fatal(err)
			}
			changedFixture := *original
			changedFixture.bytes, changedFixture.sha = body, artifactSHA
			tx, err := fixture.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := insertJoinedCanonicalSources(ctx, tx, batchRecordID, day, hours, false, false); err != nil {
				_ = tx.Rollback(ctx)
				t.Fatal(err)
			}
			if err := sealJoinedCanonicalDay(ctx, tx, day, &changedFixture); err != nil {
				_ = tx.Rollback(ctx)
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, `SELECT validate_recording_joined_stream_day($1)`, day.id); err == nil {
				_ = tx.Rollback(ctx)
				t.Fatalf("accepted rebuilt %s mutation", name)
			}
			_ = tx.Rollback(ctx)
		})
	}
	for _, invalid := range []struct {
		name            string
		wrongHour, seam bool
	}{{"swapped hour membership", true, false}, {"ledger source evidence", false, true}} {
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := insertJoinedCanonicalSources(ctx, tx, batchRecordID, day, hours, invalid.wrongHour, invalid.seam); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := sealJoinedCanonicalDay(ctx, tx, day, original); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `SELECT validate_recording_joined_stream_day($1)`, day.id); err == nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("accepted invalid %s", invalid.name)
		}
		_ = tx.Rollback(ctx)
	}
}

func TestJoinedCanonicalLedgerPublicationFeedAndExactAck(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-canonical@example.test")
	defer fixture.cleanup()
	s, pool := fixture.s, fixture.pool
	s.cfg.JoinedRecordingControlPlaneEnabled = true
	s.cfg.JoinedWorkerBootstrapToken = "joined-bootstrap-credential-32bytes"
	s.cfg.JoinedWorkerSigningKey = "joined-signing-credential-32-bytes"
	s.cfg.R2Endpoint = "https://output.example.test"
	s.cfg.R2Bucket = "joined-output"
	s.cfg.R2Region = "auto"
	s.cfg.R2AccessKeyID = "output-key"
	s.cfg.R2SecretAccessKey = "output-secret"
	ctx := context.Background()
	accountID, apiKeyID, connectionID := fixture.accountID, fixture.apiKeyID, fixture.connectionID
	req := fixture.req
	req.Apply, req.ExpectedRequestSHA256 = true, fixture.plan.RequestSHA256
	applied, plan := fixture.call(req)
	if applied.Code != http.StatusOK || plan.RequestSHA256 != fixture.plan.RequestSHA256 {
		t.Fatalf("authenticated canonical apply status=%d body=%s", applied.Code, applied.Body.String())
	}
	batchID := req.BatchID
	var batchRecordID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM recording_joined_batches WHERE batch_id=$1 AND state='building'`, batchID).
		Scan(&batchRecordID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,connection_id,
		batch_id,scope_kind,scope_id,artifact_kind,relative_path,object_key,content_type,expected_size_bytes,
		expected_sha256,canonical_bytes,publication_state) VALUES($1,$2,$3,$4,'batch_index',$4,'batch_index',
		'coverage/batch.json','joined/'||$4||'/coverage/batch.json','application/json',2,$5,'{}','sealed')`,
		batchRecordID, accountID, connectionID, batchID, sha256Bytes([]byte("{}"))); err == nil {
		t.Fatal("building batch accepted a non-ledger artifact")
	}
	ledgers, sourceLedger, gapAtomicLedger, insertFinalChild := materializeJoinedCanonicalBatch(t, fixture, batchRecordID)
	if len(ledgers) != 462 || sourceLedger == nil || gapAtomicLedger == nil || insertFinalChild == nil {
		t.Fatalf("canonical materialization ledgers=%d source=%v gap=%v final=%v", len(ledgers), sourceLedger != nil,
			gapAtomicLedger != nil, insertFinalChild != nil)
	}
	var canaryGapHourID, canarySourceHourID string
	if err := pool.QueryRow(ctx, `SELECT hour_id FROM recording_joined_hours
		WHERE stream_day_id=$1 AND source_clip_count=0 ORDER BY delivery_hour LIMIT 1`, ledgers[0].streamDayID).
		Scan(&canaryGapHourID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT hour_id FROM recording_joined_hours
		WHERE stream_day_id=$1 AND source_clip_count>0 ORDER BY delivery_hour LIMIT 1`, sourceLedger.streamDayID).
		Scan(&canarySourceHourID); err != nil {
		t.Fatal(err)
	}
	s.cfg.JoinedRecordingProtocolVersion = 1
	s.cfg.JoinedRecordingBatchID = batchID
	s.cfg.JoinedRecordingCanaryHourIDs = joinedCanaryScopeForTest(batchID, canaryGapHourID)
	recordingID, batchRecordingID := sourceLedger.recordingID, sourceLedger.batchRecordingID
	var sources []joinedrecording.SourceClip
	if err := json.Unmarshal(sourceLedger.bytes, &struct {
		Sources *[]joinedrecording.SourceClip `json:"sources"`
	}{Sources: &sources}); err != nil || len(sources) != 1 {
		t.Fatalf("source ledger sources=%d err=%v", len(sources), err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_batches SET state='frozen',frozen_at=clock_timestamp()
		WHERE id=$1`, batchRecordID); err == nil {
		t.Fatal("batch froze without a separate freeze-start statement")
	}
	repeatableFreeze, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repeatableFreeze.Exec(ctx, `UPDATE recording_joined_batches SET freeze_started_at=clock_timestamp()
		WHERE id=$1`, batchRecordID); err == nil {
		t.Fatal("repeatable-read batch freeze was accepted")
	}
	_ = repeatableFreeze.Rollback(ctx)

	waitForBlocker := func(waiterPID, blockerPID int32) {
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for waitCtx.Err() == nil {
			var blocked bool
			if err := pool.QueryRow(waitCtx, `SELECT $2::INTEGER=ANY(pg_blocking_pids($1::INTEGER))`,
				waiterPID, blockerPID).Scan(&blocked); err != nil {
				if waitCtx.Err() != nil {
					break
				}
				t.Fatal(err)
			}
			if blocked {
				return
			}
		}
		t.Fatalf("backend %d was not blocked by backend %d", waiterPID, blockerPID)
	}
	childTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var childPID int32
	if err := childTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&childPID); err != nil {
		t.Fatal(err)
	}
	if err := insertFinalChild(childTx); err != nil {
		t.Fatal(err)
	}
	freezeConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var freezePID int32
	if err := freezeConn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&freezePID); err != nil {
		t.Fatal(err)
	}
	freezeTx, err := freezeConn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	freezeResult := make(chan error, 1)
	go func() {
		command, updateErr := freezeTx.Exec(ctx, `UPDATE recording_joined_batches SET freeze_started_at=clock_timestamp()
			WHERE id=$1`, batchRecordID)
		if updateErr == nil && command.RowsAffected() != 1 {
			updateErr = fmt.Errorf("freeze start rows=%d", command.RowsAffected())
		}
		freezeResult <- updateErr
	}()
	waitForBlocker(freezePID, childPID)
	if err := childTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-freezeResult; err != nil {
		t.Fatalf("freeze start after child commit: %v", err)
	}
	if _, err := freezeTx.Exec(ctx, `SELECT validate_recording_joined_stream_day($1)`, ledgers[len(ledgers)-1].streamDayID); err != nil {
		t.Fatalf("freeze did not see committed final child: %v", err)
	}
	if command, err := freezeTx.Exec(ctx, `UPDATE recording_joined_batches SET state='frozen',frozen_at=clock_timestamp()
		WHERE id=$1`, batchRecordID); err != nil || command.RowsAffected() != 1 {
		t.Fatalf("freeze after child commit rows=%d err=%v", command.RowsAffected(), err)
	}

	lateConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var latePID int32
	if err := lateConn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&latePID); err != nil {
		t.Fatal(err)
	}
	lateResult := make(chan error, 1)
	go func() {
		_, insertErr := lateConn.Exec(ctx, `INSERT INTO recording_joined_freeze_exclusions(batch_record_id,batch_recording_id,
			recording_id,reason_code,evidence_sha256,canonical_evidence) VALUES($1,$2,$3,'late_source',$4,'{"late":true}')`,
			batchRecordID, batchRecordingID, recordingID, strings.Repeat("d", 64))
		lateResult <- insertErr
	}()
	waitForBlocker(latePID, freezePID)
	if err := freezeTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-lateResult; err == nil {
		t.Fatal("frozen batch accepted a child that waited on freeze")
	}
	lateConn.Release()
	freezeConn.Release()
	ledgerArtifactID, ledgerRelative, ledgerObject := ledgers[0].artifactID, ledgers[0].relativePath, ledgers[0].objectKey
	ledgerBytes, ledgerArtifactSHA := ledgers[0].bytes, ledgers[0].sha

	var claimToken string
	t.Run("bootstrap work scope drift mutates no lease or artifact", func(t *testing.T) {
		workScope, err := s.joinedWorkScopeIdentity()
		if err != nil {
			t.Fatal(err)
		}
		bootstrapBody, _ := json.Marshal(joinedrecording.WorkerBootstrapRequest{ProtocolVersion: 1,
			BatchID: batchID, WorkScopeIdentity: workScope})
		bootstrapReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/token", bytes.NewReader(bootstrapBody))
		bootstrapReq.Header.Set("Authorization", "Bearer "+s.cfg.JoinedWorkerBootstrapToken)
		bootstrapRec := httptest.NewRecorder()
		s.requireJoinedWorkerBootstrapAuth(http.HandlerFunc(s.handleJoinedToken)).ServeHTTP(bootstrapRec, bootstrapReq)
		var bootstrap joinedrecording.WorkerBootstrapResponse
		if bootstrapRec.Code != http.StatusOK || json.Unmarshal(bootstrapRec.Body.Bytes(), &bootstrap) != nil ||
			!bootstrap.WorkScopeIdentity.Equal(workScope) {
			t.Fatalf("bootstrap status=%d body=%s", bootstrapRec.Code, bootstrapRec.Body.String())
		}
		claimToken = bootstrap.ClaimToken
		snapshot := func() (string, string) {
			t.Helper()
			var hours, artifacts string
			if err := pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_array(id,state,attempt_count,claim_token,
				claimed_by,lease_expires_at,heartbeat_at,next_attempt_at,failure_reason_code,updated_at) ORDER BY id),'[]')::text
				FROM recording_joined_hours WHERE batch_id=$1`, batchID).Scan(&hours); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_array(id,publication_state,
				publication_attempt_count,publication_token,publication_claimed_by,publication_lease_expires_at,
				publication_heartbeat_at,publication_next_attempt_at,finalized_token,etag,version_id,published_at,
				failure_reason_code,updated_at) ORDER BY id),'[]')::text FROM recording_joined_artifacts WHERE batch_id=$1`,
				batchID).Scan(&artifacts); err != nil {
				t.Fatal(err)
			}
			return hours, artifacts
		}
		beforeHours, beforeArtifacts := snapshot()
		canaryIDs := s.cfg.JoinedRecordingCanaryHourIDs
		s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
		s.cfg.JoinedRecordingCanaryHourIDs = ""
		defer func() {
			s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeCanary
			s.cfg.JoinedRecordingCanaryHourIDs = canaryIDs
		}()
		for _, tc := range []struct {
			path    string
			handler http.HandlerFunc
		}{
			{path: "/api/v1/recording/joined/publication/claim", handler: s.handleJoinedPublicationClaim},
			{path: "/api/v1/recording/joined/claim", handler: s.handleJoinedClaim},
		} {
			body, _ := json.Marshal(joinedrecording.WorkClaimRequest{ProtocolVersion: 1, BatchID: batchID, WorkerID: "drift-worker"})
			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+claimToken)
			rec := httptest.NewRecorder()
			s.requireJoinedWorkerAuth(tc.handler).ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("drift claim path=%s status=%d body=%s", tc.path, rec.Code, rec.Body.String())
			}
		}
		afterHours, afterArtifacts := snapshot()
		if beforeHours != afterHours || beforeArtifacts != afterArtifacts {
			t.Fatal("scope drift changed joined lease or artifact state")
		}
	})
	claimBody, _ := json.Marshal(joinedrecording.PublicationClaimRequest{ProtocolVersion: 1, BatchID: batchID, WorkerID: "worker-1"})
	claimReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/claim", bytes.NewReader(claimBody))
	claimReq.Header.Set("Authorization", "Bearer "+claimToken)
	claimRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedPublicationClaim)).ServeHTTP(claimRec, claimReq)
	var publication joinedrecording.PublicationClaimResponse
	if claimRec.Code != http.StatusOK || json.Unmarshal(claimRec.Body.Bytes(), &publication) != nil ||
		publication.Ledger == nil || publication.Ledger.ArtifactID != ledgerArtifactID {
		t.Fatalf("publication claim status=%d body=%s", claimRec.Code, claimRec.Body.String())
	}
	headKeys := []string{}
	s.joinedOutputStorage = joinedOutputStoreStub{head: r2.ObjectHead{ETag: "ledger-etag", SizeBytes: int64(len(ledgerBytes))},
		headKeys: &headKeys}
	published := joinedrecording.PublishedLedger{ArtifactID: ledgerArtifactID, ObjectKey: ledgerObject, ETag: "ledger-etag",
		SizeBytes: int64(len(ledgerBytes)), SHA256: ledgerArtifactSHA}
	wrongPublished := published
	wrongPublished.ObjectKey += ".foreign"
	wrongFinalizeBody, _ := json.Marshal(joinedrecording.FinalizeLedgerRequest{ProtocolVersion: 1, Published: wrongPublished})
	wrongFinalizeReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/ledger/finalize",
		bytes.NewReader(wrongFinalizeBody))
	wrongFinalizeReq.Header.Set("Authorization", "Bearer "+publication.Ledger.OperationToken)
	wrongFinalizeRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeLedger)).ServeHTTP(wrongFinalizeRec, wrongFinalizeReq)
	if wrongFinalizeRec.Code != http.StatusConflict || len(headKeys) != 0 {
		t.Fatalf("foreign finalize status=%d head_keys=%v", wrongFinalizeRec.Code, headKeys)
	}
	finalizeBody, _ := json.Marshal(joinedrecording.FinalizeLedgerRequest{ProtocolVersion: 1, Published: published})
	finalizeReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/ledger/finalize", bytes.NewReader(finalizeBody))
	finalizeReq.Header.Set("Authorization", "Bearer "+publication.Ledger.OperationToken)
	finalizeRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeLedger)).ServeHTTP(finalizeRec, finalizeReq)
	if finalizeRec.Code != http.StatusNoContent {
		t.Fatalf("finalize status=%d body=%s", finalizeRec.Code, finalizeRec.Body.String())
	}
	if len(headKeys) != 1 || headKeys[0] != ledgerObject {
		t.Fatalf("finalize HEAD keys=%v", headKeys)
	}
	var gapManifestCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_joined_artifacts
		WHERE stream_day_id=$1 AND artifact_kind='hour_manifest' AND publication_state='sealed'`, ledgers[0].streamDayID).
		Scan(&gapManifestCount); err != nil || gapManifestCount != 12 {
		t.Fatalf("server gap-only manifests=%d err=%v", gapManifestCount, err)
	}
	exactHourReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/claim", bytes.NewReader(claimBody))
	exactHourReq.Header.Set("Authorization", "Bearer "+claimToken)
	exactHourRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedPublicationClaim)).ServeHTTP(exactHourRec, exactHourReq)
	var exactHour joinedrecording.PublicationClaimResponse
	if exactHourRec.Code != http.StatusOK || json.Unmarshal(exactHourRec.Body.Bytes(), &exactHour) != nil ||
		exactHour.Hour == nil || exactHour.Hour.HourID != canaryGapHourID {
		t.Fatalf("exact canary hour claim status=%d body=%s", exactHourRec.Code, exactHourRec.Body.String())
	}
	noForeignReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/claim", bytes.NewReader(claimBody))
	noForeignReq.Header.Set("Authorization", "Bearer "+claimToken)
	noForeignRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedPublicationClaim)).ServeHTTP(noForeignRec, noForeignReq)
	if noForeignRec.Code != http.StatusNoContent {
		t.Fatalf("canary claimed a foreign hour, day ledger, or batch index: status=%d body=%s", noForeignRec.Code, noForeignRec.Body.String())
	}
	s.cfg.JoinedRecordingCanaryHourIDs = joinedCanaryScopeForTest(batchID, canarySourceHourID)
	claimToken = mintJoinedClaimForTest(t, s, batchID)
	sourceLedgerClaimReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/claim", bytes.NewReader(claimBody))
	sourceLedgerClaimReq.Header.Set("Authorization", "Bearer "+claimToken)
	sourceLedgerClaimRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedPublicationClaim)).ServeHTTP(sourceLedgerClaimRec, sourceLedgerClaimReq)
	var sourceLedgerPublication joinedrecording.PublicationClaimResponse
	if sourceLedgerClaimRec.Code != http.StatusOK || json.Unmarshal(sourceLedgerClaimRec.Body.Bytes(), &sourceLedgerPublication) != nil ||
		sourceLedgerPublication.Ledger == nil || sourceLedgerPublication.Ledger.ArtifactID != sourceLedger.artifactID {
		t.Fatalf("exact dependency ledger claim status=%d body=%s", sourceLedgerClaimRec.Code, sourceLedgerClaimRec.Body.String())
	}
	s.joinedOutputStorage = joinedOutputStoreStub{head: r2.ObjectHead{ETag: "source-ledger-etag", SizeBytes: int64(len(sourceLedger.bytes))}}
	sourceLedgerFinalizeBody, _ := json.Marshal(joinedrecording.FinalizeLedgerRequest{ProtocolVersion: 1,
		Published: joinedrecording.PublishedLedger{ArtifactID: sourceLedger.artifactID, ObjectKey: sourceLedger.objectKey,
			ETag: "source-ledger-etag", SizeBytes: int64(len(sourceLedger.bytes)), SHA256: sourceLedger.sha}})
	sourceLedgerFinalizeReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/ledger/finalize",
		bytes.NewReader(sourceLedgerFinalizeBody))
	sourceLedgerFinalizeReq.Header.Set("Authorization", "Bearer "+sourceLedgerPublication.Ledger.OperationToken)
	sourceLedgerFinalizeRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeLedger)).ServeHTTP(sourceLedgerFinalizeRec, sourceLedgerFinalizeReq)
	if sourceLedgerFinalizeRec.Code != http.StatusNoContent {
		t.Fatalf("exact dependency ledger finalize status=%d body=%s", sourceLedgerFinalizeRec.Code, sourceLedgerFinalizeRec.Body.String())
	}
	principal := accountPrincipal{AccountID: accountID, APIKeyID: &apiKeyID, KeyScopes: []string{accountScopePull}}
	feedReq := httptest.NewRequest(http.MethodGet, "/api/v1/account/joined", nil)
	feedReq = feedReq.WithContext(context.WithValue(feedReq.Context(), accountPrincipalContextKey, principal))
	feedRec := httptest.NewRecorder()
	s.handleAccountJoined(feedRec, feedReq)
	var feed struct {
		Item *joinedArtifactItem `json:"item"`
	}
	if feedRec.Code != http.StatusOK || json.Unmarshal(feedRec.Body.Bytes(), &feed) != nil || feed.Item == nil ||
		feed.Item.ArtifactID != ledgerArtifactID || feed.Item.ConnectionID != connectionID {
		t.Fatalf("feed status=%d body=%s", feedRec.Code, feedRec.Body.String())
	}
	type heartbeatTelemetryResponse struct {
		OK                     bool  `json:"ok"`
		JoinedDeliveryAccepted *bool `json:"joined_delivery_accepted"`
	}
	heartbeatTelemetry := func(caller accountPrincipal, artifactID, cursorID int64, inventoryGeneration, blocker string, freeBytes int64) (*httptest.ResponseRecorder, heartbeatTelemetryResponse) {
		t.Helper()
		attemptedAt := time.Now().UTC().Add(-time.Second)
		body, _ := json.Marshal(connectionHeartbeatRequest{
			CursorID: cursorID, ClipsPulled: cursorID, BytesPulled: cursorID * 10,
			ClientVersion: "joined-v1", ClientPhase: "idle", ClientPreviousExit: "clean", JoinedProtocol: 1,
			Inventory:      &connectionInventoryStatus{Generation: inventoryGeneration},
			Storage:        &connectionStorageStatus{Available: true, TotalBytes: 1000, FreeBytes: freeBytes},
			JoinedDelivery: &connectionJoinedDelivery{ArtifactID: artifactID, Blocker: blocker, AttemptedAt: &attemptedAt},
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/heartbeat", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, caller))
		rec := httptest.NewRecorder()
		s.handleAccountConnectionHeartbeat(rec, req)
		var response heartbeatTelemetryResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &response)
		return rec, response
	}
	if rec, response := heartbeatTelemetry(principal, ledgerArtifactID, 100001, "eligible-report", "download_failed", 401); rec.Code != http.StatusOK ||
		!response.OK || response.JoinedDeliveryAccepted == nil || !*response.JoinedDeliveryAccepted {
		t.Fatalf("eligible joined blocker telemetry status=%d body=%s", rec.Code, rec.Body.String())
	}
	var attemptedArtifactID *int64
	var blocker string
	var attemptedAt, retryAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT joined_last_attempt_artifact_id,joined_last_blocker,joined_last_attempt_at,joined_retry_at
		FROM connections WHERE id=$1`, connectionID).Scan(&attemptedArtifactID, &blocker, &attemptedAt, &retryAt); err != nil ||
		attemptedArtifactID == nil || *attemptedArtifactID != ledgerArtifactID || blocker != "download_failed" {
		t.Fatalf("persisted joined blocker artifact=%v blocker=%q err=%v", attemptedArtifactID, blocker, err)
	}
	assertRawHeartbeat := func(id, cursorID int64, inventoryGeneration string, freeBytes int64) {
		t.Helper()
		var gotCursor, gotFree int64
		var gotGeneration string
		var lastSeen, storageReportedAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT last_cursor_id,inventory_generation,nas_storage_free_bytes,
			last_seen_at,nas_storage_reported_at FROM connections WHERE id=$1`, id).
			Scan(&gotCursor, &gotGeneration, &gotFree, &lastSeen, &storageReportedAt); err != nil ||
			gotCursor != cursorID || gotGeneration != inventoryGeneration || gotFree != freeBytes || lastSeen == nil || storageReportedAt == nil {
			t.Fatalf("raw heartbeat id=%d cursor=%d generation=%q free=%d last_seen=%v storage_at=%v err=%v",
				id, gotCursor, gotGeneration, gotFree, lastSeen, storageReportedAt, err)
		}
	}
	assertRawHeartbeat(connectionID, 100001, "eligible-report", 401)
	var foreignKeyID int64
	_, foreignAccountID := seedUserOrg(t, pool, "joined-heartbeat-foreign@example.test", true)
	if err := pool.QueryRow(ctx, `INSERT INTO account_api_keys(account_id,key_prefix,secret_hash,label,scopes)
		VALUES($1,'sir_joined_foreign','foreign-key','Foreign NAS',ARRAY['stoarama.pull']) RETURNING id`, foreignAccountID).Scan(&foreignKeyID); err != nil {
		t.Fatal(err)
	}
	var foreignConnectionID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,label,api_key_id)
		VALUES($1,'nas_pull','Foreign NAS',$2) RETURNING id`, foreignAccountID, foreignKeyID).Scan(&foreignConnectionID); err != nil {
		t.Fatal(err)
	}
	foreignPrincipal := accountPrincipal{AccountID: foreignAccountID, APIKeyID: &foreignKeyID, KeyScopes: []string{accountScopePull}}
	if rec, response := heartbeatTelemetry(foreignPrincipal, ledgerArtifactID, 200001, "foreign-report", "io_error", 302); rec.Code != http.StatusOK ||
		!response.OK || response.JoinedDeliveryAccepted == nil || *response.JoinedDeliveryAccepted {
		t.Fatalf("foreign joined blocker telemetry status=%d body=%s", rec.Code, rec.Body.String())
	}
	assertRawHeartbeat(foreignConnectionID, 200001, "foreign-report", 302)
	var foreignArtifactID *int64
	var foreignBlocker string
	if err := pool.QueryRow(ctx, `SELECT joined_last_attempt_artifact_id,joined_last_blocker FROM connections WHERE id=$1`,
		foreignConnectionID).Scan(&foreignArtifactID, &foreignBlocker); err != nil || foreignArtifactID != nil || foreignBlocker != "" {
		t.Fatalf("foreign joined telemetry mutated artifact=%v blocker=%q err=%v", foreignArtifactID, foreignBlocker, err)
	}
	wrongAck, _ := json.Marshal(joinedAckRequest{ArtifactID: ledgerArtifactID, RelativePath: "wrong.json",
		SizeBytes: int64(len(ledgerBytes)), SHA256: ledgerArtifactSHA})
	wrongReq := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", bytes.NewReader(wrongAck))
	wrongReq = wrongReq.WithContext(context.WithValue(wrongReq.Context(), accountPrincipalContextKey, principal))
	wrongRec := httptest.NewRecorder()
	s.handleAccountJoinedAck(wrongRec, wrongReq)
	if wrongRec.Code != http.StatusConflict {
		t.Fatalf("wrong ACK status=%d body=%s", wrongRec.Code, wrongRec.Body.String())
	}
	for name, ack := range map[string]joinedAckRequest{
		"whitespace path": {ArtifactID: ledgerArtifactID, RelativePath: " " + ledgerRelative,
			SizeBytes: int64(len(ledgerBytes)), SHA256: ledgerArtifactSHA},
		"uppercase sha": {ArtifactID: ledgerArtifactID, RelativePath: ledgerRelative,
			SizeBytes: int64(len(ledgerBytes)), SHA256: strings.ToUpper(ledgerArtifactSHA)},
	} {
		body, _ := json.Marshal(ack)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
		rec := httptest.NewRecorder()
		s.handleAccountJoinedAck(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s ACK status=%d body=%s", name, rec.Code, rec.Body.String())
		}
	}
	var rejectedAckRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_joined_artifact_acks WHERE artifact_id=$1`,
		ledgerArtifactID).Scan(&rejectedAckRows); err != nil || rejectedAckRows != 0 {
		t.Fatalf("noncanonical ACK rows=%d err=%v", rejectedAckRows, err)
	}
	exactAck, _ := json.Marshal(joinedAckRequest{ArtifactID: ledgerArtifactID, RelativePath: ledgerRelative,
		SizeBytes: int64(len(ledgerBytes)), SHA256: ledgerArtifactSHA})
	for attempt := 0; attempt < 2; attempt++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", bytes.NewReader(exactAck))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
		rec := httptest.NewRecorder()
		s.handleAccountJoinedAck(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("exact ACK attempt=%d status=%d body=%s", attempt, rec.Code, rec.Body.String())
		}
	}
	var ackedArtifactID *int64
	var ackedBlocker string
	var ackedAttemptedAt, ackedRetryAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT joined_last_attempt_artifact_id,joined_last_blocker,joined_last_attempt_at,joined_retry_at
		FROM connections WHERE id=$1`, connectionID).Scan(&ackedArtifactID, &ackedBlocker, &ackedAttemptedAt, &ackedRetryAt); err != nil ||
		ackedArtifactID != nil || ackedBlocker != "" || ackedAttemptedAt != nil || ackedRetryAt != nil {
		t.Fatalf("exact ACK did not clear joined telemetry artifact=%v blocker=%q attempted=%v retry=%v err=%v",
			ackedArtifactID, ackedBlocker, ackedAttemptedAt, ackedRetryAt, err)
	}
	if rec, response := heartbeatTelemetry(principal, ledgerArtifactID, 300001, "acked-report", "storage_guard", 203); rec.Code != http.StatusOK ||
		!response.OK || response.JoinedDeliveryAccepted == nil || *response.JoinedDeliveryAccepted {
		t.Fatalf("acked joined blocker telemetry status=%d body=%s", rec.Code, rec.Body.String())
	}
	assertRawHeartbeat(connectionID, 300001, "acked-report", 203)
	var afterArtifactID *int64
	var afterBlocker string
	var afterAttemptedAt, afterRetryAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT joined_last_attempt_artifact_id,joined_last_blocker,joined_last_attempt_at,joined_retry_at
		FROM connections WHERE id=$1`, connectionID).Scan(&afterArtifactID, &afterBlocker, &afterAttemptedAt, &afterRetryAt); err != nil ||
		afterArtifactID != nil || afterBlocker != "" || afterAttemptedAt != nil || afterRetryAt != nil {
		t.Fatalf("rejected ACKed telemetry changed cleared state artifact=%v blocker=%q attempted=%v retry=%v err=%v",
			afterArtifactID, afterBlocker, afterAttemptedAt, afterRetryAt, err)
	}
	// Publish the remaining ledgers through the same fenced DB transitions so
	// the source-bearing day becomes eligible for the actual worker handlers.
	for i := 1; i <= 12; i++ {
		if ledgers[i].artifactID == sourceLedger.artifactID {
			continue
		}
		token := fmt.Sprintf("00000000-0000-0000-0000-%012d", i+10)
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='publishing',
			publication_attempt_count=1,publication_token=$2,publication_claimed_by='fixture-publisher',
			publication_lease_expires_at=now()+interval '5 minutes',publication_heartbeat_at=now() WHERE id=$1`,
			ledgers[i].artifactID, token); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='published',
			publication_token=NULL,publication_claimed_by=NULL,publication_lease_expires_at=NULL,publication_heartbeat_at=NULL,
			finalized_token=$2,etag='fixture-ledger-etag',version_id='',published_at=now() WHERE id=$1`,
			ledgers[i].artifactID, token); err != nil {
			t.Fatal(err)
		}
	}
	ackLock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var filesBefore, bytesBefore int64
	if err := ackLock.QueryRow(ctx, `SELECT joined_files_pulled,joined_bytes_pulled
		FROM connections WHERE id=$1 FOR UPDATE`, connectionID).Scan(&filesBefore, &bytesBefore); err != nil {
		t.Fatal(err)
	}
	concurrentAck := joinedAckRequest{ArtifactID: gapAtomicLedger.artifactID, RelativePath: gapAtomicLedger.relativePath,
		SizeBytes: int64(len(gapAtomicLedger.bytes)), SHA256: gapAtomicLedger.sha}
	concurrentAckBody, _ := json.Marshal(concurrentAck)
	ackDone := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		go func() {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", bytes.NewReader(concurrentAckBody))
			req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
			rec := httptest.NewRecorder()
			s.handleAccountJoinedAck(rec, req)
			ackDone <- rec
		}()
	}
	blockedCtx, cancelBlocked := context.WithTimeout(ctx, 5*time.Second)
	defer cancelBlocked()
	for {
		var blocked int
		if err := pool.QueryRow(blockedCtx, `SELECT count(*) FROM pg_stat_activity a
			WHERE cardinality(pg_blocking_pids(a.pid))>0
			  AND a.query LIKE '%SELECT id FROM connections WHERE account_id=%'`).Scan(&blocked); err != nil {
			_ = ackLock.Rollback(ctx)
			t.Fatal(err)
		}
		if blocked >= 2 {
			break
		}
		if blockedCtx.Err() != nil {
			_ = ackLock.Rollback(ctx)
			t.Fatal("concurrent ACK handlers did not reach the connection fence")
		}
	}
	if err := ackLock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case rec := <-ackDone:
			if rec.Code != http.StatusOK {
				t.Fatalf("concurrent ACK status=%d body=%s", rec.Code, rec.Body.String())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent ACK handler deadlocked")
		}
	}
	var ackRows int
	var filesAfter, bytesAfter int64
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM recording_joined_artifact_acks
		WHERE artifact_id=$1 AND connection_id=$2),joined_files_pulled,joined_bytes_pulled
		FROM connections WHERE id=$2`, gapAtomicLedger.artifactID, connectionID).Scan(&ackRows, &filesAfter, &bytesAfter); err != nil ||
		ackRows != 1 || filesAfter != filesBefore+1 || bytesAfter != bytesBefore+int64(len(gapAtomicLedger.bytes)) {
		t.Fatalf("concurrent ACK rows=%d files=%d/%d bytes=%d/%d err=%v", ackRows, filesAfter, filesBefore,
			bytesAfter, bytesBefore, err)
	}
	partialTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	partialToken := "00000000-0000-0000-0000-000000000003"
	if _, err := partialTx.Exec(ctx, `UPDATE recording_joined_hours SET state='leased',attempt_count=1,claim_token=$2,
		claimed_by='partial-seal-test',lease_expires_at=now()+interval '5 minutes',heartbeat_at=now()
		WHERE stream_day_id=$1 AND delivery_hour=1`, sourceLedger.streamDayID, partialToken); err != nil {
		_ = partialTx.Rollback(ctx)
		t.Fatal(err)
	}
	partialSHA := strings.Repeat("5", 64)
	if _, err := partialTx.Exec(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,connection_id,
		batch_id,scope_kind,scope_id,stream_day_id,hour_record_id,artifact_kind,ordinal,relative_path,object_key,
		content_type,content_id,expected_size_bytes,expected_sha256)
		SELECT batch_record_id,account_id,connection_id,batch_id,'hour',hour_id,stream_day_id,id,'media',1,
		'partial.mp4','joined/'||batch_id||'/objects/'||$2||'.mp4','video/mp4',$2,1,$2
		FROM recording_joined_hours WHERE stream_day_id=$1 AND delivery_hour=1`, sourceLedger.streamDayID, partialSHA); err != nil {
		_ = partialTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := partialTx.Commit(ctx); err == nil {
		t.Fatal("partial source-hour seal committed")
	}
	var partialState string
	var partialArtifacts int
	if err := pool.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM recording_joined_artifacts a
		WHERE a.hour_record_id=h.id) FROM recording_joined_hours h WHERE h.stream_day_id=$1 AND h.delivery_hour=1`,
		sourceLedger.streamDayID).Scan(&partialState, &partialArtifacts); err != nil || partialState != "pending" || partialArtifacts != 0 {
		t.Fatalf("partial source-hour seal leaked state=%s artifacts=%d err=%v", partialState, partialArtifacts, err)
	}
	sourceClaimBody, _ := json.Marshal(joinedrecording.WorkClaimRequest{ProtocolVersion: 1, BatchID: batchID, WorkerID: "source-worker"})
	sourceClaimReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/claim", bytes.NewReader(sourceClaimBody))
	sourceClaimReq.Header.Set("Authorization", "Bearer "+claimToken)
	sourceClaimRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedClaim)).ServeHTTP(sourceClaimRec, sourceClaimReq)
	var sourceClaim joinedrecording.PreflightHourClaim
	if sourceClaimRec.Code != http.StatusOK || json.Unmarshal(sourceClaimRec.Body.Bytes(), &sourceClaim) != nil || len(sourceClaim.Sources) != 1 {
		t.Fatalf("source claim status=%d body=%s expected=%+v", sourceClaimRec.Code, sourceClaimRec.Body.String(), sources)
	}
	sourceCapability := func() *httptest.ResponseRecorder {
		t.Helper()
		body, _ := json.Marshal(joinedrecording.SourceCapabilityRequest{ProtocolVersion: 1, HourID: sourceClaim.HourID,
			ClipID: sourceClaim.Sources[0].ClipID, Operation: "head"})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/capabilities/source", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+sourceClaim.OperationToken)
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedSourceCapability)).ServeHTTP(rec, req)
		return rec
	}
	if _, err := pool.Exec(ctx, `UPDATE storage_destinations SET access_key_id=$2 WHERE id=$1`, fixture.storageID,
		s.cfg.JoinedWorkerBootstrapToken); err != nil {
		t.Fatal(err)
	}
	bootstrapAttempt := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/token", strings.NewReader(`{"protocol_version":1}`))
	bootstrapAttempt.Header.Set("Authorization", "Bearer "+s.cfg.JoinedWorkerBootstrapToken)
	bootstrapResult := httptest.NewRecorder()
	s.requireJoinedWorkerBootstrapAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(bootstrapResult, bootstrapAttempt)
	if bootstrapResult.Code != http.StatusUnauthorized {
		t.Fatalf("source access key obtained bootstrap authority status=%d", bootstrapResult.Code)
	}
	if rec := sourceCapability(); rec.Code != http.StatusUnauthorized {
		t.Fatalf("bootstrap/source access alias status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE storage_destinations SET access_key_id='key' WHERE id=$1`, fixture.storageID); err != nil {
		t.Fatal(err)
	}
	aliasedSecret, err := s.secrets.Encrypt([]byte(s.cfg.JoinedWorkerSigningKey))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE storage_destinations SET secret_access_key_enc=$2 WHERE id=$1`, fixture.storageID,
		aliasedSecret); err != nil {
		t.Fatal(err)
	}
	forged, err := joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, batchID, joinedauth.SubjectHour,
		sourceClaim.HourID, uuid.New(), joinedauth.OperationPreflight, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	forgedAttempt := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/heartbeat", nil)
	forgedAttempt.Header.Set("Authorization", "Bearer "+forged)
	forgedResult := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(forgedResult, forgedAttempt)
	if forgedResult.Code != http.StatusUnauthorized {
		t.Fatalf("source secret obtained operation authority status=%d", forgedResult.Code)
	}
	if rec := sourceCapability(); rec.Code != http.StatusUnauthorized {
		t.Fatalf("signing/source secret alias status=%d body=%s", rec.Code, rec.Body.String())
	}
	sourceSecret, err := s.secrets.Encrypt([]byte("joined-source-storage-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE storage_destinations SET secret_access_key_enc=$2 WHERE id=$1`, fixture.storageID,
		sourceSecret); err != nil {
		t.Fatal(err)
	}
	var unreferencedStorageID int64
	if err := pool.QueryRow(ctx, `INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,
		access_key_id,secret_access_key_enc,status,managed) VALUES($1,'joined-unreferenced-alias','r2',$2,'auto',
		'unreferenced','  '||$3||'  ',$4,'verified',false) RETURNING id`, accountID, joinedTestSourceEndpoint,
		s.cfg.JoinedWorkerBootstrapToken, sourceSecret).Scan(&unreferencedStorageID); err != nil {
		t.Fatal(err)
	}
	if rec := sourceCapability(); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unreferenced whitespace access alias status=%d body=%s", rec.Code, rec.Body.String())
	}
	whitespaceSigningSecret, err := s.secrets.Encrypt([]byte("  " + s.cfg.JoinedWorkerSigningKey + "  "))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE storage_destinations SET access_key_id='safe-unreferenced-key',
		secret_access_key_enc=$2 WHERE id=$1`, unreferencedStorageID, whitespaceSigningSecret); err != nil {
		t.Fatal(err)
	}
	if rec := sourceCapability(); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unreferenced whitespace secret alias status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE storage_destinations SET secret_access_key_enc=$2 WHERE id=$1`,
		unreferencedStorageID, sourceSecret); err != nil {
		t.Fatal(err)
	}
	if rec := sourceCapability(); rec.Code != http.StatusOK {
		t.Fatalf("preflight source capability status=%d body=%s", rec.Code, rec.Body.String())
	}
	var preflightLease uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT claim_token FROM recording_joined_hours WHERE hour_id=$1`,
		sourceClaim.HourID).Scan(&preflightLease); err != nil {
		t.Fatal(err)
	}
	waitForBlocked := func(fragment string) {
		t.Helper()
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for {
			var blocked bool
			if err := pool.QueryRow(waitCtx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity a
				WHERE cardinality(pg_blocking_pids(a.pid))>0 AND a.query LIKE '%'||$1||'%')`, fragment).Scan(&blocked); err != nil {
				t.Fatal(err)
			}
			if blocked {
				return
			}
			if waitCtx.Err() != nil {
				t.Fatalf("joined source capability never blocked at %q", fragment)
			}
		}
	}
	protocolTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := protocolTx.Exec(ctx, `SELECT id FROM connections WHERE id=$1 FOR UPDATE`, connectionID); err != nil {
		_ = protocolTx.Rollback(ctx)
		t.Fatal(err)
	}
	protocolResult := make(chan error, 1)
	go func() {
		protocolResult <- s.revalidateJoinedSourceCapability(ctx, sourceClaim.HourID, batchID,
			sourceClaim.Sources[0].ClipID, preflightLease, joinedauth.OperationPreflight)
	}()
	waitForBlocked("SELECT c.id FROM connections c JOIN recording_joined_hours h")
	if _, err := protocolTx.Exec(ctx, `UPDATE connections SET joined_protocol_version=0 WHERE id=$1`, connectionID); err != nil {
		_ = protocolTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := protocolTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-protocolResult; err == nil {
		t.Fatal("source capability survived a concurrent protocol downgrade")
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=1 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	mediaSHA := strings.Repeat("3", 64)
	sealRequest := joinedrecording.SealHourRequest{ProtocolVersion: 1, HourID: sourceClaim.HourID,
		SourceClaimSHA256: sourceClaim.SourceClaimSHA256, AccountedSources: sourceClaim.Sources,
		Media: []joinedrecording.SealHourMedia{{Ordinal: 1, SourceClipIDs: []int64{sourceClaim.Sources[0].ClipID},
			SizeBytes: 100, SHA256: mediaSHA, Verification: joinedCanonicalPassingVerification()}}}
	sealBody, _ := json.Marshal(sealRequest)
	sealReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/hour/seal", bytes.NewReader(sealBody))
	sealReq.Header.Set("Authorization", "Bearer "+sourceClaim.OperationToken)
	sealRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedSealHour)).ServeHTTP(sealRec, sealReq)
	var sealed joinedrecording.WorkerClaim
	if sealRec.Code != http.StatusOK || json.Unmarshal(sealRec.Body.Bytes(), &sealed) != nil ||
		len(sealed.MediaArtifactIDs) != 1 || sealed.HourManifestArtifactID <= 0 {
		t.Fatalf("source seal status=%d body=%s", sealRec.Code, sealRec.Body.String())
	}
	var mediaRows, mediaSourceRows, dispositionRows, manifestRows int
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM recording_joined_artifacts WHERE hour_record_id=h.id AND artifact_kind='media'),
		(SELECT count(*) FROM recording_joined_media_sources ms JOIN recording_joined_artifacts a ON a.id=ms.artifact_id
			WHERE a.hour_record_id=h.id),
		(SELECT count(*) FROM recording_joined_hour_dispositions WHERE hour_record_id=h.id),
		(SELECT count(*) FROM recording_joined_artifacts WHERE hour_record_id=h.id AND artifact_kind='hour_manifest')
		FROM recording_joined_hours h WHERE h.hour_id=$1`, sealed.HourID).
		Scan(&mediaRows, &mediaSourceRows, &dispositionRows, &manifestRows); err != nil ||
		mediaRows != 1 || mediaSourceRows != 1 || dispositionRows != 1 || manifestRows != 1 {
		t.Fatalf("atomic source seal media=%d media_sources=%d dispositions=%d manifests=%d err=%v",
			mediaRows, mediaSourceRows, dispositionRows, manifestRows, err)
	}
	mediaOutput := sealed.Plan.Outputs[0]
	sourceClaim.OperationToken = sealed.OperationToken
	if rec := sourceCapability(); rec.Code != http.StatusOK {
		t.Fatalf("publish source capability status=%d body=%s", rec.Code, rec.Body.String())
	}
	mediaETag, manifestETag := "media-etag", "manifest-etag"
	hourHeadKeys := []string{}
	s.joinedOutputStorage = joinedOutputStoreStub{heads: map[string]r2.ObjectHead{
		mediaOutput.ObjectKey:         {ETag: mediaETag, SizeBytes: mediaOutput.ExpectedSize},
		sealed.Plan.CoverageObjectKey: {ETag: manifestETag, SizeBytes: sealed.HourManifestExpectedSize},
	}, headKeys: &hourHeadKeys}
	publishedHour := joinedrecording.PublishedHour{HourID: sealed.HourID, RecordingID: sealed.Plan.RecordingID,
		LocalDate: sealed.Plan.LocalDate, LocalHour: sealed.Plan.LocalHour,
		Outputs: []joinedrecording.PublishedOutput{{ArtifactID: sealed.MediaArtifactIDs[0], ObjectKey: mediaOutput.ObjectKey,
			ETag: mediaETag, SizeBytes: mediaOutput.ExpectedSize, SHA256: mediaOutput.ExpectedSHA}},
		HourManifestObjectKey: sealed.Plan.CoverageObjectKey, HourManifestETag: manifestETag,
		HourManifestSizeBytes: sealed.HourManifestExpectedSize, HourManifestSHA256: sealed.HourManifestExpectedSHA}
	wrongHour := publishedHour
	wrongHour.Outputs = append([]joinedrecording.PublishedOutput{}, publishedHour.Outputs...)
	wrongHour.Outputs[0].SHA256 = strings.Repeat("4", 64)
	wrongHourBody, _ := json.Marshal(joinedrecording.FinalizeHourRequest{ProtocolVersion: 1, Published: wrongHour})
	wrongHourReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/hour/finalize",
		bytes.NewReader(wrongHourBody))
	wrongHourReq.Header.Set("Authorization", "Bearer "+sealed.OperationToken)
	wrongHourRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeHour)).ServeHTTP(wrongHourRec, wrongHourReq)
	if wrongHourRec.Code != http.StatusConflict || len(hourHeadKeys) != 0 {
		t.Fatalf("wrong hour finalize status=%d head_keys=%v body=%s", wrongHourRec.Code, hourHeadKeys, wrongHourRec.Body.String())
	}
	publishHourBody, _ := json.Marshal(joinedrecording.FinalizeHourRequest{ProtocolVersion: 1, Published: publishedHour})
	for attempt := 0; attempt < 2; attempt++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/hour/finalize", bytes.NewReader(publishHourBody))
		req.Header.Set("Authorization", "Bearer "+sealed.OperationToken)
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeHour)).ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("source hour finalize attempt=%d status=%d body=%s", attempt, rec.Code, rec.Body.String())
		}
	}
	ackArtifact := func(ack joinedAckRequest) *httptest.ResponseRecorder {
		body, _ := json.Marshal(ack)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
		rec := httptest.NewRecorder()
		s.handleAccountJoinedAck(rec, req)
		return rec
	}
	manifestAck := joinedAckRequest{ArtifactID: sealed.HourManifestArtifactID,
		RelativePath: "coverage/hours/" + sealed.HourID + ".json", SizeBytes: sealed.HourManifestExpectedSize,
		SHA256: sealed.HourManifestExpectedSHA}
	if rec := ackArtifact(manifestAck); rec.Code == http.StatusOK {
		t.Fatalf("manifest ACK bypassed ledger status=%d body=%s", rec.Code, rec.Body.String())
	}
	sourceLedgerAck := joinedAckRequest{ArtifactID: sourceLedger.artifactID, RelativePath: sourceLedger.relativePath,
		SizeBytes: int64(len(sourceLedger.bytes)), SHA256: sourceLedger.sha}
	if rec := ackArtifact(sourceLedgerAck); rec.Code != http.StatusOK {
		t.Fatalf("source ledger ACK status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := ackArtifact(manifestAck); rec.Code != http.StatusOK {
		t.Fatalf("source manifest ACK status=%d body=%s", rec.Code, rec.Body.String())
	}
	mediaAck := joinedAckRequest{ArtifactID: sealed.MediaArtifactIDs[0], RelativePath: mediaOutput.RelativePath,
		SizeBytes: mediaOutput.ExpectedSize, SHA256: mediaOutput.ExpectedSHA}
	if rec := ackArtifact(mediaAck); rec.Code != http.StatusOK {
		t.Fatalf("source media ACK status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_freeze_exclusions(batch_record_id,batch_recording_id,
		recording_id,reason_code,evidence_sha256,canonical_evidence) VALUES($1,$2,$3,'later_source',$4,'{"late":true}')`,
		batchRecordID, batchRecordingID, recordingID, strings.Repeat("e", 64)); err == nil {
		t.Fatal("frozen batch accepted a late exclusion")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,connection_id,batch_id,
		scope_kind,scope_id,artifact_kind,relative_path,object_key,content_type,expected_size_bytes,expected_sha256,
		canonical_bytes,publication_state) VALUES($1,$2,$3,$4,'batch_index',$4,'batch_index','coverage/batch.json',
		$5,'application/json',2,$6,'{}','sealed')`, batchRecordID, accountID, connectionID, batchID,
		"joined/"+batchID+"/coverage/batch.json", strings.Repeat("d", 64)); err == nil ||
		!strings.Contains(err.Error(), "joined canonical artifact SHA differs") {
		t.Fatalf("canonical JSON artifact mismatched SHA error=%v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,connection_id,batch_id,
		scope_kind,scope_id,artifact_kind,relative_path,object_key,content_type,expected_size_bytes,expected_sha256,
		canonical_bytes,publication_state,finalized_token,etag,version_id,published_at)
		VALUES($1,$2,$3,$4,'batch_index',$4,'batch_index','coverage/batch.json',$5,'application/json',2,$6,'{}',
		'published',$7,'etag','',now())`, batchRecordID, accountID, connectionID, batchID,
		"joined/"+batchID+"/coverage/batch.json", strings.Repeat("d", 64), "00000000-0000-0000-0000-000000000001"); err == nil {
		t.Fatal("artifact was born published")
	}
	malformedTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	malformedManifest := []byte(`{"status":"gap_only","source_count":0,"sources":[],"source_dispositions":[],"media":[],"scheduled_gap":{"reason_code":"scheduled_source_gap"}}`)
	command, malformedErr := malformedTx.Exec(ctx, `UPDATE recording_joined_hours SET state='sealed',
		source_only_sha256=source_claim_sha256,canonical_plan='{"expected_output_count":0}',manifest_bytes=$2,
		manifest_sha256=encode(sha256($2),'hex'),sealed_at=now() WHERE stream_day_id=$1 AND delivery_hour=1`,
		gapAtomicLedger.streamDayID, malformedManifest)
	if malformedErr == nil && command.RowsAffected() == 1 {
		_, malformedErr = malformedTx.Exec(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,
			connection_id,batch_id,scope_kind,scope_id,stream_day_id,hour_record_id,artifact_kind,ordinal,relative_path,
			object_key,content_type,expected_size_bytes,expected_sha256,canonical_bytes,publication_state)
			SELECT batch_record_id,account_id,connection_id,batch_id,'hour',hour_id,stream_day_id,id,'hour_manifest',1,
			'coverage/hours/'||hour_id||'.json','joined/'||batch_id||'/coverage/hours/'||hour_id||'.json',
			'application/json',$2,encode(sha256($3),'hex'),$3,'sealed' FROM recording_joined_hours
			WHERE stream_day_id=$1 AND delivery_hour=1`, gapAtomicLedger.streamDayID, len(malformedManifest), malformedManifest)
	}
	if malformedErr == nil {
		malformedErr = malformedTx.Commit(ctx)
	} else {
		_ = malformedTx.Rollback(ctx)
	}
	if malformedErr == nil {
		t.Fatal("malformed source-free seal committed")
	}
	var malformedState string
	var malformedArtifacts int
	if err := pool.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM recording_joined_artifacts a
		WHERE a.hour_record_id=h.id) FROM recording_joined_hours h WHERE h.stream_day_id=$1 AND h.delivery_hour=1`,
		gapAtomicLedger.streamDayID).Scan(&malformedState, &malformedArtifacts); err != nil || malformedState != "pending" || malformedArtifacts != 0 {
		t.Fatalf("malformed source-free seal leaked state=%s artifacts=%d err=%v", malformedState, malformedArtifacts, err)
	}
	sealTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var pendingHourID int64
	if err := sealTx.QueryRow(ctx, `SELECT id FROM recording_joined_hours WHERE stream_day_id=$1 AND delivery_hour=1`,
		gapAtomicLedger.streamDayID).Scan(&pendingHourID); err != nil {
		t.Fatal(err)
	}
	leaseToken := "00000000-0000-0000-0000-000000000002"
	if _, err := sealTx.Exec(ctx, `UPDATE recording_joined_hours SET state='leased',attempt_count=1,claim_token=$2,
		claimed_by='worker-atomicity-test',lease_expires_at=date_trunc('second',now()+interval '5 minutes'),heartbeat_at=now()
		WHERE id=$1`, pendingHourID, leaseToken); err == nil {
		t.Fatal("source-free hour entered worker lease path")
	}
	_ = sealTx.Rollback(ctx)
	var state string
	var strayMedia int
	if err := pool.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM recording_joined_artifacts
		WHERE hour_record_id=h.id AND artifact_kind='media') FROM recording_joined_hours h WHERE id=$1`, pendingHourID).
		Scan(&state, &strayMedia); err != nil || state != "pending" || strayMedia != 0 {
		t.Fatalf("rejected source-free lease state=%s stray_media=%d err=%v", state, strayMedia, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='sealed',source_only_sha256=source_claim_sha256,
		canonical_plan='{}',manifest_bytes='{}',manifest_sha256=encode(sha256('{}'::bytea),'hex'),sealed_at=now()
		WHERE stream_day_id=$1 AND delivery_hour=2`, sourceLedger.streamDayID); err == nil {
		t.Fatal("source-bearing hour bypassed its worker lease")
	}
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	s.cfg.JoinedRecordingCanaryHourIDs = ""
	frozenClaimToken := mintJoinedClaimForTest(t, s, batchID)
	frozenPublicationBody, _ := json.Marshal(joinedrecording.PublicationClaimRequest{ProtocolVersion: 1,
		BatchID: batchID, WorkerID: "frozen-batch-worker"})
	frozenPublicationRequest := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/claim",
		bytes.NewReader(frozenPublicationBody))
	frozenPublicationRequest.Header.Set("Authorization", "Bearer "+frozenClaimToken)
	frozenPublicationRecorder := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedPublicationClaim)).ServeHTTP(frozenPublicationRecorder,
		frozenPublicationRequest)
	var frozenPublication joinedrecording.PublicationClaimResponse
	if frozenPublicationRecorder.Code != http.StatusOK || json.Unmarshal(frozenPublicationRecorder.Body.Bytes(), &frozenPublication) != nil ||
		frozenPublication.Kind != "ledger" || frozenPublication.Ledger == nil || frozenPublication.Ledger.BatchID != batchID {
		t.Fatalf("frozen-batch ledger claim status=%d body=%s", frozenPublicationRecorder.Code, frozenPublicationRecorder.Body.String())
	}
	publishJoinedCanonicalRemainderForIndex(t, fixture, batchRecordID)
	var missingHourID, missingState string
	var missingPriority int64
	if err := pool.QueryRow(ctx, `SELECT h.hour_id,h.state,h.priority_ordinal FROM recording_joined_hours h
		WHERE h.batch_record_id=$1 AND NOT EXISTS(SELECT 1 FROM recording_joined_artifacts a
		WHERE a.hour_record_id=h.id AND a.artifact_kind='hour_manifest' AND a.publication_state='published')
		ORDER BY h.priority_ordinal LIMIT 1`, batchRecordID).Scan(&missingHourID, &missingState, &missingPriority); err == nil {
		t.Fatalf("unpublished hour before index id=%s state=%s priority=%d", missingHourID, missingState, missingPriority)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
	sealIndex := func(request joinedSealBatchIndexRequest) *httptest.ResponseRecorder {
		body, _ := json.Marshal(request)
		httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/recording/joined/batches/index/seal", bytes.NewReader(body))
		httpReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: fixture.sessionToken})
		recorder := httptest.NewRecorder()
		s.requireAdminAuth(http.HandlerFunc(s.handleAdminJoinedSealBatchIndex)).ServeHTTP(recorder, httpReq)
		return recorder
	}
	assertIndexDuration := func(operation string, started time.Time) {
		t.Helper()
		elapsed := time.Since(started)
		if elapsed >= joinedBatchIndexOperationTimeout {
			t.Fatalf("batch-index %s exceeded server deadline: %s", operation, elapsed)
		}
	}
	previewRequest := joinedSealBatchIndexRequest{ProtocolVersion: 1, BatchID: batchID}
	unauthorizedBody, _ := json.Marshal(previewRequest)
	unauthorizedRequest := httptest.NewRequest(http.MethodPost, "/api/v1/admin/recording/joined/batches/index/seal",
		bytes.NewReader(unauthorizedBody))
	unauthorizedRequest.Header.Set("Authorization", "Bearer "+s.cfg.ServiceToken)
	unauthorized := httptest.NewRecorder()
	s.requireAdminAuth(http.HandlerFunc(s.handleAdminJoinedSealBatchIndex)).ServeHTTP(unauthorized, unauthorizedRequest)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("batch-index seal accepted generic service auth status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
	previewStarted := time.Now()
	preview := sealIndex(previewRequest)
	assertIndexDuration("preview", previewStarted)
	var previewed joinedSealBatchIndexResponse
	if preview.Code != http.StatusOK || json.Unmarshal(preview.Body.Bytes(), &previewed) != nil ||
		!lowerHexSHA256(previewed.SHA256) || previewed.ArtifactID != 0 || previewed.State != "frozen" {
		t.Fatalf("batch-index preview status=%d body=%s", preview.Code, preview.Body.String())
	}
	wrongIndex := previewRequest
	wrongIndex.Apply, wrongIndex.ExpectedSHA256 = true, strings.Repeat("f", 64)
	if response := sealIndex(wrongIndex); response.Code != http.StatusConflict {
		t.Fatalf("wrong batch-index hash status=%d body=%s", response.Code, response.Body.String())
	}
	applyIndex := previewRequest
	applyIndex.Apply, applyIndex.ExpectedSHA256 = true, previewed.SHA256
	indexBlockerTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := indexBlockerTx.Exec(ctx, `SELECT id FROM recording_joined_batches WHERE id=$1 FOR UPDATE`, batchRecordID); err != nil {
		_ = indexBlockerTx.Rollback(ctx)
		t.Fatal(err)
	}
	blockedStarted := time.Now()
	blockedApply := sealIndex(applyIndex)
	blockedElapsed := time.Since(blockedStarted)
	if blockedApply.Code != http.StatusConflict || blockedElapsed >= 20*time.Second {
		_ = indexBlockerTx.Rollback(ctx)
		t.Fatalf("blocked batch-index apply status=%d elapsed=%s body=%s", blockedApply.Code, blockedElapsed, blockedApply.Body.String())
	}
	var blockedState string
	var partialIndexes, partialReferences int
	if err := indexBlockerTx.QueryRow(ctx, `SELECT state,
		(SELECT count(*) FROM recording_joined_artifacts WHERE batch_record_id=b.id AND artifact_kind='batch_index'),
		(SELECT count(*) FROM recording_joined_batch_index_refs WHERE batch_record_id=b.id)
		FROM recording_joined_batches b WHERE id=$1`, batchRecordID).Scan(&blockedState, &partialIndexes, &partialReferences); err != nil ||
		blockedState != "frozen" || partialIndexes != 0 || partialReferences != 0 {
		_ = indexBlockerTx.Rollback(ctx)
		t.Fatalf("blocked batch-index apply leaked state=%s indexes=%d refs=%d err=%v", blockedState, partialIndexes,
			partialReferences, err)
	}
	if err := indexBlockerTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	protocolBlockerTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := protocolBlockerTx.Exec(ctx, `UPDATE connections SET joined_protocol_version=0 WHERE id=$1`, connectionID); err != nil {
		_ = protocolBlockerTx.Rollback(ctx)
		t.Fatal(err)
	}
	protocolBlockedStarted := time.Now()
	protocolBlocked := sealIndex(applyIndex)
	protocolBlockedElapsed := time.Since(protocolBlockedStarted)
	if protocolBlocked.Code != http.StatusConflict || protocolBlockedElapsed >= 20*time.Second {
		_ = protocolBlockerTx.Rollback(ctx)
		t.Fatalf("protocol-blocked batch-index apply status=%d elapsed=%s body=%s", protocolBlocked.Code,
			protocolBlockedElapsed, protocolBlocked.Body.String())
	}
	var protocolBlockedState string
	if err := protocolBlockerTx.QueryRow(ctx, `SELECT state FROM recording_joined_batches WHERE id=$1`, batchRecordID).
		Scan(&protocolBlockedState); err != nil || protocolBlockedState != "frozen" {
		_ = protocolBlockerTx.Rollback(ctx)
		t.Fatalf("protocol-blocked batch-index state=%s err=%v", protocolBlockedState, err)
	}
	if err := protocolBlockerTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	protocolRejected := sealIndex(applyIndex)
	if protocolRejected.Code != http.StatusConflict {
		t.Fatalf("protocol-v0 batch-index apply status=%d body=%s", protocolRejected.Code, protocolRejected.Body.String())
	}
	var protocolIndexes, protocolReferences int
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM recording_joined_artifacts WHERE batch_record_id=$1 AND artifact_kind='batch_index'),
		(SELECT count(*) FROM recording_joined_batch_index_refs WHERE batch_record_id=$1)`, batchRecordID).
		Scan(&protocolIndexes, &protocolReferences); err != nil || protocolIndexes != 0 || protocolReferences != 0 {
		t.Fatalf("protocol-v0 batch-index apply leaked indexes=%d refs=%d err=%v", protocolIndexes, protocolReferences, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=1 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	type applyResult struct {
		recorder *httptest.ResponseRecorder
		elapsed  time.Duration
	}
	applyResults := make(chan applyResult, 2)
	applyStart := make(chan struct{})
	for range 2 {
		go func() {
			<-applyStart
			started := time.Now()
			applyResults <- applyResult{recorder: sealIndex(applyIndex), elapsed: time.Since(started)}
		}()
	}
	close(applyStart)
	var indexReceipt joinedSealBatchIndexResponse
	created, alreadySealed := 0, 0
	applyStarted := time.Now()
	for range 2 {
		result := <-applyResults
		if result.elapsed >= joinedBatchIndexOperationTimeout {
			t.Fatalf("concurrent batch-index apply exceeded server deadline: %s", result.elapsed)
		}
		var receipt joinedSealBatchIndexResponse
		if result.recorder.Code != http.StatusOK || json.Unmarshal(result.recorder.Body.Bytes(), &receipt) != nil ||
			receipt.ArtifactID <= 0 || receipt.State != "index_sealed" || receipt.SHA256 != previewed.SHA256 {
			t.Fatalf("concurrent batch-index apply status=%d body=%s", result.recorder.Code, result.recorder.Body.String())
		}
		if indexReceipt.ArtifactID == 0 {
			indexReceipt = receipt
		} else if receipt.ArtifactID != indexReceipt.ArtifactID || receipt.SHA256 != indexReceipt.SHA256 {
			t.Fatalf("concurrent batch-index apply identity differs first=%+v second=%+v", indexReceipt, receipt)
		}
		if receipt.AlreadySealed {
			alreadySealed++
		} else {
			created++
		}
	}
	assertIndexDuration("apply", applyStarted)
	if created != 1 || alreadySealed != 1 {
		t.Fatalf("concurrent batch-index apply created=%d already_sealed=%d", created, alreadySealed)
	}
	retryIndex := sealIndex(applyIndex)
	var retryReceipt joinedSealBatchIndexResponse
	if retryIndex.Code != http.StatusOK || json.Unmarshal(retryIndex.Body.Bytes(), &retryReceipt) != nil ||
		retryReceipt.ArtifactID != indexReceipt.ArtifactID || !retryReceipt.AlreadySealed {
		t.Fatalf("batch-index retry status=%d body=%s", retryIndex.Code, retryIndex.Body.String())
	}
	indexClaimToken := mintJoinedClaimForTest(t, s, batchID)
	publicationBody, _ := json.Marshal(joinedrecording.PublicationClaimRequest{ProtocolVersion: 1, BatchID: batchID, WorkerID: "index-worker"})
	publicationRequest := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/claim", bytes.NewReader(publicationBody))
	publicationRequest.Header.Set("Authorization", "Bearer "+indexClaimToken)
	publicationRecorder := httptest.NewRecorder()
	claimStarted := time.Now()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedPublicationClaim)).ServeHTTP(publicationRecorder, publicationRequest)
	assertIndexDuration("claim", claimStarted)
	var indexClaim joinedrecording.PublicationClaimResponse
	if publicationRecorder.Code != http.StatusOK || json.Unmarshal(publicationRecorder.Body.Bytes(), &indexClaim) != nil ||
		indexClaim.Kind != "batch_index" || indexClaim.BatchIndex == nil || indexClaim.BatchIndex.ArtifactID != indexReceipt.ArtifactID {
		t.Fatalf("batch-index claim status=%d body=%s", publicationRecorder.Code, publicationRecorder.Body.String())
	}
	s.joinedOutputStorage = joinedOutputStoreStub{heads: map[string]r2.ObjectHead{
		indexReceipt.ObjectKey: {ETag: "index-etag", SizeBytes: indexReceipt.SizeBytes},
	}}
	publishedIndex := joinedrecording.PublishedBatchIndex{ArtifactID: indexReceipt.ArtifactID, ObjectKey: indexReceipt.ObjectKey,
		ETag: "index-etag", SizeBytes: indexReceipt.SizeBytes, SHA256: indexReceipt.SHA256}
	finalizeIndexBody, _ := json.Marshal(joinedrecording.FinalizeBatchIndexRequest{ProtocolVersion: 1, Published: publishedIndex})
	finalizeIndexRequest := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/index/finalize", bytes.NewReader(finalizeIndexBody))
	finalizeIndexRequest.Header.Set("Authorization", "Bearer "+indexClaim.BatchIndex.OperationToken)
	finalizeIndexRecorder := httptest.NewRecorder()
	finalizeStarted := time.Now()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeBatchIndex)).ServeHTTP(finalizeIndexRecorder, finalizeIndexRequest)
	assertIndexDuration("finalize", finalizeStarted)
	if finalizeIndexRecorder.Code != http.StatusNoContent {
		t.Fatalf("batch-index finalize status=%d body=%s", finalizeIndexRecorder.Code, finalizeIndexRecorder.Body.String())
	}
	t.Log("JOINED_CANONICAL_LIFECYCLE_EXECUTED")
}

func publishJoinedCanonicalRemainderForIndex(t *testing.T, fixture joinedHistoricalTier1Fixture, batchRecordID int64) {
	t.Helper()
	ctx := context.Background()
	rows, err := fixture.pool.Query(ctx, `SELECT id FROM recording_joined_artifacts WHERE batch_record_id=$1
		AND artifact_kind='allocation_ledger' ORDER BY id`, batchRecordID)
	if err != nil {
		t.Fatal(err)
	}
	var ledgerIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ledgerIDs = append(ledgerIDs, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	for _, id := range ledgerIDs {
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var state string
		var currentToken *uuid.UUID
		var leaseCurrent bool
		if err := tx.QueryRow(ctx, `SELECT publication_state,publication_token,
			COALESCE(publication_lease_expires_at>now(),false) FROM recording_joined_artifacts WHERE id=$1 FOR UPDATE`, id).
			Scan(&state, &currentToken, &leaseCurrent); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if state != "published" {
			token := uuid.New()
			if state == "publishing" && leaseCurrent && currentToken != nil {
				token = *currentToken
			} else if state != "sealed" && state != "publishing" {
				_ = tx.Rollback(ctx)
				t.Fatalf("unexpected ledger publication state %q", state)
			}
			if state == "sealed" || !leaseCurrent {
				if _, err := tx.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='publishing',
				publication_attempt_count=publication_attempt_count+1,publication_token=$2,publication_claimed_by='index-fixture',
				publication_lease_expires_at=now()+interval '5 minutes',publication_heartbeat_at=now() WHERE id=$1`, id, token); err != nil {
					_ = tx.Rollback(ctx)
					t.Fatal(err)
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='published',publication_token=NULL,
				publication_claimed_by=NULL,publication_lease_expires_at=NULL,publication_heartbeat_at=NULL,finalized_token=$2,
				etag='fixture-ledger-etag',version_id='',published_at=now() WHERE id=$1`, id, token); err != nil {
				_ = tx.Rollback(ctx)
				t.Fatal(err)
			}
		}
		if err := sealJoinedGapOnlyHoursTx(ctx, tx, id); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	manifestRows, err := fixture.pool.Query(ctx, `SELECT id FROM recording_joined_artifacts WHERE batch_record_id=$1
		AND artifact_kind='hour_manifest' AND publication_state<>'published' ORDER BY id`, batchRecordID)
	if err != nil {
		t.Fatal(err)
	}
	var manifestIDs []int64
	for manifestRows.Next() {
		var id int64
		if err := manifestRows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		manifestIDs = append(manifestIDs, id)
	}
	if err := manifestRows.Err(); err != nil {
		t.Fatal(err)
	}
	manifestRows.Close()
	for _, id := range manifestIDs {
		tx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var state string
		var currentToken *uuid.UUID
		var leaseCurrent bool
		if err := tx.QueryRow(ctx, `SELECT publication_state,publication_token,
			COALESCE(publication_lease_expires_at>now(),false) FROM recording_joined_artifacts WHERE id=$1 FOR UPDATE`, id).
			Scan(&state, &currentToken, &leaseCurrent); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		token := uuid.New()
		if state == "publishing" && leaseCurrent && currentToken != nil {
			token = *currentToken
		} else if state != "sealed" && state != "publishing" {
			_ = tx.Rollback(ctx)
			t.Fatalf("unexpected hour manifest publication state %q", state)
		}
		if state == "sealed" || !leaseCurrent {
			if _, err := tx.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='publishing',
				publication_attempt_count=publication_attempt_count+1,publication_token=$2,publication_claimed_by='index-fixture',
				publication_lease_expires_at=now()+interval '5 minutes',publication_heartbeat_at=now() WHERE id=$1`, id, token); err != nil {
				_ = tx.Rollback(ctx)
				t.Fatal(err)
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='published',publication_token=NULL,
			publication_claimed_by=NULL,publication_lease_expires_at=NULL,publication_heartbeat_at=NULL,finalized_token=$2,
			etag='fixture-manifest-etag',version_id='',published_at=now() WHERE id=$1`, id, token); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func joinedCanonicalPassingVerification() joinedrecording.Verification {
	track := &joinedrecording.TrackFingerprint{MediaType: "video", PacketCount: 1,
		PacketChainSHA256: strings.Repeat("a", 64), PacketTimingSHA256: strings.Repeat("b", 64),
		PacketTimeBases: []string{"1/1"}, FirstPacketPTSSeconds: "0", LastPacketPTSSeconds: "0",
		FirstPacketDTSSeconds: "0", LastPacketDTSSeconds: "0", PacketDurationSeconds: "1",
		DecodeTimelineSpanSeconds: "1", DecodedFrames: 1, TimestampStatus: "monotonic"}
	sourceTrack := *track
	sourceTrack.TimestampStatus = "source_clips_independent"
	return joinedrecording.Verification{Status: "passed", PacketPayloadOrderStatus: "passed",
		DecodedFrameTotalsStatus: "passed", DecodedAudioTotalsStatus: "passed", OutputTimestampStatus: "passed",
		StrictDecodeStatus: "passed", SourceFingerprint: joinedrecording.MediaFingerprint{DurationSeconds: 60,
			Tracks: map[string]*joinedrecording.TrackFingerprint{"video": &sourceTrack}},
		OutputFingerprint: joinedrecording.MediaFingerprint{DurationSeconds: 60,
			Tracks: map[string]*joinedrecording.TrackFingerprint{"video": track}}}
}
