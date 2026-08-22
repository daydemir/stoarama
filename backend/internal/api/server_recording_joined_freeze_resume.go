package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

// applyJoinedTier1FreezeResumable advances one durable recording chunk per
// call. The advisory lock is deliberately the transaction's first statement:
// raw purge uses the same fence, so scope creation wins or loses atomically.
func (s *Server) applyJoinedTier1FreezeResumable(ctx context.Context, req joinedTier1FreezeRequest) (joinedTier1FreezePlan, bool, error) {
	tool, err := joinedrecording.InspectMediaToolEvidence(ctx)
	if err != nil {
		return joinedTier1FreezePlan{}, false, fmt.Errorf("inspect joined media tool: %w", err)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return joinedTier1FreezePlan{}, false, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(137,1)`); err != nil {
		return joinedTier1FreezePlan{}, false, err
	}

	plan, batchRecordID, state, created, err := s.loadOrInitializeJoinedTier1Snapshot(ctx, tx, req, tool)
	if err != nil {
		return plan, false, err
	}
	if state != "snapshotting" {
		if state != "building" && state != "frozen" && state != "index_sealed" && state != "published" {
			return plan, false, fmt.Errorf("Tier-1 batch state %q cannot resume", state)
		}
		if err := tx.Commit(ctx); err != nil {
			return plan, false, err
		}
		return plan, false, nil
	}

	var nextOrdinal int
	err = tx.QueryRow(ctx, `SELECT br.priority_ordinal
		FROM recording_joined_batch_recordings br
		LEFT JOIN recording_joined_snapshot_chunks chunk ON chunk.batch_recording_id=br.id
		WHERE br.batch_record_id=$1 AND chunk.id IS NULL
		ORDER BY br.priority_ordinal LIMIT 1`, batchRecordID).Scan(&nextOrdinal)
	if errors.Is(err, pgx.ErrNoRows) {
		command, updateErr := tx.Exec(ctx, `UPDATE recording_joined_batches SET state='building'
			WHERE id=$1 AND state='snapshotting'`, batchRecordID)
		if updateErr != nil {
			return plan, false, updateErr
		}
		if command.RowsAffected() != 1 {
			return plan, false, errors.New("Tier-1 finalization lost snapshotting ownership")
		}
	} else if err != nil {
		return plan, false, err
	} else if err := s.snapshotJoinedTier1Recording(ctx, tx, batchRecordID, nextOrdinal, plan); err != nil {
		return plan, false, err
	}
	if err := ctx.Err(); err != nil {
		return plan, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return plan, false, err
	}
	return plan, created, nil
}

func (s *Server) joinedTier1FreezeProgress(ctx context.Context, batchID string) (joinedTier1FreezeProgress, error) {
	var progress joinedTier1FreezeProgress
	var next int
	err := s.pool.QueryRow(ctx, `SELECT b.state,
		(SELECT count(*) FROM recording_joined_snapshot_chunks c WHERE c.batch_record_id=b.id),b.expected_recordings,
		(SELECT count(*) FROM recording_joined_stream_days d WHERE d.batch_record_id=b.id),b.expected_stream_days,
		COALESCE((SELECT min(br.priority_ordinal) FROM recording_joined_batch_recordings br
		 LEFT JOIN recording_joined_snapshot_chunks c ON c.batch_recording_id=br.id
		 WHERE br.batch_record_id=b.id AND c.id IS NULL),0)
		FROM recording_joined_batches b WHERE b.batch_id=$1`, batchID).
		Scan(&progress.State, &progress.CompletedRecordings, &progress.ExpectedRecordings,
			&progress.CompletedStreamDays, &progress.ExpectedStreamDays, &next)
	if err != nil {
		return progress, fmt.Errorf("load Tier-1 freeze progress: %w", err)
	}
	if next > 0 {
		progress.NextPriorityOrdinal = &next
	}
	return progress, nil
}

func (s *Server) loadOrInitializeJoinedTier1Snapshot(ctx context.Context, tx pgx.Tx, req joinedTier1FreezeRequest,
	tool joinedrecording.MediaToolEvidence) (joinedTier1FreezePlan, int64, string, bool, error) {
	var plan joinedTier1FreezePlan
	var batchRecordID int64
	var requestBytes []byte
	var requestSHA, state string
	var protocol int
	err := tx.QueryRow(ctx, `SELECT b.id,b.freeze_request_bytes,b.freeze_request_sha256,b.state,c.joined_protocol_version
		FROM recording_joined_batches b JOIN connections c ON c.id=b.connection_id
		WHERE b.batch_id=$1 FOR UPDATE OF b,c`, req.BatchID).
		Scan(&batchRecordID, &requestBytes, &requestSHA, &state, &protocol)
	if err == nil {
		if protocol != joinedrecording.JoinedProtocolVersion {
			return plan, 0, "", false, errors.New("Tier-1 connection protocol is disabled")
		}
		if requestSHA != req.ExpectedRequestSHA256 {
			return plan, 0, "", false, errors.New("Tier-1 batch key already has different immutable evidence")
		}
		if json.Unmarshal(requestBytes, &plan) != nil || plan.SchemaVersion != 2 {
			return plan, 0, "", false, errors.New("stored Tier-1 request is not canonical v2")
		}
		plan.RequestSHA256 = requestSHA
		if !joinedTier1FreezeRequestMatchesPlan(req, plan) {
			return plan, 0, "", false, errors.New("Tier-1 batch key differs from the explicit immutable request")
		}
		return plan, batchRecordID, state, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return plan, 0, "", false, err
	}

	if err := tx.QueryRow(ctx, `SELECT final_plan_bytes,final_plan_sha256 FROM recording_joined_dry_runs
		WHERE batch_id=$1 AND generation=$2 AND state='ready' AND final_plan_sha256=$3 FOR SHARE`,
		req.BatchID, req.Generation, req.ExpectedRequestSHA256).Scan(&requestBytes, &requestSHA); err != nil {
		return plan, 0, "", false, errors.New("ready checkpointed Tier-1 dry-run plan not found")
	}
	if err := json.Unmarshal(requestBytes, &plan); err != nil || plan.SchemaVersion != 2 ||
		requestSHA != req.ExpectedRequestSHA256 || !joinedTier1FreezeRequestMatchesPlan(req, plan) {
		return plan, 0, "", false, errors.New("checkpointed Tier-1 dry-run plan differs")
	}
	plan.RequestSHA256 = requestSHA
	if plan.MediaTool.IdentitySHA256 != tool.IdentitySHA256 {
		return plan, 0, "", false, errors.New("checkpointed Tier-1 media tool differs")
	}
	mediaToolJSON, err := json.Marshal(plan.MediaTool)
	if err != nil {
		return plan, 0, "", false, fmt.Errorf("marshal media tool evidence: %w", err)
	}
	if err := tx.QueryRow(ctx, `INSERT INTO recording_joined_batches(account_id,connection_id,batch_id,generation,
		source_endpoint,qualification_run_id,qualification_cohort_sha256,qualification_windows_sha256,
		selected_qualification_windows_sha256,qualification_jobs_sha256,qualification_frozen_at,
		ordered_recording_ids_sha256,selection_basis,policy_version,eligibility_cutoff,media_tool,media_tool_sha256,
		freeze_request_bytes,freeze_request_sha256,frozen_denominator_sha256,freeze_exclusions_sha256,
		expected_recordings,expected_stream_days,expected_scheduled_hours,expected_source_clips,expected_source_bytes,
		expected_freeze_exclusions)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,33,462,5544,$22,$23,$24)
		RETURNING id`, plan.AccountID, plan.ConnectionID, plan.BatchID, plan.Generation, plan.SourceEndpoint,
		plan.SelectionAuthority.QualificationRunID, plan.SelectionAuthority.QualificationCohortSHA256,
		plan.SelectionAuthority.QualificationWindowsSHA256, plan.SelectionAuthority.SelectedQualificationWindowsSHA256,
		plan.QualificationJobsSHA256, plan.SelectionAuthority.QualificationRunFrozenAt,
		plan.SelectionAuthority.OrderedRecordingIDSHA256, plan.SelectionAuthority.SelectionBasis, plan.PolicyVersion,
		plan.SelectionAuthority.Cutoff, mediaToolJSON, plan.MediaTool.IdentitySHA256, requestBytes, plan.RequestSHA256,
		plan.FrozenDenominatorSHA256, plan.FreezeExclusionsSHA256, plan.ProvisionalSourceClips,
		plan.ProvisionalSourceBytes, plan.ProvisionalExclusions).Scan(&batchRecordID); err != nil {
		return plan, 0, "", false, err
	}
	for _, recording := range plan.Recordings {
		qualificationJSON, err := json.Marshal(recording.Qualification)
		if err != nil {
			return plan, 0, "", false, fmt.Errorf("marshal qualification for recording %d: %w", recording.Frozen.RecordingID, err)
		}
		var batchRecordingID int64
		if err := tx.QueryRow(ctx, `INSERT INTO recording_joined_batch_recordings(batch_record_id,account_id,connection_id,
			batch_id,qualification_run_id,selection_tier,recording_id,priority_ordinal,timezone,folder_name,naming_metadata,
			first_local_date,last_local_date,qualification,qualification_sha256,qualification_policy_version,completed_at,
			authoritative_job_ids) VALUES($1,$2,$3,$4,$5,'good+',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) RETURNING id`,
			batchRecordID, plan.AccountID, plan.ConnectionID, plan.BatchID, plan.SelectionAuthority.QualificationRunID,
			recording.Frozen.RecordingID, recording.Frozen.PriorityOrdinal, recording.Frozen.Timezone,
			recording.Frozen.FolderName, recording.Frozen.NamingMetadata, recording.Qualification.Days[0].LocalDate,
			recording.Qualification.Days[13].LocalDate, qualificationJSON, recording.Frozen.QualificationSHA256,
			plan.SelectionAuthority.QualificationRuleVersion, recording.Frozen.CompletedAt,
			joinedTier1JobIDs(recording.Qualification.Days)).Scan(&batchRecordingID); err != nil {
			return plan, 0, "", false, err
		}
		for i, scope := range recording.SnapshotDays {
			day := recording.Qualification.Days[i]
			if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_snapshot_scopes(batch_record_id,batch_recording_id,
				recording_id,priority_ordinal,local_date,date_ordinal,recording_job_id,scheduled_start_at,scheduled_end_at,
				completed_at,high_water_clip_id,expected_source_clips,expected_source_bytes,expected_source_sha256)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, batchRecordID, batchRecordingID,
				recording.Frozen.RecordingID, recording.Frozen.PriorityOrdinal, scope.LocalDate, scope.DateOrdinal,
				scope.RecordingJobID, day.WindowStart, day.WindowEnd, day.CompletedAt, scope.HighWaterClipID,
				scope.ExpectedSourceClips, scope.ExpectedSourceBytes, scope.ExpectedSourceSHA256); err != nil {
				return plan, 0, "", false, err
			}
		}
	}
	return plan, batchRecordID, "snapshotting", true, nil
}

func (s *Server) snapshotJoinedTier1Recording(ctx context.Context, tx pgx.Tx, batchRecordID int64, priority int,
	plan joinedTier1FreezePlan) error {
	if priority < 1 || priority > len(plan.Recordings) {
		return errors.New("Tier-1 snapshot priority differs")
	}
	recording := plan.Recordings[priority-1]
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE joined_tier1_apply_candidates ON COMMIT DROP AS
		SELECT c.id clip_id,c.recording_id,c.recording_job_id,c.storage_destination_id,sd.account_id,sd.provider,c.endpoint,
			sd.endpoint destination_endpoint,sd.region,c.bucket,sd.bucket destination_bucket,c.object_key,c.etag ingest_etag,
			c.size_bytes,c.sha256,c.clip_start_at start_at,c.clip_end_at end_at,c.created_at clip_created_at,c.released_at,
			c.capture_lease_token,c.capture_sequence,c.capture_attempt_id,c.timestamp_contract_version,c.timestamp_contract,
			c.timestamp_contract_status,c.timestamp_contract_reason,scope.scheduled_start_at window_start,
			scope.scheduled_end_at window_end,b.eligibility_cutoff cutoff
		FROM recording_joined_snapshot_scopes scope JOIN recording_joined_batches b ON b.id=scope.batch_record_id
		JOIN recording_clips c ON c.recording_id=scope.recording_id AND c.recording_job_id=scope.recording_job_id
			AND c.id<=scope.high_water_clip_id
		JOIN storage_destinations sd ON sd.id=c.storage_destination_id
		WHERE scope.batch_record_id=$1 AND scope.priority_ordinal=$2 AND c.purged_at IS NULL`, batchRecordID, priority); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE joined_tier1_apply_sources ON COMMIT DROP AS
		SELECT c.clip_id,c.recording_id,c.recording_job_id,c.storage_destination_id,c.account_id,c.provider,c.endpoint,
			c.destination_endpoint,c.region,c.bucket,c.object_key,c.ingest_etag,c.size_bytes,c.sha256,c.start_at,c.end_at,
			c.clip_created_at,c.destination_bucket,c.released_at,c.capture_lease_token,c.capture_sequence,c.capture_attempt_id,
			c.timestamp_contract_version,c.timestamp_contract,c.timestamp_contract_status,c.timestamp_contract_reason,
			row_number() OVER(PARTITION BY c.recording_job_id ORDER BY c.start_at,c.clip_id)::integer day_ordinal
		FROM joined_tier1_apply_candidates c WHERE c.clip_created_at<=c.cutoff AND c.end_at>c.window_start
			AND c.start_at<c.window_end AND c.size_bytes>0`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE joined_tier1_apply_exclusions ON COMMIT DROP AS
		SELECT evidence.*,encode(sha256(convert_to(evidence.canonical_evidence::text,'UTF8')),'hex') evidence_sha256 FROM (
		SELECT c.clip_id,c.recording_id,CASE WHEN c.clip_created_at>c.cutoff THEN 'after_cutoff'
			WHEN c.end_at<=c.window_start OR c.start_at>=c.window_end THEN 'outside_qualification_window'
			ELSE 'nonpositive_source_size' END reason_code,
			jsonb_build_object('clip_id',c.clip_id,'recording_id',c.recording_id,'recording_job_id',c.recording_job_id,
			'created_at',c.clip_created_at,'clip_start_at',c.start_at,'clip_end_at',c.end_at,'size_bytes',c.size_bytes,
			'window_start_at',c.window_start,'window_end_at',c.window_end,'cutoff',c.cutoff) canonical_evidence
		FROM joined_tier1_apply_candidates c WHERE c.clip_created_at>c.cutoff OR c.end_at<=c.window_start
			OR c.start_at>=c.window_end OR c.size_bytes<=0) evidence`); err != nil {
		return err
	}

	mini := plan
	mini.Recordings = []joinedTier1FreezeRecording{recording}
	mini.ExpectedStreamDays = 14
	mini.Recordings[0].SnapshotDays = nil
	mini.Recordings[0].ExpectedSourceClips = 0
	mini.Recordings[0].ExpectedSourceBytes = 0
	projections, err := populateJoinedTier1FrozenEvidence(ctx, tx, &mini, true)
	if err != nil {
		return err
	}
	if mini.ProvisionalSourceClips != recording.ExpectedSourceClips ||
		mini.ProvisionalSourceBytes != recording.ExpectedSourceBytes ||
		mini.ProvisionalExclusions != recording.ExpectedExclusions ||
		mini.FreezeExclusionsSHA256 != recording.ExpectedExclusionsSHA256 {
		return errors.New("Tier-1 recording snapshot differs from approved v2 facts")
	}
	for i, expected := range recording.SnapshotDays {
		if projections[i].SourceCount != expected.ExpectedSourceClips ||
			projections[i].SourceBytes != expected.ExpectedSourceBytes ||
			projections[i].FrozenSourceSHA256 != expected.ExpectedSourceSHA256 {
			return fmt.Errorf("Tier-1 recording day %d snapshot differs", i+1)
		}
	}

	var batchRecordingID int64
	if err := tx.QueryRow(ctx, `SELECT id FROM recording_joined_batch_recordings
		WHERE batch_record_id=$1 AND priority_ordinal=$2`, batchRecordID, priority).Scan(&batchRecordingID); err != nil {
		return err
	}
	for i, day := range recording.Qualification.Days {
		projection := projections[i]
		if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_stream_days(batch_record_id,batch_recording_id,account_id,
			connection_id,batch_id,recording_id,local_date,date_ordinal,recording_job_id,scheduled_start_at,scheduled_end_at,
			completed_at,source_clip_count,source_bytes,source_snapshot_sha256)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, batchRecordID, batchRecordingID,
			plan.AccountID, plan.ConnectionID, plan.BatchID, recording.Frozen.RecordingID, day.LocalDate,
			day.QualificationWindowOrdinal, day.JobID, day.WindowStart, day.WindowEnd, day.CompletedAt,
			projection.SourceCount, projection.SourceBytes, projection.FrozenSourceSHA256); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_freeze_exclusions(batch_record_id,batch_recording_id,
		recording_id,clip_id,reason_code,evidence_sha256,canonical_evidence)
		SELECT $1,$2,recording_id,clip_id,reason_code,evidence_sha256,canonical_evidence
		FROM joined_tier1_apply_exclusions`, batchRecordID, batchRecordingID); err != nil {
		return err
	}
	if err := s.insertJoinedTier1SourceSnapshotChunk(ctx, tx, batchRecordID, plan.AccountID,
		plan.ConnectionID, batchRecordingID); err != nil {
		return err
	}
	receiptBytes, err := json.Marshal(struct {
		Priority      int    `json:"priority"`
		RecordingID   int64  `json:"recording_id"`
		SourceClips   int64  `json:"source_clips"`
		SourceBytes   int64  `json:"source_bytes"`
		Exclusions    int64  `json:"exclusions"`
		ExclusionsSHA string `json:"exclusions_sha256"`
	}{priority, recording.Frozen.RecordingID, recording.ExpectedSourceClips, recording.ExpectedSourceBytes,
		recording.ExpectedExclusions, recording.ExpectedExclusionsSHA256})
	if err != nil {
		return fmt.Errorf("marshal snapshot receipt for recording %d: %w", recording.Frozen.RecordingID, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_snapshot_chunks(batch_record_id,batch_recording_id,
		priority_ordinal,recording_id,expected_source_clips,expected_source_bytes,expected_exclusions,
		expected_exclusions_sha256,actual_source_clips,actual_source_bytes,actual_exclusions,
		actual_exclusions_sha256,receipt_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$5,$6,$7,$8,$9)`,
		batchRecordID, batchRecordingID, priority, recording.Frozen.RecordingID, recording.ExpectedSourceClips,
		recording.ExpectedSourceBytes, recording.ExpectedExclusions, recording.ExpectedExclusionsSHA256,
		sha256Bytes(receiptBytes)); err != nil {
		return err
	}
	if s.joinedFreezeChunkHook != nil {
		if err := s.joinedFreezeChunkHook(ctx, priority); err != nil {
			return fmt.Errorf("after Tier-1 source snapshot chunk: %w", err)
		}
	}
	return nil
}

func (s *Server) insertJoinedTier1SourceSnapshotChunk(ctx context.Context, tx pgx.Tx, batchRecordID, accountID,
	connectionID, batchRecordingID int64) error {
	var expected int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(sum(source_clip_count),0)
		FROM recording_joined_stream_days WHERE batch_recording_id=$1`, batchRecordingID).Scan(&expected); err != nil {
		return err
	}
	command, err := tx.Exec(ctx, `INSERT INTO recording_joined_source_snapshots(batch_record_id,stream_day_id,account_id,
		connection_id,recording_id,recording_job_id,clip_id,storage_destination_id,day_ordinal,provider,endpoint,region,bucket,
		object_key,ingest_etag,size_bytes,sha256,start_at,end_at,clip_created_at,released_at,capture_lease_token,
		capture_sequence,capture_attempt_id,timestamp_contract_version,timestamp_contract,timestamp_contract_status,
		timestamp_contract_reason)
		SELECT $1,d.id,$2,$3,t.recording_id,t.recording_job_id,t.clip_id,t.storage_destination_id,t.day_ordinal,t.provider,
			t.endpoint,t.region,t.bucket,t.object_key,t.ingest_etag,t.size_bytes,t.sha256,t.start_at,t.end_at,t.clip_created_at,
			t.released_at,t.capture_lease_token,t.capture_sequence,t.capture_attempt_id,t.timestamp_contract_version,
			t.timestamp_contract,t.timestamp_contract_status,t.timestamp_contract_reason
		FROM joined_tier1_apply_sources t JOIN recording_joined_stream_days d
			ON d.batch_record_id=$1 AND d.batch_recording_id=$4 AND d.recording_id=t.recording_id
			AND d.recording_job_id=t.recording_job_id ORDER BY d.date_ordinal,t.day_ordinal`,
		batchRecordID, accountID, connectionID, batchRecordingID)
	if err != nil {
		return fmt.Errorf("insert Tier-1 source snapshot chunk: %w", err)
	}
	if command.RowsAffected() != expected {
		return fmt.Errorf("insert Tier-1 source snapshot chunk: rows=%d want=%d", command.RowsAffected(), expected)
	}
	return nil
}
