package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type stitchPlanCandidate struct {
	AccountID, RecordingID, JobID                                      int64
	Rank, ClipDuration, LayoutChanges                                  int
	Deadline, FireAt, WindowEnd, JobCompleted, HealthCalculated, DBNow time.Time
	IdempotencyKey                                                     string
}

type stitchPlanSummary struct {
	Selected   int            `json:"selected"`
	Created    int            `json:"created"`
	Existing   int            `json:"existing"`
	Stale      int            `json:"stale"`
	Rejected   int            `json:"rejected"`
	Rejections map[string]int `json:"rejections"`
	DryRun     bool           `json:"dry_run"`
}

func runRecordingStitch(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || args[0] != "plan" {
		log.Fatal("recording-stitch requires plan")
	}
	fs := flag.NewFlagSet("recording-stitch plan", flag.ExitOnError)
	accountID := fs.Int64("account-id", 0, "exact account id")
	limit := fs.Int("limit", 20, "maximum windows to plan")
	apply := fs.Bool("apply", false, "persist immutable tasks")
	_ = fs.Parse(args[1:])
	if len(fs.Args()) != 0 || *accountID <= 0 || *limit < 1 || *limit > 32 {
		log.Fatal("--account-id and --limit 1..32 are required")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='30s'`); err != nil {
		log.Fatal(err)
	}
	candidates, err := selectNativeStitchCandidates(ctx, tx, *accountID, *limit)
	if err != nil {
		log.Fatal(err)
	}
	summary := stitchPlanSummary{Selected: len(candidates), DryRun: !*apply, Rejections: map[string]int{}}
	for _, c := range candidates {
		outcome, reason := planOneNativeStitch(ctx, tx, c, *apply)
		switch outcome {
		case "created":
			summary.Created++
		case "existing":
			summary.Existing++
		case "stale":
			summary.Stale++
		default:
			summary.Rejected++
			summary.Rejections[reason]++
		}
	}
	if *apply {
		if err := tx.Commit(ctx); err != nil {
			log.Fatalf("commit stitch plan: %v", err)
		}
	} else {
		_ = tx.Rollback(ctx)
	}
	printJSON(summary)
}

func selectNativeStitchCandidates(ctx context.Context, tx pgx.Tx, accountID int64, limit int) ([]stitchPlanCandidate, error) {
	if accountID <= 0 || limit < 1 || limit > 32 {
		return nil, fmt.Errorf("invalid bounded stitch candidate selection")
	}
	rows, err := tx.Query(ctx, `WITH eligible AS (
		SELECT t.account_id,e.recording_id,e.rank,t.deadline_at,j.id,j.fire_at,j.window_end_at,j.completed_at,
		       j.clip_duration_sec,j.idempotency_key,now() db_now,h.calculated_at,h.layout_change_count,
		       row_number() OVER(PARTITION BY e.recording_id ORDER BY (h.layout_change_count=0) DESC,j.window_end_at DESC) wave
		FROM recording_campaign_tracks t
		JOIN recording_campaign_roster_entries e ON e.track_id=t.id AND e.role='primary' AND e.status IN('protect','probation')
		JOIN recordings r ON r.id=e.recording_id AND r.account_id=t.account_id AND r.mode='continuous'
		JOIN recording_jobs j ON j.recording_id=r.id AND j.kind='continuous_window' AND j.status='done'
		JOIN recording_window_health h ON h.recording_id=r.id AND h.job_id=j.id
		WHERE t.account_id=$1 AND t.campaign_key='delivery30' AND t.state IN('active','complete')
		  AND j.window_end_at<=t.deadline_at AND j.window_end_at>=t.deadline_at-interval '14 days'
		  AND j.completed_at>=j.window_end_at AND j.window_end_at<=now()-interval '10 minutes'
		  AND j.idempotency_key='reccont:'||j.recording_id||':'||extract(epoch from j.fire_at)::bigint
		  AND h.metric_version>=2 AND h.expected_seconds=extract(epoch from j.window_end_at-j.fire_at)::bigint AND h.expected_seconds=43200
		  AND h.coverage_pct>=95 AND h.largest_gap_seconds<=900 AND h.gap_over_5m_count<=1 AND h.gap_over_30s_count<=6 AND h.overlap_count=0
		  AND NOT EXISTS(SELECT 1 FROM recording_native_stitch_tasks existing
		                 WHERE existing.account_id=t.account_id AND existing.recording_job_id=j.id AND existing.policy_version=$3)
	) SELECT account_id,recording_id,rank,deadline_at,id,fire_at,window_end_at,completed_at,clip_duration_sec,idempotency_key,db_now,calculated_at,layout_change_count
	FROM eligible ORDER BY wave,(layout_change_count=0) DESC,rank,window_end_at DESC LIMIT $2`, accountID, limit, stitchcert.PolicyVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	candidates := []stitchPlanCandidate{}
	for rows.Next() {
		var c stitchPlanCandidate
		if err := rows.Scan(&c.AccountID, &c.RecordingID, &c.Rank, &c.Deadline, &c.JobID, &c.FireAt, &c.WindowEnd, &c.JobCompleted, &c.ClipDuration, &c.IdempotencyKey, &c.DBNow, &c.HealthCalculated, &c.LayoutChanges); err != nil {
			return nil, err
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

type consumedStitchIntent struct {
	recording, destination, max int64
	endpoint, bucket, key, path string
}

func planOneNativeStitch(ctx context.Context, tx pgx.Tx, c stitchPlanCandidate, apply bool) (string, string) {
	var status, kind, idempotency string
	var completed *time.Time
	var scheduled, fire, windowEnd time.Time
	var clipDuration int
	err := tx.QueryRow(ctx, `SELECT status,kind,completed_at,scheduled_for,fire_at,window_end_at,clip_duration_sec,idempotency_key FROM recording_jobs WHERE id=$1 AND recording_id=$2 FOR SHARE`, c.JobID, c.RecordingID).Scan(&status, &kind, &completed, &scheduled, &fire, &windowEnd, &clipDuration, &idempotency)
	if err != nil || status != "done" || kind != "continuous_window" || completed == nil || completed.Before(windowEnd) || !fire.Equal(c.FireAt) || !windowEnd.Equal(c.WindowEnd) || windowEnd.Sub(fire) != 12*time.Hour || idempotency != fmt.Sprintf("reccont:%d:%d", c.RecordingID, fire.Unix()) || scheduled.Before(fire) {
		return "rejected", "invalid_scheduler_job_snapshot"
	}
	intentRows, err := tx.Query(ctx, `SELECT status,expires_at,recording_id,storage_destination_id,endpoint,bucket,object_key,display_path,max_size_bytes FROM recording_upload_intents WHERE recording_job_id=$1 FOR SHARE`, c.JobID)
	if err != nil {
		return "rejected", "intent_query_failed"
	}
	consumed := []consumedStitchIntent{}
	active := false
	for intentRows.Next() {
		var s, endpoint, bucket, key, path string
		var expires time.Time
		var recording, destination, max int64
		if intentRows.Scan(&s, &expires, &recording, &destination, &endpoint, &bucket, &key, &path, &max) != nil {
			intentRows.Close()
			return "rejected", "intent_scan_failed"
		}
		if s == "pending" && expires.After(c.DBNow) {
			active = true
		}
		if s == "consumed" {
			consumed = append(consumed, consumedStitchIntent{recording, destination, max, endpoint, bucket, key, path})
		}
	}
	intentRows.Close()
	if active {
		return "rejected", "active_upload_intent"
	}
	for _, v := range consumed {
		var exists bool
		if tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_clips WHERE recording_id=$1 AND recording_job_id=$2 AND storage_destination_id=$3 AND endpoint=$4 AND bucket=$5 AND object_key=$6 AND display_path=$7 AND size_bytes<=$8 AND capture_lease_token IS NOT NULL AND capture_sequence>0)`, v.recording, c.JobID, v.destination, v.endpoint, v.bucket, v.key, v.path, v.max).Scan(&exists) != nil || !exists {
			return "rejected", "consumed_intent_without_exact_clip"
		}
	}
	var healthAt time.Time
	var metric int
	var healthRaw []byte
	err = tx.QueryRow(ctx, `SELECT calculated_at,metric_version,jsonb_build_object('expected_seconds',expected_seconds,'covered_seconds',covered_seconds,'coverage_pct',coverage_pct,'largest_gap_seconds',largest_gap_seconds,'gap_count',gap_count,'gap_over_30s_count',gap_over_30s_count,'gap_over_5m_count',gap_over_5m_count,'overlap_count',overlap_count,'overlap_seconds',overlap_seconds,'layout_change_count',layout_change_count,'clip_count',clip_count,'metric_version',metric_version) FROM recording_window_health WHERE recording_id=$1 AND job_id=$2 FOR SHARE`, c.RecordingID, c.JobID).Scan(&healthAt, &metric, &healthRaw)
	if err != nil || metric < 2 {
		return "rejected", "health_missing_or_old"
	}
	clips, manifestSHA, total, err := loadPlannerManifest(ctx, tx, c.AccountID, c.RecordingID, c.JobID, c.FireAt, c.WindowEnd)
	if err != nil {
		return "rejected", "manifest_invalid"
	}
	var latestCreated *time.Time
	var currentCount int
	if err = tx.QueryRow(ctx, `SELECT count(*),max(created_at) FROM recording_clips WHERE recording_id=$1 AND clip_end_at>$2 AND clip_start_at<$3`, c.RecordingID, c.FireAt, c.WindowEnd).Scan(&currentCount, &latestCreated); err != nil || currentCount != len(clips) || latestCreated == nil || latestCreated.After(healthAt) {
		return "rejected", "late_or_changed_clips"
	}
	var existingState, existingSHA string
	err = tx.QueryRow(ctx, `SELECT state,clip_manifest_sha256 FROM recording_native_stitch_tasks WHERE account_id=$1 AND recording_job_id=$2 AND policy_version=$3 FOR UPDATE`, c.AccountID, c.JobID, stitchcert.PolicyVersion).Scan(&existingState, &existingSHA)
	if err == nil {
		if existingSHA == manifestSHA {
			return "existing", ""
		}
		if existingState == "pending" && apply {
			_, _ = tx.Exec(ctx, `UPDATE recording_native_stitch_tasks SET state='stale',last_reason_code='server_manifest_changed' WHERE account_id=$1 AND recording_job_id=$2 AND policy_version=$3`, c.AccountID, c.JobID, stitchcert.PolicyVersion)
		}
		return "stale", "manifest_lineage_changed"
	}
	if err != pgx.ErrNoRows {
		return "rejected", "existing_task_query_failed"
	}
	if !apply {
		return "created", ""
	}
	priority := c.Rank*10 + c.LayoutChanges
	if priority < 1 || priority > 10000 {
		return "rejected", "priority_out_of_range"
	}
	manifestJSON, _ := json.Marshal(clips)
	jobFacts, _ := json.Marshal(map[string]any{"kind": kind, "fire_at": fire, "window_end_at": windowEnd, "scheduled_for": scheduled, "completed_at": completed, "clip_duration_sec": clipDuration, "idempotency_key": idempotency})
	var taskID int64
	err = tx.QueryRow(ctx, `INSERT INTO recording_native_stitch_tasks(account_id,recording_id,recording_job_id,window_start_at,window_end_at,health_calculated_at,health_metric_version,health_facts,job_schedule_facts,clip_manifest,clip_manifest_sha256,clip_count,source_bytes,policy_version,priority) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING id`, c.AccountID, c.RecordingID, c.JobID, c.FireAt, c.WindowEnd, healthAt, metric, healthRaw, jobFacts, manifestJSON, manifestSHA, len(clips), total, stitchcert.PolicyVersion, priority).Scan(&taskID)
	if err != nil {
		return "rejected", "task_insert_failed"
	}
	for _, clip := range clips {
		if _, err = tx.Exec(ctx, `INSERT INTO recording_native_stitch_task_clips(task_id,ordinal,clip_id,relative_path,size_bytes,sha256) VALUES($1,$2,$3,$4,$5,$6)`, taskID, clip.Ordinal, clip.ClipID, clip.RelativePath, clip.SizeBytes, clip.SHA256); err != nil {
			return "rejected", "task_clip_insert_failed"
		}
	}
	return "created", ""
}

func loadPlannerManifest(ctx context.Context, tx pgx.Tx, accountID, recordingID, jobID int64, start, end time.Time) ([]stitchcert.ManifestClip, string, int64, error) {
	rows, err := tx.Query(ctx, `SELECT c.id,c.recording_id,COALESCE(c.recording_job_id,0),COALESCE(c.display_path,''),c.size_bytes,lower(c.sha256),c.clip_start_at,c.clip_end_at,c.capture_lease_token,c.capture_sequence,c.purged_at,c.capture_attempt_id,c.timestamp_contract_version,c.timestamp_contract_status,COALESCE(c.timestamp_contract_reason,''),c.timestamp_contract FROM recording_clips c JOIN recordings r ON r.id=c.recording_id WHERE r.account_id=$1 AND c.recording_id=$2 AND c.clip_end_at>$3 AND c.clip_start_at<$4 ORDER BY c.clip_start_at,c.id FOR SHARE OF c`, accountID, recordingID, start, end)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()
	clips := []stitchcert.ManifestClip{}
	for rows.Next() {
		var c stitchcert.ManifestClip
		var token *uuid.UUID
		var seq *int64
		var purged *time.Time
		var attempt *uuid.UUID
		var version, status *string
		var reason string
		var contract []byte
		if err := rows.Scan(&c.ClipID, &c.RecordingID, &c.RecordingJobID, &c.RelativePath, &c.SizeBytes, &c.SHA256, &c.ClipStartAt, &c.ClipEndAt, &token, &seq, &purged, &attempt, &version, &status, &reason, &contract); err != nil {
			return nil, "", 0, err
		}
		if c.RecordingJobID != jobID || token == nil || seq == nil || purged != nil {
			return nil, "", 0, fmt.Errorf("noncanonical clip")
		}
		c.Ordinal = len(clips) + 1
		c.CaptureGeneration = plannerGeneration(token)
		c.CaptureSequence = *seq
		if attempt != nil {
			c.CaptureAttemptID = attempt.String()
		}
		if version != nil {
			c.TimestampContractVersion = *version
		}
		if status != nil {
			c.TimestampContractStatus = *status
		}
		c.TimestampContractReason = reason
		if len(contract) != 0 {
			var typed stitchcert.TimestampContract
			if err := json.Unmarshal(contract, &typed); err != nil {
				return nil, "", 0, err
			}
			c.TimestampContractSHA256, _, err = stitchcert.CanonicalSHA(typed)
			if err != nil {
				return nil, "", 0, err
			}
		}
		clips = append(clips, c)
	}
	total, err := stitchcert.ValidateManifest(clips, recordingID, jobID, start, end)
	if err != nil {
		return nil, "", 0, err
	}
	digest, _, err := stitchcert.CanonicalSHA(clips)
	return clips, digest, total, err
}

func plannerGeneration(token *uuid.UUID) string {
	sum := sha256.Sum256([]byte(token.String()))
	return "sha256:" + hex.EncodeToString(sum[:])
}
