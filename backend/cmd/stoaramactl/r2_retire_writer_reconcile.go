package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type r2WriterReconcileOptions struct{ out, bucket string }

func parseR2WriterReconcileArgs(args []string) (r2WriterReconcileOptions, error) {
	var o r2WriterReconcileOptions
	fs := flag.NewFlagSet("r2-retire reconcile-writers", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&o.out, "out", "", "new JSONL evidence file (never overwritten)")
	fs.StringVar(&o.bucket, "bucket", "stoarama", "retirement bucket")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() != 0 || o.out == "" || o.bucket == "" {
		return o, errors.New("require --out FILE and nonempty --bucket; reconciliation is dry-run only")
	}
	return o, nil
}

type r2WriterCategory string

const (
	r2WriterRecordings      r2WriterCategory = "recordings"
	r2WriterJobs            r2WriterCategory = "recording_jobs"
	r2WriterStreams         r2WriterCategory = "streams_on"
	r2WriterRuntime         r2WriterCategory = "capture_runtime"
	r2WriterCaptureLease    r2WriterCategory = "capture_leases"
	r2WriterRecordingUpload r2WriterCategory = "recording_upload_intents"
	r2WriterUpload          r2WriterCategory = "upload_intents"
	r2WriterOperation       r2WriterCategory = "wire_operations"
	r2WriterCapability      r2WriterCategory = "capabilities"
	r2WriterTransfer        r2WriterCategory = "transfer_jobs"
	r2WriterInference       r2WriterCategory = "inference_jobs"
	r2WriterRestore         r2WriterCategory = "restore_requests"
	r2WriterStitch          r2WriterCategory = "stitch_tasks"
	r2WriterNativeStitch    r2WriterCategory = "native_stitch_tasks"
	r2WriterPresentation    r2WriterCategory = "presentation_tasks"
	r2WriterJoinedBatch     r2WriterCategory = "joined_batches"
	r2WriterJoinedHour      r2WriterCategory = "joined_hours"
	r2WriterJoinedDay       r2WriterCategory = "joined_days"
	r2WriterJoinedArtifact  r2WriterCategory = "joined_artifacts"
)

type r2WriterAuditSpec struct {
	category r2WriterCategory
	query    string
}

// These are exactly the blocker predicates in migration 0187. Select only
// lifecycle/owner observations: never bearer tokens, URLs, plans or media blobs.
var r2WriterAuditSpecs = []r2WriterAuditSpec{
	{r2WriterRecordings, `SELECT jsonb_build_object('id',id,'stream_id',stream_id,'state',status,'updated_at',updated_at,'last_clip_at',last_clip_at) FROM recordings WHERE status='active' ORDER BY id`},
	{r2WriterJobs, `SELECT jsonb_build_object('id',id,'recording_id',recording_id,'state',status,'owner',lease_owner,'lease_expires_at',lease_expires_at,'updated_at',updated_at) FROM recording_jobs WHERE status IN ('pending','leased') ORDER BY id`},
	{r2WriterStreams, `SELECT jsonb_build_object('id',s.id,'state',s.recording_state,'updated_at',s.updated_at,'execution_class',s.execution_class,'runtime_state',r.status,'last_frame_at',r.last_frame_at,'runtime_updated_at',r.updated_at,'owner',l.lease_owner,'heartbeat_at',l.heartbeat_at,'lease_expires_at',l.lease_expires_at) FROM streams s LEFT JOIN stream_capture_runtime r ON r.stream_id=s.id LEFT JOIN capture_session_leases l ON l.stream_id=s.id WHERE s.recording_state='on' ORDER BY s.id`},
	{r2WriterRuntime, `SELECT jsonb_build_object('id',r.stream_id,'state',r.status,'execution_class',r.execution_class,'updated_at',r.updated_at,'last_resolved_at',r.last_resolved_at,'last_frame_at',r.last_frame_at,'stream_state',s.recording_state,'owner',l.lease_owner,'heartbeat_at',l.heartbeat_at,'lease_expires_at',l.lease_expires_at) FROM stream_capture_runtime r JOIN streams s ON s.id=r.stream_id LEFT JOIN capture_session_leases l ON l.stream_id=r.stream_id WHERE r.status IN ('resolving','running') ORDER BY r.stream_id`},
	{r2WriterCaptureLease, `SELECT jsonb_build_object('id',stream_id,'owner',lease_owner,'heartbeat_at',heartbeat_at,'lease_expires_at',lease_expires_at,'updated_at',updated_at) FROM capture_session_leases WHERE lease_expires_at>clock_timestamp() ORDER BY stream_id`},
	{r2WriterRecordingUpload, `SELECT jsonb_build_object('id',id,'recording_id',recording_id,'expires_at',expires_at) FROM recording_upload_intents WHERE bucket=$1 AND expires_at>clock_timestamp() ORDER BY id`},
	{r2WriterUpload, `SELECT jsonb_build_object('id',id,'expires_at',expires_at) FROM upload_intents WHERE bucket=$1 AND expires_at>clock_timestamp() ORDER BY id`},
	{r2WriterOperation, `SELECT jsonb_build_object('id',id,'bucket',bucket,'started_at',started_at) FROM r2_retirement_writer_operations WHERE completed_at IS NULL ORDER BY id`},
	{r2WriterCapability, `SELECT jsonb_build_object('id',id,'expires_at',expires_at) FROM r2_retirement_writer_capabilities WHERE bucket=$1 AND expires_at>clock_timestamp() ORDER BY id`},
	{r2WriterTransfer, `SELECT jsonb_build_object('id',id,'state',status,'owner',lease_owner,'lease_expires_at',lease_expires_at,'updated_at',updated_at) FROM clip_transfer_jobs WHERE status IN ('pending','leased') ORDER BY id`},
	{r2WriterInference, `SELECT jsonb_build_object('id',id,'state',status,'owner',lease_owner,'lease_expires_at',lease_expires_at,'updated_at',updated_at) FROM inference_box_jobs WHERE status IN ('pending','leased') ORDER BY id`},
	{r2WriterRestore, `SELECT jsonb_build_object('id',id,'state',state,'put_expires_at',put_expires_at,'lease_expires_at',lease_expires_at,'leased_at',leased_at,'completed_at',completed_at) FROM nas_restore_requests WHERE state IN ('pending','leased') OR put_expires_at>clock_timestamp() ORDER BY id`},
	{r2WriterStitch, `SELECT jsonb_build_object('id',t.id,'state',t.state,'scope_id',t.scope_id,'recording_id',t.recording_id,'plan_sha256',t.plan_sha256,'attempt',t.attempt,'lease_expires_at',t.lease_expires_at,'updated_at',t.updated_at,'cleanup_plan_id',t.cleanup_plan_id,'stream_id',s.stream_id,'scope_paused',s.paused,'recording_state',r.status) FROM nas_stitch_tasks t JOIN nas_stitch_scopes s ON s.id=t.scope_id JOIN recordings r ON r.id=t.recording_id WHERE t.state IN ('pending','leased') ORDER BY t.id`},
	{r2WriterNativeStitch, `SELECT jsonb_build_object('id',id,'state',state,'lease_expires_at',lease_expires_at,'updated_at',updated_at) FROM recording_native_stitch_tasks WHERE state IN ('pending','leased') ORDER BY id`},
	{r2WriterPresentation, `SELECT jsonb_build_object('id',id,'state',state,'lease_expires_at',lease_expires_at,'updated_at',updated_at) FROM recording_presentation_v2_probe_tasks WHERE state IN ('awaiting_retention','pending','leased') ORDER BY id`},
	{r2WriterJoinedBatch, `SELECT jsonb_build_object('id',b.id,'state',b.state,'batch_id',b.batch_id,'account_id',b.account_id,'connection_id',b.connection_id,'created_at',b.created_at,'frozen_at',b.frozen_at,'claims_paused',a.claims_paused,'admission_updated_at',a.updated_at) FROM recording_joined_batches b LEFT JOIN recording_joined_admission_controls a ON a.batch_record_id=b.id WHERE b.state IN ('snapshotting','building','frozen','index_sealed') ORDER BY b.id`},
	{r2WriterJoinedHour, `SELECT jsonb_build_object('id',id,'state',state,'batch_record_id',batch_record_id,'batch_id',batch_id,'hour_id',hour_id,'recording_id',recording_id,'owner',claimed_by,'heartbeat_at',heartbeat_at,'lease_expires_at',lease_expires_at,'attempt_count',attempt_count,'updated_at',updated_at) FROM recording_joined_hours WHERE state IN ('pending','leased') ORDER BY id`},
	{r2WriterJoinedDay, `SELECT jsonb_build_object('id',id,'state',state,'batch_record_id',batch_record_id,'batch_id',batch_id) FROM recording_joined_stream_days WHERE state='pending' ORDER BY id`},
	{r2WriterJoinedArtifact, `SELECT jsonb_build_object('id',id,'state',publication_state,'batch_record_id',batch_record_id,'batch_id',batch_id,'artifact_kind',artifact_kind,'owner',publication_claimed_by,'heartbeat_at',publication_heartbeat_at,'lease_expires_at',publication_lease_expires_at,'attempt_count',publication_attempt_count,'published_at',published_at,'updated_at',updated_at) FROM recording_joined_artifacts WHERE publication_state IN ('sealed','publishing') ORDER BY id`},
}

type r2WriterAuditHeader struct {
	Type           string                   `json:"type"`
	Version        int                      `json:"version"`
	DryRun         bool                     `json:"dry_run"`
	ObservedAt     time.Time                `json:"observed_at"`
	Bucket         string                   `json:"bucket"`
	IndexesReady   bool                     `json:"indexes_ready"`
	WritersDrained bool                     `json:"writers_drained"`
	Fence          json.RawMessage          `json:"fence"`
	Counts         map[r2WriterCategory]int `json:"counts"`
	ProvenDead     int                      `json:"proven_dead"`
	ApplyCommand   *string                  `json:"apply_command"`
}

type r2WriterAuditItem struct {
	Type              string           `json:"type"`
	Category          r2WriterCategory `json:"category"`
	Observation       json.RawMessage  `json:"observation"`
	ObservationSHA256 string           `json:"observation_sha256"`
	ProvenDead        bool             `json:"proven_dead"`
	Blocker           string           `json:"blocker"`
}

// Reporting is supported independently of lifecycle cancellation. The database
// cannot prove process death on an unidentified/remote owner. In particular an
// expired lease is not cancellation authority. Do not expose --apply until a
// reviewed owner-proof protocol and atomic audited terminal transitions exist.
func r2WriterAudit(ctx context.Context, pool *pgxpool.Pool, bucket string) (r2WriterAuditHeader, []r2WriterAuditItem, error) {
	h := r2WriterAuditHeader{Type: "writer_reconciliation_dry_run", Version: 1, DryRun: true, Bucket: bucket, Counts: make(map[r2WriterCategory]int)}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return h, nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL statement_timeout='15s'; SET LOCAL work_mem='16MB'`); err != nil {
		return h, nil, err
	}
	err = tx.QueryRow(ctx, `SELECT clock_timestamp(),r2_retirement_delivery_indexes_ready(),r2_retirement_writers_drained($1),coalesce((SELECT jsonb_agg(jsonb_build_object('bucket',bucket,'activated_at',activated_at,'drain_not_before',drain_not_before)) FROM r2_retirement_writer_fences),'[]'::jsonb)`, bucket).Scan(&h.ObservedAt, &h.IndexesReady, &h.WritersDrained, &h.Fence)
	if err != nil {
		return h, nil, fmt.Errorf("read readiness header: %w", err)
	}
	var items []r2WriterAuditItem
	for _, spec := range r2WriterAuditSpecs {
		h.Counts[spec.category] = 0
		args := []any{}
		if spec.category == r2WriterRecordingUpload || spec.category == r2WriterUpload || spec.category == r2WriterCapability {
			args = append(args, bucket)
		}
		rows, err := tx.Query(ctx, spec.query, args...)
		if err != nil {
			return h, nil, fmt.Errorf("read %s: %w", spec.category, err)
		}
		for rows.Next() {
			var observation json.RawMessage
			if err := rows.Scan(&observation); err != nil {
				rows.Close()
				return h, nil, err
			}
			digest := sha256.Sum256(observation)
			blocker := "owner_process_stop_and_no_recent_writes_not_proven"
			if spec.category == r2WriterJoinedBatch || spec.category == r2WriterJoinedHour || spec.category == r2WriterJoinedDay || spec.category == r2WriterJoinedArtifact {
				blocker = "owner_stop_and_supported_audited_terminal_transition_not_proven"
			}
			items = append(items, r2WriterAuditItem{Type: "writer", Category: spec.category, Observation: observation, ObservationSHA256: hex.EncodeToString(digest[:]), Blocker: blocker})
			h.Counts[spec.category]++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return h, nil, fmt.Errorf("read %s rows: %w", spec.category, err)
		}
	}
	// clock_timestamp() predicates can expire within this read-only snapshot.
	// A mismatch is diagnostic evidence, never a reason to mark a row reconciled.
	if err = tx.Commit(ctx); err != nil {
		return h, nil, err
	}
	return h, items, nil
}

func writeR2WriterAudit(path string, h r2WriterAuditHeader, items []r2WriterAuditItem) (err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(path)
		}
	}()
	enc := json.NewEncoder(f)
	if err = enc.Encode(h); err != nil {
		return err
	}
	for _, item := range items {
		if err = enc.Encode(item); err != nil {
			return err
		}
	}
	if err = f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

func runR2WriterReconcile(ctx context.Context, cfg config.Config, args []string) {
	opts, err := parseR2WriterReconcileArgs(args)
	if err != nil {
		log.Fatal(err)
	}
	pool, err := r2RetireOpenPool(ctx, cfg, true)
	if err != nil {
		log.Fatal("writer reconciliation database unavailable")
	}
	defer pool.Close()
	h, items, err := r2WriterAudit(ctx, pool, opts.bucket)
	if err != nil {
		log.Fatal("writer reconciliation observation failed: ", err)
	}
	if err = writeR2WriterAudit(opts.out, h, items); err != nil {
		log.Fatal("write writer reconciliation evidence: ", err)
	}
	log.Printf("dry-run only: %d blockers, %d proven dead; evidence %s; no apply command authorized", len(items), h.ProvenDead, opts.out)
}
