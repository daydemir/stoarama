package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

const finishTarget = 10000
const finishEvidenceAge = 5 * time.Minute
const finishLock int64 = 7343226011199

type finishStop string

const (
	finishEvidence        finishStop = "evidence_unavailable"
	finishRecovery        finishStop = "cleanup_recovery_problems"
	finishUnknownPlan     finishStop = "unknown_enabled_cleanup"
	finishMismatch        finishStop = "copy_mismatch_latched"
	finishShutdown        finishStop = "shutdown_or_recovery_failed"
	finishMaster          finishStop = "deletion_master_mismatch"
	finishDatabase        finishStop = "database_unavailable"
	finishImport          finishStop = "manifest_import_failed"
	finishBusy            finishStop = "tick_already_running"
	finishOutput          finishStop = "status_write_failed"
	finishListingConflict finishStop = "listing_size_conflict"
)

func (s finishStop) Error() string { return string(s) }

// The sole controller collects these observations. This command never clears a
// latch, changes a master, or trusts absent fields as evidence of safety.
type finishSafety struct {
	UpdatedAt         time.Time        `json:"updated_at"`
	RecoveryProblems  int64            `json:"recovery_problems"`
	RecoveryCheckedAt time.Time        `json:"recovery_checked_at"`
	RecoveryIssued    int64            `json:"recovery_issued_items"`
	RecoveryVerified  int64            `json:"recovery_verified_items"`
	ShutdownFailed    bool             `json:"shutdown_or_recovery_failed"`
	RecoveryError     *string          `json:"recovery_error"`
	RecoveryPlanID    int64            `json:"recovery_plan_id"`
	RecoveryPlanSHA   string           `json:"recovery_plan_sha256"`
	LastError         *string          `json:"last_error"`
	HeldError         *string          `json:"controller_held_error"`
	LocalMismatch     int64            `json:"local_copy_mismatch_latched"`
	LocalCheckedAt    time.Time        `json:"local_copy_checked_at"`
	HoldsCheckedAt    time.Time        `json:"holds_checked_at"`
	MasterCheckedAt   time.Time        `json:"master_checked_at"`
	KnownPlans        []int64          `json:"known_enabled_plans"`
	KnownBatches      []int64          `json:"known_enabled_batches"`
	Master            bool             `json:"master_deletion_enabled"`
	ExpectedMaster    bool             `json:"expected_deletion_enabled"`
	Holds             map[string]int64 `json:"holds_by_reason"`
}
type finishCopy struct {
	Schema          string                 `json:"schema"`
	UpdatedAt       time.Time              `json:"updated_at"`
	QueueDepth      int64                  `json:"queue_depth"`
	VerifiedInPlace nasCopyStat            `json:"verified_in_place"`
	Downloaded      nasCopyStat            `json:"downloaded"`
	DownloadedBytes int64                  `json:"downloaded_bytes"`
	CommittedBytes  int64                  `json:"committed_bytes"`
	Blocked         int64                  `json:"blocked"`
	BlockedByReason map[string]nasCopyStat `json:"blocked_by_reason"`
	MismatchLatched int64                  `json:"mismatch_latched"`
}
type finishStatus struct {
	Schema             string           `json:"schema"`
	UpdatedAt          time.Time        `json:"updated_at"`
	Stop               finishStop       `json:"stop_reason,omitempty"`
	QueueDepth         int64            `json:"queue_depth"`
	ItemsPerMinute     float64          `json:"items_per_min"`
	Copy               finishCopy       `json:"copy"`
	Holds              map[string]int64 `json:"holds_by_reason"`
	CleanupShouldRun   bool             `json:"cleanup_should_run"`
	CleanupReason      string           `json:"cleanup_reason"`
	ImportedItems      int              `json:"imported_items"`
	ImportedManifests  int              `json:"imported_manifests"`
	ListingConflicts   int64            `json:"listing_size_conflicts"`
	ListingConflictIDs []int64          `json:"listing_size_conflict_source_ids"`
	Exhausted          bool             `json:"candidates_exhausted"`
}

func finishFresh(t, now time.Time) bool {
	return !t.IsZero() && now.Sub(t) <= finishEvidenceAge && t.Sub(now) <= 30*time.Second
}

func finishReadJSON(path string, required []string, out any) error {
	f, err := os.Open(path)
	if err != nil {
		return finishEvidence
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 1<<20+1))
	if err != nil || len(b) > 1<<20 {
		return finishEvidence
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(b, &fields) != nil {
		return finishEvidence
	}
	for _, key := range required {
		if v, ok := fields[key]; !ok || (key != "recovery_error" && key != "last_error" && key != "controller_held_error" && bytes.Equal(bytes.TrimSpace(v), []byte("null"))) {
			return finishEvidence
		}
	}
	if json.Unmarshal(b, out) != nil {
		return finishEvidence
	}
	return nil
}
func finishReadEvidence(safetyPath, copyPath string) (finishSafety, finishCopy, error) {
	var s finishSafety
	var c finishCopy
	if err := finishReadJSON(safetyPath, []string{"updated_at", "recovery_problems", "recovery_checked_at", "recovery_issued_items", "recovery_verified_items", "recovery_plan_id", "recovery_plan_sha256", "last_error", "controller_held_error", "local_copy_mismatch_latched", "local_copy_checked_at", "holds_checked_at", "master_checked_at", "shutdown_or_recovery_failed", "recovery_error", "known_enabled_plans", "known_enabled_batches", "master_deletion_enabled", "expected_deletion_enabled", "holds_by_reason"}, &s); err != nil {
		return s, c, err
	}
	if err := finishReadJSON(copyPath, []string{"schema", "updated_at", "queue_depth", "verified_in_place", "downloaded", "downloaded_bytes", "committed_bytes", "blocked", "blocked_by_reason", "mismatch_latched"}, &c); err != nil {
		return s, c, err
	}
	if !finishFresh(s.UpdatedAt, time.Now()) || !finishFresh(c.UpdatedAt, time.Now()) || c.Schema != "stoarama.nas-r2-copy-status.v1" || s.RecoveryProblems < 0 || c.QueueDepth < 0 || c.Blocked < 0 || c.MismatchLatched < 0 {
		return s, c, finishEvidence
	}
	for _, ids := range [][]int64{s.KnownPlans, s.KnownBatches} {
		seen := map[int64]bool{}
		for _, id := range ids {
			if id <= 0 || seen[id] {
				return s, c, finishEvidence
			}
			seen[id] = true
		}
	}
	for _, n := range s.Holds {
		if n < 0 {
			return s, c, finishEvidence
		}
	}
	return s, c, nil
}
func finishCheck(ctx context.Context, pool *pgxpool.Pool, s finishSafety, c finishCopy) error {
	if !finishFresh(s.UpdatedAt, time.Now()) || !finishFresh(c.UpdatedAt, time.Now()) {
		return finishEvidence
	}
	if s.RecoveryProblems > 0 {
		return finishRecovery
	}
	if !finishFresh(s.RecoveryCheckedAt, time.Now()) || !finishFresh(s.LocalCheckedAt, time.Now()) || !finishFresh(s.HoldsCheckedAt, time.Now()) || !finishFresh(s.MasterCheckedAt, time.Now()) || s.RecoveryIssued < 0 || s.RecoveryVerified != s.RecoveryIssued || s.RecoveryPlanID <= 0 || len(s.RecoveryPlanSHA) != 64 || s.LocalMismatch < 0 {
		return finishEvidence
	}
	var issued int64
	var planSHA string
	if err := pool.QueryRow(ctx, `SELECT p.plan_sha256,(SELECT count(*) FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued) FROM nas_cleanup_plans p WHERE p.id=$1 AND p.connection_id=13`, s.RecoveryPlanID).Scan(&planSHA, &issued); err != nil {
		return finishDatabase
	}
	if issued != s.RecoveryIssued || planSHA != s.RecoveryPlanSHA || len(s.KnownBatches) > 0 || len(s.KnownPlans) > 1 || (len(s.KnownPlans) == 1 && s.KnownPlans[0] != s.RecoveryPlanID) {
		return finishEvidence
	}
	if s.ShutdownFailed || s.RecoveryError != nil || (s.LastError != nil && (s.HeldError == nil || *s.LastError != *s.HeldError)) {
		return finishShutdown
	}
	if c.MismatchLatched > 0 || s.LocalMismatch > 0 {
		return finishMismatch
	}
	var unknown bool
	err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_plans WHERE mode='enabled' AND NOT(id=ANY($1::bigint[]))) OR EXISTS(SELECT 1 FROM nas_cleanup_batches WHERE mode='enabled' AND NOT(id=ANY($2::bigint[])))`, s.KnownPlans, s.KnownBatches).Scan(&unknown)
	if err != nil {
		return finishDatabase
	}
	if unknown {
		return finishUnknownPlan
	}
	if s.Master != s.ExpectedMaster || (s.Master && len(s.KnownPlans)+len(s.KnownBatches) == 0) {
		return finishMaster
	}
	return nil
}

// Listing is a scheduling hint, never custody proof. A conflicting path/size
// observation is held out of the absent tier for reconciliation.
const finishWindowSQL = `WITH windows AS (
 SELECT * FROM jsonb_to_recordset($1::jsonb) AS w(stream_id bigint,timezone text,start_at timestamptz,end_at timestamptz)
), generation AS (
 SELECT generation FROM nas_listing_generations WHERE connection_id=13 AND completed_at IS NOT NULL ORDER BY started_at DESC,generation DESC LIMIT 1
)`
const finishEligibleSQL = `
FROM recording_clips c JOIN recordings r ON r.id=c.recording_id AND r.account_id=47 JOIN windows w ON w.stream_id=r.stream_id LEFT JOIN streams s ON s.id=r.stream_id
WHERE c.purged_at IS NULL AND c.clip_start_at<w.end_at AND c.clip_end_at>w.start_at AND c.clip_end_at>c.clip_start_at
 AND coalesce(nullif(s.local_timezone,''),r.cron_timezone,'')=w.timezone
 AND c.bucket<>'' AND c.object_key LIKE 'managed/%' AND c.size_bytes>0 AND c.size_bytes<=$2 AND lower(c.sha256) ~ '^[a-f0-9]{64}$' AND c.display_path<>''
 AND NOT EXISTS(SELECT 1 FROM nas_r2_copy_items q WHERE q.connection_id=13 AND q.kind='raw' AND q.source_id=c.id)
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_reserved_files f WHERE f.kind='raw' AND f.source_id=c.id)
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id WHERE h.clip_id=c.id AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued)))
 AND NOT EXISTS(SELECT 1 FROM r2_retirement_reservations x WHERE x.kind='raw' AND x.source_id=c.id)
 AND NOT EXISTS(SELECT 1 FROM r2_object_retirements x WHERE x.kind='raw' AND x.source_id=c.id)`
const finishListingConflictSQL = `EXISTS(SELECT 1 FROM nas_listing_files f WHERE f.connection_id=13 AND f.generation=(SELECT generation FROM generation) AND f.relative_path=c.display_path AND f.size_bytes<>c.size_bytes)`
const finishCandidatesSQL = finishWindowSQL + `
SELECT c.id,c.recording_id,r.stream_id,c.bucket,c.object_key,c.display_path,c.size_bytes,lower(c.sha256),c.clip_start_at,c.clip_end_at,
 EXISTS(SELECT 1 FROM nas_listing_files f WHERE f.connection_id=13 AND f.generation=(SELECT generation FROM generation) AND f.relative_path=c.display_path AND f.size_bytes=c.size_bytes) AS likely_present` + finishEligibleSQL + ` AND NOT ` + finishListingConflictSQL + ` ORDER BY likely_present DESC,c.id LIMIT $3`

func finishPolicyJSON() ([]byte, error) {
	_, windows, err := api.Keep28Policy()
	if err != nil {
		return nil, err
	}
	rowsJSON := make([]map[string]any, 0, len(windows))
	for _, w := range windows {
		rowsJSON = append(rowsJSON, map[string]any{"stream_id": w.StreamID, "timezone": w.Timezone, "start_at": w.Start, "end_at": w.End})
	}
	return json.Marshal(rowsJSON)
}
func finishListingConflicts(ctx context.Context, pool *pgxpool.Pool, status *finishStatus) error {
	data, err := finishPolicyJSON()
	if err != nil {
		return err
	}
	return pool.QueryRow(ctx, finishWindowSQL+` SELECT count(*),coalesce((array_agg(c.id ORDER BY c.id))[1:50],'{}'::bigint[]) `+finishEligibleSQL+` AND `+finishListingConflictSQL, data, api.NASR2CopyMaxFile).Scan(&status.ListingConflicts, &status.ListingConflictIDs)
}
func finishCandidates(ctx context.Context, pool *pgxpool.Pool, limit int) ([]api.NASR2CopyItem, error) {
	data, err := finishPolicyJSON()
	if err != nil {
		return nil, err
	}
	var listing bool
	if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_listing_generations WHERE connection_id=13 AND completed_at IS NOT NULL)`).Scan(&listing); err != nil {
		return nil, err
	}
	if !listing {
		return nil, finishEvidence
	}
	rows, err := pool.Query(ctx, finishCandidatesSQL, data, api.NASR2CopyMaxFile, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []api.NASR2CopyItem{}
	var tier bool
	for rows.Next() {
		i := api.NASR2CopyItem{Kind: api.NASR2CopyRaw}
		var present bool
		if err = rows.Scan(&i.SourceID, &i.RecordingID, &i.StreamID, &i.Bucket, &i.ObjectKey, &i.RelativePath, &i.SizeBytes, &i.SHA256, &i.StartAt, &i.EndAt, &present); err != nil {
			return nil, err
		}
		if len(items) > 0 && present != tier {
			break
		}
		tier = present
		items = append(items, i)
	}
	return items, rows.Err()
}
func finishManifest(dir string, items []api.NASR2CopyItem) (nasCopyHeader, string, []api.NASR2CopyItem, error) {
	var h nasCopyHeader
	var body bytes.Buffer
	for _, i := range items {
		if err := json.NewEncoder(&body).Encode(i); err != nil {
			return h, "", nil, err
		}
	}
	source := sha256.Sum256(body.Bytes())
	policy, _, err := api.Keep28Policy()
	if err != nil {
		return h, "", nil, err
	}
	h = nasCopyHeader{Version: 1, PolicySHA256: policy, SourceSHA256: hex.EncodeToString(source[:]), Items: int64(len(items))}
	for _, i := range items {
		h.Bytes += i.SizeBytes
	}
	var manifest bytes.Buffer
	if err = json.NewEncoder(&manifest).Encode(map[string]any{"header": h}); err != nil {
		return h, "", nil, err
	}
	manifest.Write(body.Bytes())
	sum := sha256.Sum256(manifest.Bytes())
	digest := hex.EncodeToString(sum[:])
	path := filepath.Join(dir, digest+".jsonl")
	if err = os.WriteFile(path, manifest.Bytes(), 0600); err != nil {
		return h, "", nil, err
	}
	parsed, validated, err := readNASCopyBatch(path, digest)
	return parsed, digest, validated, err
}
func finishDepth(ctx context.Context, pool *pgxpool.Pool, status *finishStatus) error {
	return pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE state IN ('pending','leased')),count(*) FILTER(WHERE state='verified' AND verified_at>=clock_timestamp()-interval '5 minutes')/5.0 FROM nas_r2_copy_items WHERE connection_id=13 AND kind='raw'`).Scan(&status.QueueDepth, &status.ItemsPerMinute)
}
func finishTick(ctx context.Context, pool *pgxpool.Pool, safetyPath, copyPath string) (status finishStatus, err error) {
	status = finishStatus{Schema: "stoarama.finish-status.v1", CleanupReason: "copy_not_holding_on_free_space"}
	defer func() {
		status.UpdatedAt = time.Now().UTC()
		if err != nil {
			var stop finishStop
			if !errors.As(err, &stop) {
				stop = finishImport
			}
			status.Stop = stop
			status.CleanupShouldRun = false
			status.CleanupReason = "safety_stop"
		}
	}()
	conn, e := pool.Acquire(ctx)
	if e != nil {
		return status, finishDatabase
	}
	defer conn.Release()
	var locked bool
	if e = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, finishLock).Scan(&locked); e != nil {
		return status, finishDatabase
	}
	if !locked {
		return status, finishBusy
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, e := conn.Exec(cleanup, `SELECT pg_advisory_unlock($1)`, finishLock); e != nil {
			_ = conn.Conn().Close(cleanup)
		}
	}()
	dir, e := os.MkdirTemp("", "stoarama-finish-")
	if e != nil {
		return status, finishImport
	}
	defer os.RemoveAll(dir)
	for {
		s, c, e := finishReadEvidence(safetyPath, copyPath)
		status.Copy = c
		status.QueueDepth = c.QueueDepth
		status.Holds = s.Holds
		if e != nil {
			return status, e
		}
		if e = finishCheck(ctx, pool, s, c); e != nil {
			return status, e
		}
		if e = finishDepth(ctx, pool, &status); e != nil {
			return status, finishDatabase
		}
		status.CleanupShouldRun = s.Holds["free_space"] > 0
		status.CleanupReason = "copy_not_holding_on_free_space"
		if status.CleanupShouldRun {
			status.CleanupReason = "copy_holding_on_free_space"
		}
		// At most 10k new rows per tick, even while workers consume the queue.
		remaining := finishTarget - int(status.QueueDepth)
		if left := finishTarget - status.ImportedItems; remaining > left {
			remaining = left
		}
		if remaining <= 0 {
			return status, nil
		}
		if remaining > 5000 {
			remaining = 5000
		}
		items, e := finishCandidates(ctx, pool, remaining)
		if e != nil {
			if errors.Is(e, finishEvidence) {
				return status, e
			}
			return status, finishDatabase
		}
		if len(items) == 0 {
			if e = finishListingConflicts(ctx, pool, &status); e != nil {
				return status, finishDatabase
			}
			status.Exhausted = status.ListingConflicts == 0
			if status.ListingConflicts > 0 {
				return status, finishListingConflict
			}
			return status, nil
		}
		n := 0
		var total int64
		for _, i := range items {
			if total+i.SizeBytes > nasCopyBatchBytes {
				break
			}
			total += i.SizeBytes
			n++
		}
		if n == 0 {
			return status, finishImport
		}
		h, digest, validated, e := finishManifest(dir, items[:n])
		if e != nil {
			return status, finishImport
		}
		// Re-read latches immediately before each bounded import. Each item still
		// passes the existing live source/hold validation and queue equality check.
		s, c, e = finishReadEvidence(safetyPath, copyPath)
		if e != nil {
			return status, e
		}
		if e = finishCheck(ctx, pool, s, c); e != nil {
			return status, e
		}
		if e = queueNASR2Copy(ctx, pool, h, digest, validated, true); e != nil {
			return status, finishImport
		}
		status.ImportedItems += n
		status.ImportedManifests++
	}
}
func finishWriteStatus(path string, status finishStatus) error {
	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".finish-status-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(append(data, '\n')); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(f.Name(), path)
}
func runFinish(ctx context.Context, cfg config.Config, args []string) int {
	fs := flag.NewFlagSet("finish tick", flag.ContinueOnError)
	safety := fs.String("safety", "", "fresh controller safety JSON")
	copyStatus := fs.String("copy-status", "", "fresh nas-r2-copy status --json output (A1 required)")
	out := fs.String("status", "", "atomic output JSON path")
	if len(args) == 0 || args[0] != "tick" || fs.Parse(args[1:]) != nil || fs.NArg() != 0 || *safety == "" || *copyStatus == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: stoaramactl finish tick --safety FILE --copy-status FILE --status FILE")
		return 2
	}
	status := finishStatus{Schema: "stoarama.finish-status.v1", UpdatedAt: time.Now().UTC(), Stop: finishDatabase, CleanupReason: "safety_stop"}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	pc, err := nasR2CopyPoolConfig(cfg.DatabaseURL, true)
	if err == nil {
		var pool *pgxpool.Pool
		pool, err = pgxpool.NewWithConfig(ctx, pc)
		if err == nil {
			status, err = finishTick(ctx, pool, *safety, *copyStatus)
			pool.Close()
		}
	}
	if finishWriteStatus(*out, status) != nil {
		fmt.Fprintln(os.Stderr, finishOutput)
		return 1
	}
	_ = json.NewEncoder(os.Stdout).Encode(status)
	if err != nil {
		fmt.Fprintln(os.Stderr, status.Stop)
		return 1
	}
	return 0
}
