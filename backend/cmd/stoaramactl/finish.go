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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const finishTarget = 10000
const finishEvidenceAge = 5 * time.Minute
const finishSpaceAge time.Duration = 10 * time.Minute

// The accepted sample uses the matched server challenge's issuance time.
const finishLateDebitWindow time.Duration = api.NASStorageBudgetMaxAge
const finishCleanupHeadroom int64 = 300_000_000_000
const finishRecoveryAge = 7 * time.Hour // six-hour refresh plus verification/scheduling margin
const finishLock int64 = 7343226011199

type finishStop string

const (
	finishEvidence      finishStop = "evidence_unavailable"
	finishRecovery      finishStop = "cleanup_recovery_problems"
	finishRecoveryStale finishStop = "recovery_evidence_stale"
	finishUnknownPlan   finishStop = "unknown_enabled_cleanup"
	finishMismatch      finishStop = "copy_mismatch_latched"
	finishShutdown      finishStop = "shutdown_or_recovery_failed"
	finishMaster        finishStop = "deletion_master_mismatch"
	finishDatabase      finishStop = "database_unavailable"
	finishImport        finishStop = "manifest_import_failed"
	finishBusy          finishStop = "tick_already_running"
	finishOutput        finishStop = "status_write_failed"
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
	MasterCheckedAt   time.Time        `json:"master_checked_at"`
	KnownPlans        []int64          `json:"known_enabled_plans"`
	KnownBatches      []int64          `json:"known_enabled_batches"`
	Master            bool             `json:"master_deletion_enabled"`
	ExpectedMaster    bool             `json:"expected_deletion_enabled"`
	Holds             map[string]int64 `json:"holds_by_reason"`
}
type finishCopy = nasCopyStatus

type finishTier uint8

const (
	finishExactInventory finishTier = iota + 1
	finishEarlyUnknown
	finishLateUnknown
)

type finishLateHold string

const (
	finishLateSpaceUnknown  finishLateHold = "nas_space_evidence_unavailable"
	finishLateSpaceLow      finishLateHold = "nas_headroom_below_threshold"
	finishLateBudgetHeld    finishLateHold = "next_download_exceeds_budget"
	finishLateProofStale    finishLateHold = "nas_storage_budget_proof_stale"
	finishLateClientUnknown finishLateHold = "nas_storage_client_not_budget_eligible"
)

type finishStatus struct {
	NASStorageBudgetAt          *time.Time       `json:"nas_storage_budget_at,omitempty"`
	NASSpaceClientVersion       string           `json:"nas_storage_client_version"`
	ImportedHistoricalInventory int              `json:"imported_historical_inventory"`
	ImportedEarlyUnknown        int              `json:"imported_early_unknown"`
	ImportedLateUnknown         int              `json:"imported_late_unknown"`
	NASSpaceCheckedAt           time.Time        `json:"nas_space_checked_at"`
	NASFreeBytes                *int64           `json:"nas_storage_free_bytes,omitempty"`
	NASSpaceReportedAt          *time.Time       `json:"nas_storage_reported_at,omitempty"`
	LateTierHold                finishLateHold   `json:"late_tier_hold_reason,omitempty"`
	LateTierBudgetBytes         int64            `json:"late_tier_budget_bytes"`
	QueueReservedBytes          int64            `json:"queue_reserved_bytes"`
	Schema                      string           `json:"schema"`
	UpdatedAt                   time.Time        `json:"updated_at"`
	Stop                        finishStop       `json:"stop_reason,omitempty"`
	QueueDepth                  int64            `json:"queue_depth"`
	ItemsPerMinute              float64          `json:"items_per_min"`
	Copy                        finishCopy       `json:"copy"`
	Holds                       map[string]int64 `json:"holds_by_reason"`
	CleanupShouldRun            bool             `json:"cleanup_should_run"`
	CleanupReason               string           `json:"cleanup_reason"`
	ImportedItems               int              `json:"imported_items"`
	ImportedManifests           int              `json:"imported_manifests"`
	Exhausted                   bool             `json:"candidates_exhausted"`
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
	if err := finishReadJSON(safetyPath, []string{"updated_at", "recovery_problems", "recovery_checked_at", "recovery_issued_items", "recovery_verified_items", "recovery_plan_id", "recovery_plan_sha256", "last_error", "controller_held_error", "local_copy_mismatch_latched", "local_copy_checked_at", "master_checked_at", "shutdown_or_recovery_failed", "recovery_error", "known_enabled_plans", "known_enabled_batches", "master_deletion_enabled", "expected_deletion_enabled", "holds_by_reason"}, &s); err != nil {
		return s, c, err
	}
	if err := finishReadJSON(copyPath, []string{"schema", "updated_at", "queue_depth", "verified_in_place", "downloaded", "downloaded_bytes", "committed_bytes", "blocked", "blocked_by_reason", "mismatch_latched", "verified_in_place_items_per_minute", "downloaded_items_per_minute"}, &c); err != nil {
		return s, c, err
	}
	if !finishFresh(s.UpdatedAt, time.Now()) || !finishFresh(c.UpdatedAt, time.Now()) || c.Schema != "stoarama.nas-r2-copy-status.v1" || s.RecoveryProblems < 0 || c.QueueDepth < 0 || c.Blocked < 0 || c.MismatchLatched < 0 || c.VerifiedInPlaceItemsPerMinute < 0 || c.DownloadedItemsPerMinute < 0 {
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
	if !finishFresh(s.LocalCheckedAt, time.Now()) || !finishFresh(s.MasterCheckedAt, time.Now()) || s.LocalMismatch < 0 {
		return finishEvidence
	}
	if s.RecoveryPlanID <= 0 || len(s.RecoveryPlanSHA) != 64 {
		return finishRecoveryStale
	}
	var issued int64
	var planSHA string
	var newestIssued time.Time
	if err := pool.QueryRow(ctx, `SELECT p.plan_sha256,(SELECT count(*) FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued),coalesce((SELECT max(i.updated_at) FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued),timestamptz '1970-01-01') FROM nas_cleanup_plans p WHERE p.id=$1 AND p.connection_id=13`, s.RecoveryPlanID).Scan(&planSHA, &issued, &newestIssued); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return finishRecoveryStale
		}
		return finishDatabase
	}
	if s.RecoveryCheckedAt.IsZero() || time.Since(s.RecoveryCheckedAt) > finishRecoveryAge || time.Until(s.RecoveryCheckedAt) > 30*time.Second || !s.RecoveryCheckedAt.After(newestIssued) || s.RecoveryIssued < 0 || s.RecoveryVerified != s.RecoveryIssued || issued != s.RecoveryIssued || planSHA != s.RecoveryPlanSHA {
		return finishRecoveryStale
	}
	if len(s.KnownBatches) > 0 || len(s.KnownPlans) > 1 || (len(s.KnownPlans) == 1 && s.KnownPlans[0] != s.RecoveryPlanID) {
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

// Inventory is a priority hint, never refreshed custody proof. Sources without
// exact inventory use the pinned window's end relative to the old pull cutoff.
const finishWindowSQL = `WITH windows AS (
 SELECT * FROM jsonb_to_recordset($1::jsonb) AS w(stream_id bigint,timezone text,start_at timestamptz,end_at timestamptz)
)`
const finishEligibleSQL = `
FROM recording_clips c JOIN recordings r ON r.id=c.recording_id AND r.account_id=47 JOIN windows w ON w.stream_id=r.stream_id LEFT JOIN streams s ON s.id=r.stream_id
LEFT JOIN nas_inventory_files n ON n.connection_id=13 AND n.clip_id=c.id AND n.state='present' AND n.relative_path=c.display_path AND n.size_bytes=c.size_bytes AND n.sha256=lower(c.sha256)
WHERE c.purged_at IS NULL AND c.clip_start_at<w.end_at AND c.clip_end_at>w.start_at AND c.clip_end_at>c.clip_start_at
 AND coalesce(nullif(s.local_timezone,''),r.cron_timezone,'')=w.timezone
 AND c.bucket<>'' AND c.object_key LIKE 'managed/%' AND c.size_bytes>0 AND c.size_bytes<=$2 AND lower(c.sha256) ~ '^[a-f0-9]{64}$' AND c.display_path<>''
 AND NOT EXISTS(SELECT 1 FROM nas_r2_copy_items q WHERE q.connection_id=13 AND q.kind='raw' AND q.source_id=c.id)
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_reserved_files f WHERE f.kind='raw' AND f.source_id=c.id)
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id WHERE h.clip_id=c.id AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued)))
 AND NOT EXISTS(SELECT 1 FROM r2_retirement_reservations x WHERE x.kind='raw' AND x.source_id=c.id)
 AND NOT EXISTS(SELECT 1 FROM r2_object_retirements x WHERE x.kind='raw' AND x.source_id=c.id)`
const finishCandidateSelectSQL = `SELECT c.id,c.recording_id,r.stream_id,c.bucket,c.object_key,c.display_path,c.size_bytes,lower(c.sha256),c.clip_start_at,c.clip_end_at `

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
func finishCandidates(ctx context.Context, pool *pgxpool.Pool, limit int) ([]api.NASR2CopyItem, finishTier, error) {
	data, err := finishPolicyJSON()
	if err != nil {
		return nil, 0, err
	}
	// Separate joins let PostgreSQL plan indexed exact inventory and anti-joins;
	// sorting a CASE over every raw object made a 1.16m-row pass nearly time out.
	for _, selection := range []struct {
		tier      finishTier
		predicate string
	}{
		{finishExactInventory, `n.clip_id IS NOT NULL`},
		{finishEarlyUnknown, `n.clip_id IS NULL AND w.end_at<timestamptz '2026-09-24 00:00:00+00'`},
		{finishLateUnknown, `n.clip_id IS NULL AND w.end_at>=timestamptz '2026-09-24 00:00:00+00'`},
	} {
		rows, err := pool.Query(ctx, finishWindowSQL+finishCandidateSelectSQL+finishEligibleSQL+` AND `+selection.predicate+` ORDER BY r.stream_id,c.id LIMIT $3`, data, api.NASR2CopyMaxFile, limit)
		if err != nil {
			return nil, 0, err
		}
		items := []api.NASR2CopyItem{}
		for rows.Next() {
			i := api.NASR2CopyItem{Kind: api.NASR2CopyRaw}
			if err = rows.Scan(&i.SourceID, &i.RecordingID, &i.StreamID, &i.Bucket, &i.ObjectKey, &i.RelativePath, &i.SizeBytes, &i.SHA256, &i.StartAt, &i.EndAt); err != nil {
				rows.Close()
				return nil, 0, err
			}
			items = append(items, i)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, 0, err
		}
		if len(items) > 0 {
			return items, selection.tier, nil
		}
	}
	return nil, 0, nil
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
	return pool.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE connection_id=13 AND kind='raw' AND state IN ('pending','leased')`).Scan(&status.QueueDepth)
}

// Current storage evidence is independent of historical hold counters.
// Full admission debits do not depend on potentially ambiguous completion labels.
// Inclusive cutoff retains a terminal write completed exactly T ago.
const finishLateDebitsSQL = `SELECT coalesce(sum((item->>'size_bytes')::bigint),0) FROM nas_r2_copy_items WHERE connection_id=13 AND kind='raw' AND (state IN ('pending','leased') OR (state IN ('verified','blocked') AND updated_at >= $1))`

func finishBudgetFresh(asOf, issued time.Time) bool {
	return !issued.IsZero() && !issued.After(asOf) && asOf.Sub(issued) <= finishLateDebitWindow
}
func finishCleanupValve(ctx context.Context, pool *pgxpool.Pool, status *finishStatus) error {
	status.CleanupShouldRun = false
	if err := pool.QueryRow(ctx, `SELECT nas_storage_free_bytes,nas_storage_reported_at,nas_storage_client_version,nas_storage_budget_at,clock_timestamp() FROM connections WHERE id=13`).Scan(&status.NASFreeBytes, &status.NASSpaceReportedAt, &status.NASSpaceClientVersion, &status.NASStorageBudgetAt, &status.NASSpaceCheckedAt); err != nil {
		return err
	}
	status.CleanupReason = "nas_space_heartbeat_missing"
	if status.NASFreeBytes == nil || status.NASSpaceReportedAt == nil || *status.NASFreeBytes < 0 {
		return nil
	}
	status.CleanupReason = "nas_space_heartbeat_stale"
	if status.NASSpaceCheckedAt.Sub(*status.NASSpaceReportedAt) > finishSpaceAge || status.NASSpaceReportedAt.Sub(status.NASSpaceCheckedAt) > 30*time.Second {
		return nil
	}
	status.CleanupReason = "nas_headroom_sufficient"
	if *status.NASFreeBytes-api.NASR2CopyFloor < finishCleanupHeadroom {
		status.CleanupShouldRun = true
		status.CleanupReason = "nas_headroom_below_threshold"
	}
	return nil
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
		status.ItemsPerMinute = float64(c.VerifiedInPlaceItemsPerMinute) + float64(c.DownloadedItemsPerMinute)
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
		if e = finishCleanupValve(ctx, pool, &status); e != nil {
			return status, finishDatabase
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
		items, tier, e := finishCandidates(ctx, pool, remaining)
		if e != nil {
			if errors.Is(e, finishEvidence) {
				return status, e
			}
			return status, finishDatabase
		}
		if len(items) == 0 {
			status.Exhausted = true
			return status, nil
		}
		if tier == finishLateUnknown {
			if e = finishCleanupValve(ctx, pool, &status); e != nil {
				return status, finishDatabase
			}
			if status.CleanupReason != "nas_headroom_sufficient" {
				status.LateTierHold = finishLateSpaceUnknown
				if status.CleanupShouldRun {
					status.LateTierHold = finishLateSpaceLow
				}
				return status, nil
			}
			if status.NASSpaceClientVersion != api.NASStorageBudgetClientVersion {
				status.LateTierHold = finishLateClientUnknown
				return status, nil
			}
			if status.NASStorageBudgetAt == nil || !finishBudgetFresh(status.NASSpaceCheckedAt, *status.NASStorageBudgetAt) {
				status.LateTierHold = finishLateProofStale
				return status, nil
			}
			if e = pool.QueryRow(ctx, finishLateDebitsSQL, status.NASSpaceCheckedAt.Add(-finishLateDebitWindow)).Scan(&status.QueueReservedBytes); e != nil {
				return status, finishDatabase
			}

			status.LateTierBudgetBytes = max(int64(0), *status.NASFreeBytes-api.NASR2CopyFloor-api.NASR2CopyRangeBytes-status.QueueReservedBytes)
		}

		n := 0
		var total int64
		for _, i := range items {
			if total+i.SizeBytes > nasCopyBatchBytes || (tier == finishLateUnknown && total+i.SizeBytes > status.LateTierBudgetBytes) {
				break
			}
			total += i.SizeBytes
			n++
		}
		if n == 0 {
			if tier == finishLateUnknown {
				status.LateTierHold = finishLateBudgetHeld
				return status, nil
			}
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
		switch tier {
		case finishExactInventory:
			status.ImportedHistoricalInventory += n
		case finishEarlyUnknown:
			status.ImportedEarlyUnknown += n
		case finishLateUnknown:
			status.ImportedLateUnknown += n
		}
		if tier == finishLateUnknown {
			status.LateTierBudgetBytes -= total
			status.QueueReservedBytes += total
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
