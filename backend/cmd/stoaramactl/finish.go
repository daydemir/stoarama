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

// Three tiers at 60s fit inside the 4-minute tick deadline; writes and other tick queries keep the 10s copy budget.
const finishCandidateStatementTimeout = "60s"

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
type finishCleanupOwner struct {
	PlanID  int64  `json:"plan_id"`
	PlanSHA string `json:"plan_sha256"`
}

type finishSafety struct {
	CleanupOwner      *finishCleanupOwner `json:"cleanup_owner,omitempty"`
	UpdatedAt         time.Time           `json:"updated_at"`
	RecoveryProblems  int64               `json:"recovery_problems"`
	RecoveryCheckedAt time.Time           `json:"recovery_checked_at"`
	RecoveryIssued    int64               `json:"recovery_issued_items"`
	RecoveryVerified  int64               `json:"recovery_verified_items"`
	ShutdownFailed    bool                `json:"shutdown_or_recovery_failed"`
	RecoveryError     *string             `json:"recovery_error"`
	RecoveryPlanID    int64               `json:"recovery_plan_id"`
	RecoveryPlanSHA   string              `json:"recovery_plan_sha256"`
	LastError         *string             `json:"last_error"`
	HeldError         *string             `json:"controller_held_error"`
	LocalMismatch     int64               `json:"local_copy_mismatch_latched"`
	LocalCheckedAt    time.Time           `json:"local_copy_checked_at"`
	MasterCheckedAt   time.Time           `json:"master_checked_at"`
	KnownPlans        []int64             `json:"known_enabled_plans"`
	KnownBatches      []int64             `json:"known_enabled_batches"`
	Master            bool                `json:"master_deletion_enabled"`
	ExpectedMaster    bool                `json:"expected_deletion_enabled"`
	Holds             map[string]int64    `json:"holds_by_reason"`
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
	finishRefreshDisabled   finishLateHold = "refresh_disabled"
	finishLateBudgetHeld    finishLateHold = "next_download_exceeds_budget"
	finishLateProofStale    finishLateHold = "nas_storage_budget_proof_stale"
	finishLateClientUnknown finishLateHold = "nas_storage_client_not_budget_eligible"
)

type finishStatus struct {
	RefreshAdmittedItems        int                    `json:"refresh_admitted_items"`
	RefreshHeldItems            int                    `json:"refresh_held_items"`
	RefreshHold                 finishLateHold         `json:"refresh_hold_reason,omitempty"`
	NASStorageBudgetAt          *time.Time             `json:"nas_storage_budget_at,omitempty"`
	NASSpaceClientVersion       string                 `json:"nas_storage_client_version"`
	ImportedHistoricalInventory int                    `json:"imported_historical_inventory"`
	ImportedEarlyUnknown        int                    `json:"imported_early_unknown"`
	ImportedLateUnknown         int                    `json:"imported_late_unknown"`
	NASSpaceCheckedAt           time.Time              `json:"nas_space_checked_at"`
	NASFreeBytes                *int64                 `json:"nas_storage_free_bytes,omitempty"`
	NASSpaceReportedAt          *time.Time             `json:"nas_storage_reported_at,omitempty"`
	LateTierHold                finishLateHold         `json:"late_tier_hold_reason,omitempty"`
	LateTierBudgetBytes         int64                  `json:"late_tier_budget_bytes"`
	QueueReservedBytes          int64                  `json:"queue_reserved_bytes"`
	Schema                      string                 `json:"schema"`
	UpdatedAt                   time.Time              `json:"updated_at"`
	DatabaseFailure             *finishDatabaseFailure `json:"database_failure,omitempty"`
	Stop                        finishStop             `json:"stop_reason,omitempty"`
	QueueDepth                  int64                  `json:"queue_depth"`
	ItemsPerMinute              float64                `json:"items_per_min"`
	Copy                        finishCopy             `json:"copy"`
	Holds                       map[string]int64       `json:"holds_by_reason"`
	CleanupShouldRun            bool                   `json:"cleanup_should_run"`
	CleanupReason               string                 `json:"cleanup_reason"`
	ImportedItems               int                    `json:"imported_items"`
	ImportedManifests           int                    `json:"imported_manifests"`
	Exhausted                   bool                   `json:"candidates_exhausted"`
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
		return finishDBError(finishRecoveryQuery, 0, err)
	}
	if s.RecoveryCheckedAt.IsZero() || time.Since(s.RecoveryCheckedAt) > finishRecoveryAge || time.Until(s.RecoveryCheckedAt) > 30*time.Second || !s.RecoveryCheckedAt.After(newestIssued) || s.RecoveryIssued < 0 || s.RecoveryVerified != s.RecoveryIssued || issued != s.RecoveryIssued || planSHA != s.RecoveryPlanSHA {
		return finishRecoveryStale
	}
	if s.CleanupOwner != nil {
		owner := s.CleanupOwner
		if owner.PlanID <= 0 || len(owner.PlanSHA) != 64 || len(s.KnownPlans) != 1 || s.KnownPlans[0] != owner.PlanID || !s.ExpectedMaster || s.HeldError != nil {
			return finishEvidence
		}
		var matches bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_plans WHERE id=$1 AND plan_sha256=$2 AND account_id=47 AND connection_id=13 AND mode IN ('dry_run','enabled','paused','retired'))`, owner.PlanID, owner.PlanSHA).Scan(&matches); err != nil {
			return finishDatabase
		}
		if !matches {
			return finishUnknownPlan
		}
	}
	if len(s.KnownBatches) > 0 || len(s.KnownPlans) > 1 || (s.CleanupOwner == nil && len(s.KnownPlans) == 1 && s.KnownPlans[0] != s.RecoveryPlanID) {
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
		return finishDBError(finishEnabledQuery, 0, err)
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
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_incident_sources i WHERE i.clip_id=c.id)
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_incident_roster i WHERE (i.kind='raw' AND i.source_id=c.id) OR (i.bucket=c.bucket AND i.object_key=c.object_key))
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id WHERE h.clip_id=c.id AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued)))
 AND NOT EXISTS(SELECT 1 FROM r2_retirement_reservations x WHERE x.kind='raw' AND x.source_id=c.id)
 AND NOT EXISTS(SELECT 1 FROM r2_object_retirements x WHERE x.kind='raw' AND x.source_id=c.id)`
const finishCandidateSelectSQL = `SELECT c.id,c.recording_id,r.stream_id,c.bucket,c.object_key,c.display_path,c.size_bytes,lower(c.sha256),c.clip_start_at,c.clip_end_at `

// Fence the selective catalog/inventory/hold joins before the indexed reservation
// function. Otherwise PostgreSQL pushes that function down to every catalog row.
func finishCandidateSQL(predicate string) string {
	return finishWindowSQL + `, eligible(id,recording_id,stream_id,bucket,object_key,display_path,size_bytes,sha256,clip_start_at,clip_end_at) AS MATERIALIZED (` +
		finishCandidateSelectSQL + finishEligibleSQL + ` AND ` + predicate +
		`) SELECT * FROM eligible c WHERE NOT nas_cleanup_file_reserved('raw',c.id) ORDER BY c.stream_id,c.id LIMIT $3`
}

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

// beginFinishCandidateTx opens a read-only transaction whose statement budget
// covers the catalog candidate selection without raising the pooled 10s budget.
func beginFinishCandidateTx(ctx context.Context, pool *pgxpool.Pool) (pgx.Tx, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `SET LOCAL statement_timeout = '`+finishCandidateStatementTimeout+`'`); err != nil {
		rollbackFinishCandidateTx(tx)
		return nil, err
	}
	return tx, nil
}

// rollbackFinishCandidateTx releases the transaction; cancellation must not leave it open.
func rollbackFinishCandidateTx(tx pgx.Tx) {
	cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(cleanup)
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
		tx, err := beginFinishCandidateTx(ctx, pool)
		if err != nil {
			return nil, 0, finishDBError(finishCandidateBegin, selection.tier, err)
		}
		rows, err := tx.Query(ctx, finishCandidateSQL(selection.predicate), data, api.NASR2CopyMaxFile, limit)
		if err != nil {
			rollbackFinishCandidateTx(tx)
			return nil, 0, finishDBError(finishCandidateQuery, selection.tier, err)
		}
		items := []api.NASR2CopyItem{}
		for rows.Next() {
			i := api.NASR2CopyItem{Kind: api.NASR2CopyRaw}
			if err = rows.Scan(&i.SourceID, &i.RecordingID, &i.StreamID, &i.Bucket, &i.ObjectKey, &i.RelativePath, &i.SizeBytes, &i.SHA256, &i.StartAt, &i.EndAt); err != nil {
				rows.Close()
				rollbackFinishCandidateTx(tx)
				return nil, 0, finishDBError(finishCandidateScan, selection.tier, err)
			}
			items = append(items, i)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			rollbackFinishCandidateTx(tx)
			return nil, 0, finishDBError(finishCandidateQuery, selection.tier, err)
		}
		if err = tx.Commit(ctx); err != nil {
			rollbackFinishCandidateTx(tx)
			return nil, 0, finishDBError(finishCandidateCommit, selection.tier, err)
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
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return h, "", nil, err
	}
	defer f.Close()
	if _, err = f.Write(manifest.Bytes()); err != nil {
		return h, "", nil, err
	}
	if err = f.Sync(); err != nil {
		return h, "", nil, err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return h, "", nil, err
	}
	defer directory.Close()
	if err = directory.Sync(); err != nil {
		return h, "", nil, err
	}
	parsed, validated, err := readNASCopyBatch(path, digest)
	return parsed, digest, validated, err
}
func finishDepth(ctx context.Context, pool *pgxpool.Pool, status *finishStatus) error {
	return pool.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE connection_id=13 AND kind='raw' AND state IN ('pending','leased','refresh_pending','refresh_leased')`).Scan(&status.QueueDepth)
}

// Current storage evidence is independent of historical hold counters.
// Full admission debits do not depend on potentially ambiguous completion labels.
// Inclusive cutoff retains a terminal write completed exactly T ago.
const finishLateDebitsSQL = `SELECT coalesce(sum((item->>'size_bytes')::bigint),0) FROM nas_r2_copy_items WHERE connection_id=13 AND kind='raw' AND (state IN ('pending','leased','refresh_pending','refresh_leased') OR (state IN ('verified','blocked') AND updated_at >= $1))`

func finishProofFresh(asOf, issued time.Time, maxAge time.Duration) bool {
	return !issued.IsZero() && !issued.After(asOf) && asOf.Sub(issued) <= maxAge
}
func finishCleanupValve(ctx context.Context, pool r2RetireQuerier, status *finishStatus) error {
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
	status.CleanupReason = "nas_space_proof_missing"
	if !api.SupportsNASStorageBudget(status.NASSpaceClientVersion) || status.NASStorageBudgetAt == nil {
		return nil
	}
	status.CleanupReason = "nas_space_proof_stale"
	if !finishProofFresh(status.NASSpaceCheckedAt, *status.NASStorageBudgetAt, finishSpaceAge) {
		return nil
	}
	status.CleanupReason = "nas_headroom_sufficient"
	if *status.NASFreeBytes-api.NASR2CopyFloor < finishCleanupHeadroom {
		status.CleanupShouldRun = true
		status.CleanupReason = "nas_headroom_below_threshold"
	}
	return nil
}
func finishTick(ctx context.Context, pool *pgxpool.Pool, safetyPath, copyPath string) (finishStatus, error) {
	return finishTickWithRefresh(ctx, pool, safetyPath, copyPath, finishRefreshOptions{})
}
func finishTickWithRefresh(ctx context.Context, pool *pgxpool.Pool, safetyPath, copyPath string, refresh finishRefreshOptions) (status finishStatus, err error) {
	status = finishStatus{Schema: "stoarama.finish-status.v1", CleanupReason: "copy_not_holding_on_free_space"}
	defer func() {
		status.UpdatedAt = time.Now().UTC()
		if err != nil {
			var stop finishStop
			if !errors.As(err, &stop) {
				stop = finishImport
			}
			status.Stop = stop
			errors.As(err, &status.DatabaseFailure)
			status.CleanupShouldRun = false
			status.CleanupReason = "safety_stop"
		}
	}()
	conn, e := pool.Acquire(ctx)
	if e != nil {
		return status, finishDBError(finishPoolAcquire, 0, e)
	}
	defer conn.Release()
	var locked bool
	if e = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, finishLock).Scan(&locked); e != nil {
		return status, finishDBError(finishTickLock, 0, e)
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
	// The wrapper owns this private attempt directory. Retain exact manifests
	// on CLI failure/interruption; only wrapper success removes its scratch.
	dir := filepath.Dir(safetyPath)
	refreshFed := false
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
			return status, finishDBError(finishQueueDepth, 0, e)
		}
		if e = finishCleanupValve(ctx, pool, &status); e != nil {
			return status, finishDBError(finishStorageQuery, 0, e)
		}
		if !refreshFed {
			refreshFed = true
			var refreshDepth int
			if e = pool.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE connection_id=13 AND state IN ('refresh_pending','refresh_leased')`).Scan(&refreshDepth); e != nil {
				return status, finishDBError(finishRefreshDepth, 0, e)
			}
			slots := finishRefreshShare(int(status.QueueDepth), refreshDepth)
			n, feedErr := feedKeep28Refresh(ctx, pool, refresh, slots, &status)
			if feedErr != nil {
				return status, feedErr
			}
			status.RefreshAdmittedItems = n
			status.ImportedItems += n
			if n > 0 {
				if e = finishDepth(ctx, pool, &status); e != nil {
					return status, finishDBError(finishQueueDepth, 0, e)
				}
			}
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
			return status, finishDBError(finishCandidateQuery, 0, e)
		}
		if len(items) == 0 {
			status.Exhausted = true
			return status, nil
		}
		if tier == finishLateUnknown {
			if e = finishCleanupValve(ctx, pool, &status); e != nil {
				return status, finishDBError(finishStorageQuery, 0, e)
			}
			if !api.SupportsNASStorageBudget(status.NASSpaceClientVersion) {
				status.LateTierHold = finishLateClientUnknown
				return status, nil
			}
			if status.NASStorageBudgetAt == nil || !finishProofFresh(status.NASSpaceCheckedAt, *status.NASStorageBudgetAt, finishLateDebitWindow) {
				status.LateTierHold = finishLateProofStale
				return status, nil
			}
			if status.CleanupReason != "nas_headroom_sufficient" {
				status.LateTierHold = finishLateSpaceUnknown
				if status.CleanupShouldRun {
					status.LateTierHold = finishLateSpaceLow
				}
				return status, nil
			}
			if e = pool.QueryRow(ctx, finishLateDebitsSQL, status.NASSpaceCheckedAt.Add(-finishLateDebitWindow)).Scan(&status.QueueReservedBytes); e != nil {
				return status, finishDBError(finishDebitQuery, 0, e)
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
	refresh := finishRefreshOptions{}
	fs.StringVar(&refresh.Picks, "refresh-picks", "", "B2 owner picks for optional verify-existing refresh")
	fs.StringVar(&refresh.PicksSHA, "expected-refresh-picks-sha256", "", "pinned owner picks digest")
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
			status, err = finishTickWithRefresh(ctx, pool, *safety, *copyStatus, refresh)
			pool.Close()
		}
	}
	if err != nil && status.DatabaseFailure == nil && status.Stop == finishDatabase {
		err = finishDBError(finishPoolOpen, 0, err)
		errors.As(err, &status.DatabaseFailure)
	}
	if finishWriteStatus(*out, status) != nil {
		fmt.Fprintln(os.Stderr, finishOutput)
		return 1
	}
	_ = json.NewEncoder(os.Stdout).Encode(status)
	if err != nil {
		if status.DatabaseFailure != nil {
			fmt.Fprintln(os.Stderr, status.DatabaseFailure)
		} else {
			fmt.Fprintln(os.Stderr, status.Stop)
		}
		return 1
	}
	return 0
}
