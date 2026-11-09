package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

// NAS cleanup throughput (2026-11). Nothing here adds deletion authority:
// every permit still comes from cleanupObserveOne -> cleanupMayIssue with the
// same per-file checks. What changes is how much work each check costs and
// how many files share one HTTP round trip or one owner approval.

const (
	// Observations or receipts per batched request.
	nasCleanupBatchItemsMax = 64
	// Plans per approved batch.
	nasCleanupMaxBatchPlans = 500
	// Parallel R2 reads per batched observation request.
	nasCleanupRecoveryParallel = 8
)

// cleanupObserveEnv carries per-transaction caches for a batch of
// observations. The zero value (single-item endpoint) caches nothing.
type cleanupObserveEnv struct {
	// budgetHeld is set only when the budget advisory lock was taken in the
	// top-level transaction (never in a savepoint, whose rollback could release
	// it); issued then stays exact until commit because no other transaction
	// can issue a permit meanwhile.
	budgetHeld   bool
	issued       int64
	fingerprints map[int64]string
	prefetched   map[int64]cleanupPrefetchedRecovery
	// Enabled plans whose execution fence was taken at top level before the
	// budget lock (batch path); see cleanupObserveOne.
	lockedPlans map[int64]bool
}

// cleanupRecoveryPermits: a permit needs a full streamed-SHA verification
// (r2_sha_verified) or, during execution only, the conditional HEAD bound to
// the generation a dry run SHA-verified (r2_etag_bound). Every other state
// (nas_only_at_check, unchecked, conflict, ...) never permits.
func cleanupRecoveryPermits(mode, recovery string) bool {
	return recovery == "r2_sha_verified" || (mode == "enabled" && recovery == "r2_etag_bound")
}

// cleanupExpireApprovals pauses every enabled plan and batch whose approval
// has passed, so an expired approval neither lingers as 'enabled' (blocking
// new enables and purges) nor leaves a batch waiting forever. Pausing grants
// nothing; outstanding 30 s permits may still be receipted.
func (s *Server) cleanupExpireApprovals(ctx context.Context) {
	if s.pool == nil {
		return
	}
	_, err := s.pool.Exec(ctx, `WITH pz AS (UPDATE nas_cleanup_plans SET mode='paused',approved_until=NULL,approval_reason=left('auto-paused: approval expired; '||approval_reason,500) WHERE mode='enabled' AND (approved_until IS NULL OR approved_until<=now()) RETURNING id)
UPDATE nas_cleanup_batches SET mode='paused',approved_until=NULL,approval_reason=left('auto-paused: approval expired; '||approval_reason,500) WHERE mode='enabled' AND (approved_until<=now() OR plan_ids && ARRAY(SELECT id FROM pz))`)
	if err != nil {
		log.Printf("nas cleanup approval expiry deferred: %v", err)
	}
}

func (e *cleanupObserveEnv) budgetAllows(ctx context.Context, tx pgx.Tx, size int64) (bool, error) {
	if e == nil || !e.budgetHeld {
		return cleanupBudgetAllows(ctx, tx, size)
	}
	if size <= 0 || e.issued+size > nasCleanupMaxIssuedBytes24h {
		return false, nil
	}
	e.issued += size
	return true, nil
}

// rankingsUnchanged returns the execution fast path for the best-14 replay,
// or nil when it cannot apply (dry runs, no activation record).
func (e *cleanupObserveEnv) rankingsUnchanged(ctx context.Context, tx pgx.Tx, mode string, stored []byte) cleanupRankingsUnchanged {
	if mode != "enabled" || len(stored) == 0 {
		return nil
	}
	var pinned map[string]cleanupExecutionRanking
	if json.Unmarshal(stored, &pinned) != nil {
		return nil
	}
	return func(stream int64, best string) bool {
		want, ok := pinned[strconv.FormatInt(stream, 10)]
		if !ok || want.BestStart != best || want.Fingerprint == "" {
			return false
		}
		current, ok := e.fingerprints[stream]
		if !ok {
			if err := tx.QueryRow(ctx, cleanupRankingsFingerprintSQL, stream).Scan(&current); err != nil {
				return false
			}
			if e.fingerprints == nil {
				e.fingerprints = map[int64]string{}
			}
			e.fingerprints[stream] = current
		}
		return current == want.Fingerprint
	}
}

type cleanupExecutionRanking struct {
	BestStart   string    `json:"best_start"`
	Fingerprint string    `json:"fingerprint"`
	At          time.Time `json:"at"`
}

// cleanupActivateRankings runs the full live best-14 replay once per stream of
// a plan that is being enabled and records its result with the fingerprint of
// the exact rows it was computed from (one statement, one snapshot). Only a
// replay that yields the pinned best_start is recorded; otherwise nothing is
// stored and every observation runs the full replay (and keeps) as before.
func cleanupActivateRankings(ctx context.Context, tx pgx.Tx, planID int64) error {
	policy, err := cleanupEmbeddedPolicy()
	if err != nil {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT (e->>'stream_id')::bigint FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) e WHERE p.id=$1`, planID)
	if err != nil {
		return err
	}
	var streams []int64
	for rows.Next() {
		var stream int64
		if err = rows.Scan(&stream); err != nil {
			break
		}
		streams = append(streams, stream)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	pinned := map[string]cleanupExecutionRanking{}
	for _, stream := range streams {
		window, ok := policy.Streams[stream]
		if !ok {
			continue
		}
		var best, fingerprint string
		if err = tx.QueryRow(ctx, cleanupRankingsWithFingerprintSQL, stream).Scan(&best, &fingerprint); err != nil {
			return err
		}
		if best == window.BestStart && fingerprint != "" {
			pinned[strconv.FormatInt(stream, 10)] = cleanupExecutionRanking{BestStart: best, Fingerprint: fingerprint, At: time.Now().UTC()}
		}
	}
	var stored any
	if len(pinned) != 0 {
		b, _ := json.Marshal(pinned)
		stored = string(b)
	}
	_, err = tx.Exec(ctx, `UPDATE nas_cleanup_plans SET execution_rankings=$2::jsonb WHERE id=$1`, planID, stored)
	return err
}

// cleanupEnableGates are the per-plan enable conditions shared by the single
// plan control and by batch enable/advance. It must run under the public
// cleanup fence. Status 0 means every gate passed.
func cleanupEnableGates(ctx context.Context, tx pgx.Tx, id int64) (int, map[string]any) {
	var bad int
	err := tx.QueryRow(ctx, `SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND (state<>'would_delete' OR proof IS NULL OR updated_at<now()-interval '24 hours' OR recovery<>'r2_sha_verified' OR coalesce(recovery_evidence->>'etag','')='')`, id).Scan(&bad)
	if err != nil || bad != 0 {
		return 409, map[string]any{"error": "fresh complete dry run with a verified R2 recovery copy (and its ETag evidence) for every item required"}
	}
	// Under the exclusive public cleanup fence, so two concurrent enables are
	// serialized and the second sees the first.
	var enabled int
	var planBytes int64
	err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM nas_cleanup_plans WHERE mode='enabled' AND id<>$1),(SELECT coalesce(sum((e->>'size_bytes')::bigint),0) FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) e WHERE p.id=$1)`, id).Scan(&enabled, &planBytes)
	if err != nil {
		return 500, map[string]any{"error": "cleanup cap evidence unavailable"}
	}
	if enabled+1 > nasCleanupMaxEnabledPlans {
		return 409, map[string]any{"error": "another cleanup plan is already enabled; pause it first (at most one enabled plan)"}
	}
	if planBytes > nasCleanupMaxPlanBytes {
		return 409, map[string]any{"error": "plan exceeds the per-plan deletion cap", "plan_bytes": planBytes, "max_plan_bytes": nasCleanupMaxPlanBytes}
	}
	return 0, nil
}

// cleanupRecoveryEvidence identifies the exact R2 object generation whose
// streamed bytes hashed to the catalog SHA during a dry run.
type cleanupRecoveryEvidence struct {
	ObjectKey  string    `json:"object_key"`
	ETag       string    `json:"etag"`
	VersionID  string    `json:"version_id,omitempty"`
	SizeBytes  int64     `json:"size_bytes"`
	SHA256     string    `json:"sha256"`
	VerifiedAt time.Time `json:"verified_at"`
}

// cleanupRecoveryFull is the full recovery check: HEAD, then stream exactly
// that ETag generation (If-Match closes the HEAD->GET race) and hash it.
func (s *Server) cleanupRecoveryFull(ctx context.Context, i nasCleanupItem) (string, *cleanupRecoveryEvidence) {
	if s.r2 == nil {
		return "unchecked", nil
	}
	head, err := s.r2.Head(ctx, i.ObjectKey)
	if err != nil {
		// Only a proven 404 is NAS-only. Authorization/timeouts never prove absence.
		if r2.IsNotFound(err) {
			return "nas_only_at_check", nil
		}
		return "unchecked", nil
	}
	if head.SizeBytes != i.SizeBytes {
		return "conflict", nil
	}
	if head.ETag == "" {
		return "r2_present_unverified", nil
	}
	// Bounded streaming verification. No media is saved on the backend or Jelly.
	c, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	source, err := s.r2.OpenExact(c, i.ObjectKey, head.ETag, head.VersionID)
	if err != nil {
		return "r2_present_unverified", nil
	}
	defer source.Close()
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(source, i.SizeBytes+1))
	if err != nil || n != i.SizeBytes {
		return "r2_present_unverified", nil
	}
	if hex.EncodeToString(hash.Sum(nil)) != i.SHA256 {
		return "conflict", nil
	}
	return "r2_sha_verified", &cleanupRecoveryEvidence{ObjectKey: i.ObjectKey, ETag: head.ETag, VersionID: head.VersionID, SizeBytes: i.SizeBytes, SHA256: i.SHA256, VerifiedAt: time.Now().UTC()}
}

// cleanupRecoveryBound re-proves, without re-downloading, that the object is
// still the exact generation the dry run byte-verified: a conditional HEAD
// (If-Match on the verified ETag, plus the version id when known) must
// succeed with the same size, ETag and version, and any object-recorded
// sha256 must equal the catalog SHA. ETags are content-derived (MD5 or the
// multipart MD5-of-MD5s), so a replaced object cannot keep the ETag. A 404 is
// nas_only_at_check, a failed precondition or any difference is a conflict,
// and every other error is unchecked; none of them can yield a permit.
func (s *Server) cleanupRecoveryBound(ctx context.Context, i nasCleanupItem, ev cleanupRecoveryEvidence) string {
	var head r2.ObjectHead
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		c, cancel := context.WithTimeout(ctx, 10*time.Second)
		head, err = s.r2.HeadExact(c, i.ObjectKey, ev.ETag, ev.VersionID)
		cancel()
		if err == nil || r2.IsNotFound(err) || r2.IsPreconditionFailed(err) || ctx.Err() != nil {
			break
		}
		time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
	}
	switch {
	case err == nil:
	case r2.IsNotFound(err):
		return "nas_only_at_check"
	case r2.IsPreconditionFailed(err):
		return "conflict"
	default:
		return "unchecked"
	}
	if head.ETag != ev.ETag || head.SizeBytes != i.SizeBytes || (ev.VersionID != "" && head.VersionID != ev.VersionID) {
		return "conflict"
	}
	if head.SHA256Metadata != "" && strings.ToLower(head.SHA256Metadata) != i.SHA256 {
		return "conflict"
	}
	return "r2_etag_bound"
}

// usableEvidence returns the dry-run evidence only when it binds exactly
// this catalog item (key, size and SHA) to a non-empty ETag.
func usableEvidence(raw []byte, i nasCleanupItem) (cleanupRecoveryEvidence, bool) {
	var ev cleanupRecoveryEvidence
	if len(raw) == 0 || json.Unmarshal(raw, &ev) != nil {
		return ev, false
	}
	return ev, ev.ETag != "" && ev.ObjectKey == i.ObjectKey && ev.SizeBytes == i.SizeBytes && ev.SHA256 == i.SHA256
}

type cleanupPrefetchedRecovery struct {
	mode      string
	objectKey string
	evidence  []byte
	recovery  string
	out       *cleanupRecoveryEvidence
	method    string
}

// cleanupRecoveryCompute decides the recovery state for one item: dry runs
// always byte-verify; executions use the dry-run evidence when it is usable
// and otherwise byte-verify again.
func (s *Server) cleanupRecoveryCompute(ctx context.Context, i nasCleanupItem, mode string, evidence []byte) (string, *cleanupRecoveryEvidence, string) {
	if s.r2 == nil {
		return "unchecked", nil, "none"
	}
	if mode == "enabled" {
		if ev, ok := usableEvidence(evidence, i); ok {
			return s.cleanupRecoveryBound(ctx, i, ev), nil, "etag_bound"
		}
	}
	recovery, out := s.cleanupRecoveryFull(ctx, i)
	return recovery, out, "full_sha"
}

// cleanupRecoveryFor uses a result prefetched for this exact (mode, object,
// evidence) before the transaction began, or computes it now.
func (s *Server) cleanupRecoveryFor(ctx context.Context, id int64, i nasCleanupItem, mode string, evidence []byte, env *cleanupObserveEnv) (string, *cleanupRecoveryEvidence, string) {
	if env != nil {
		if p, ok := env.prefetched[id]; ok && p.mode == mode && p.objectKey == i.ObjectKey && string(p.evidence) == string(evidence) {
			return p.recovery, p.out, p.method
		}
	}
	return s.cleanupRecoveryCompute(ctx, i, mode, evidence)
}

// cleanupBatchDigest binds an ordered batch of plans to the cleanup policy.
// prepare_keep28_plans.py computes the same digest (see its batch_digest).
func cleanupBatchDigest(policy string, ids []int64, shas []string) string {
	var b strings.Builder
	b.WriteString("stoarama-nas-cleanup-batch-v1\npolicy ")
	b.WriteString(policy)
	b.WriteString("\n")
	for n, id := range ids {
		b.WriteString("plan ")
		b.WriteString(strconv.FormatInt(id, 10))
		b.WriteString(" ")
		b.WriteString(shas[n])
		b.WriteString("\n")
	}
	return cleanupSHA([]byte(b.String()))
}

type cleanupBatchObservation struct {
	ItemID int64 `json:"item_id"`
	cleanupObserve
}

// handleAccountNASCleanupObserveBatch records up to 64 observations in one
// transaction, each in its own savepoint with exactly the single-item rules.
// R2 work runs in parallel before the transaction; its result is used only for
// the exact (mode, object, evidence) it was computed for.
func (s *Server) handleAccountNASCleanupObserveBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientVersion string                    `json:"client_version"`
		Observations  []cleanupBatchObservation `json:"observations"`
	}
	if util.DecodeJSON(r, &req) != nil || len(req.ClientVersion) == 0 || len(req.ClientVersion) > 64 || len(req.Observations) < 1 || len(req.Observations) > nasCleanupBatchItemsMax {
		util.WriteError(w, 400, "client version and 1 to 64 observations required")
		return
	}
	seen := map[int64]bool{}
	for _, o := range req.Observations {
		if o.ItemID <= 0 || seen[o.ItemID] {
			util.WriteError(w, 400, "distinct positive item ids required")
			return
		}
		seen[o.ItemID] = true
	}
	ctx := r.Context()
	cid, ok := s.nasRestoreConnection(ctx, w, r)
	if !ok {
		return
	}
	results := make([]map[string]any, len(req.Observations))
	valid := make([]bool, len(req.Observations))
	ids := []int64{}
	for n, o := range req.Observations {
		if msg := cleanupObserveRequestError(o.cleanupObserve); msg != "" {
			results[n] = map[string]any{"item_id": o.ItemID, "status": 400, "error": msg}
			continue
		}
		valid[n] = true
		if o.Outcome == "match" {
			ids = append(ids, o.ItemID)
		}
	}
	env := &cleanupObserveEnv{prefetched: s.cleanupPrefetchRecovery(ctx, cid, ids)}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "cleanup observation begin failed")
		return
	}
	defer tx.Rollback(ctx)
	var anyEnabled bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE i.id=ANY($1) AND p.connection_id=$2 AND p.mode='enabled')`, ids, cid).Scan(&anyEnabled); err != nil {
		util.WriteError(w, 500, "cleanup observation begin failed")
		return
	}
	if anyEnabled && s.cfg.NASCleanupDeletionEnabled {
		// Same lock order as the single-item path: every enabled plan's
		// execution fence (ascending id), then the budget lock, taken once at
		// top level with an exact running total until commit.
		env.lockedPlans = map[int64]bool{}
		var plans []int64
		if err = tx.QueryRow(ctx, `SELECT coalesce(array_agg(DISTINCT p.id ORDER BY p.id),'{}') FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE i.id=ANY($1) AND p.connection_id=$2 AND p.mode='enabled'`, ids, cid).Scan(&plans); err != nil {
			util.WriteError(w, 500, "cleanup plan execution fence failed")
			return
		}
		for _, plan := range plans {
			if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPlanLockBase+plan); err != nil {
				util.WriteError(w, 500, "cleanup plan execution fence failed")
				return
			}
			env.lockedPlans[plan] = true
		}
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupBudgetLock); err == nil {
			err = tx.QueryRow(ctx, cleanupIssuedBytes24hSQL).Scan(&env.issued)
		}
		if err != nil {
			util.WriteError(w, 500, "cleanup deletion budget unavailable")
			return
		}
		env.budgetHeld = true
	}
	for n, o := range req.Observations {
		if !valid[n] {
			continue
		}
		sp, err := tx.Begin(ctx)
		if err != nil {
			util.WriteError(w, 500, "cleanup observation save failed")
			return
		}
		result := s.cleanupObserveOne(ctx, sp, cid, o.ItemID, o.cleanupObserve, env)
		if result.status == 200 && result.write {
			err = sp.Commit(ctx)
		} else {
			err = sp.Rollback(ctx)
		}
		if err != nil {
			util.WriteError(w, 500, "cleanup observation save failed")
			return
		}
		body := map[string]any{"item_id": o.ItemID, "status": result.status}
		for k, v := range result.body {
			body[k] = v
		}
		results[n] = body
	}
	if err = tx.Commit(ctx); err != nil {
		util.WriteError(w, 500, "cleanup observation save failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"results": results})
}

// cleanupPrefetchRecovery reads (without locks) what each observed item will
// need from R2 and computes it in parallel. Observations recheck mode, object
// and evidence under lock and recompute on any difference.
func (s *Server) cleanupPrefetchRecovery(ctx context.Context, cid int64, ids []int64) map[int64]cleanupPrefetchedRecovery {
	out := map[int64]cleanupPrefetchedRecovery{}
	if s.r2 == nil || len(ids) == 0 {
		return out
	}
	rows, err := s.pool.Query(ctx, `SELECT i.id,coalesce(i.item,p.manifest->(i.ordinal-1)),p.mode,i.recovery_evidence FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE i.id=ANY($1) AND p.connection_id=$2 AND i.state='leased' AND p.mode IN ('dry_run','enabled')`, ids, cid)
	if err != nil {
		return out
	}
	type job struct {
		id       int64
		item     nasCleanupItem
		mode     string
		evidence []byte
	}
	var jobs []job
	for rows.Next() {
		var j job
		var raw []byte
		if rows.Scan(&j.id, &raw, &j.mode, &j.evidence) != nil || json.Unmarshal(raw, &j.item) != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	rows.Close()
	var mu sync.Mutex
	var wg sync.WaitGroup
	slots := make(chan struct{}, nasCleanupRecoveryParallel)
	for _, j := range jobs {
		wg.Add(1)
		slots <- struct{}{}
		go func(j job) {
			defer wg.Done()
			defer func() { <-slots }()
			recovery, ev, method := s.cleanupRecoveryCompute(ctx, j.item, j.mode, j.evidence)
			mu.Lock()
			out[j.id] = cleanupPrefetchedRecovery{mode: j.mode, objectKey: j.item.ObjectKey, evidence: j.evidence, recovery: recovery, out: ev, method: method}
			mu.Unlock()
		}(j)
	}
	wg.Wait()
	return out
}

// handleAccountNASCleanupResultBatch records up to 64 deletion receipts with
// exactly the single-receipt rules (idempotent replays; anything but a clean
// deletion pauses the plan and its batch).
func (s *Server) handleAccountNASCleanupResultBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientVersion string           `json:"client_version"`
		Receipts      []cleanupReceipt `json:"receipts"`
	}
	if util.DecodeJSON(r, &req) != nil || len(req.ClientVersion) == 0 || len(req.ClientVersion) > 64 || len(req.Receipts) < 1 || len(req.Receipts) > nasCleanupBatchItemsMax {
		util.WriteError(w, 400, "client version and 1 to 64 receipts required")
		return
	}
	seen := map[int64]bool{}
	for _, receipt := range req.Receipts {
		if receipt.ItemID <= 0 || seen[receipt.ItemID] || !validCleanupReceipt(receipt) {
			util.WriteError(w, 400, "invalid cleanup deletion receipt")
			return
		}
		seen[receipt.ItemID] = true
	}
	ctx := r.Context()
	cid, ok := s.nasRestoreConnection(ctx, w, r)
	if !ok {
		return
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "cleanup receipt save failed")
		return
	}
	defer tx.Rollback(ctx)
	results := make([]map[string]any, 0, len(req.Receipts))
	advance := map[int64]bool{}
	for _, receipt := range req.Receipts {
		id := receipt.ItemID
		updated, paused, planID, err := cleanupApplyReceipt(ctx, tx, cid, id, receipt)
		if err != nil {
			util.WriteError(w, 500, "cleanup receipt save failed")
			return
		}
		if updated != 1 {
			results = append(results, map[string]any{"item_id": id, "status": 409, "error": "cleanup receipt differs or task is not deletion-issued"})
			continue
		}
		if receipt.Outcome == "deleted" {
			advance[planID] = true
		}
		results = append(results, map[string]any{"item_id": id, "status": 200, "ok": true, "state": receipt.Outcome, "plan_paused": paused})
	}
	if err = tx.Commit(ctx); err != nil {
		util.WriteError(w, 500, "cleanup receipt save failed")
		return
	}
	for planID := range advance {
		s.cleanupAdvanceBatchAfter(ctx, planID)
	}
	util.WriteJSON(w, 200, map[string]any{"results": results})
}

// cleanupAdvanceBatchAfter advances an enabled batch when a clean deletion
// receipt may have completed its current plan. Errors only delay advance.
func (s *Server) cleanupAdvanceBatchAfter(ctx context.Context, planID int64) {
	if planID <= 0 || s.pool == nil {
		return
	}
	var remaining int
	var inBatch bool
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND state<>'deleted'),EXISTS(SELECT 1 FROM nas_cleanup_batches WHERE mode='enabled' AND $1=ANY(plan_ids))`, planID).Scan(&remaining, &inBatch); err != nil || remaining != 0 || !inBatch {
		return
	}
	if err := s.cleanupAdvanceBatch(ctx); err != nil {
		log.Printf("nas cleanup batch advance deferred: %v", err)
	}
}

// cleanupAdvanceBatchIfIdle lets the lease path finish advancing (or pause) an
// enabled batch whose current plan is no longer enabled.
func (s *Server) cleanupAdvanceBatchIfIdle(ctx context.Context) {
	if s.pool == nil {
		return
	}
	var pending bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_batches WHERE mode='enabled') AND NOT EXISTS(SELECT 1 FROM nas_cleanup_plans WHERE mode='enabled')`).Scan(&pending); err != nil || !pending {
		return
	}
	if err := s.cleanupAdvanceBatch(ctx); err != nil {
		log.Printf("nas cleanup batch advance deferred: %v", err)
	}
}

const cleanupBatchAutoPauseSQL = `UPDATE nas_cleanup_batches SET mode='paused',approved_until=NULL,approval_reason=left('auto-paused: '||$2::text,500) WHERE id=$1 AND mode='enabled'`

// cleanupAdvanceBatch moves the enabled batch forward by at most one plan.
// Under the public cleanup fence it walks the batch in order:
//   - a plan whose items are all 'deleted' is complete (an enabled one is
//     closed as paused with reason "batch-completed", keeping its holds);
//   - the first plan that is not complete is the current plan. If it is
//     enabled, nothing happens. If it is in dry_run with no issued deletion,
//     it is enabled after passing every per-plan enable gate again (fresh
//     <=24h complete dry run with verified R2 copies, <=100 GB, one enabled
//     plan, master switch, unexpired batch approval) and its best-14 replay is
//     activated. Anything else (paused, retired, issued but incomplete, digest
//     drift, a failed gate) pauses the batch. A batch never re-enables a plan.
func (s *Server) cleanupAdvanceBatch(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err != nil {
		return err
	}
	var batchID int64
	var ids []int64
	var shas []string
	var reason string
	var until *time.Time
	err = tx.QueryRow(ctx, `SELECT id,plan_ids,plan_sha256s,approval_reason,approved_until FROM nas_cleanup_batches WHERE mode='enabled' AND account_id=47 AND connection_id=13 FOR UPDATE`).Scan(&batchID, &ids, &shas, &reason, &until)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	pause := func(why string) error {
		if _, err := tx.Exec(ctx, cleanupBatchAutoPauseSQL, batchID, "batch "+strconv.FormatInt(batchID, 10)+": "+why); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	current := int64(0)
	for n, id := range ids {
		var mode, sha, binding string
		var total, notDeleted int
		var issued bool
		err = tx.QueryRow(ctx, `SELECT mode,plan_sha256,report_sha256,(SELECT count(*) FROM nas_cleanup_items WHERE plan_id=p.id),(SELECT count(*) FROM nas_cleanup_items WHERE plan_id=p.id AND state<>'deleted'),EXISTS(SELECT 1 FROM nas_cleanup_items WHERE plan_id=p.id AND delete_ever_issued) FROM nas_cleanup_plans p WHERE id=$1 AND account_id=47 AND connection_id=13 FOR UPDATE`, id).Scan(&mode, &sha, &binding, &total, &notDeleted, &issued)
		if err != nil {
			return pause("plan " + strconv.FormatInt(id, 10) + " unavailable")
		}
		if sha != shas[n] || binding != cleanupPolicySHA {
			return pause("plan " + strconv.FormatInt(id, 10) + " identity or policy differs from the approved batch")
		}
		if total > 0 && notDeleted == 0 {
			if mode == "enabled" {
				if _, err = tx.Exec(ctx, `UPDATE nas_cleanup_plans SET mode='paused',approved_until=NULL,approval_reason=left('batch-completed: batch '||$2::text||' plan finished cleanly',500) WHERE id=$1`, id, strconv.FormatInt(batchID, 10)); err != nil {
					return err
				}
			}
			continue
		}
		if mode == "enabled" {
			if until == nil || !until.After(time.Now()) {
				if _, err = tx.Exec(ctx, `UPDATE nas_cleanup_plans SET mode='paused',approved_until=NULL,approval_reason=left('auto-paused: approval expired; '||approval_reason,500) WHERE id=$1`, id); err != nil {
					return err
				}
				return pause("approval expired")
			}
			return tx.Commit(ctx)
		}
		if mode != "dry_run" || issued {
			return pause("plan " + strconv.FormatInt(id, 10) + " stopped before finishing cleanly (" + mode + ")")
		}
		current = id
		break
	}
	if current == 0 {
		if _, err = tx.Exec(ctx, `UPDATE nas_cleanup_batches SET mode='completed',approved_until=NULL WHERE id=$1`, batchID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if !s.cfg.NASCleanupDeletionEnabled {
		return pause("master switch is off")
	}
	if until == nil || !until.After(time.Now().Add(time.Minute)) {
		return pause("approval expired")
	}
	if status, body := cleanupEnableGates(ctx, tx, current); status != 0 {
		why, _ := body["error"].(string)
		return pause("plan " + strconv.FormatInt(current, 10) + " not enableable: " + why)
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_cleanup_plans SET mode='enabled',allow_nas_only=false,approved_until=$2,approval_reason=left('batch '||$3::text||': '||$4::text,500) WHERE id=$1`, current, *until, strconv.FormatInt(batchID, 10), reason); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_cleanup_items SET state='pending',lease_expires_at=NULL WHERE plan_id=$1`, current); err != nil {
		return err
	}
	if err = cleanupActivateRankings(ctx, tx, current); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// handleAdminNASCleanupBatchEnable approves an ordered list of plans once. It
// requires the master switch, the exact batch digest, a 1..24 h approval
// window, no enabled plan or batch, and that every plan is a policy-current
// dry-run plan with no issued deletion that passes every per-plan enable gate
// now. Only the first plan is enabled; later plans are re-gated on advance.
func (s *Server) handleAdminNASCleanupBatchEnable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PlanIDs           []int64 `json:"plan_ids"`
		BatchSHA          string  `json:"batch_sha256"`
		Reason            string  `json:"reason"`
		ApprovalHours     int     `json:"approval_hours"`
		AllowLongApproval bool    `json:"allow_long_approval,omitempty"`
	}
	if util.DecodeJSON(r, &req) != nil || len(req.BatchSHA) != 64 || !lowerHex(req.BatchSHA) || len(req.Reason) < 1 || len(req.Reason) > 500 || len(req.PlanIDs) < 1 || len(req.PlanIDs) > nasCleanupMaxBatchPlans {
		util.WriteError(w, 400, "1 to 500 plan ids, the exact batch digest and a reason required")
		return
	}
	approval := time.Duration(req.ApprovalHours) * time.Hour
	if approval < time.Hour || approval > nasCleanupMaxApproval || (approval > nasCleanupLongApproval && !req.AllowLongApproval) {
		util.WriteError(w, 400, "approval_hours must be 1..24; above 4 requires allow_long_approval")
		return
	}
	seen := map[int64]bool{}
	for _, id := range req.PlanIDs {
		if id <= 0 || seen[id] {
			util.WriteError(w, 400, "distinct positive plan ids required")
			return
		}
		seen[id] = true
	}
	if !s.cfg.NASCleanupDeletionEnabled {
		util.WriteError(w, 409, "NAS deletion master switch is off")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "begin cleanup batch failed")
		return
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err != nil {
		util.WriteError(w, 500, "public cleanup fence failed")
		return
	}
	var busy int
	if err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM nas_cleanup_batches WHERE mode='enabled')+(SELECT count(*) FROM nas_cleanup_plans WHERE mode='enabled')`).Scan(&busy); err != nil || busy != 0 {
		util.WriteError(w, 409, "another cleanup plan or batch is enabled; pause it first")
		return
	}
	shas := make([]string, len(req.PlanIDs))
	var total int64
	for n, id := range req.PlanIDs {
		var mode, binding string
		var issued bool
		var planBytes int64
		err = tx.QueryRow(ctx, `SELECT plan_sha256,mode,report_sha256,EXISTS(SELECT 1 FROM nas_cleanup_items WHERE plan_id=p.id AND delete_ever_issued),(SELECT coalesce(sum(coalesce(i.size_bytes,(p.manifest->(i.ordinal-1)->>'size_bytes')::bigint)),0) FROM nas_cleanup_items i WHERE i.plan_id=p.id) FROM nas_cleanup_plans p WHERE id=$1 AND account_id=47 AND connection_id=13 FOR UPDATE`, id).Scan(&shas[n], &mode, &binding, &issued, &planBytes)
		if err != nil {
			util.WriteError(w, 409, "plan "+strconv.FormatInt(id, 10)+" unavailable")
			return
		}
		if binding != cleanupPolicySHA || mode != "dry_run" || issued {
			util.WriteError(w, 409, "plan "+strconv.FormatInt(id, 10)+" must be a policy-current dry-run plan with no issued deletion")
			return
		}
		if status, body := cleanupEnableGates(ctx, tx, id); status != 0 {
			body["plan_id"] = id
			util.WriteJSON(w, status, body)
			return
		}
		total += planBytes
	}
	if digest := cleanupBatchDigest(cleanupPolicySHA, req.PlanIDs, shas); digest != req.BatchSHA {
		util.WriteJSON(w, 409, map[string]any{"error": "batch identity differs", "batch_sha256": digest})
		return
	}
	var id int64
	if err = tx.QueryRow(ctx, `INSERT INTO nas_cleanup_batches(account_id,connection_id,batch_sha256,report_sha256,plan_ids,plan_sha256s,mode,approval_reason,approved_until) VALUES(47,13,$1,$2,$3,$4,'enabled',$5,now()+$6::bigint*interval '1 second') RETURNING id`, req.BatchSHA, cleanupPolicySHA, req.PlanIDs, shas, req.Reason, int64(approval/time.Second)).Scan(&id); err != nil {
		util.WriteError(w, 500, "cleanup batch save failed")
		return
	}
	if err = tx.Commit(ctx); err != nil {
		util.WriteError(w, 500, "cleanup batch save failed")
		return
	}
	// Enabling the first plan is the ordinary advance step (fresh gates).
	if err = s.cleanupAdvanceBatch(ctx); err != nil {
		util.WriteJSON(w, 500, map[string]any{"error": "cleanup batch first plan enable failed; the batch stays enabled and advance is retried on the next lease", "detail": err.Error()})
		return
	}
	var mode, why string
	if err = s.pool.QueryRow(ctx, `SELECT mode,approval_reason FROM nas_cleanup_batches WHERE id=$1`, id).Scan(&mode, &why); err != nil || mode != "enabled" {
		util.WriteJSON(w, 409, map[string]any{"error": "cleanup batch did not start", "id": id, "mode": mode, "reason": why})
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "batch_sha256": req.BatchSHA, "plans": len(req.PlanIDs), "bytes": total, "mode": mode, "approval_hours": req.ApprovalHours})
}

// handleAdminNASCleanupBatchControl pauses a batch (and its enabled plan).
// There is no resume: approving the remaining plans again is a new batch.
func (s *Server) handleAdminNASCleanupBatchControl(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		BatchSHA string `json:"batch_sha256"`
		Mode     string `json:"mode"`
		Reason   string `json:"reason"`
	}
	if util.DecodeJSON(r, &req) != nil || len(req.BatchSHA) != 64 || req.Mode != "paused" || len(req.Reason) < 1 || len(req.Reason) > 500 {
		util.WriteError(w, 400, "exact batch digest, mode paused and reason required")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "begin cleanup batch control failed")
		return
	}
	defer tx.Rollback(ctx)
	var sha, mode string
	var ids []int64
	if err = tx.QueryRow(ctx, `SELECT batch_sha256,mode,plan_ids FROM nas_cleanup_batches WHERE id=$1 AND account_id=47 AND connection_id=13 FOR UPDATE`, id).Scan(&sha, &mode, &ids); err != nil || sha != req.BatchSHA {
		util.WriteError(w, 409, "batch identity differs")
		return
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_cleanup_plans SET mode='paused',approved_until=NULL,approval_reason=$2 WHERE id=ANY($1) AND mode='enabled'`, ids, req.Reason); err == nil && mode == "enabled" {
		_, err = tx.Exec(ctx, `UPDATE nas_cleanup_batches SET mode='paused',approved_until=NULL,approval_reason=$2 WHERE id=$1`, id, req.Reason)
	}
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "cleanup batch control failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "mode": "paused", "batch_sha256": sha})
}

func (s *Server) handleAdminNASCleanupBatchStatus(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var sha, mode, reason string
	var until *time.Time
	var plans []byte
	err := s.pool.QueryRow(r.Context(), `SELECT b.batch_sha256,b.mode,b.approval_reason,b.approved_until,(SELECT coalesce(jsonb_agg(jsonb_build_object('plan_id',p.id,'plan_sha256',p.plan_sha256,'mode',p.mode,'approval_reason',p.approval_reason,'states',(SELECT coalesce(jsonb_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM nas_cleanup_items WHERE plan_id=p.id GROUP BY state) q)) ORDER BY x.n),'[]') FROM unnest(b.plan_ids) WITH ORDINALITY x(id,n) JOIN nas_cleanup_plans p ON p.id=x.id) FROM nas_cleanup_batches b WHERE b.id=$1 AND b.account_id=47 AND b.connection_id=13`, id).Scan(&sha, &mode, &reason, &until, &plans)
	if err != nil {
		util.WriteError(w, 404, "cleanup batch unavailable")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "batch_sha256": sha, "mode": mode, "approval_reason": reason, "approved_until": until, "master_deletion_enabled": s.cfg.NASCleanupDeletionEnabled, "plans": json.RawMessage(plans)})
}
