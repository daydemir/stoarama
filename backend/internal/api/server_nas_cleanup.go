package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The independently reviewed folder report pins which stream owns each NAS
// folder (path confinement). The keep28 policy, not the report's old folder
// actions, decides KEEP per file; see server_nas_cleanup_policy.go.
// Admission queues immutable proposals, not destructive grants. It spans audit
// I/O while connection heartbeats keep updating the FK parent. READ COMMITTED
// avoids false40001 failures; source locks and grant-time identity/policy checks
// still fence every destructive operation.
var nasCleanupAdmissionOptions = pgx.TxOptions{IsoLevel: pgx.ReadCommitted}

// Belt-and-braces deletion caps (safety council, 2026-11). They are code
// constants on purpose, not configuration: raising any of them is a separate,
// reviewed code change. All of them fail closed.
const (
	// At most this many plans may be in mode 'enabled' at any time, checked
	// under the public cleanup fence when a plan is enabled.
	nasCleanupMaxEnabledPlans = 1
	// A plan whose manifest exceeds this many media bytes cannot be enabled.
	nasCleanupMaxPlanBytes int64 = 100_000_000_000 // 100 GB
	// Rolling 24h deletion budget across all plans, counted from issued
	// deletion permits (delete_ever_issued). No permit is issued beyond it.
	// Raised from 3 TB to 5 TB by owner decision (2026-11, PR #436); the NAS
	// client's local byte cap (CLEANUP_MAX_BYTES_24H) is the same value.
	nasCleanupMaxIssuedBytes24h int64 = 5_000_000_000_000 // 5 TB
	// Serializes the budget check-and-issue step across observations.
	nasCleanupBudgetLock int64 = 7343226011102
	// Per-plan execution lock base (key = base + plan id). Observations of an
	// enabled plan take it before any row lock so they can auto-pause the plan
	// without two FOR SHARE holders deadlocking on the plan row.
	nasCleanupPlanLockBase int64 = 7343226100000000
	// Public cleanup fence: enabling a plan or batch, batch advance and new
	// source references serialize on it (see migration 0167).
	nasCleanupPublicFence int64 = 7343226011101
	// Serialize admissions before catalog/source locks. Separate from the
	// public fence: admission's frozen-index I/O must not block public reads.
	nasCleanupAdmissionLock int64 = 7343226011103
	// Items per plan. Plans are still bounded by nasCleanupMaxPlanBytes; the
	// item limit only keeps one admission request bounded.
	nasCleanupMaxPlanItems = 10000
	// Default and maximum approval window of an enabled plan or batch.
	nasCleanupDefaultApproval = 2 * time.Hour
	nasCleanupMaxApproval     = 24 * time.Hour
	// Windows above this need an explicit allow_long_approval.
	nasCleanupLongApproval = 4 * time.Hour
)

const nasCleanupReportSHA = "64eee0bc589c9be4dcab5b5ea7668270291a07ce4fd0db98dcd0a484e19d84c1"
const nasCleanupReportKey = "staging/best14-20261021/nas-retention-20261025/current-folder-preparation-20261031/" + nasCleanupReportSHA + "/retention-only-folder-report-109.json.gz"

type nasCleanupItem struct {
	PermitReason      string    `json:"permit_reason,omitempty"`
	StitchTaskID      int64     `json:"stitch_task_id,omitempty"`
	StitchOutputPath  string    `json:"stitch_output_path,omitempty"`
	StitchOutputSHA   string    `json:"stitch_output_sha256,omitempty"`
	StitchOutputSize  int64     `json:"stitch_output_size_bytes,omitempty"`
	StartAt           time.Time `json:"start_at"`
	EndAt             time.Time `json:"end_at"`
	Timezone          string    `json:"timezone"`
	SourceIDs         []int64   `json:"source_ids"`
	SourceManifestSHA string    `json:"source_manifest_sha256"`
	Kind              string    `json:"kind"`
	SourceID          int64     `json:"source_id"`
	RecordingID       int64     `json:"recording_id"`
	StreamID          int64     `json:"stream_id"`
	RelativePath      string    `json:"relative_path"`
	SizeBytes         int64     `json:"size_bytes"`
	SHA256            string    `json:"sha256"`
	Bucket            string    `json:"bucket"`
	ObjectKey         string    `json:"object_key"`
	SidecarRequired   bool      `json:"sidecar_required"`
	Frozen            [5]int64  `json:"frozen_identity"`
	FrozenSidecar     [5]int64  `json:"frozen_sidecar_identity"`
}
type nasCleanupReport struct {
	RegisteredSnapshotAt time.Time `json:"registered_snapshot_at"`
	Streams              []struct {
		ID     int64    `json:"stream_id"`
		Best14 []string `json:"selected_registered_14"`
	} `json:"streams"`
	Nodes []struct {
		Path   string  `json:"path"`
		Action string  `json:"direct_files_action"`
		Owners []int64 `json:"owner_stream_ids"`
	} `json:"complete_node_actions"`
}

func cleanupSHA(b []byte) string { d := sha256.Sum256(b); return hex.EncodeToString(d[:]) }
func validCleanupPath(p string) bool {
	return validNASRelativePath(p) && path.Clean(p) == p && !strings.ContainsAny(p, "\x00\r\n") && !strings.HasPrefix(p, "#") && !strings.HasPrefix(p, "@")
}

// permits confines a file to a reviewed folder owned by exactly its own stream.
// Under keep28 every stream's folders are eligible regardless of the old
// folder action; the per-file window check decides KEEP.
func (p nasCleanupReport) permits(i nasCleanupItem) bool {
	for _, n := range p.Nodes {
		if n.Path == path.Dir(i.RelativePath) && len(n.Owners) == 1 && n.Owners[0] == i.StreamID && i.StreamID > 0 {
			return true
		}
	}
	return false
}
func (s *Server) cleanupReport(ctx context.Context) (nasCleanupReport, error) {
	s.nasCleanupMu.Lock()
	defer s.nasCleanupMu.Unlock()
	if s.nasCleanupReportCache != nil {
		return *s.nasCleanupReportCache, nil
	}
	var report nasCleanupReport
	if s.r2 == nil {
		return report, errors.New("report store unavailable")
	}
	h, err := s.r2.Head(ctx, nasCleanupReportKey)
	if err != nil || h.SizeBytes != 430412 {
		return report, errors.New("reviewed report size unavailable")
	}
	b, err := s.r2.Get(ctx, nasCleanupReportKey)
	if err != nil || cleanupSHA(b) != nasCleanupReportSHA {
		return report, errors.New("reviewed report hash mismatch")
	}
	z, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return report, err
	}
	defer z.Close()
	b, err = io.ReadAll(io.LimitReader(z, 32<<20))
	if err != nil {
		return report, err
	}
	err = json.Unmarshal(b, &report)
	if err == nil && len(report.Nodes) != 3304 {
		err = errors.New("incomplete reviewed report")
	}
	if err == nil {
		s.nasCleanupReportCache = &report
	}
	return report, err
}

// Catalog identity is derived on the server; NAS-supplied paths never authorize
// themselves. Unsupported/unknown files remain KEEP.
func cleanupCatalogItem(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, kind string, id int64, bucket string) (nasCleanupItem, error) {
	i := nasCleanupItem{Kind: kind, SourceID: id}
	var query string
	switch kind {
	case "raw":
		query = `SELECT c.recording_id,r.stream_id,c.display_path,c.size_bytes,lower(c.sha256),c.object_key,c.bucket,c.clip_start_at,c.clip_end_at,coalesce(nullif(s.local_timezone,''),r.cron_timezone),ARRAY[c.id] FROM recording_clips c JOIN recordings r ON r.id=c.recording_id JOIN streams s ON s.id=r.stream_id WHERE r.account_id=47 AND c.id=$1 AND c.bucket=$2 FOR SHARE OF c`
		i.SidecarRequired = true
	case "collated":
		query = `SELECT h.recording_id,r.stream_id,'joined/'||o.nas_relative_path,o.size_bytes,lower(o.sha256),o.object_key,$2::text,o.start_at,o.end_at,coalesce(nullif(s.local_timezone,''),r.cron_timezone),o.source_clip_ids FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id JOIN streams s ON s.id=r.stream_id WHERE r.account_id=47 AND o.id=$1 FOR SHARE OF o`
	default:
		return i, errors.New("unsupported cleanup kind; KEEP")
	}
	err := q.QueryRow(ctx, query, id, bucket).Scan(&i.RecordingID, &i.StreamID, &i.RelativePath, &i.SizeBytes, &i.SHA256, &i.ObjectKey, &i.Bucket, &i.StartAt, &i.EndAt, &i.Timezone, &i.SourceIDs)
	if err != nil {
		return i, err
	}
	if !validCleanupPath(i.RelativePath) || i.SizeBytes <= 0 || i.SizeBytes > 32<<30 || len(i.SHA256) != 64 || !lowerHex(i.SHA256) || i.ObjectKey == "" {
		return i, errors.New("incomplete cleanup catalog identity")
	}
	if len(i.SourceIDs) < 1 || len(i.SourceIDs) > 10000 || !i.EndAt.After(i.StartAt) {
		return i, errors.New("invalid current source span")
	}
	var raw []byte
	err = q.QueryRow(ctx, cleanupSourceMetadataSQL, i.SourceIDs).Scan(&raw)
	if err != nil {
		return i, err
	}
	i.SourceManifestSHA = cleanupSHA(raw)
	return i, nil
}
func (s *Server) handleAdminNASCleanupCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Label string `json:"label"`
		Items []struct {
			Kind     string `json:"kind"`
			SourceID int64  `json:"source_id"`
		} `json:"items"`
	}
	if util.DecodeJSON(r, &req) != nil || !relayArtifactName.MatchString(req.Label) || len(req.Label) > 80 || len(req.Items) < 1 || len(req.Items) > nasCleanupMaxPlanItems {
		util.WriteError(w, 400, "label and 1 to 10000 exact source identities required")
		return
	}
	ctx := r.Context()
	report, err := s.cleanupReport(ctx)
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	policy, err := s.cleanupPolicy(ctx)
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	tx, err := s.pool.BeginTx(ctx, nasCleanupAdmissionOptions)
	if err != nil {
		util.WriteError(w, 500, "begin cleanup plan failed")
		return
	}
	defer tx.Rollback(ctx)
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM connections WHERE id=13 AND account_id=47 AND kind='nas_pull')`).Scan(&exists); err != nil || !exists {
		util.WriteError(w, 409, "NAS connection unavailable")
		return
	}
	// Serialize admissions before taking catalog locks; persistence checks
	// reservations again after the frozen-index I/O. A concurrent close can
	// only release never-issued reservations once its transaction commits.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupAdmissionLock); err != nil {
		util.WriteError(w, 503, "cleanup admission fence unavailable")
		return
	}
	items := []nasCleanupItem{}
	seen := map[string]bool{}
	for _, sel := range req.Items {
		i, e := cleanupCatalogItem(ctx, tx, sel.Kind, sel.SourceID, s.r2.Bucket())
		// Early admission filter only; every observation rechecks the file and
		// all of its sources against the window and the live best14 replay.
		if e != nil || !report.permits(i) || seen[i.RelativePath] || !policy.outside(i.StreamID, i.StartAt, i.EndAt) {
			util.WriteError(w, 409, "source identity is unknown, duplicated, outside its stream's folders or inside the keep28 window")
			return
		}
		seen[i.RelativePath] = true
		items = append(items, i)
	}
	var planBytes int64
	for _, i := range items {
		planBytes += i.SizeBytes
	}
	if planBytes > nasCleanupMaxPlanBytes {
		util.WriteJSON(w, 409, map[string]any{"error": "plan exceeds the per-plan deletion cap", "plan_bytes": planBytes, "max_plan_bytes": nasCleanupMaxPlanBytes})
		return
	}
	if err = s.bindCleanupFrozenInventory(ctx, items); err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	id, sha, err := persistNASCleanupPlan(ctx, tx, req.Label, items)
	if err != nil {
		detail := map[string]any{"error": "cleanup plan admission failed"}
		var failure *cleanupPersistFailure
		if errors.As(err, &failure) {
			detail["stage"] = failure.stage
		}
		var databaseError *pgconn.PgError
		status := 409
		if errors.As(err, &databaseError) {
			detail["database_code"] = databaseError.Code
			detail["constraint"] = databaseError.ConstraintName
			if databaseError.Code == "40001" || databaseError.Code == "40P01" || databaseError.Code == "55P03" {
				status = 503
				detail["retryable"] = true
				w.Header().Set("Retry-After", "3")
			}
		}
		util.WriteJSON(w, status, detail)
		return
	}
	if tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "commit cleanup plan failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "plan_sha256": sha, "items": len(items), "mode": "dry_run", "deletion_enabled": false})
}
func (s *Server) handleAdminNASCleanupStatus(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var manifest, jsonStates, results []byte
	var label, sha, mode, reason, binding string
	var priority int
	var allow bool
	var until *time.Time
	err := s.pool.QueryRow(r.Context(), `SELECT label,plan_sha256,report_sha256,mode,priority,allow_nas_only,approval_reason,approved_until,manifest,(SELECT coalesce(jsonb_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM nas_cleanup_items WHERE plan_id=p.id GROUP BY state) q),(SELECT coalesce(jsonb_agg(jsonb_build_object('item_id',id,'ordinal',ordinal,'state',state,'recovery',recovery,'policy_reason',policy_reason,'attempt',attempt,'proof',proof,'outcome',outcome,'delete_ever_issued',delete_ever_issued,'stage_timings',stage_timings,'updated_at',updated_at) ORDER BY ordinal),'[]') FROM nas_cleanup_items WHERE plan_id=p.id) FROM nas_cleanup_plans p WHERE id=$1 AND account_id=47 AND connection_id=13`, id).Scan(&label, &sha, &binding, &mode, &priority, &allow, &reason, &until, &manifest, &jsonStates, &results)
	if err != nil {
		util.WriteError(w, 404, "cleanup plan unavailable")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "label": label, "plan_sha256": sha, "policy_sha256": binding, "policy_current": binding == cleanupPolicySHA, "mode": mode, "allow_nas_only": allow, "approval_reason": reason, "approved_until": until, "priority": priority, "items": json.RawMessage(manifest), "states": json.RawMessage(jsonStates), "master_deletion_enabled": s.cfg.NASCleanupDeletionEnabled, "results": json.RawMessage(results)})
}

// Arming is deliberately not automatic. It requires the exact digest and a
// successful complete dry run, an explicit NAS-only decision, and expires.
func (s *Server) handleAdminNASCleanupControl(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		PlanSHA      string `json:"plan_sha256"`
		Mode         string `json:"mode"`
		AllowNASOnly bool   `json:"allow_nas_only"`
		Priority     *int   `json:"priority,omitempty"`
		Reason       string `json:"reason"`
		// Optional approval window for enable, 1..24 hours (default 1).
		ApprovalHours     *int `json:"approval_hours,omitempty"`
		AllowLongApproval bool `json:"allow_long_approval,omitempty"`
	}
	if util.DecodeJSON(r, &req) != nil || len(req.PlanSHA) != 64 || !lowerHex(req.PlanSHA) || (req.Mode != "enabled" && req.Mode != "paused" && req.Mode != "dry_run") || len(req.Reason) < 1 || len(req.Reason) > 500 {
		util.WriteError(w, 400, "exact plan digest,mode and reason required")
		return
	}
	// Deleting a NAS file without a verified R2 recovery copy was removed
	// (safety council, 2026-11): allow_nas_only is always rejected and no permit
	// is ever issued for nas_only_at_check items. Re-introducing NAS-only
	// deletion requires its own separately reviewed change; do not relax this
	// check (or the nas_cleanup_plans_no_nas_only constraint) in passing.
	if req.AllowNASOnly {
		util.WriteError(w, 400, "NAS-only deletion was removed; only items with a verified R2 recovery copy can ever be deleted")
		return
	}
	if req.Priority != nil && (req.Mode != "dry_run" || *req.Priority < -100 || *req.Priority > 100) {
		util.WriteError(w, 400, "bounded priority is read-only")
		return
	}
	approval := nasCleanupDefaultApproval
	if req.ApprovalHours != nil {
		approval = time.Duration(*req.ApprovalHours) * time.Hour
		if req.Mode != "enabled" || approval < time.Hour || approval > nasCleanupMaxApproval || (approval > nasCleanupLongApproval && !req.AllowLongApproval) {
			util.WriteError(w, 400, "approval_hours is 1..24 (above 4 requires allow_long_approval) and only applies to enable")
			return
		}
	}
	// Backend independent master switch defaults OFF even if an operator arms a
	// batch. Setting it is a separate explicit deployment/configuration action.
	if req.Mode == "enabled" && !s.cfg.NASCleanupDeletionEnabled {
		util.WriteError(w, 409, "NAS deletion master switch is off")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "begin cleanup control failed")
		return
	}
	defer tx.Rollback(ctx)
	if req.Mode == "enabled" {
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err != nil {
			util.WriteError(w, 500, "public cleanup fence failed")
			return
		}
	}
	var current, currentMode, binding string
	if err = tx.QueryRow(ctx, `SELECT plan_sha256,mode,report_sha256 FROM nas_cleanup_plans WHERE id=$1 AND account_id=47 AND connection_id=13 FOR UPDATE`, id).Scan(&current, &currentMode, &binding); err != nil || current != req.PlanSHA {
		util.WriteError(w, 409, "plan identity differs")
		return
	}
	if currentMode == "retired" {
		util.WriteError(w, 409, "retired plans are terminal")
		return
	}
	// Plans admitted under a superseded policy may only be paused or retired.
	if req.Mode != "paused" && binding != cleanupPolicySHA {
		util.WriteError(w, 409, "plan is bound to a superseded cleanup policy; retire it and prepare a new plan")
		return
	}
	if req.Mode == "dry_run" {
		var issued bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_items WHERE plan_id=$1 AND delete_ever_issued)`, id).Scan(&issued); err != nil {
			util.WriteError(w, 500, "cleanup resume evidence unavailable")
			return
		}
		if issued || (currentMode != "dry_run" && currentMode != "paused") {
			util.WriteError(w, 409, "only nonexecuting plans with no issued deletion may resume; otherwise reconcile")
			return
		}
		if currentMode == "paused" {
			// Pausing releases unissued source holds. Take the original source-row
			// fences again before restoring a read-only hold; never rewrite the manifest
			// or turn terminal/conflicting observations back into pending work.
			rows, e := tx.Query(ctx, `SELECT c.id FROM recording_clips c JOIN nas_cleanup_source_holds h ON h.clip_id=c.id WHERE h.plan_id=$1 ORDER BY c.id FOR UPDATE OF c`, id)
			if e != nil {
				util.WriteError(w, 500, "cleanup source resume fence unavailable")
				return
			}
			for rows.Next() {
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				util.WriteError(w, 500, "cleanup source resume fence unavailable")
				return
			}
		}
	}
	if req.Mode == "enabled" {
		var batches int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM nas_cleanup_batches WHERE mode='enabled'`).Scan(&batches); err != nil {
			util.WriteError(w, 500, "cleanup batch evidence unavailable")
			return
		}
		if batches != 0 {
			util.WriteError(w, 409, "a cleanup batch is enabled; pause it first (at most one enabled plan)")
			return
		}
		if status, body := cleanupEnableGates(ctx, tx, id); status != 0 {
			util.WriteJSON(w, status, body)
			return
		}
	}
	_, err = tx.Exec(ctx, `UPDATE nas_cleanup_plans SET mode=$2,allow_nas_only=false,approval_reason=$3,approved_until=CASE WHEN $2='enabled' THEN now()+$5::bigint*interval '1 second' END,priority=coalesce($4,priority) WHERE id=$1`, id, req.Mode, req.Reason, req.Priority, int64(approval/time.Second))
	if err == nil && req.Mode == "enabled" {
		_, err = tx.Exec(ctx, `UPDATE nas_cleanup_items SET state='pending',lease_expires_at=NULL WHERE plan_id=$1`, id)
		if err == nil {
			err = cleanupActivateRankings(ctx, tx, id)
		}
	}
	if err == nil && req.Mode != "enabled" {
		// Pausing (or resuming the dry run of) a plan stops any enabled batch
		// that contains it; a batch never re-enables a plan an operator stopped.
		_, err = tx.Exec(ctx, `UPDATE nas_cleanup_batches SET mode='paused',approved_until=NULL,approval_reason=left('paused with plan '||$3::text||': '||$2::text,500) WHERE mode='enabled' AND $1=ANY(plan_ids)`, id, req.Reason, strconv.FormatInt(id, 10))
	}
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "cleanup control failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "mode": req.Mode, "plan_sha256": current})
}

// Retirement is a terminal, non-destructive state transition for plans bound
// to a superseded cleanup policy. No row is deleted (guard triggers forbid it):
// unfinished items become 'keep', and migration 0170 makes holds of retired
// plans stop blocking R2 purges exactly as paused holds do. Enabled plans and
// plans that ever issued a deletion are never retired here. Without
// expected_plans the call is a read-only preview.
func (s *Server) handleAdminNASCleanupRetireSuperseded(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Reason        string `json:"reason"`
		ExpectedPlans *int   `json:"expected_plans,omitempty"`
	}
	if util.DecodeJSON(r, &req) != nil || len(req.Reason) < 1 || len(req.Reason) > 500 || (req.ExpectedPlans != nil && *req.ExpectedPlans < 1) {
		util.WriteError(w, 400, "reason and optional positive expected_plans required")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "begin cleanup retirement failed")
		return
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id FROM nas_cleanup_plans p WHERE account_id=47 AND connection_id=13 AND report_sha256<>$1 AND mode IN ('dry_run','paused') AND NOT EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued) ORDER BY id FOR UPDATE`, cleanupPolicySHA)
	if err != nil {
		util.WriteError(w, 500, "superseded cleanup plans unavailable")
		return
	}
	ids := []int64{}
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	var blocked int
	if err == nil {
		err = tx.QueryRow(ctx, `SELECT count(*) FROM nas_cleanup_plans WHERE account_id=47 AND connection_id=13 AND report_sha256<>$1 AND mode<>'retired' AND NOT (id=ANY($2))`, cleanupPolicySHA, ids).Scan(&blocked)
	}
	if err != nil {
		util.WriteError(w, 500, "superseded cleanup plans unavailable")
		return
	}
	result := map[string]any{"policy_sha256": cleanupPolicySHA, "plans": len(ids), "plan_ids": ids, "not_retirable": blocked}
	if req.ExpectedPlans == nil {
		result["preview"] = true
		util.WriteJSON(w, 200, result)
		return
	}
	if *req.ExpectedPlans != len(ids) {
		util.WriteJSON(w, 409, map[string]any{"error": "superseded plan count differs from expected_plans", "plans": len(ids)})
		return
	}
	_, err = tx.Exec(ctx, `UPDATE nas_cleanup_items SET state='keep',lease_expires_at=NULL,policy_reason='retired: superseded cleanup policy',updated_at=now() WHERE plan_id=ANY($1) AND state IN ('pending','leased')`, ids)
	if err == nil {
		_, err = tx.Exec(ctx, `UPDATE nas_cleanup_plans SET mode='retired',allow_nas_only=false,approved_until=NULL,approval_reason=$2 WHERE id=ANY($1)`, ids, req.Reason)
	}
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "cleanup retirement failed")
		return
	}
	result["retired"] = len(ids)
	util.WriteJSON(w, 200, result)
}

type cleanupTask struct {
	ItemID        int64           `json:"item_id"`
	Attempt       int             `json:"attempt"`
	PlanSHA       string          `json:"plan_sha256"`
	ExpiresAt     time.Time       `json:"expires_at"`
	Item          nasCleanupItem  `json:"item"`
	ApprovedProof json.RawMessage `json:"approved_proof,omitempty"`
}
type cleanupObserve struct {
	Attempt       int                `json:"attempt"`
	PlanSHA       string             `json:"plan_sha256"`
	Outcome       string             `json:"outcome"`
	Proof         json.RawMessage    `json:"proof"`
	Error         string             `json:"error"`
	ClientVersion string             `json:"client_version"`
	Timings       map[string]float64 `json:"timings,omitempty"`
	// RequestID (protocol 2) makes a lost permit response replayable: only the
	// identical request gets the already-issued permit back, never a new one.
	RequestID string `json:"request_id,omitempty"`
	// MD5 of the media bytes (protocol 2, lease advertises recovery_md5),
	// computed by the client in the same read as the proof's SHA-256.
	MD5 string `json:"md5,omitempty"`
}

// Prefer the least-leased pending item so unresolved client journals cannot
// repeatedly take the front of the queue from untouched work.
// Plans are ordered without sorting/locking the entire pending file backlog.
// The lateral probe uses nas_cleanup_pending and skips locked rows before the
// outer LIMIT; it can keep looking in the next eligible plan if one is busy.
const cleanupLeaseLoadSQL = `SELECT i.id,i.attempt,p.plan_sha256,coalesce(i.item,p.manifest->(i.ordinal-1)),i.proof,p.mode
FROM nas_cleanup_plans p
JOIN LATERAL (
 SELECT candidate.* FROM nas_cleanup_items candidate
 WHERE candidate.plan_id=p.id AND candidate.state='pending'
 ORDER BY candidate.attempt,candidate.id LIMIT 1 FOR UPDATE SKIP LOCKED
) i ON true
WHERE p.connection_id=$1 AND p.report_sha256=$3
 AND (p.mode='dry_run' OR (p.mode='enabled' AND p.approved_until>now() AND $2))
ORDER BY i.attempt,p.priority DESC,p.id LIMIT 1`

// Bytes of execution work already leased but not yet observed. Counted with
// the issued bytes so a lease never hands out work the budget cannot permit.
const cleanupLeasedExecutionBytesSQL = `SELECT coalesce(sum(coalesce(i.size_bytes,(p.manifest->(i.ordinal-1)->>'size_bytes')::bigint)),0) FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE p.mode='enabled' AND i.state='leased'`

const (
	// Protocol 1 (legacy clients): at most 8 dry-run tasks, execution serial.
	nasCleanupLeaseMaxV1 = 8
	// Protocol 2: up to 64 tasks per lease, including execution tasks, bounded
	// by the client's remaining local caps and by the server budget.
	nasCleanupLeaseMaxV2 = 64
)

func (s *Server) handleAccountNASCleanupLease(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientVersion  string `json:"client_version"`
		Protocol       int    `json:"protocol"`
		Limit          int    `json:"limit"`
		ExecutionFiles *int   `json:"execution_files,omitempty"`
		ExecutionBytes *int64 `json:"execution_bytes,omitempty"`
		// Protocol 2: a retried request returns the tasks it already leased.
		RequestID string `json:"request_id,omitempty"`
	}
	if util.DecodeJSON(r, &req) != nil || len(req.ClientVersion) == 0 || len(req.ClientVersion) > 64 || req.Limit < 0 {
		util.WriteError(w, 400, "cleanup protocol1, client version and bounded limit required")
		return
	}
	switch req.Protocol {
	case 1:
		if req.Limit > nasCleanupLeaseMaxV1 || req.ExecutionFiles != nil || req.ExecutionBytes != nil || req.RequestID != "" {
			util.WriteError(w, 400, "cleanup protocol1, client version and bounded limit required")
			return
		}
	case 2:
		if req.Limit < 1 || req.Limit > nasCleanupLeaseMaxV2 || (req.RequestID != "" && (len(req.RequestID) != 32 || !lowerHex(req.RequestID))) || req.ExecutionFiles == nil || req.ExecutionBytes == nil || *req.ExecutionFiles < 0 || *req.ExecutionFiles > nasCleanupLeaseMaxV2 || *req.ExecutionBytes < 0 {
			util.WriteError(w, 400, "cleanup protocol2 requires a bounded limit and the client's remaining execution caps")
			return
		}
	default:
		util.WriteError(w, 400, "cleanup protocol1, client version and bounded limit required")
		return
	}
	if req.Limit == 0 {
		req.Limit = 1
	}
	ctx := r.Context()
	cid, ok := s.nasRestoreConnection(ctx, w, r)
	if !ok {
		return
	}
	// A batch whose current plan finished (or stopped) advances or pauses
	// before work is leased; it never runs inside the lease transaction.
	s.cleanupExpireApprovals(ctx)
	s.cleanupAdvanceBatchIfIdle(ctx)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "cleanup lease transaction failed")
		return
	}
	defer tx.Rollback(ctx)
	// An execution lease that expires without a report is an execution
	// anomaly: its enabled plan (and its batch) is paused in the same statement.
	_, err = tx.Exec(ctx, cleanupExpireLeasesSQL, cid, "execution lease expired without a report")
	if err != nil {
		util.WriteError(w, 500, "cleanup lease expiry failed")
		return
	}
	tasks := make([]cleanupTask, 0, req.Limit)
	if req.RequestID != "" {
		replayed, err := cleanupLeaseReplay(ctx, tx, cid, req.RequestID)
		if err != nil {
			util.WriteError(w, 500, "cleanup lease load failed")
			return
		}
		if len(replayed) != 0 {
			util.WriteJSON(w, 200, map[string]any{"tasks": replayed, "retry_after_sec": 0, "replayed": true, "recovery_md5": cleanupLeaseAdvertisesMD5})
			return
		}
	}
	executing := false
	// Execution work is only offered while it could be permitted; otherwise
	// the probe skips enabled plans and keeps leasing dry-run work.
	allowExecution := s.cfg.NASCleanupDeletionEnabled && (req.Protocol == 1 || *req.ExecutionFiles > 0)
	budgetKnown := false
	var budgetLeft, execBytes int64
	for len(tasks) < req.Limit {
		var task cleanupTask
		var raw, approved []byte
		var mode string
		err = tx.QueryRow(ctx, cleanupLeaseLoadSQL, cid, allowExecution, cleanupPolicySHA).Scan(&task.ItemID, &task.Attempt, &task.PlanSHA, &raw, &approved, &mode)
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil || json.Unmarshal(raw, &task.Item) != nil {
			util.WriteError(w, 500, "cleanup lease load failed")
			return
		}
		if mode == "enabled" {
			// Protocol 1: an execution lease is always alone. Leave it pending if
			// this response already contains dry-run work; the client drains that
			// work first. Protocol 2 never mixes execution with dry-run work.
			if len(tasks) != 0 && (req.Protocol == 1 || !executing) {
				break
			}
			if !budgetKnown {
				var issued, leased int64
				if err = tx.QueryRow(ctx, cleanupIssuedBytes24hSQL).Scan(&issued); err == nil {
					err = tx.QueryRow(ctx, cleanupLeasedExecutionBytesSQL).Scan(&leased)
				}
				if err != nil {
					util.WriteError(w, 500, "cleanup deletion budget unavailable")
					return
				}
				budgetKnown, budgetLeft = true, nasCleanupMaxIssuedBytes24h-issued-leased
			}
			// Never lease execution work the rolling budget (or, protocol 2, the
			// client's own remaining local caps) could not permit: it would only
			// turn into a budget refusal that auto-pauses the plan.
			// (An invalid size is still leased and then refused, and auto-paused,
			// by the permit check itself.)
			if (task.Item.SizeBytes > 0 && execBytes+task.Item.SizeBytes > budgetLeft) ||
				(req.Protocol == 2 && (len(tasks)+1 > *req.ExecutionFiles || execBytes+task.Item.SizeBytes > *req.ExecutionBytes)) {
				if executing {
					break
				}
				allowExecution = false
				continue
			}
			execBytes += task.Item.SizeBytes
			executing = true
		} else if executing {
			break
		}
		task.Attempt++
		// Microsecond precision: exactly what PostgreSQL stores, so a replayed
		// lease reports the identical expiry.
		task.ExpiresAt = time.Now().UTC().Add(30 * time.Minute).Truncate(time.Microsecond)
		if mode == "enabled" {
			task.ApprovedProof = approved
		}
		var requestID any
		if req.RequestID != "" {
			requestID = req.RequestID
		}
		_, err = tx.Exec(ctx, `UPDATE nas_cleanup_items SET state='leased',attempt=$2,lease_expires_at=$3,lease_request_id=$4,updated_at=now() WHERE id=$1`, task.ItemID, task.Attempt, task.ExpiresAt, requestID)
		if err != nil {
			util.WriteError(w, 500, "cleanup lease save failed")
			return
		}
		tasks = append(tasks, task)
		if mode == "enabled" && req.Protocol == 1 {
			break
		}
	}
	if err = tx.Commit(ctx); err != nil {
		util.WriteError(w, 500, "cleanup lease save failed")
		return
	}
	retry := 0
	if len(tasks) == 0 {
		retry = 120
	}
	body := map[string]any{"tasks": tasks, "retry_after_sec": retry}
	if req.Protocol == 2 && cleanupLeaseAdvertisesMD5 {
		// Observations may carry the media MD5 (see cleanupRecoveryMD5).
		body["recovery_md5"] = true
	}
	util.WriteJSON(w, 200, body)
}

// cleanupLeaseReplay returns the still-leased, unexpired tasks of an earlier
// identical lease request (its response was lost), with the same attempt and
// expiry. Nothing is leased again and no lease is extended.
func cleanupLeaseReplay(ctx context.Context, tx pgx.Tx, cid int64, requestID string) ([]cleanupTask, error) {
	rows, err := tx.Query(ctx, `SELECT i.id,i.attempt,p.plan_sha256,coalesce(i.item,p.manifest->(i.ordinal-1)),i.proof,p.mode,i.lease_expires_at FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE i.lease_request_id=$1 AND i.state='leased' AND i.lease_expires_at>now() AND p.connection_id=$2 AND p.report_sha256=$3 ORDER BY i.id FOR UPDATE OF i`, requestID, cid, cleanupPolicySHA)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks := []cleanupTask{}
	for rows.Next() {
		var task cleanupTask
		var raw, approved []byte
		var mode string
		if err = rows.Scan(&task.ItemID, &task.Attempt, &task.PlanSHA, &raw, &approved, &mode, &task.ExpiresAt); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &task.Item); err != nil {
			return nil, err
		}
		if mode == "enabled" {
			task.ApprovedProof = approved
		} else if mode != "dry_run" {
			continue
		}
		task.ExpiresAt = task.ExpiresAt.UTC()
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func validCleanupProof(raw []byte, i nasCleanupItem) bool {
	var p struct {
		Root          [2]int64   `json:"root"`
		Ancestors     [][2]int64 `json:"ancestors"`
		Identity      [5]int64   `json:"identity"`
		SHA           string     `json:"sha256"`
		ParentMembers string     `json:"parent_members_sha256"`
		Sidecar       *struct {
			Identity [5]int64                   `json:"identity"`
			SHA      string                     `json:"sha256"`
			Binding  map[string]json.RawMessage `json:"binding"`
		} `json:"sidecar"`
	}
	if len(raw) > 16384 || json.Unmarshal(raw, &p) != nil || p.Root[0] <= 0 || p.Root[1] <= 0 || len(p.Ancestors) != len(strings.Split(i.RelativePath, "/"))-1 || p.Identity != i.Frozen || p.Identity[4] != p.Root[0] || p.SHA != i.SHA256 || len(p.ParentMembers) != 64 || !lowerHex(p.ParentMembers) {
		return false
	}
	for _, a := range p.Ancestors {
		if a[0] != p.Root[0] || a[1] <= 0 {
			return false
		}
	}
	if i.SidecarRequired {
		if p.Sidecar == nil || p.Sidecar.Identity != i.FrozenSidecar || len(p.Sidecar.SHA) != 64 || !lowerHex(p.Sidecar.SHA) {
			return false
		}
		want := map[string]any{"clip_id": i.SourceID, "recording_id": i.RecordingID, "relative_path": i.RelativePath, "size_bytes": i.SizeBytes, "sha256": i.SHA256}
		for field, expected := range map[string]time.Time{"clip_start_at": i.StartAt, "clip_end_at": i.EndAt} {
			var raw string
			if json.Unmarshal(p.Sidecar.Binding[field], &raw) != nil {
				return false
			}
			actual, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil || !actual.Equal(expected) {
				return false
			}
		}
		for k, v := range want {
			b, _ := json.Marshal(v)
			if !bytes.Equal(p.Sidecar.Binding[k], b) {
				return false
			}
		}
	} else if p.Sidecar != nil {
		return false
	}
	return true
}

// cleanupMayIssue is the only path to a deletion permit. A permit requires an
// enabled, unexpired plan, the backend master switch, the exact dry-run file
// proof and a fresh streamed-SHA-verified R2 recovery copy. nas_only_at_check
// (and every other recovery state) never yields a permit; see
// handleAdminNASCleanupControl before ever changing that.
func cleanupMayIssue(mode string, master bool, until *time.Time, sameFile bool, recovery string) bool {
	return mode == "enabled" && master && until != nil && until.After(time.Now()) && sameFile && cleanupRecoveryPermits(mode, recovery)
}

// Media bytes of every item whose deletion permit was issued in the last 24h.
// updated_at only moves forward, so later receipts can only keep an item in
// the window longer (fail closed).
// The write-once item size avoids detoasting the plan manifest per issued
// item; legacy rows (NULL) still read it from the manifest.
const cleanupIssuedBytes24hSQL = `SELECT coalesce(sum(coalesce(i.size_bytes,(p.manifest->(i.ordinal-1)->>'size_bytes')::bigint)),0) FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE i.delete_ever_issued AND i.updated_at>now()-interval '24 hours'`

// cleanupBudgetAllows takes the budget lock for the rest of tx and reports
// whether issuing one more permit of size bytes stays within the rolling cap.
func cleanupBudgetAllows(ctx context.Context, tx pgx.Tx, size int64) (bool, error) {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupBudgetLock); err != nil {
		return false, err
	}
	var issued int64
	if err := tx.QueryRow(ctx, cleanupIssuedBytes24hSQL).Scan(&issued); err != nil {
		return false, err
	}
	return size > 0 && issued+size <= nasCleanupMaxIssuedBytes24h, nil
}

// Auto-pause uses the same state transition as an operator pause.
const cleanupAutoPauseSet = `mode='paused',approved_until=NULL,approval_reason=left('auto-paused: '||$2::text,500)`

// cleanupRecovery is the full recovery check (see cleanupRecoveryFull).
func (s *Server) cleanupRecovery(ctx context.Context, i nasCleanupItem) string {
	recovery, _ := s.cleanupRecoveryFull(ctx, i)
	return recovery
}

// Share the immutable plan/mode fence across independent preflights; control
// still requires an exclusive parent lock and cannot overtake observations.
// New plans read their write-once item copy; legacy rows fall back to the
// manifest (COALESCE is lazy, so the manifest is only detoasted for them).
const cleanupObservationLoadSQL = `SELECT coalesce(i.item,p.manifest->(i.ordinal-1)),p.mode,i.state,p.plan_sha256,p.approved_until,i.attempt,i.lease_expires_at,i.proof,i.plan_id,i.outcome,p.report_sha256,i.recovery_evidence,p.execution_rankings,i.permit_expires_at,i.recovery FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE i.id=$1 AND p.connection_id=$2 FOR UPDATE OF i FOR SHARE OF p`

// cleanupObserveRequestError validates one observation; "" means valid.
func cleanupObserveRequestError(req cleanupObserve) string {
	if req.Attempt < 1 || len(req.PlanSHA) != 64 || !lowerHex(req.PlanSHA) || len(req.Error) > 500 || len(req.ClientVersion) > 64 {
		return "invalid cleanup observation"
	}
	if req.RequestID != "" && (len(req.RequestID) != 32 || !lowerHex(req.RequestID)) {
		return "invalid cleanup observation request id"
	}
	if req.MD5 != "" && (len(req.MD5) != 32 || !lowerHex(req.MD5)) {
		return "invalid cleanup observation md5"
	}
	if len(req.Timings) > 3 {
		return "invalid cleanup timings"
	}
	for name, value := range req.Timings {
		if (name != "namespace_ms" && name != "hash_ms" && name != "proof_ms") || value < 0 || value > 1800000 {
			return "invalid cleanup timings"
		}
	}
	if req.Outcome != "match" && req.Outcome != "missing" && req.Outcome != "conflict" && req.Outcome != "unverified" {
		return "invalid cleanup outcome"
	}
	return ""
}

func (s *Server) handleAccountNASCleanupObserve(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "itemId")
	if !ok {
		return
	}
	var req cleanupObserve
	if util.DecodeJSON(r, &req) != nil {
		util.WriteError(w, 400, "invalid cleanup observation")
		return
	}
	if msg := cleanupObserveRequestError(req); msg != "" {
		util.WriteError(w, 400, msg)
		return
	}
	ctx := r.Context()
	cid, ok := s.nasRestoreConnection(ctx, w, r)
	if !ok {
		return
	}
	// R2 work happens before the transaction (no locks held across it).
	env := &cleanupObserveEnv{}
	if req.Outcome == "match" {
		env.prefetched = s.cleanupPrefetchRecovery(ctx, cid, map[int64]string{id: req.MD5})
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "cleanup observation begin failed")
		return
	}
	defer tx.Rollback(ctx)
	result := s.cleanupObserveOne(ctx, tx, cid, id, req, env)
	if result.status == 200 && result.write && tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "cleanup observation save failed")
		return
	}
	if result.status == 503 {
		w.Header().Set("Retry-After", "1")
	}
	util.WriteJSON(w, result.status, result.body)
}

type cleanupObserveResult struct {
	status int
	body   map[string]any
	// write is true when the observation changed state and must be committed.
	write bool
}

func cleanupObserveError(status int, msg string) cleanupObserveResult {
	return cleanupObserveResult{status: status, body: map[string]any{"error": msg}}
}

// cleanupObserveOne records one exact-file observation inside tx and decides
// whether it earns a single-use deletion permit. Both the single-item and the
// batched endpoint use it; a batch runs each call in its own savepoint.
func (s *Server) cleanupObserveOne(ctx context.Context, tx pgx.Tx, cid, id int64, req cleanupObserve, env *cleanupObserveEnv) cleanupObserveResult {
	var raw, oldProof, oldOutcome, evidence, rankings []byte
	var mode, state, sha, binding, oldRecovery string
	var until, permitUntil *time.Time
	var attempt int
	var lease *time.Time
	var planID, fencedPlan int64
	// Enabled-plan observations may UPDATE the plan row (auto-pause). Serialize
	// them per plan with an advisory lock taken before any row lock, so two
	// concurrent observations never both hold FOR SHARE and then wait on each
	// other's UPDATE. Dry-run observations stay fully parallel.
	var preMode string
	if err := tx.QueryRow(ctx, `SELECT i.plan_id,p.mode FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE i.id=$1 AND p.connection_id=$2`, id, cid).Scan(&fencedPlan, &preMode); err != nil {
		return cleanupObserveError(409, "cleanup task identity differs")
	}
	if preMode == "enabled" && env.budgetHeld && !env.lockedPlans[fencedPlan] {
		// Lock order is always plan fence(s) before the budget lock. A batch that
		// already holds the budget lock must not take a plan fence it did not
		// take up front (the plan became enabled meanwhile): retry instead.
		return cleanupObserveResult{status: 503, body: map[string]any{"error": "cleanup plan mode changed; retry", "retryable": true}}
	}
	if preMode == "enabled" {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPlanLockBase+fencedPlan); err != nil {
			return cleanupObserveError(500, "cleanup plan execution fence failed")
		}
	} else {
		fencedPlan = 0
	}
	err := tx.QueryRow(ctx, cleanupObservationLoadSQL, id, cid).Scan(&raw, &mode, &state, &sha, &until, &attempt, &lease, &oldProof, &planID, &oldOutcome, &binding, &evidence, &rankings, &permitUntil, &oldRecovery)
	if err != nil || sha != req.PlanSHA || attempt != req.Attempt {
		return cleanupObserveError(409, "cleanup task identity differs")
	}
	if mode == "enabled" && fencedPlan != planID {
		// The plan was enabled between the two reads; retry under the fence.
		return cleanupObserveResult{status: 503, body: map[string]any{"error": "cleanup plan mode changed; retry", "retryable": true}}
	}
	var item nasCleanupItem
	if json.Unmarshal(raw, &item) != nil {
		return cleanupObserveError(500, "invalid cleanup manifest")
	}
	// A repeated dry-run observation is idempotent; a delete permit is never
	// reissued after a lost response. Such a task requires operator reconciliation.
	if state != "leased" {
		retryBody, _ := json.Marshal(req)
		// The exact same request (same request_id and body) whose response was
		// lost gets back the one permit already issued, with its original
		// expiry; nothing is extended or issued again.
		// It is only handed back while the plan is still enabled, its approval is
		// unexpired and the master switch is on; otherwise the caller gets the
		// non-deleting reconcile answer below.
		if state == "delete_issued" && req.RequestID != "" && req.Outcome == "match" && permitUntil != nil && permitUntil.After(time.Now()) && nasSpaceAuditJSONEqual(oldOutcome, retryBody) &&
			mode == "enabled" && until != nil && until.After(time.Now()) && s.stitchDeletionMaster(item) && (item.PermitReason == "" || s.stitchInclusionCurrent(ctx, tx, item) == nil) {
			return cleanupObserveResult{status: 200, body: map[string]any{"action": "delete_issued", "delete": true, "item_id": id, "attempt": attempt, "plan_sha256": sha, "proof_sha256": cleanupSHA(req.Proof), "recovery": oldRecovery, "expires_at": permitUntil.UTC(), "plan_paused": false, "replayed": true}}
		}
		if state != "delete_issued" && state != "deleted" && nasSpaceAuditJSONEqual(oldOutcome, retryBody) {
			return cleanupObserveResult{status: 200, body: map[string]any{"action": state, "delete": false, "item_id": id, "attempt": attempt, "plan_sha256": sha, "proof_sha256": cleanupSHA(req.Proof)}}
		}
		if state == "delete_issued" && req.Outcome == "match" && nasSpaceAuditJSONEqual(oldProof, req.Proof) {
			return cleanupObserveResult{status: 200, body: map[string]any{"action": "reconcile", "delete": false, "item_id": id, "attempt": attempt, "plan_sha256": sha, "proof_sha256": cleanupSHA(req.Proof)}}
		}
		if state == "would_delete" && req.Outcome == "match" && nasSpaceAuditJSONEqual(oldProof, req.Proof) {
			return cleanupObserveResult{status: 200, body: map[string]any{"action": "would_delete", "delete": false, "item_id": id, "attempt": attempt, "plan_sha256": sha, "proof_sha256": cleanupSHA(req.Proof)}}
		}
		return cleanupObserveError(409, "cleanup observation is stale or terminal")
	}
	if lease == nil || lease.Before(time.Now()) {
		return cleanupObserveError(409, "cleanup lease expired")
	}
	stageTimings := map[string]any{"client": req.Timings}
	policyReason := ""
	action := req.Outcome
	recovery := "unchecked"
	var recoveryEvidence *cleanupRecoveryEvidence
	recoveryAnomaly := false
	deletion := false
	if req.Outcome == "match" {
		if !validCleanupProof(req.Proof, item) {
			return cleanupObserveError(400, "complete exact physical cleanup proof required")
		}
		var bucket string
		if s.r2 != nil {
			bucket = s.r2.Bucket()
		}
		current, e := cleanupCatalogItem(ctx, tx, item.Kind, item.SourceID, bucket)
		a, _ := json.Marshal(current)
		bitem := item
		bitem.Frozen = [5]int64{}
		bitem.FrozenSidecar = [5]int64{}
		bitem.PermitReason = ""
		bitem.StitchTaskID = 0
		bitem.StitchOutputPath = ""
		bitem.StitchOutputSHA = ""
		bitem.StitchOutputSize = 0
		b, _ := json.Marshal(bitem)
		if binding != cleanupPolicySHA || mode == "retired" {
			action = "keep"
			policyReason = "plan bound to a superseded cleanup policy or retired"
		} else if e != nil || !bytes.Equal(a, b) {
			action = "conflict"
		} else {
			policyStarted := time.Now()
			e = s.cleanupCurrentPolicyCheck(ctx, tx, item, mode == "dry_run", stageTimings, env.rankingsUnchanged(ctx, tx, mode, rankings))
			stageTimings["policy_ms"] = time.Since(policyStarted).Milliseconds()
			if e != nil {
				action = "keep"
				policyReason = e.Error()
			} else {
				pre, ok := s.cleanupRecoveryFor(id, item, mode, evidence, req.MD5, env)
				if !ok {
					// Never do R2 I/O while holding observation locks.
					return cleanupObserveResult{status: 503, body: map[string]any{"error": "recovery check not ready; retry", "retryable": true}}
				}
				recovery, recoveryEvidence = pre.recovery, pre.out
				stageTimings["recovery_mode"] = pre.method
				action = "would_delete"
				if recovery == "conflict" {
					action = "conflict"
				}
				if pre.anomaly {
					recoveryAnomaly = true
					policyReason = "R2 MD5/ETag match disagreed with the full SHA-256 audit"
				}
				if cleanupMayIssue(mode, s.stitchDeletionMaster(item), until, cleanupSameFileProof(oldProof, req.Proof), recovery) {
					allowed, e := env.budgetAllows(ctx, tx, item.SizeBytes)
					if e != nil {
						return cleanupObserveError(500, "cleanup deletion budget unavailable")
					}
					if allowed {
						action = "delete_issued"
						deletion = true
					} else {
						policyReason = "rolling 24h deletion budget reached; KEEP"
					}
				}
			}
		}
	}
	proof := any(nil)
	if len(req.Proof) > 0 {
		proof = string(req.Proof)
	}
	var evidenceJSON any
	if recoveryEvidence != nil {
		b, _ := json.Marshal(recoveryEvidence)
		evidenceJSON = string(b)
	}
	expires := time.Now().UTC().Add(30 * time.Second).Truncate(time.Microsecond)
	observed, _ := json.Marshal(req)
	stages, _ := json.Marshal(stageTimings)
	// Dry runs replace the recovery evidence (or clear it when the copy was not
	// verified); executions only ever add evidence from a full verification.
	_, err = tx.Exec(ctx, `UPDATE nas_cleanup_items SET state=$2,delete_ever_issued=delete_ever_issued OR $2='delete_issued',lease_expires_at=NULL,proof=$3::jsonb,proof_sha256=$4,recovery=$5,outcome=$6::jsonb,policy_reason=$7,stage_timings=$8::jsonb,
 recovery_evidence=CASE WHEN $9::boolean THEN $10::jsonb ELSE coalesce($10::jsonb,recovery_evidence) END,
 permit_expires_at=CASE WHEN $2='delete_issued' THEN $11::timestamptz ELSE permit_expires_at END,updated_at=now() WHERE id=$1`,
		id, action, proof, cleanupSHA(req.Proof), recovery, string(observed), policyReason, string(stages), mode == "dry_run", evidenceJSON, expires)
	// During execution every healthy observation becomes a permit. Anything
	// else (unverified, conflict, missing, client error, policy or recovery
	// mismatch, budget) pauses the whole plan in the same transaction.
	paused := false
	if err == nil && mode == "enabled" && action != "delete_issued" {
		reason := "item " + strconv.FormatInt(id, 10) + " observed " + action
		if req.Error != "" {
			reason += ": " + req.Error
		} else if policyReason != "" {
			reason += ": " + policyReason
		}
		_, err = tx.Exec(ctx, `UPDATE nas_cleanup_plans SET `+cleanupAutoPauseSet+` WHERE id=$1 AND mode='enabled'`, planID, reason)
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE nas_cleanup_batches SET `+cleanupAutoPauseSet+` WHERE mode='enabled' AND $1=ANY(plan_ids)`, planID, reason)
		}
		paused = err == nil
	}
	if err == nil && recoveryAnomaly && mode == "dry_run" {
		// An audit disagreement stops the dry run of the whole plan too.
		reason := "item " + strconv.FormatInt(id, 10) + ": " + policyReason
		_, err = tx.Exec(ctx, `UPDATE nas_cleanup_plans SET `+cleanupAutoPauseSet+` WHERE id=$1 AND mode='dry_run'`, planID, reason)
		paused = err == nil
	}
	if err != nil {
		return cleanupObserveError(500, "cleanup observation save failed")
	}
	return cleanupObserveResult{status: 200, write: true, body: map[string]any{"action": action, "delete": deletion, "item_id": id, "attempt": attempt, "plan_sha256": sha, "proof_sha256": cleanupSHA(req.Proof), "recovery": recovery, "expires_at": expires, "plan_paused": paused}}
}

type cleanupReceipt struct {
	ItemID   int64  `json:"item_id,omitempty"`
	Attempt  int    `json:"attempt"`
	PlanSHA  string `json:"plan_sha256"`
	ProofSHA string `json:"proof_sha256"`
	Outcome  string `json:"outcome"`
	Error    string `json:"error"`
}

// media_deleted_sidecar_failed is the honest receipt for a media unlink whose
// sidecar step then failed (clients from 2026-11 on).
func validCleanupReceipt(req cleanupReceipt) bool {
	return req.Attempt >= 1 && len(req.PlanSHA) == 64 && len(req.ProofSHA) == 64 && (req.Outcome == "deleted" || req.Outcome == "unverified" || req.Outcome == "media_deleted_sidecar_failed") && len(req.Error) <= 500
}

// cleanupApplyReceipt records one deletion receipt. Any receipt other than a
// clean deletion pauses its enabled plan (and its batch) in the same
// statement. A replay of the identical receipt is idempotent.
func cleanupApplyReceipt(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, cid, id int64, req cleanupReceipt) (updated int, paused bool, planID int64, err error) {
	req.ItemID = 0
	body, _ := json.Marshal(req)
	var pausedPlans int
	var plan *int64
	err = q.QueryRow(ctx, `WITH u AS (UPDATE nas_cleanup_items i SET state=$6,outcome=$7::jsonb,updated_at=now() FROM nas_cleanup_plans p WHERE i.id=$1 AND p.id=i.plan_id AND p.connection_id=$2 AND p.plan_sha256=$3 AND i.attempt=$4 AND i.proof_sha256=$5 AND (i.state='delete_issued' OR (i.state=$6 AND i.outcome=$7::jsonb)) RETURNING i.plan_id),
pz AS (UPDATE nas_cleanup_plans SET mode='paused',approved_until=NULL,approval_reason=left('auto-paused: '||$8::text,500) WHERE id IN (SELECT plan_id FROM u) AND mode='enabled' AND $6<>'deleted' RETURNING id),
bz AS (UPDATE nas_cleanup_batches SET mode='paused',approved_until=NULL,approval_reason=left('auto-paused: '||$8::text,500) WHERE mode='enabled' AND $6<>'deleted' AND plan_ids && ARRAY(SELECT plan_id FROM u) RETURNING id)
SELECT (SELECT count(*) FROM u)::int, (SELECT count(*) FROM pz)::int+(SELECT count(*) FROM bz)::int, (SELECT min(plan_id) FROM u)`, id, cid, req.PlanSHA, req.Attempt, req.ProofSHA, req.Outcome, string(body), "item "+strconv.FormatInt(id, 10)+" deletion receipt "+req.Outcome).Scan(&updated, &pausedPlans, &plan)
	if plan != nil {
		planID = *plan
	}
	return updated, pausedPlans > 0, planID, err
}

// Echo only after the exact item/attempt/plan/proof receipt was accepted.
func cleanupReceiptAck(id int64, req cleanupReceipt, paused bool) map[string]any {
	return map[string]any{"ok": true, "item_id": id, "attempt": req.Attempt, "plan_sha256": req.PlanSHA,
		"proof_sha256": req.ProofSHA, "state": req.Outcome, "plan_paused": paused}
}

func (s *Server) handleAccountNASCleanupResult(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "itemId")
	if !ok {
		return
	}
	var req cleanupReceipt
	if util.DecodeJSON(r, &req) != nil || req.ItemID != 0 || !validCleanupReceipt(req) {
		util.WriteError(w, 400, "invalid cleanup deletion receipt")
		return
	}
	cid, ok := s.nasRestoreConnection(r.Context(), w, r)
	if !ok {
		return
	}
	updated, paused, planID, err := cleanupApplyReceipt(r.Context(), s.pool, cid, id, req)
	if err != nil || updated != 1 {
		util.WriteError(w, 409, "cleanup receipt differs or task is not deletion-issued")
		return
	}
	if req.Outcome == "deleted" {
		s.cleanupAdvanceBatchAfter(r.Context(), planID)
	}
	util.WriteJSON(w, 200, cleanupReceiptAck(id, req, paused))
}

// Each file is the destructive scope. Sibling membership may change after a
// separately receipted deletion, but the current parent snapshot must remain
// stable across this file's own hash/permit/unlink interval on the NAS.
func cleanupSameFileProof(a, b []byte) bool {
	var x, y map[string]json.RawMessage
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	delete(x, "parent_members_sha256")
	delete(y, "parent_members_sha256")
	aa, _ := json.Marshal(x)
	bb, _ := json.Marshal(y)
	return nasSpaceAuditJSONEqual(aa, bb)
}

func persistNASCleanupPlan(ctx context.Context, tx pgx.Tx, label string, items []nasCleanupItem) (int64, string, error) {
	stage := "admission_fence"
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupAdmissionLock); err != nil {
		return 0, "", &cleanupPersistFailure{stage: stage, err: err}
	}
	// Same-label retries still return the immutable original plan. A different
	// label must never admit an issued file, even from an old policy or retired
	// plan. Missing legacy item rows reserve the identity conservatively.
	manifest, _ := json.Marshal(items)
	var reserved bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(
 SELECT 1 FROM nas_cleanup_reserved_files f
 JOIN jsonb_to_recordset($1::jsonb) AS requested(kind text,source_id bigint)
 ON f.kind=requested.kind AND f.source_id=requested.source_id
 WHERE f.account_id=47 AND f.connection_id=13 AND f.label<>$2)`, string(manifest), label).Scan(&reserved); err != nil {
		return 0, "", &cleanupPersistFailure{stage: "file_reservation", err: err}
	}
	if reserved {
		return 0, "", &cleanupPersistFailure{stage: "file_reservation", err: errors.New("cleanup file already reserved or deletion ever issued")}
	}
	stage = "plan_insert"
	sha := cleanupSHA(manifest)
	var id int64
	err := tx.QueryRow(ctx, `INSERT INTO nas_cleanup_plans(account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES(47,13,$1,$2,$3,$4) ON CONFLICT(label) DO NOTHING RETURNING id`, label, cleanupPolicySHA, string(manifest), sha).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		var old string
		stage = "plan_collision"
		var oldBinding string
		err = tx.QueryRow(ctx, `SELECT id,plan_sha256,report_sha256 FROM nas_cleanup_plans WHERE label=$1`, label).Scan(&id, &old, &oldBinding)
		if err == nil && (old != sha || oldBinding != cleanupPolicySHA) {
			err = errors.New("label already has a different plan")
		}
	} else if err == nil {
		stage = "item_insert"
		// item/size_bytes are copied from the manifest stored in this very
		// transaction (never from request input); both are write-once.
		var inserted int
		err = tx.QueryRow(ctx, `WITH ins AS (INSERT INTO nas_cleanup_items(plan_id,ordinal,item,size_bytes) SELECT p.id,e.n::integer,e.value,CASE WHEN (e.value->>'size_bytes')::bigint>0 THEN (e.value->>'size_bytes')::bigint END FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n) WHERE p.id=$1 RETURNING 1) SELECT count(*) FROM ins`, id).Scan(&inserted)
		if err == nil && inserted != len(items) {
			err = errors.New("cleanup item rows differ from the manifest")
		}
	}
	if err == nil {
		for _, item := range items {
			// Ordered source locks serialize admission against old purge binaries.
			stage = "source_lock"
			rows, e := tx.Query(ctx, `SELECT id FROM recording_clips WHERE id=ANY($1) ORDER BY id FOR UPDATE`, item.SourceIDs)
			if e != nil {
				err = e
				break
			}
			for rows.Next() {
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				err = e
				break
			}
			stage = "source_hold"
			_, err = tx.Exec(ctx, `INSERT INTO nas_cleanup_source_holds(plan_id,clip_id) SELECT $1,unnest($2::bigint[]) ON CONFLICT DO NOTHING`, id, item.SourceIDs)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		return id, sha, &cleanupPersistFailure{stage: stage, err: err}
	}
	return id, sha, nil
}

type cleanupPersistFailure struct {
	stage string
	err   error
}

func (e *cleanupPersistFailure) Error() string { return e.stage + ": " + e.err.Error() }
func (e *cleanupPersistFailure) Unwrap() error { return e.err }
