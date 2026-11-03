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
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// This first cleanup policy is pinned to the independently reviewed report.
// A different selection requires a new reviewed report, never a wider prefix.
// Admission queues immutable proposals, not destructive grants. It spans audit
// I/O while connection heartbeats keep updating the FK parent. READ COMMITTED
// avoids false40001 failures; source locks and grant-time identity/policy checks
// still fence every destructive operation.
var nasCleanupAdmissionOptions = pgx.TxOptions{IsoLevel: pgx.ReadCommitted}

const nasCleanupReportSHA = "64eee0bc589c9be4dcab5b5ea7668270291a07ce4fd0db98dcd0a484e19d84c1"
const nasCleanupReportKey = "staging/best14-20261021/nas-retention-20261025/current-folder-preparation-20261031/" + nasCleanupReportSHA + "/retention-only-folder-report-109.json.gz"

type nasCleanupItem struct {
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
func (p nasCleanupReport) permits(i nasCleanupItem) bool {
	for _, n := range p.Nodes {
		if n.Path == path.Dir(i.RelativePath) && n.Action == "RETENTION_DELETE_CLEARED" && len(n.Owners) == 1 && n.Owners[0] == i.StreamID {
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
	if util.DecodeJSON(r, &req) != nil || !relayArtifactName.MatchString(req.Label) || len(req.Label) > 80 || len(req.Items) < 1 || len(req.Items) > 500 {
		util.WriteError(w, 400, "label and1to500 exact source identities required")
		return
	}
	ctx := r.Context()
	report, err := s.cleanupReport(ctx)
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
	items := []nasCleanupItem{}
	seen := map[string]bool{}
	for _, sel := range req.Items {
		i, e := cleanupCatalogItem(ctx, tx, sel.Kind, sel.SourceID, s.r2.Bucket())
		if e != nil || !report.permits(i) || seen[i.RelativePath] {
			util.WriteError(w, 409, "source identity is unknown, duplicated or outside reviewed DELETE nodes")
			return
		}
		seen[i.RelativePath] = true
		items = append(items, i)
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
	var label, sha, mode, reason string
	var priority int
	var allow bool
	var until *time.Time
	err := s.pool.QueryRow(r.Context(), `SELECT label,plan_sha256,mode,priority,allow_nas_only,approval_reason,approved_until,manifest,(SELECT coalesce(jsonb_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM nas_cleanup_items WHERE plan_id=p.id GROUP BY state) q),(SELECT coalesce(jsonb_agg(jsonb_build_object('item_id',id,'ordinal',ordinal,'state',state,'recovery',recovery,'policy_reason',policy_reason,'attempt',attempt,'proof',proof,'outcome',outcome,'delete_ever_issued',delete_ever_issued,'stage_timings',stage_timings,'updated_at',updated_at) ORDER BY ordinal),'[]') FROM nas_cleanup_items WHERE plan_id=p.id) FROM nas_cleanup_plans p WHERE id=$1 AND account_id=47 AND connection_id=13`, id).Scan(&label, &sha, &mode, &priority, &allow, &reason, &until, &manifest, &jsonStates, &results)
	if err != nil {
		util.WriteError(w, 404, "cleanup plan unavailable")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "label": label, "plan_sha256": sha, "mode": mode, "allow_nas_only": allow, "approval_reason": reason, "approved_until": until, "priority": priority, "items": json.RawMessage(manifest), "states": json.RawMessage(jsonStates), "master_deletion_enabled": s.cfg.NASCleanupDeletionEnabled, "results": json.RawMessage(results)})
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
	}
	if util.DecodeJSON(r, &req) != nil || len(req.PlanSHA) != 64 || !lowerHex(req.PlanSHA) || (req.Mode != "enabled" && req.Mode != "paused" && req.Mode != "dry_run") || len(req.Reason) < 1 || len(req.Reason) > 500 {
		util.WriteError(w, 400, "exact plan digest,mode and reason required")
		return
	}
	if (req.Priority != nil && (req.Mode != "dry_run" || *req.Priority < -100 || *req.Priority > 100)) || (req.Mode == "dry_run" && req.AllowNASOnly) {
		util.WriteError(w, 400, "bounded priority is read-only; NAS-only permission cannot accompany dry-run resume")
		return
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
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7343226011101)`); err != nil {
			util.WriteError(w, 500, "public cleanup fence failed")
			return
		}
	}
	var current, currentMode string
	if err = tx.QueryRow(ctx, `SELECT plan_sha256,mode FROM nas_cleanup_plans WHERE id=$1 AND account_id=47 AND connection_id=13 FOR UPDATE`, id).Scan(&current, &currentMode); err != nil || current != req.PlanSHA {
		util.WriteError(w, 409, "plan identity differs")
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
		var bad int
		err = tx.QueryRow(ctx, `SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND (state<>'would_delete' OR proof IS NULL OR updated_at<now()-interval '24 hours' OR (recovery<>'r2_sha_verified' AND NOT ($2 AND recovery='nas_only_at_check')))`, id, req.AllowNASOnly).Scan(&bad)
		if err != nil || bad != 0 {
			util.WriteError(w, 409, "fresh complete dry run and chosen recovery policy required")
			return
		}
	}
	_, err = tx.Exec(ctx, `UPDATE nas_cleanup_plans SET mode=$2,allow_nas_only=$3,approval_reason=$4,approved_until=CASE WHEN $2='enabled' THEN now()+interval '1 hour' END,priority=coalesce($5,priority) WHERE id=$1`, id, req.Mode, req.AllowNASOnly, req.Reason, req.Priority)
	if err == nil && req.Mode == "enabled" {
		_, err = tx.Exec(ctx, `UPDATE nas_cleanup_items SET state='pending',lease_expires_at=NULL WHERE plan_id=$1`, id)
	}
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "cleanup control failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "mode": req.Mode, "plan_sha256": current})
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
}

// Plans are ordered without sorting/locking the entire pending file backlog.
// The lateral probe uses nas_cleanup_pending and skips locked rows before the
// outer LIMIT; it can keep looking in the next eligible plan if one is busy.
const cleanupLeaseLoadSQL = `SELECT i.id,i.attempt,p.plan_sha256,p.manifest->(i.ordinal-1),i.proof,p.mode
FROM nas_cleanup_plans p
JOIN LATERAL (
 SELECT candidate.* FROM nas_cleanup_items candidate
 WHERE candidate.plan_id=p.id AND candidate.state='pending'
 ORDER BY candidate.id LIMIT 1 FOR UPDATE SKIP LOCKED
) i ON true
WHERE p.connection_id=$1
 AND (p.mode='dry_run' OR (p.mode='enabled' AND p.approved_until>now() AND $2))
ORDER BY p.priority DESC,p.id LIMIT 1`

func (s *Server) handleAccountNASCleanupLease(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientVersion string `json:"client_version"`
		Protocol      int    `json:"protocol"`
		Limit         int    `json:"limit"`
	}
	if util.DecodeJSON(r, &req) != nil || req.Protocol != 1 || len(req.ClientVersion) == 0 || len(req.ClientVersion) > 64 || req.Limit < 0 || req.Limit > 8 {
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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "cleanup lease transaction failed")
		return
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `UPDATE nas_cleanup_items i SET state=CASE WHEN attempt>=3 THEN 'unverified' ELSE 'pending' END,lease_expires_at=NULL,updated_at=now() FROM nas_cleanup_plans p WHERE p.id=i.plan_id AND p.connection_id=$1 AND i.state='leased' AND i.lease_expires_at<now()`, cid)
	if err != nil {
		util.WriteError(w, 500, "cleanup lease expiry failed")
		return
	}
	tasks := make([]cleanupTask, 0, req.Limit)
	for len(tasks) < req.Limit {
		var task cleanupTask
		var raw, approved []byte
		var mode string
		err = tx.QueryRow(ctx, cleanupLeaseLoadSQL, cid, s.cfg.NASCleanupDeletionEnabled).Scan(&task.ItemID, &task.Attempt, &task.PlanSHA, &raw, &approved, &mode)
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil || json.Unmarshal(raw, &task.Item) != nil {
			util.WriteError(w, 500, "cleanup lease load failed")
			return
		}
		// An execution lease is always alone. Leave it pending if this response
		// already contains dry-run work; the client drains that work first.
		if mode == "enabled" && len(tasks) != 0 {
			break
		}
		task.Attempt++
		task.ExpiresAt = time.Now().UTC().Add(30 * time.Minute)
		if mode == "enabled" {
			task.ApprovedProof = approved
		}
		_, err = tx.Exec(ctx, `UPDATE nas_cleanup_items SET state='leased',attempt=$2,lease_expires_at=$3,updated_at=now() WHERE id=$1`, task.ItemID, task.Attempt, task.ExpiresAt)
		if err != nil {
			util.WriteError(w, 500, "cleanup lease save failed")
			return
		}
		tasks = append(tasks, task)
		if mode == "enabled" {
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
	util.WriteJSON(w, 200, map[string]any{"tasks": tasks, "retry_after_sec": retry})
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
func (s *Server) cleanupRecovery(ctx context.Context, i nasCleanupItem) string {
	if s.r2 == nil {
		return "unchecked"
	}
	head, err := s.r2.Head(ctx, i.ObjectKey)
	if err != nil {
		// Only a proven404 is NAS-only. Authorization/timeouts never prove absence.
		if r2.IsNotFound(err) {
			return "nas_only_at_check"
		}
		return "unchecked"
	}
	if head.SizeBytes != i.SizeBytes {
		return "conflict"
	}
	// Bounded streaming verification. No media is saved on the backend or Jelly.
	c, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	source, err := s.r2.Open(c, i.ObjectKey)
	if err != nil {
		return "r2_present_unverified"
	}
	defer source.Close()
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(source, i.SizeBytes+1))
	if err != nil || n != i.SizeBytes {
		return "r2_present_unverified"
	}
	if hex.EncodeToString(hash.Sum(nil)) != i.SHA256 {
		return "conflict"
	}
	return "r2_sha_verified"
}

// Share the immutable plan/mode fence across independent preflights; control
// still requires an exclusive parent lock and cannot overtake observations.
const cleanupObservationLoadSQL = `SELECT p.manifest->(i.ordinal-1),p.mode,i.state,p.plan_sha256,p.approved_until,i.attempt,i.lease_expires_at,i.proof,p.allow_nas_only,i.outcome FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE i.id=$1 AND p.connection_id=$2 FOR UPDATE OF i FOR SHARE OF p`

func (s *Server) handleAccountNASCleanupObserve(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "itemId")
	if !ok {
		return
	}
	var req cleanupObserve
	if util.DecodeJSON(r, &req) != nil || req.Attempt < 1 || len(req.PlanSHA) != 64 || !lowerHex(req.PlanSHA) || len(req.Error) > 500 || len(req.ClientVersion) > 64 {
		util.WriteError(w, 400, "invalid cleanup observation")
		return
	}
	if len(req.Timings) > 3 {
		util.WriteError(w, 400, "invalid cleanup timings")
		return
	}
	for name, value := range req.Timings {
		if (name != "namespace_ms" && name != "hash_ms" && name != "proof_ms") || value < 0 || value > 1800000 {
			util.WriteError(w, 400, "invalid cleanup timings")
			return
		}
	}
	if req.Outcome != "match" && req.Outcome != "missing" && req.Outcome != "conflict" && req.Outcome != "unverified" {
		util.WriteError(w, 400, "invalid cleanup outcome")
		return
	}
	ctx := r.Context()
	cid, ok := s.nasRestoreConnection(ctx, w, r)
	if !ok {
		return
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "cleanup observation begin failed")
		return
	}
	defer tx.Rollback(ctx)
	var raw, oldProof, oldOutcome []byte
	var mode, state, sha string
	var until *time.Time
	var attempt int
	var lease *time.Time
	var allow bool
	err = tx.QueryRow(ctx, cleanupObservationLoadSQL, id, cid).Scan(&raw, &mode, &state, &sha, &until, &attempt, &lease, &oldProof, &allow, &oldOutcome)
	if err != nil || sha != req.PlanSHA || attempt != req.Attempt {
		util.WriteError(w, 409, "cleanup task identity differs")
		return
	}
	// A repeated dry-run observation is idempotent; a delete permit is never
	// reissued after a lost response. Such a task requires operator reconciliation.
	if state != "leased" {
		retryBody, _ := json.Marshal(req)
		if state != "delete_issued" && state != "deleted" && nasSpaceAuditJSONEqual(oldOutcome, retryBody) {
			util.WriteJSON(w, 200, map[string]any{"action": state, "delete": false})
			return
		}
		if state == "delete_issued" && req.Outcome == "match" && nasSpaceAuditJSONEqual(oldProof, req.Proof) {
			util.WriteJSON(w, 200, map[string]any{"action": "reconcile", "delete": false, "proof_sha256": cleanupSHA(req.Proof)})
			return
		}
		if state == "would_delete" && req.Outcome == "match" && nasSpaceAuditJSONEqual(oldProof, req.Proof) {
			util.WriteJSON(w, 200, map[string]any{"action": "would_delete", "delete": false})
			return
		}
		util.WriteError(w, 409, "cleanup observation is stale or terminal")
		return
	}
	if lease == nil || lease.Before(time.Now()) {
		util.WriteError(w, 409, "cleanup lease expired")
		return
	}
	var item nasCleanupItem
	if json.Unmarshal(raw, &item) != nil {
		util.WriteError(w, 500, "invalid cleanup manifest")
		return
	}
	stageTimings := map[string]any{"client": req.Timings}
	policyReason := ""
	action := req.Outcome
	recovery := "unchecked"
	deletion := false
	if req.Outcome == "match" {
		if !validCleanupProof(req.Proof, item) {
			util.WriteError(w, 400, "complete exact physical cleanup proof required")
			return
		}
		current, e := cleanupCatalogItem(ctx, tx, item.Kind, item.SourceID, s.r2.Bucket())
		a, _ := json.Marshal(current)
		bitem := item
		bitem.Frozen = [5]int64{}
		bitem.FrozenSidecar = [5]int64{}
		b, _ := json.Marshal(bitem)
		if e != nil || !bytes.Equal(a, b) {
			action = "conflict"
		} else {
			policyStarted := time.Now()
			e = s.cleanupCurrentPolicyMode(ctx, tx, item, mode == "dry_run", stageTimings)
			stageTimings["policy_ms"] = time.Since(policyStarted).Milliseconds()
			if e != nil {
				action = "keep"
				policyReason = e.Error()
			} else {
				recoveryStarted := time.Now()
				recovery = s.cleanupRecovery(ctx, item)
				stageTimings["recovery_ms"] = time.Since(recoveryStarted).Milliseconds()
				action = "would_delete"
				if recovery == "conflict" {
					action = "conflict"
				}
				if mode == "enabled" && s.cfg.NASCleanupDeletionEnabled && until != nil && until.After(time.Now()) && cleanupSameFileProof(oldProof, req.Proof) && (recovery == "r2_sha_verified" || (allow && recovery == "nas_only_at_check")) {
					action = "delete_issued"
					deletion = true
				}
			}
		}
	}
	proof := any(nil)
	if len(req.Proof) > 0 {
		proof = string(req.Proof)
	}
	observed, _ := json.Marshal(req)
	stages, _ := json.Marshal(stageTimings)
	_, err = tx.Exec(ctx, `UPDATE nas_cleanup_items SET state=$2,delete_ever_issued=delete_ever_issued OR $2='delete_issued',lease_expires_at=NULL,proof=$3::jsonb,proof_sha256=$4,recovery=$5,outcome=$6::jsonb,policy_reason=$7,stage_timings=$8::jsonb,updated_at=now() WHERE id=$1`, id, action, proof, cleanupSHA(req.Proof), recovery, string(observed), policyReason, string(stages))
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "cleanup observation save failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"action": action, "delete": deletion, "item_id": id, "attempt": attempt, "plan_sha256": sha, "proof_sha256": cleanupSHA(req.Proof), "recovery": recovery, "expires_at": time.Now().UTC().Add(30 * time.Second)})
}
func (s *Server) handleAccountNASCleanupResult(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "itemId")
	if !ok {
		return
	}
	var req struct {
		Attempt  int    `json:"attempt"`
		PlanSHA  string `json:"plan_sha256"`
		ProofSHA string `json:"proof_sha256"`
		Outcome  string `json:"outcome"`
		Error    string `json:"error"`
	}
	if util.DecodeJSON(r, &req) != nil || req.Attempt < 1 || len(req.PlanSHA) != 64 || len(req.ProofSHA) != 64 || (req.Outcome != "deleted" && req.Outcome != "unverified") || len(req.Error) > 500 {
		util.WriteError(w, 400, "invalid cleanup deletion receipt")
		return
	}
	cid, ok := s.nasRestoreConnection(r.Context(), w, r)
	if !ok {
		return
	}
	body, _ := json.Marshal(req)
	tag, err := s.pool.Exec(r.Context(), `UPDATE nas_cleanup_items i SET state=$6,outcome=$7::jsonb,updated_at=now() FROM nas_cleanup_plans p WHERE i.id=$1 AND p.id=i.plan_id AND p.connection_id=$2 AND p.plan_sha256=$3 AND i.attempt=$4 AND i.proof_sha256=$5 AND (i.state='delete_issued' OR (i.state=$6 AND i.outcome=$7::jsonb))`, id, cid, req.PlanSHA, req.Attempt, req.ProofSHA, req.Outcome, string(body))
	if err != nil || tag.RowsAffected() != 1 {
		util.WriteError(w, 409, "cleanup receipt differs or task is not deletion-issued")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"ok": true, "state": req.Outcome})
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
	stage := "plan_insert"
	manifest, _ := json.Marshal(items)
	sha := cleanupSHA(manifest)
	var id int64
	err := tx.QueryRow(ctx, `INSERT INTO nas_cleanup_plans(account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES(47,13,$1,$2,$3,$4) ON CONFLICT(label) DO NOTHING RETURNING id`, label, nasCleanupReportSHA, string(manifest), sha).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		var old string
		stage = "plan_collision"
		err = tx.QueryRow(ctx, `SELECT id,plan_sha256 FROM nas_cleanup_plans WHERE label=$1`, label).Scan(&id, &old)
		if err == nil && old != sha {
			err = errors.New("label already has a different plan")
		}
	} else if err == nil {
		stage = "item_insert"
		_, err = tx.Exec(ctx, `INSERT INTO nas_cleanup_items(plan_id,ordinal) SELECT $1,generate_series(1,$2::integer)`, id, len(items))
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
