package api

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

type nasRedownloadRequest struct {
	ClipID       int64  `json:"clip_id"`
	SHA256       string `json:"sha256"`
	SizeBytes    int64  `json:"size_bytes"`
	PolicySHA256 string `json:"policy_sha256"`
}

// Read-only, connection-13-only recovery download. Unlike the public browser
// download this permits released clips, but never purged/reserved/held clips.
// The caller supplies identity, never a bucket/key or download destination.
func (s *Server) handleAccountConnectionRedownload(w http.ResponseWriter, r *http.Request) {
	cid, ok := s.nasRestoreConnection(r.Context(), w, r)
	if !ok {
		return
	}
	if cid != 13 {
		util.WriteError(w, 403, "redownload requires connection 13")
		return
	}
	var req nasRedownloadRequest
	if util.DecodeJSON(r, &req) != nil || req.ClipID <= 0 || req.SizeBytes <= 0 || len(req.SHA256) != 64 || !lowerHex(req.SHA256) {
		util.WriteError(w, 400, "invalid copy identity")
		return
	}
	policy, windows, err := Keep28Policy()
	if err != nil || req.PolicySHA256 != policy {
		util.WriteError(w, 409, "keep28 policy changed")
		return
	}
	if s.r2 == nil {
		util.WriteError(w, 503, "archive unavailable")
		return
	}
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		util.WriteError(w, 503, "copy database unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	// Shared admission/public locks exclude a cleanup admission/activation while
	// the source identity and short-lived capability are checked and signed.
	for _, key := range []int64{nasCleanupAdmissionLock, nasCleanupPublicFence} {
		if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared($1)`, key); err != nil {
			util.WriteError(w, 503, "copy fence unavailable")
			return
		}
	}
	principal, _ := accountPrincipalFromContext(r.Context())
	var streamID int64
	var bucket, key, sha, tz, path string
	var size int64
	var start, end time.Time
	var held, purged bool
	var payload []byte
	var token *string
	err = tx.QueryRow(r.Context(), nasRedownloadSQL, req.ClipID, principal.AccountID).Scan(&streamID, &bucket, &key, &sha, &size, &path, &start, &end, &tz, &purged, &held, &payload, &token)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 404, "copy clip not found")
		return
	}
	if err != nil {
		util.WriteError(w, 503, "copy identity unavailable")
		return
	}
	window, exists := windows[streamID]
	if !exists || tz != window.Timezone || !end.After(start) || !start.Before(window.End) || !end.After(window.Start) ||
		bucket != s.r2.Bucket() || !strings.HasPrefix(key, "managed/") || sha != req.SHA256 || size != req.SizeBytes || !validNASRelativePath(path) || purged || held {
		util.WriteError(w, 409, "copy source is outside policy, held, retired or changed")
		return
	}
	head, err := s.r2.Head(r.Context(), key)
	if err != nil || head.ETag == "" || head.SizeBytes != size || (head.SHA256Metadata != "" && !strings.EqualFold(head.SHA256Metadata, sha)) {
		util.WriteError(w, 409, "copy R2 identity unavailable")
		return
	}
	cap, err := s.r2.PresignGetExactRequest(r.Context(), key, head.ETag, head.VersionID, 15*time.Minute)
	if err != nil {
		util.WriteError(w, 503, "copy capability unavailable")
		return
	}
	var clip map[string]any
	if json.Unmarshal(payload, &clip) != nil {
		util.WriteError(w, 503, "copy metadata unavailable")
		return
	}
	clip["capture_generation"] = nil
	if token != nil {
		sum := sha256.Sum256([]byte(*token))
		clip["capture_generation"] = fmt.Sprintf("sha256:%x", sum[:])
	}
	util.WriteJSON(w, 200, map[string]any{"policy_sha256": policy, "clip": clip, "url": cap.URL, "headers": cap.Headers, "expires_at": time.Now().UTC().Add(15 * time.Minute)})
}

const nasRedownloadSQL = `
SELECT coalesce(r.stream_id,0),coalesce(c.bucket,''),coalesce(c.object_key,''),lower(coalesce(c.sha256,'')),
 c.size_bytes,c.display_path,c.clip_start_at,c.clip_end_at,coalesce(nullif(s.local_timezone,''),r.cron_timezone,''),c.purged_at IS NOT NULL,
 (EXISTS(SELECT 1 FROM nas_cleanup_reserved_files f WHERE f.kind='raw' AND f.source_id=c.id)
 OR EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
   WHERE h.clip_id=c.id AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued)))
 OR EXISTS(SELECT 1 FROM r2_retirement_reservations x WHERE x.kind='raw' AND x.source_id=c.id)
 OR EXISTS(SELECT 1 FROM r2_object_retirements x WHERE x.kind='raw' AND x.source_id=c.id)),
 jsonb_build_object('clip_id',c.id,'recording_id',c.recording_id,'size_bytes',c.size_bytes,'sha256',lower(c.sha256),
 'relative_path',c.display_path,'clip_start_at',c.clip_start_at,'clip_end_at',c.clip_end_at,
 'recording_job_id',to_jsonb(c)->'recording_job_id','capture_sequence',to_jsonb(c)->'capture_sequence',
 'capture_attempt_id',to_jsonb(c)->'capture_attempt_id','timestamp_contract_version',to_jsonb(c)->'timestamp_contract_version',
 'timestamp_contract',to_jsonb(c)->'timestamp_contract','timestamp_contract_status',to_jsonb(c)->'timestamp_contract_status',
 'timestamp_contract_reason',to_jsonb(c)->'timestamp_contract_reason'),to_jsonb(c)->>'capture_lease_token'
FROM recording_clips c JOIN recordings r ON r.id=c.recording_id LEFT JOIN streams s ON s.id=r.stream_id
WHERE c.id=$1 AND r.account_id=$2`
