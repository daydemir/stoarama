package api

import (
	"context"
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

type nasRedownloadSource struct {
	streamID                         int64
	bucket, key, sha, timezone, path string
	size                             int64
	start, end                       time.Time
	held, purged                     bool
	payload                          []byte
	token                            *string
}

func loadNASRedownloadSource(ctx context.Context, query func(context.Context, string, ...any) pgx.Row, clipID, accountID int64) (nasRedownloadSource, error) {
	var source nasRedownloadSource
	err := query(ctx, nasRedownloadSQL, clipID, accountID).Scan(&source.streamID, &source.bucket, &source.key, &source.sha,
		&source.size, &source.path, &source.start, &source.end, &source.timezone, &source.purged, &source.held, &source.payload, &source.token)
	return source, err
}

func (source nasRedownloadSource) eligible(req nasRedownloadRequest, windows map[int64]Keep28Window, bucket string) bool {
	window, exists := windows[source.streamID]
	return exists && source.timezone == window.Timezone && source.end.After(source.start) && source.start.Before(window.End) && source.end.After(window.Start) &&
		source.bucket == bucket && strings.HasPrefix(source.key, "managed/") && source.sha == req.SHA256 && source.size == req.SizeBytes &&
		validNASRelativePath(source.path) && !source.purged && !source.held
}

func (source nasRedownloadSource) clipPayload() (json.RawMessage, error) {
	var clip map[string]json.RawMessage
	if err := json.Unmarshal(source.payload, &clip); err != nil {
		return nil, err
	}
	var generation *string
	if source.token != nil {
		sum := sha256.Sum256([]byte(*source.token))
		value := fmt.Sprintf("sha256:%x", sum[:])
		generation = &value
	}
	// The NAS sidecar is shared with the account clip feed, which serializes
	// UTC time.Time values. PostgreSQL JSON timestamps use a numeric offset.
	for field, value := range map[string]any{
		"clip_start_at": source.start.UTC(), "clip_end_at": source.end.UTC(), "capture_generation": generation,
	} {
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		clip[field] = data
	}
	return json.Marshal(clip)
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
	principal, _ := accountPrincipalFromContext(r.Context())
	source, err := loadNASRedownloadSource(r.Context(), s.pool.QueryRow, req.ClipID, principal.AccountID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 404, "copy clip not found")
		return
	}
	if err != nil {
		util.WriteError(w, 503, "copy identity unavailable")
		return
	}
	if !source.eligible(req, windows, s.r2.Bucket()) {
		util.WriteError(w, 409, "copy source is outside policy, held, retired or changed")
		return
	}
	// HEAD issues no authority and holds no database transaction or fence.
	head, err := s.r2.Head(r.Context(), source.key)
	if err != nil || head.ETag == "" || head.SizeBytes != source.size || (head.SHA256Metadata != "" && !strings.EqualFold(head.SHA256Metadata, source.sha)) {
		util.WriteError(w, 409, "copy R2 identity unavailable")
		return
	}
	tx, err := BeginNASR2CopyTx(r.Context(), s.pool)
	if err != nil {
		util.WriteError(w, 503, "copy database unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	// COPY/deploy fence first, then the existing cleanup fences, then the
	// shared maintenance singleton. Never wait for1192 while holding a cleanup
	// fence or the singleton: the controller takes1192 before its row lock.
	for _, key := range []int64{nasR2CopyLock, nasCleanupAdmissionLock, nasCleanupPublicFence} {
		if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared($1)`, key); err != nil {
			util.WriteError(w, 503, "copy fence unavailable")
			return
		}
	}
	if err = cleanupMaintenanceCheck(r.Context(), tx); err != nil {
		util.WriteError(w, 409, "redownload maintenance gate unavailable or active")
		return
	}
	current, err := loadNASRedownloadSource(r.Context(), tx.QueryRow, req.ClipID, principal.AccountID)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && (!current.eligible(req, windows, s.r2.Bucket()) || current.key != source.key)) {
		util.WriteError(w, 409, "copy source changed during HEAD")
		return
	}
	if err != nil {
		util.WriteError(w, 503, "copy identity unavailable")
		return
	}
	var allowed bool
	if err = tx.QueryRow(r.Context(), `SELECT nas_delivery_allowed($1,'raw',$2)`, cid, req.ClipID).Scan(&allowed); err != nil {
		util.WriteError(w, 503, "copy work window unavailable")
		return
	}
	if !allowed {
		util.WriteError(w, 409, "copy source outside active work window")
		return
	}
	cap, err := s.r2.PresignGetExactRequest(r.Context(), current.key, head.ETag, head.VersionID, 15*time.Minute)
	if err != nil {
		util.WriteError(w, 503, "copy capability unavailable")
		return
	}
	clip, err := current.clipPayload()
	if err != nil {
		util.WriteError(w, 503, "copy metadata unavailable")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"policy_sha256": policy, "clip": clip, "url": cap.URL, "headers": cap.Headers, "expires_at": time.Now().UTC().Add(15 * time.Minute)})
}

const nasRedownloadSQL = `
SELECT coalesce(r.stream_id,0),coalesce(c.bucket,''),coalesce(c.object_key,''),lower(coalesce(c.sha256,'')),
 c.size_bytes,c.display_path,c.clip_start_at,c.clip_end_at,coalesce(nullif(s.local_timezone,''),r.cron_timezone,''),c.purged_at IS NOT NULL,
 (EXISTS(SELECT 1 FROM nas_cleanup_incident_sources i WHERE i.clip_id=c.id)
 OR EXISTS(SELECT 1 FROM nas_cleanup_incident_roster i WHERE (i.kind='raw' AND i.source_id=c.id) OR (i.bucket=c.bucket AND i.object_key=c.object_key))
 OR nas_cleanup_file_reserved('raw',c.id)
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

// LockNASCleanupR2Retirement shares admission/activation fencing with the
// operator CLI. Keep the lock order here so API and CLI cannot drift apart.
func LockNASCleanupR2Retirement(ctx context.Context, tx pgx.Tx) error {
	for _, key := range []int64{nasCleanupAdmissionLock, nasCleanupPublicFence} {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, key); err != nil {
			return err
		}
	}
	return nil
}
