package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type nasCleanupCandidateCreateRequest struct {
	AccountID    int64   `json:"account_id"`
	ConnectionID int64   `json:"connection_id"`
	RecordingIDs []int64 `json:"recording_ids"`
}

func (s *Server) handleAdminNASCleanupCandidateItems(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		util.WriteError(w, 400, "invalid candidate id")
		return
	}
	rows, err := s.pool.Query(r.Context(), `SELECT item.ordinal,item.clip_id,item.recording_id,item.recording_job_id,
	 item.window_start,item.window_end,item.relative_path,item.size_bytes,item.content_sha256,item.sidecar_relative_path,
	 item.sidecar_size_bytes,item.sidecar_sha256,item.file_mtime_ns,item.file_ctime_ns,item.file_inode,item.file_device,
	 verification.status,verification.observed_etag,
	 verification.observed_version_id,verification.observed_size_bytes,verification.observed_sha256,verification.verified_at
	 FROM nas_cleanup_candidate_items item JOIN r2_content_verifications verification ON verification.id=item.verification_id
	 WHERE item.run_id=$1 ORDER BY item.ordinal`, id)
	if err != nil {
		util.WriteError(w, 500, "read candidate items")
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0)
	for rows.Next() {
		var ordinal int
		var clipID, recordingID, size, sidecarSize int64
		var mtime, ctime, inode, device int64
		var jobID *int64
		var start, end time.Time
		var path, sha, sidecarPath, sidecarSHA, status string
		var etag, version, observedSHA *string
		var observedSize *int64
		var verifiedAt *time.Time
		if err := rows.Scan(&ordinal, &clipID, &recordingID, &jobID, &start, &end, &path, &size, &sha,
			&sidecarPath, &sidecarSize, &sidecarSHA, &mtime, &ctime, &inode, &device,
			&status, &etag, &version, &observedSize, &observedSHA, &verifiedAt); err != nil {
			util.WriteError(w, 500, "scan candidate items")
			return
		}
		items = append(items, map[string]any{"ordinal": ordinal, "clip_id": clipID, "recording_id": recordingID,
			"recording_job_id": jobID, "window_start": start, "window_end": end, "nas_relative_path": path,
			"size_bytes": size, "nas_sha256": sha, "sidecar_relative_path": sidecarPath,
			"sidecar_size_bytes": sidecarSize, "sidecar_sha256": sidecarSHA, "r2_status": status,
			"file_mtime_ns": mtime, "file_ctime_ns": ctime, "file_inode": inode, "file_device": device,
			"r2_etag": etag, "r2_version_id": version, "r2_size_bytes": observedSize,
			"r2_sha256": observedSHA, "r2_verified_at": verifiedAt})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, 500, "read candidate items")
		return
	}
	if len(items) == 0 {
		util.WriteError(w, 404, "candidate not found")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "qualification_only": true, "items": items})
}

type nasCleanupCandidateItem struct {
	ClipID, RecordingID               int64
	JobID                             *int64
	Start, End                        time.Time
	Path                              string
	Size                              int64
	SHA                               string
	Verified                          time.Time
	MTime, CTime, Inode, Device       int64
	SidecarPath                       string
	SidecarSize                       int64
	SidecarSHA                        string
	DestinationID                     int64
	Endpoint, Bucket, ObjectKey, ETag string
}

func normalizeCandidateRecordingIDs(ids []int64) ([]int64, error) {
	if len(ids) == 0 || len(ids) > 100 {
		return nil, errors.New("recording_ids must contain 1 to 100 IDs")
	}
	out := append([]int64(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	for i, id := range out {
		if id <= 0 || (i > 0 && id == out[i-1]) {
			return nil, errors.New("recording_ids must be unique positive integers")
		}
	}
	return out, nil
}
func candidateDigest(accountID, connectionID int64, ids []int64, generation, inventoryDigest string, started, completed time.Time, items []nasCleanupCandidateItem) string {
	h := sha256.New()
	enc := json.NewEncoder(h)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(struct {
		Version                     string
		AccountID, ConnectionID     int64
		RecordingIDs                []int64
		Generation, InventoryDigest string
		Started, Completed          time.Time
		Items                       []nasCleanupCandidateItem
	}{"stoarama-nas-candidate-v1", accountID, connectionID, ids, generation, inventoryDigest, started.UTC(), completed.UTC(), items})
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Server) handleAdminNASCleanupCandidateCreate(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok || p.Role != accountRoleAdmin || p.UserID <= 0 {
		util.WriteError(w, 403, "operator session required")
		return
	}
	var req nasCleanupCandidateCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, 400, err.Error())
		return
	}
	ids, err := normalizeCandidateRecordingIDs(req.RecordingIDs)
	if err != nil || req.AccountID <= 0 || req.ConnectionID <= 0 {
		if err == nil {
			err = errors.New("account_id and connection_id are required")
		}
		util.WriteError(w, 400, err.Error())
		return
	}
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		util.WriteError(w, 500, err.Error())
		return
	}
	defer tx.Rollback(r.Context())
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock($1)`, req.ConnectionID); err != nil {
		util.WriteError(w, 500, err.Error())
		return
	}
	var generation, digest, inProgress string
	var started, completed time.Time
	var skipped, liveRev, treeRev int64
	err = tx.QueryRow(r.Context(), `SELECT inventory_generation,inventory_digest,inventory_scan_started_at,inventory_scan_completed_at,inventory_scan_rows_skipped,inventory_in_progress_generation,inventory_live_revision,inventory_tree_revision FROM connections WHERE id=$1 AND account_id=$2 FOR SHARE`, req.ConnectionID, req.AccountID).Scan(&generation, &digest, &started, &completed, &skipped, &inProgress, &liveRev, &treeRev)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 404, "connection not found")
		return
	}
	if err != nil {
		util.WriteError(w, 500, err.Error())
		return
	}
	if generation == "" || len(digest) != 64 || skipped != 0 || inProgress != "" || liveRev != treeRev || completed.Before(started) || time.Since(started) > 72*time.Hour || time.Since(started) < -5*time.Minute || time.Since(completed) > 72*time.Hour || time.Since(completed) < -5*time.Minute {
		util.WriteError(w, 409, "UNKNOWN: a fresh complete skip-free consistent inventory is required")
		return
	}
	var recCount int
	if err = tx.QueryRow(r.Context(), `SELECT count(*) FROM recordings WHERE account_id=$1 AND id=ANY($2) AND status='paused'`, req.AccountID, ids).Scan(&recCount); err != nil {
		util.WriteError(w, 500, err.Error())
		return
	}
	if recCount != len(ids) {
		util.WriteError(w, 409, "UNKNOWN: every requested recording must exist in the account and be paused")
		return
	}
	var protectedCount int
	if err = tx.QueryRow(r.Context(), `SELECT count(DISTINCT recording_id) FROM (
	  SELECT recording_id FROM recording_qualification_members WHERE account_id=$1 AND recording_id=ANY($2)
	  UNION ALL SELECT recording_id FROM protected_campaign_recordings WHERE account_id=$1 AND recording_id=ANY($2)
	  UNION ALL SELECT id FROM recordings WHERE account_id=$1 AND id=ANY($2) AND status='active'
	) protected`, req.AccountID, ids).Scan(&protectedCount); err != nil {
		util.WriteError(w, 500, err.Error())
		return
	}
	if protectedCount != 0 {
		util.WriteError(w, 409, "UNKNOWN: active, campaign-protected, and qualification cohort recordings are excluded from cleanup candidates")
		return
	}
	var expectedCount, expectedBytes int64
	if err = tx.QueryRow(r.Context(), `SELECT count(*),COALESCE(sum(size_bytes),0) FROM recording_clips WHERE recording_id=ANY($1) AND purged_at IS NULL`, ids).Scan(&expectedCount, &expectedBytes); err != nil {
		util.WriteError(w, 500, err.Error())
		return
	}
	if expectedCount == 0 {
		util.WriteError(w, 409, "UNKNOWN: requested recordings have no retained clip metadata")
		return
	}
	var trustedDestinationCount int64
	if err = tx.QueryRow(r.Context(), `SELECT count(*) FROM recording_clips c JOIN recordings rec ON rec.id=c.recording_id JOIN storage_destinations sd ON sd.id=c.storage_destination_id AND sd.account_id=rec.account_id AND sd.endpoint=c.endpoint AND sd.bucket=c.bucket WHERE c.recording_id=ANY($1) AND c.purged_at IS NULL AND rec.account_id=$2`, ids, req.AccountID).Scan(&trustedDestinationCount); err != nil {
		util.WriteError(w, 500, err.Error())
		return
	}
	if trustedDestinationCount != expectedCount {
		util.WriteError(w, 409, "UNKNOWN: every recovery object must match its account's trusted destination endpoint and bucket")
		return
	}
	rows, err := tx.Query(r.Context(), `SELECT c.id,c.recording_id,c.recording_job_id,c.clip_start_at,c.clip_end_at,i.relative_path,i.size_bytes,lower(i.sha256),i.verified_at,i.file_mtime_ns,i.file_ctime_ns,i.file_inode,i.file_device,i.sidecar_relative_path,i.sidecar_size_bytes,i.sidecar_sha256,c.storage_destination_id,c.endpoint,c.bucket,c.object_key,c.etag FROM recording_clips c JOIN nas_inventory_files i ON i.connection_id=$1 AND i.clip_id=c.id AND i.recording_id=c.recording_id JOIN recordings rec ON rec.id=c.recording_id WHERE c.recording_id=ANY($2) AND c.purged_at IS NULL AND rec.account_id=$3 AND rec.status='paused' AND i.state='present' AND i.seen_generation=$4 AND i.relative_path=c.display_path AND i.size_bytes=c.size_bytes AND lower(i.sha256)=lower(c.sha256) AND i.verified_at >= $5 AND i.file_ctime_ns IS NOT NULL AND i.file_inode IS NOT NULL AND i.file_device IS NOT NULL AND i.sidecar_relative_path IS NOT NULL AND i.sidecar_size_bytes IS NOT NULL AND i.sidecar_sha256 IS NOT NULL AND NOT EXISTS(SELECT 1 FROM nas_inventory_files x WHERE x.connection_id=i.connection_id AND x.relative_path=i.relative_path AND x.clip_id<>i.clip_id AND x.state IN('present','mismatch')) AND NOT EXISTS(SELECT 1 FROM nas_inventory_unmatched_files u WHERE u.connection_id=i.connection_id AND u.relative_path=i.relative_path AND u.state='present') ORDER BY c.recording_id,c.clip_start_at,c.id`, req.ConnectionID, ids, req.AccountID, generation, started)
	if err != nil {
		util.WriteError(w, 500, err.Error())
		return
	}
	defer rows.Close()
	items := []nasCleanupCandidateItem{}
	var actualBytes int64
	for rows.Next() {
		var x nasCleanupCandidateItem
		if err = rows.Scan(&x.ClipID, &x.RecordingID, &x.JobID, &x.Start, &x.End, &x.Path, &x.Size, &x.SHA, &x.Verified, &x.MTime, &x.CTime, &x.Inode, &x.Device, &x.SidecarPath, &x.SidecarSize, &x.SidecarSHA, &x.DestinationID, &x.Endpoint, &x.Bucket, &x.ObjectKey, &x.ETag); err != nil {
			util.WriteError(w, 500, err.Error())
			return
		}
		items = append(items, x)
		actualBytes += x.Size
	}
	if err = rows.Err(); err != nil {
		util.WriteError(w, 500, err.Error())
		return
	}
	if int64(len(items)) != expectedCount || actualBytes != expectedBytes {
		util.WriteError(w, 409, "UNKNOWN: inventory does not exactly cover the complete database clip set")
		return
	}
	canonical := candidateDigest(req.AccountID, req.ConnectionID, ids, generation, digest, started, completed, items)
	runID := uuid.New()
	_, err = tx.Exec(r.Context(), `INSERT INTO nas_cleanup_candidate_runs(id,account_id,connection_id,recording_ids,inventory_generation,inventory_digest,inventory_started_at,inventory_completed_at,state,item_count,total_bytes,unknown_count,nas_rehash_required,request_digest,created_by_user_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'queued',$9,$10,$9,true,$11,$12)`, runID, req.AccountID, req.ConnectionID, ids, generation, digest, started, completed, len(items), actualBytes, canonical, p.UserID)
	if err != nil {
		util.WriteError(w, 409, fmt.Sprintf("create candidate: %v", err))
		return
	}
	for ordinal, x := range items {
		var verificationID int64
		err = tx.QueryRow(r.Context(), `INSERT INTO r2_content_verifications(storage_destination_id,endpoint_snapshot,bucket,object_key,expected_size_bytes,expected_sha256) VALUES($1,$2,$3,$4,$5,$6) RETURNING id`, x.DestinationID, x.Endpoint, x.Bucket, x.ObjectKey, x.Size, x.SHA).Scan(&verificationID)
		if err != nil {
			util.WriteError(w, 500, err.Error())
			return
		}
		_, err = tx.Exec(r.Context(), `INSERT INTO nas_cleanup_candidate_items(run_id,ordinal,clip_id,recording_id,recording_job_id,window_start,window_end,relative_path,size_bytes,content_sha256,inventory_verified_at,file_mtime_ns,file_ctime_ns,file_inode,file_device,sidecar_relative_path,sidecar_size_bytes,sidecar_sha256,storage_destination_id,recovery_bucket,recovery_object_key,recovery_etag,verification_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)`, runID, ordinal+1, x.ClipID, x.RecordingID, x.JobID, x.Start, x.End, x.Path, x.Size, x.SHA, x.Verified, x.MTime, x.CTime, x.Inode, x.Device, x.SidecarPath, x.SidecarSize, x.SidecarSHA, x.DestinationID, x.Bucket, x.ObjectKey, x.ETag, verificationID)
		if err != nil {
			util.WriteError(w, 500, err.Error())
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, 500, err.Error())
		return
	}
	util.WriteJSON(w, 201, map[string]any{"id": runID, "state": "queued", "request_digest": canonical, "final_digest": nil, "item_count": len(items), "total_bytes": actualBytes, "unknown_count": len(items), "nas_rehash_required": true, "qualification_only": true, "reclaim_bytes": 0})
}

func (s *Server) handleAdminNASCleanupCandidateGet(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		util.WriteError(w, 400, "invalid candidate id")
		return
	}
	var out struct {
		ID                                        uuid.UUID `json:"id"`
		AccountID, ConnectionID                   int64
		RecordingIDs                              []int64
		Generation, Digest, State, Request, Error string
		Final                                     *string
		ItemCount, TotalBytes, Unknown            int64
		Rehash                                    bool
		Created                                   time.Time
		Finished                                  *time.Time
	}
	err = s.pool.QueryRow(r.Context(), `SELECT id,account_id,connection_id,recording_ids,inventory_generation,inventory_digest,state,request_digest,final_digest,error_code,item_count,total_bytes,unknown_count,nas_rehash_required,created_at,finished_at FROM nas_cleanup_candidate_runs WHERE id=$1`, id).Scan(&out.ID, &out.AccountID, &out.ConnectionID, &out.RecordingIDs, &out.Generation, &out.Digest, &out.State, &out.Request, &out.Final, &out.Error, &out.ItemCount, &out.TotalBytes, &out.Unknown, &out.Rehash, &out.Created, &out.Finished)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 404, "candidate not found")
		return
	}
	if err != nil {
		util.WriteError(w, 500, err.Error())
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": out.ID, "account_id": out.AccountID, "connection_id": out.ConnectionID, "recording_ids": out.RecordingIDs, "inventory_generation": out.Generation, "inventory_digest": out.Digest, "state": out.State, "request_digest": out.Request, "final_digest": out.Final, "error_code": out.Error, "item_count": out.ItemCount, "total_bytes": out.TotalBytes, "unknown_count": out.Unknown, "nas_rehash_required": out.Rehash, "created_at": out.Created, "finished_at": out.Finished, "qualification_only": true, "reclaim_bytes": 0})
}
