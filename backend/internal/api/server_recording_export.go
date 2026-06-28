package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// r2DeleteBatchLimit bounds a single DeleteObjects call (S3/R2 cap is 1000
// keys per request); the bulk-delete loop chunks beyond it.
const r2DeleteBatchLimit = 1000

// clipDestination is the decrypted location + credentials snapshot for one
// clip, joined from recording_clips -> storage_destinations under the account
// scope. It mirrors the decrypt->client->op pattern in handleRecordingUploadIntent.
type clipDestination struct {
	objectKey          string
	thumbnailObjectKey string
	sizeBytes          int64
	region             string
	bucket             string
	endpoint           string
	accessKeyID        string
	secretEnc          []byte
}

// buildClipClient decrypts the destination secret and builds an r2 client for a
// clip's bucket, mirroring handleRecordingUploadIntent.
func (s *Server) buildClipClient(r *http.Request, d clipDestination) (*r2.Client, error) {
	secret, err := s.secrets.Decrypt(d.secretEnc)
	if err != nil {
		return nil, fmt.Errorf("decrypt destination secret: %w", err)
	}
	return r2.New(r.Context(), r2.Config{
		AccessKey: d.accessKeyID,
		SecretKey: string(secret),
		Region:    d.region,
		Bucket:    d.bucket,
		Endpoint:  d.endpoint,
	})
}

// handleAccountRecordingClipDownload presigns a GET against the clip's bucket so
// a browser-session researcher can download one clip. Ownership + location are
// resolved in a single SELECT scoped to the account; a purged clip is 410 Gone.
func (s *Server) handleAccountRecordingClipDownload(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage credential key is unset")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	clipID, ok := parseInt64Path(w, r, "clipId")
	if !ok {
		return
	}

	var (
		d        clipDestination
		purgedAt *time.Time
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT c.object_key, c.thumbnail_object_key, c.size_bytes, c.purged_at,
		       sd.region, sd.bucket, sd.endpoint, sd.access_key_id, sd.secret_access_key_enc
		FROM recording_clips c
		JOIN recordings r ON r.id = c.recording_id
		JOIN storage_destinations sd ON sd.id = c.storage_destination_id
		WHERE c.id=$1 AND c.recording_id=$2 AND r.account_id=$3
	`, clipID, recordingID, principal.AccountID).Scan(
		&d.objectKey, &d.thumbnailObjectKey, &d.sizeBytes, &purgedAt,
		&d.region, &d.bucket, &d.endpoint, &d.accessKeyID, &d.secretEnc,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "clip not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load clip: %v", err))
		return
	}
	if purgedAt != nil {
		util.WriteError(w, http.StatusGone, "clip was deleted")
		return
	}

	client, err := s.buildClipClient(r, d)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("build destination client: %v", err))
		return
	}
	url, err := client.PresignGet(r.Context(), d.objectKey, s.cfg.R2SignGetTTL)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("presign download: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"url":            url,
		"object_key":     d.objectKey,
		"size_bytes":     d.sizeBytes,
		"expires_in_sec": int(s.cfg.R2SignGetTTL.Seconds()),
	})
}

// handleAccountRecordingClipDelete deletes one clip's object(s) from the user's
// bucket, then tombstones the row (purged_at=now()). The row is NEVER deleted:
// the (bucket,object_key) unique key and the nightly billing snapshot's
// purged_at IS NULL exclusion both depend on it. Order matters: the object is
// removed first and purged_at is set only on a successful delete, so a failed
// delete leaves a retryable (still-live) row. An already-purged clip returns 200
// idempotently.
func (s *Server) handleAccountRecordingClipDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage credential key is unset")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	clipID, ok := parseInt64Path(w, r, "clipId")
	if !ok {
		return
	}

	var (
		d        clipDestination
		purgedAt *time.Time
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT c.object_key, c.thumbnail_object_key, c.purged_at,
		       sd.region, sd.bucket, sd.endpoint, sd.access_key_id, sd.secret_access_key_enc
		FROM recording_clips c
		JOIN recordings r ON r.id = c.recording_id
		JOIN storage_destinations sd ON sd.id = c.storage_destination_id
		WHERE c.id=$1 AND c.recording_id=$2 AND r.account_id=$3
	`, clipID, recordingID, principal.AccountID).Scan(
		&d.objectKey, &d.thumbnailObjectKey, &purgedAt,
		&d.region, &d.bucket, &d.endpoint, &d.accessKeyID, &d.secretEnc,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "clip not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load clip: %v", err))
		return
	}
	if purgedAt != nil {
		util.WriteJSON(w, http.StatusOK, map[string]any{"id": clipID, "purged": true})
		return
	}

	client, err := s.buildClipClient(r, d)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("build destination client: %v", err))
		return
	}
	keys := []string{d.objectKey}
	if d.thumbnailObjectKey != "" {
		keys = append(keys, d.thumbnailObjectKey)
	}
	if err := client.DeleteObjects(r.Context(), keys); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete clip objects: %v", err))
		return
	}

	// Only tombstone after the object delete succeeded, so a failure above is retryable.
	if _, err := s.pool.Exec(r.Context(), `
		UPDATE recording_clips SET purged_at=now() WHERE id=$1
	`, clipID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("tombstone clip: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"id": clipID, "purged": true})
}

// handleAccountRecordingClipsDeleteAll deletes every non-purged clip of one
// recording. Per destination it batches the object deletes (chunked at the
// 1000-key R2 cap) and then tombstones only the clips whose objects were
// removed, preserving the object-then-purged_at order per chunk.
func (s *Server) handleAccountRecordingClipsDeleteAll(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage credential key is unset")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}

	var ownerOK bool
	if err := s.pool.QueryRow(r.Context(), `
		SELECT EXISTS(SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2)
	`, recordingID, principal.AccountID).Scan(&ownerOK); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
		return
	}
	if !ownerOK {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}

	rows, err := s.pool.Query(r.Context(), `
		SELECT c.id, c.object_key, c.thumbnail_object_key,
		       sd.region, sd.bucket, sd.endpoint, sd.access_key_id, sd.secret_access_key_enc
		FROM recording_clips c
		JOIN storage_destinations sd ON sd.id = c.storage_destination_id
		WHERE c.recording_id=$1 AND c.purged_at IS NULL
		ORDER BY c.id ASC
	`, recordingID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list clips: %v", err))
		return
	}
	defer rows.Close()

	type clipDelete struct {
		id   int64
		keys []string
	}
	// Group by destination snapshot (region/bucket/endpoint/creds) so one client
	// can batch-delete all of a destination's objects.
	type destGroup struct {
		dest  clipDestination
		clips []clipDelete
	}
	groups := make(map[string]*destGroup)
	order := make([]string, 0, 4)
	for rows.Next() {
		var (
			clipID    int64
			objectKey string
			thumbKey  string
			d         clipDestination
		)
		if err := rows.Scan(&clipID, &objectKey, &thumbKey,
			&d.region, &d.bucket, &d.endpoint, &d.accessKeyID, &d.secretEnc); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan clip: %v", err))
			return
		}
		gk := d.endpoint + "\x00" + d.bucket + "\x00" + d.accessKeyID
		g, present := groups[gk]
		if !present {
			g = &destGroup{dest: d}
			groups[gk] = g
			order = append(order, gk)
		}
		keys := []string{objectKey}
		if thumbKey != "" {
			keys = append(keys, thumbKey)
		}
		g.clips = append(g.clips, clipDelete{id: clipID, keys: keys})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate clips: %v", err))
		return
	}

	deleted := 0
	for _, gk := range order {
		g := groups[gk]
		client, err := s.buildClipClient(r, g.dest)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("build destination client: %v", err))
			return
		}
		// Chunk so neither the object keys nor the tombstone id list exceed the
		// R2 1000-key cap, and so object-then-purged_at holds per chunk.
		var (
			chunkKeys []string
			chunkIDs  []int64
		)
		flush := func() bool {
			if len(chunkKeys) == 0 {
				return true
			}
			if err := client.DeleteObjects(r.Context(), chunkKeys); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete clip objects: %v", err))
				return false
			}
			if _, err := s.pool.Exec(r.Context(), `
				UPDATE recording_clips SET purged_at=now() WHERE id = ANY($1) AND purged_at IS NULL
			`, chunkIDs); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("tombstone clips: %v", err))
				return false
			}
			deleted += len(chunkIDs)
			chunkKeys = chunkKeys[:0]
			chunkIDs = chunkIDs[:0]
			return true
		}
		for _, c := range g.clips {
			if len(chunkKeys)+len(c.keys) > r2DeleteBatchLimit {
				if !flush() {
					return
				}
			}
			chunkKeys = append(chunkKeys, c.keys...)
			chunkIDs = append(chunkIDs, c.id)
		}
		if !flush() {
			return
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
}

type clipTransferRequest struct {
	TargetStorageDestinationID int64 `json:"target_storage_destination_id"`
}

type clipTransferJobResponse struct {
	ID                         int64      `json:"id"`
	RecordingClipID            int64      `json:"recording_clip_id"`
	TargetStorageDestinationID int64      `json:"target_storage_destination_id"`
	Status                     string     `json:"status"`
	BytesCopied                int64      `json:"bytes_copied"`
	ErrorText                  string     `json:"error_text"`
	CreatedAt                  time.Time  `json:"created_at"`
	CompletedAt                *time.Time `json:"completed_at"`
}

// handleAccountRecordingClipTransfer enqueues an async COPY of one clip into
// another storage destination the same account owns. The copy is performed by
// the clip-transfer worker in recorder-control (streamed GET source -> PUT
// target). It is never a move: the source clip is left intact (purged_at
// untouched). Re-enqueue of an already in-flight/done transfer is a no-op that
// returns the current job (the idempotency_key dedups on clip+target).
func (s *Server) handleAccountRecordingClipTransfer(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	clipID, ok := parseInt64Path(w, r, "clipId")
	if !ok {
		return
	}
	var req clipTransferRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.TargetStorageDestinationID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "target_storage_destination_id is required")
		return
	}

	// Load the clip under the account scope: it must belong to a recording the
	// caller owns and must not be purged. sourceDestID lets us reject a no-op
	// copy onto the clip's own destination.
	var (
		sourceDestID int64
		objectKey    string
		purgedAt     *time.Time
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT c.storage_destination_id, c.object_key, c.purged_at
		FROM recording_clips c
		JOIN recordings rec ON rec.id = c.recording_id
		WHERE c.id=$1 AND c.recording_id=$2 AND rec.account_id=$3
	`, clipID, recordingID, principal.AccountID).Scan(&sourceDestID, &objectKey, &purgedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "clip not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load clip: %v", err))
		return
	}
	if purgedAt != nil {
		util.WriteError(w, http.StatusGone, "clip was deleted")
		return
	}
	if req.TargetStorageDestinationID == sourceDestID {
		util.WriteError(w, http.StatusBadRequest, "target is the clip's own storage destination; nothing to copy")
		return
	}

	// The target destination must be owned by the same account.
	var targetKeyPrefix string
	err = s.pool.QueryRow(r.Context(), `
		SELECT key_prefix FROM storage_destinations WHERE id=$1 AND account_id=$2
	`, req.TargetStorageDestinationID, principal.AccountID).Scan(&targetKeyPrefix)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "target storage destination not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load target destination: %v", err))
		return
	}

	targetObjectKey := buildClipTransferObjectKey(targetKeyPrefix, recordingID, clipID, objectKey)
	idempotencyKey := fmt.Sprintf("xfer:%d:%d", clipID, req.TargetStorageDestinationID)

	// Insert-or-fetch: a new clip+target enqueues; a re-enqueue is a no-op that
	// returns the existing job row (and its current status).
	var resp clipTransferJobResponse
	err = s.pool.QueryRow(r.Context(), `
		WITH ins AS (
			INSERT INTO clip_transfer_jobs
				(account_id, recording_clip_id, target_storage_destination_id, target_object_key, idempotency_key)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING id, recording_clip_id, target_storage_destination_id, status, bytes_copied, error_text, created_at, completed_at
		)
		SELECT id, recording_clip_id, target_storage_destination_id, status, bytes_copied, error_text, created_at, completed_at FROM ins
		UNION ALL
		SELECT id, recording_clip_id, target_storage_destination_id, status, bytes_copied, error_text, created_at, completed_at
		FROM clip_transfer_jobs WHERE idempotency_key=$5
		LIMIT 1
	`, principal.AccountID, clipID, req.TargetStorageDestinationID, targetObjectKey, idempotencyKey).Scan(
		&resp.ID, &resp.RecordingClipID, &resp.TargetStorageDestinationID, &resp.Status,
		&resp.BytesCopied, &resp.ErrorText, &resp.CreatedAt, &resp.CompletedAt,
	)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("enqueue clip transfer: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, resp)
}

// handleAccountRecordingTransfers lists the transfer jobs for a recording's
// clips, account-scoped. The target destination name is joined in so the UI can
// label each row.
func (s *Server) handleAccountRecordingTransfers(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}

	var ownerOK bool
	if err := s.pool.QueryRow(r.Context(), `
		SELECT EXISTS(SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2)
	`, recordingID, principal.AccountID).Scan(&ownerOK); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
		return
	}
	if !ownerOK {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}

	rows, err := s.pool.Query(r.Context(), `
		SELECT j.id, j.recording_clip_id, j.target_storage_destination_id, sd.name,
		       j.status, j.bytes_copied, j.error_text, j.created_at, j.completed_at
		FROM clip_transfer_jobs j
		JOIN recording_clips c ON c.id = j.recording_clip_id
		JOIN storage_destinations sd ON sd.id = j.target_storage_destination_id
		WHERE c.recording_id=$1 AND j.account_id=$2
		ORDER BY j.id DESC
	`, recordingID, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list transfers: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 16)
	for rows.Next() {
		var (
			id          int64
			clipID      int64
			targetID    int64
			targetName  string
			status      string
			bytesCopied int64
			errorText   string
			createdAt   time.Time
			completedAt *time.Time
		)
		if err := rows.Scan(&id, &clipID, &targetID, &targetName, &status, &bytesCopied, &errorText, &createdAt, &completedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan transfer: %v", err))
			return
		}
		items = append(items, map[string]any{
			"id":                            id,
			"recording_clip_id":             clipID,
			"target_storage_destination_id": targetID,
			"target_name":                   targetName,
			"status":                        status,
			"bytes_copied":                  bytesCopied,
			"error_text":                    errorText,
			"created_at":                    createdAt.UTC(),
			"completed_at":                  completedAt,
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate transfers: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

// buildClipTransferObjectKey is the deterministic target key for a copied clip:
// the target destination's key_prefix joined to recordings/<recordingID>/
// <clipID>-<basename(sourceObjectKey)>. It is stable across re-enqueues so the
// idempotency_key (clip+target) and the resulting object map one-to-one. Empty,
// "." and ".." segments are dropped so a stored prefix cannot traverse out of
// the recordings namespace (mirrors buildRecordingClipObjectKey).
func buildClipTransferObjectKey(keyPrefix string, recordingID, clipID int64, sourceObjectKey string) string {
	parts := make([]string, 0, 5)
	for _, seg := range strings.Split(strings.Trim(strings.TrimSpace(keyPrefix), "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			continue
		}
		parts = append(parts, seg)
	}
	base := sourceObjectKey
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[i+1:]
	}
	if base == "" {
		base = "clip"
	}
	parts = append(parts,
		"recordings",
		fmt.Sprintf("%d", recordingID),
		fmt.Sprintf("%d-%s", clipID, base),
	)
	return strings.Join(parts, "/")
}
