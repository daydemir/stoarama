package api

import (
	"errors"
	"fmt"
	"net/http"
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
