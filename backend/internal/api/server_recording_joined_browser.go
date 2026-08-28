package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

type recordingJoinedFile struct {
	ArtifactID   int64      `json:"artifact_id"`
	HourID       string     `json:"hour_id"`
	Kind         string     `json:"kind"`
	ContentType  string     `json:"content_type"`
	RelativePath string     `json:"relative_path"`
	SizeBytes    int64      `json:"size_bytes"`
	SHA256       string     `json:"sha256"`
	PublishedAt  *time.Time `json:"published_at,omitempty"`
	DownloadPath string     `json:"download_path"`
}

func (s *Server) handleAccountRecordingJoinedList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	rows, err := s.pool.Query(r.Context(), `SELECT a.id,h.hour_id,a.artifact_kind,a.content_type,a.relative_path,
		a.expected_size_bytes,a.expected_sha256,a.published_at FROM recording_joined_artifacts a
		JOIN recording_joined_hours h ON h.id=a.hour_record_id AND h.recording_id=$1
		JOIN recordings rec ON rec.id=h.recording_id AND rec.account_id=$2
		WHERE a.account_id=$2 AND a.batch_record_id=h.batch_record_id AND a.artifact_kind IN ('hour_manifest','media')
		AND ((a.artifact_kind='media' AND a.published_at IS NOT NULL) OR (a.artifact_kind='hour_manifest' AND a.publication_state='published'))
		ORDER BY h.local_date,h.delivery_hour,a.ordinal,a.id`, recordingID, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "list joined recording files")
		return
	}
	defer rows.Close()
	files := make([]recordingJoinedFile, 0)
	for rows.Next() {
		var file recordingJoinedFile
		if err := rows.Scan(&file.ArtifactID, &file.HourID, &file.Kind, &file.ContentType, &file.RelativePath, &file.SizeBytes, &file.SHA256, &file.PublishedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "read joined recording files")
			return
		}
		file.DownloadPath = fmt.Sprintf("/api/v1/account/recordings/%d/joined/%d/download", recordingID, file.ArtifactID)
		files = append(files, file)
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read joined recording files")
		return
	}
	if len(files) == 0 {
		var exists bool
		if err := s.pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2)`, recordingID, principal.AccountID).Scan(&exists); err != nil || !exists {
			util.WriteError(w, http.StatusNotFound, "recording not found")
			return
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"recording_id": recordingID, "files": files})
}

func (s *Server) handleAccountRecordingJoinedDownload(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	artifactID, ok := parseInt64Path(w, r, "joinedId")
	if !ok {
		return
	}
	var objectKey, etag, versionID, contentType, sha256 string
	var sizeBytes int64
	err := s.pool.QueryRow(r.Context(), `SELECT a.object_key,a.etag,a.version_id,a.content_type,a.expected_size_bytes,a.expected_sha256
		FROM recording_joined_artifacts a JOIN recording_joined_hours h ON h.id=a.hour_record_id AND h.recording_id=$1
		JOIN recordings rec ON rec.id=h.recording_id AND rec.account_id=$2 WHERE a.id=$3 AND a.account_id=$2
		AND a.batch_record_id=h.batch_record_id AND a.artifact_kind IN ('hour_manifest','media')
		AND ((a.artifact_kind='media' AND a.published_at IS NOT NULL) OR (a.artifact_kind='hour_manifest' AND a.publication_state='published'))`, recordingID, principal.AccountID, artifactID).Scan(&objectKey, &etag, &versionID, &contentType, &sizeBytes, &sha256)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "joined recording file not found")
		return
	}
	store := s.joinedOutputStore()
	if err != nil || store == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording file is unavailable")
		return
	}
	head, err := store.Head(r.Context(), objectKey)
	if err != nil || head.SizeBytes != sizeBytes || head.ETag != etag || head.VersionID != versionID {
		util.WriteError(w, http.StatusConflict, "joined recording file identity changed")
		return
	}
	var stillCurrent bool
	err = s.pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM recording_joined_artifacts a
		JOIN recording_joined_hours h ON h.id=a.hour_record_id AND h.recording_id=$1
		JOIN recordings rec ON rec.id=h.recording_id AND rec.account_id=$2
		WHERE a.id=$3 AND a.account_id=$2 AND a.object_key=$4 AND a.etag=$5 AND a.version_id=$6
		AND a.expected_size_bytes=$7 AND a.batch_record_id=h.batch_record_id
		AND a.artifact_kind IN ('hour_manifest','media')
		AND ((a.artifact_kind='media' AND a.published_at IS NOT NULL) OR (a.artifact_kind='hour_manifest' AND a.publication_state='published')))`,
		recordingID, principal.AccountID, artifactID, objectKey, etag, versionID, sizeBytes).Scan(&stillCurrent)
	if err != nil || !stillCurrent {
		util.WriteError(w, http.StatusConflict, "joined recording file changed while signing")
		return
	}
	body, err := store.OpenExact(r.Context(), objectKey, etag, versionID)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "joined recording file identity changed")
		return
	}
	defer body.Close()
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", sizeBytes))
	w.Header().Set("Content-Disposition", "inline")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if _, err := io.Copy(w, io.LimitReader(body, sizeBytes+1)); err != nil {
		return
	}
}
