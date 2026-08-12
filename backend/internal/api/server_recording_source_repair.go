package api

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

type recordingSourceRepairRequest struct {
	AccountID                 int64  `json:"account_id"`
	StreamID                  int64  `json:"stream_id"`
	JobID                     int64  `json:"job_id"`
	ExpectedCurrentSourceHash string `json:"expected_current_source_sha256"`
	ReplacementSourceURL      string `json:"replacement_source_url"`
	Reason                    string `json:"reason"`
}

var probeRecordingSourceRepair = probeRecordingStreamReachable

func sourceURLHash(raw string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(raw)))
	return hex.EncodeToString(sum[:])
}

// handleAdminRecordingSourceRepair is intentionally limited to a pending,
// zero-clip continuous cloud job. That narrow state lets the job row lock fence
// scheduler claims while catalog and recording snapshots change atomically.
func (s *Server) handleAdminRecordingSourceRepair(w http.ResponseWriter, r *http.Request) {
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req recordingSourceRepairRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	replacement := strings.TrimSpace(req.ReplacementSourceURL)
	reason := sanitizeRecordingSurrenderError(req.Reason, "source repair")
	_, hashErr := hex.DecodeString(req.ExpectedCurrentSourceHash)
	if req.AccountID <= 0 || req.StreamID <= 0 || req.JobID <= 0 || len(req.ExpectedCurrentSourceHash) != 64 || hashErr != nil || replacement == "" || strings.TrimSpace(req.Reason) == "" {
		util.WriteError(w, http.StatusBadRequest, "account_id, stream_id, job_id, expected_current_source_sha256, replacement_source_url, and reason are required")
		return
	}
	resolved, headers, err := resolveRecordingStreamURL(r.Context(), "", replacement, "")
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "replacement source could not be resolved")
		return
	}
	validatedIP, sourceKind, err := validateRecordingStreamURL(resolved)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "replacement source is invalid")
		return
	}
	if err := probeRecordingSourceRepair(r.Context(), resolved, validatedIP, headers); err != nil {
		util.WriteError(w, http.StatusBadRequest, "replacement source is not reachable")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin source repair")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var jobStatus string
	var jobRecordingID int64
	var jobKind string
	var windowOpen bool
	if err := tx.QueryRow(r.Context(), `SELECT recording_id,status,kind,COALESCE(window_end_at>now(),false) FROM recording_jobs WHERE id=$1 FOR UPDATE`, req.JobID).Scan(&jobRecordingID, &jobStatus, &jobKind, &windowOpen); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusConflict, "source repair precondition changed")
		} else {
			util.WriteError(w, http.StatusInternalServerError, "lock recording job")
		}
		return
	}
	if jobRecordingID != recordingID || jobStatus != "pending" || jobKind != "continuous_window" || !windowOpen {
		util.WriteError(w, http.StatusConflict, "source repair requires the expected pending job")
		return
	}
	var accountID, streamID int64
	var currentURL, captureVia, mode string
	if err := tx.QueryRow(r.Context(), `SELECT account_id,COALESCE(stream_id,0),stream_url,capture_via,mode FROM recordings WHERE id=$1 AND status='active' FOR UPDATE`, recordingID).Scan(&accountID, &streamID, &currentURL, &captureVia, &mode); err != nil {
		util.WriteError(w, http.StatusConflict, "source repair recording precondition changed")
		return
	}
	if accountID != req.AccountID || streamID != req.StreamID || captureVia != "cloud" || mode != "continuous" {
		util.WriteError(w, http.StatusConflict, "source repair recording precondition changed")
		return
	}
	var clipCount int
	if err := tx.QueryRow(r.Context(), `SELECT count(*) FROM recording_clips WHERE recording_job_id=$1`, req.JobID).Scan(&clipCount); err != nil || clipCount != 0 {
		util.WriteError(w, http.StatusConflict, "source repair requires a zero-clip job")
		return
	}
	current, err := s.loadStreamForAssignmentTx(r.Context(), tx, req.StreamID)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "catalog source precondition changed")
		return
	}
	newHash := sourceURLHash(replacement)
	if sourceURLHash(currentURL) == newHash && sourceURLHash(current.SourceURL) == newHash {
		if err := tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit idempotent source repair")
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"recording_id": recordingID, "stream_id": streamID, "job_id": req.JobID, "status": "pending", "source_sha256": newHash, "repaired": false, "idempotent": true})
		return
	}
	if !strings.EqualFold(sourceURLHash(currentURL), req.ExpectedCurrentSourceHash) || !strings.EqualFold(sourceURLHash(current.SourceURL), req.ExpectedCurrentSourceHash) {
		util.WriteError(w, http.StatusConflict, "source repair source-hash precondition changed")
		return
	}
	updated := current
	updated.SourceURL = replacement
	if _, err := tx.Exec(r.Context(), `UPDATE streams SET source_url=$2,updated_at=now() WHERE id=$1`, req.StreamID, replacement); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "update catalog source")
		return
	}
	if _, err := tx.Exec(r.Context(), `UPDATE recordings SET stream_url=$2,source_kind=$3,last_error_text='',last_error_at=NULL,consecutive_failures=0,updated_at=now() WHERE id=$1`, recordingID, replacement, sourceKind); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "update recording source")
		return
	}
	if err := insertStreamSourceRevisionTx(r.Context(), tx, streamSourceRevisionInput{Actor: "api.recording_source_repair", Reason: reason, Previous: current, Current: updated, Metadata: map[string]any{"account_id": accountID, "recording_id": recordingID, "job_id": req.JobID, "old_source_sha256": sourceURLHash(currentURL), "new_source_sha256": sourceURLHash(replacement)}}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "record source revision")
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, accountID, nil, "recording_source_repaired", "service", "stoaramactl", map[string]any{"recording_id": recordingID, "stream_id": streamID, "job_id": req.JobID, "old_source_sha256": sourceURLHash(currentURL), "new_source_sha256": sourceURLHash(replacement), "reason": reason}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "audit source repair")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit source repair")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"recording_id": recordingID, "stream_id": streamID, "job_id": req.JobID, "status": "pending", "source_sha256": sourceURLHash(replacement), "repaired": true})
}
