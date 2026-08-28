package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/daydemir/stoarama/backend/internal/util"
)

const recordingUploadIntentReconcileSafetyMargin = 2 * time.Minute

type recordingUploadIntentReconcileRequest struct {
	AccountID          int64     `json:"account_id"`
	RecordingIDs       []int64   `json:"recording_ids"`
	ExpiresBefore      time.Time `json:"expires_before"`
	Apply              bool      `json:"apply"`
	ExpectedPlanSHA256 string    `json:"expected_plan_sha256,omitempty"`
	Reason             string    `json:"reason,omitempty"`
}

type recordingUploadIntentReconcileCandidate struct {
	IntentID    string    `json:"intent_id"`
	RecordingID int64     `json:"recording_id"`
	JobID       int64     `json:"recording_job_id"`
	ExpiresAt   time.Time `json:"expires_at"`
	JobStatus   string    `json:"job_status"`
	JobAttempt  int       `json:"job_attempt"`
}

type recordingUploadIntentReconcileResponse struct {
	Apply          bool                                      `json:"apply"`
	AccountID      int64                                     `json:"account_id"`
	RecordingIDs   []int64                                   `json:"recording_ids"`
	ExpiresBefore  time.Time                                 `json:"expires_before"`
	CandidateCount int                                       `json:"candidate_count"`
	Candidates     []recordingUploadIntentReconcileCandidate `json:"candidates"`
	PlanSHA256     string                                    `json:"plan_sha256"`
	ExpiredCount   int64                                     `json:"expired_count"`
}

type recordingUploadIntentReconcilePlan struct {
	Version       int                                       `json:"version"`
	AccountID     int64                                     `json:"account_id"`
	RecordingIDs  []int64                                   `json:"recording_ids"`
	ExpiresBefore time.Time                                 `json:"expires_before"`
	Candidates    []recordingUploadIntentReconcileCandidate `json:"candidates"`
}

type recordingUploadIntentJobFence struct {
	RecordingID int64
	Status      string
	Attempt     int
}

func normalizeRecordingIDs(ids []int64) ([]int64, error) {
	if len(ids) == 0 || len(ids) > 50 {
		return nil, errors.New("recording_ids must contain 1 to 50 ids")
	}
	seen := make(map[int64]struct{}, len(ids))
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			return nil, errors.New("recording_ids must be positive")
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func recordingUploadIntentPlanSHA(plan recordingUploadIntentReconcilePlan) (string, error) {
	raw, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// handleAdminRecordingUploadIntentReconcile expires only presign ledger rows
// that can no longer authorize a PUT and are not owned by the job's current
// live lease generation. The dry-run/apply plan hash and row locks make a
// generation change between review and mutation fail closed.
func (s *Server) handleAdminRecordingUploadIntentReconcile(w http.ResponseWriter, r *http.Request) {
	var req recordingUploadIntentReconcileRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	ids, err := normalizeRecordingIDs(req.RecordingIDs)
	if err != nil || req.AccountID <= 0 || req.ExpiresBefore.IsZero() {
		util.WriteError(w, http.StatusBadRequest, "account_id, recording_ids, and expires_before are required")
		return
	}
	cutoff := req.ExpiresBefore.UTC()
	if cutoff.After(time.Now().UTC().Add(-recordingUploadIntentReconcileSafetyMargin)) {
		util.WriteError(w, http.StatusBadRequest, "expires_before must be at least two minutes in the past")
		return
	}
	if req.Apply {
		expected := strings.ToLower(strings.TrimSpace(req.ExpectedPlanSHA256))
		if len(expected) != 64 {
			util.WriteError(w, http.StatusBadRequest, "expected_plan_sha256 is required when apply=true")
			return
		}
		if strings.TrimSpace(req.Reason) == "" {
			util.WriteError(w, http.StatusBadRequest, "reason is required when apply=true")
			return
		}
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin recording upload-intent reconciliation")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SET LOCAL lock_timeout='1s'; SET LOCAL statement_timeout='15s'`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "bound recording upload-intent reconciliation")
		return
	}
	// Lock the bounded account-scoped job set first. The upload-intent ledger has
	// an index on recording_job_id but no recording_id/expiry index, so beginning
	// with that ledger can degrade into a fleet-wide scan. Splitting the lock and
	// lookup retains the same generation fence while forcing the second query
	// through the existing per-job index.
	jobRows, err := tx.Query(r.Context(), `
		SELECT j.id,j.recording_id,j.status,j.attempt_count
		FROM recording_jobs j
		JOIN recordings rec ON rec.id=j.recording_id
		WHERE rec.account_id=$1
		  AND j.recording_id=ANY($2::bigint[])
		  AND NOT (j.status='leased' AND j.lease_expires_at>now())
		ORDER BY j.id
		FOR UPDATE OF j
	`, req.AccountID, ids)
	if err != nil {
		log.Printf("recording upload-intent reconciliation lock jobs account=%d err=%v", req.AccountID, err)
		util.WriteError(w, http.StatusInternalServerError, "lock recording upload-intent jobs")
		return
	}
	jobIDs := make([]int64, 0)
	jobFences := make(map[int64]recordingUploadIntentJobFence)
	for jobRows.Next() {
		var jobID int64
		var fence recordingUploadIntentJobFence
		if err = jobRows.Scan(&jobID, &fence.RecordingID, &fence.Status, &fence.Attempt); err != nil {
			jobRows.Close()
			util.WriteError(w, http.StatusInternalServerError, "scan recording upload-intent jobs")
			return
		}
		jobIDs = append(jobIDs, jobID)
		jobFences[jobID] = fence
	}
	if err = jobRows.Err(); err != nil {
		jobRows.Close()
		log.Printf("recording upload-intent reconciliation iterate jobs account=%d err=%v", req.AccountID, err)
		util.WriteError(w, http.StatusInternalServerError, "iterate recording upload-intent jobs")
		return
	}
	jobRows.Close()

	candidates := make([]recordingUploadIntentReconcileCandidate, 0)
	if len(jobIDs) > 0 {
		rows, queryErr := tx.Query(r.Context(), `
			SELECT ui.id::text,ui.recording_id,ui.recording_job_id,ui.expires_at
			FROM recording_upload_intents ui
			WHERE ui.recording_job_id=ANY($1::bigint[])
			  AND ui.status='pending'
			  AND ui.expires_at<=$2
			  AND ui.expires_at<=now()-$3::interval
			ORDER BY ui.recording_id,ui.recording_job_id,ui.expires_at,ui.id
			FOR UPDATE OF ui
		`, jobIDs, cutoff, recordingUploadIntentReconcileSafetyMargin.String())
		if queryErr != nil {
			log.Printf("recording upload-intent reconciliation load intents account=%d jobs=%d err=%v", req.AccountID, len(jobIDs), queryErr)
			util.WriteError(w, http.StatusInternalServerError, "load expired recording upload intents")
			return
		}
		for rows.Next() {
			var candidate recordingUploadIntentReconcileCandidate
			if err = rows.Scan(&candidate.IntentID, &candidate.RecordingID, &candidate.JobID,
				&candidate.ExpiresAt); err != nil {
				rows.Close()
				util.WriteError(w, http.StatusInternalServerError, "scan expired recording upload intents")
				return
			}
			fence, ok := jobFences[candidate.JobID]
			if !ok || fence.RecordingID != candidate.RecordingID {
				rows.Close()
				util.WriteError(w, http.StatusConflict, "recording upload-intent ownership changed")
				return
			}
			candidate.JobStatus = fence.Status
			candidate.JobAttempt = fence.Attempt
			candidate.ExpiresAt = candidate.ExpiresAt.UTC()
			candidates = append(candidates, candidate)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			log.Printf("recording upload-intent reconciliation iterate intents account=%d jobs=%d err=%v", req.AccountID, len(jobIDs), err)
			util.WriteError(w, http.StatusInternalServerError, "iterate expired recording upload intents")
			return
		}
		rows.Close()
	}
	plan := recordingUploadIntentReconcilePlan{Version: 1, AccountID: req.AccountID, RecordingIDs: ids, ExpiresBefore: cutoff, Candidates: candidates}
	planSHA, err := recordingUploadIntentPlanSHA(plan)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "hash recording upload-intent plan")
		return
	}
	response := recordingUploadIntentReconcileResponse{
		Apply: req.Apply, AccountID: req.AccountID, RecordingIDs: ids,
		ExpiresBefore: cutoff, CandidateCount: len(candidates), Candidates: candidates,
		PlanSHA256: planSHA,
	}
	if !req.Apply {
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit recording upload-intent dry run")
			return
		}
		util.WriteJSON(w, http.StatusOK, response)
		return
	}
	if !strings.EqualFold(strings.TrimSpace(req.ExpectedPlanSHA256), planSHA) {
		util.WriteError(w, http.StatusConflict, "expected_plan_sha256 does not match current plan")
		return
	}
	intentIDs := make([]uuid.UUID, 0, len(candidates))
	for _, candidate := range candidates {
		id, parseErr := uuid.Parse(candidate.IntentID)
		if parseErr != nil {
			util.WriteError(w, http.StatusInternalServerError, "invalid stored recording upload-intent id")
			return
		}
		intentIDs = append(intentIDs, id)
	}
	if len(intentIDs) > 0 {
		result, updateErr := tx.Exec(r.Context(), `UPDATE recording_upload_intents SET status='expired' WHERE id=ANY($1::uuid[]) AND status='pending'`, intentIDs)
		if updateErr != nil {
			util.WriteError(w, http.StatusInternalServerError, "expire recording upload intents")
			return
		}
		response.ExpiredCount = result.RowsAffected()
		if response.ExpiredCount != int64(len(intentIDs)) {
			util.WriteError(w, http.StatusConflict, "recording upload-intent plan changed during apply")
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit recording upload-intent reconciliation")
		return
	}
	log.Printf("recording upload-intent reconciliation account=%d recordings=%d expired=%d reason=%q", req.AccountID, len(ids), response.ExpiredCount, sanitizeRecordingSurrenderError(req.Reason, "upload intent reconciliation"))
	util.WriteJSON(w, http.StatusOK, response)
}
