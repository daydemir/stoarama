package api

import (
	"fmt"
	"net/http"

	"github.com/daydemir/stoarama/backend/internal/util"
)

type batchCancelRequest struct {
	RecordingIDs []int64 `json:"recording_ids"`
}

func uniqueRecordingIDs(input []int64) ([]int64, error) {
	if len(input) == 0 || len(input) > 50 {
		return nil, fmt.Errorf("recording_ids must contain 1 to 50 ids")
	}
	seen := make(map[int64]struct{}, len(input))
	ids := make([]int64, 0, len(input))
	for _, id := range input {
		if id <= 0 {
			return nil, fmt.Errorf("recording_ids must contain positive integers")
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("recording_ids contains duplicate id %d", id)
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *Server) cancelRecordings(w http.ResponseWriter, r *http.Request, accountID int64, ids []int64) bool {
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin cancel tx: %v", err))
		return false
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	rows, err := tx.Query(r.Context(), `
		UPDATE recordings SET status='canceled', next_fire_at=NULL, paused_at=NULL, updated_at=now()
		WHERE account_id=$1 AND id=ANY($2::bigint[])
		RETURNING id
	`, accountID, ids)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("cancel recordings: %v", err))
		return false
	}
	count := 0
	for rows.Next() {
		count++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("cancel recordings: %v", err))
		return false
	}
	if count != len(ids) {
		util.WriteError(w, http.StatusNotFound, "one or more recordings were not found")
		return false
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE recording_jobs
		SET status='canceled', lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
		WHERE recording_id=ANY($1::bigint[]) AND status IN ('pending','leased')
	`, ids); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("cancel recording jobs: %v", err))
		return false
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit cancel tx: %v", err))
		return false
	}
	return true
}

func (s *Server) handleAccountRecordingsBatchCancel(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req batchCancelRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	ids, err := uniqueRecordingIDs(req.RecordingIDs)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.cancelRecordings(w, r, principal.AccountID, ids) {
		return
	}
	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "recordings_canceled", "account", principal.Email, map[string]any{"recording_ids": ids, "count": len(ids)})
	util.WriteJSON(w, http.StatusOK, map[string]any{"recording_ids": ids, "canceled": len(ids)})
}
