package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/util"
)

type adminStreamSoftDeleteRequest struct {
	Reason string `json:"reason"`
}

func (s *Server) handleAdminStreamSoftDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req adminStreamSoftDeleteRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			util.WriteError(w, http.StatusBadRequest, "invalid json")
			return
		}
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		util.WriteError(w, http.StatusBadRequest, "reason is required")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin stream delete: %v", err))
		return
	}
	defer tx.Rollback(r.Context())

	var activeRecordings int64
	if err := tx.QueryRow(r.Context(), `
		SELECT COUNT(*)::bigint
		FROM recordings
		WHERE stream_id=$1 AND status='active'
	`, id).Scan(&activeRecordings); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check active recordings: %v", err))
		return
	}
	if activeRecordings > 0 {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("stream has %d active recording(s)", activeRecordings))
		return
	}

	ct, err := tx.Exec(r.Context(), `
		UPDATE streams
		SET deleted_at=now(),
		    deleted_by_account_id=$2,
		    deleted_reason=$3,
		    recording_state='off',
		    updated_at=now()
		WHERE id=$1 AND deleted_at IS NULL
	`, id, principal.AccountID, reason)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("soft delete stream: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "stream not found")
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, nil, "stream_soft_deleted", "stream", principal.Email, map[string]any{
		"stream_id": id,
		"reason":    reason,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert stream delete auth event: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit stream delete: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "stream_id": id})
}
