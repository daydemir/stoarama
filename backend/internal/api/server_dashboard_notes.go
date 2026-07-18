package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const maxStreamNoteLength = 10000

type dashboardStreamNoteRequest struct {
	Note string `json:"note"`
}

func (s *Server) handleDashboardStreamNotePut(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok || principal.AccountID <= 0 {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req dashboardStreamNoteRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.Note = strings.TrimSpace(req.Note)
	if len(req.Note) > maxStreamNoteLength {
		util.WriteError(w, http.StatusBadRequest, "note must be at most 10000 characters")
		return
	}
	if req.Note == "" {
		s.deleteDashboardStreamNote(w, r, principal.AccountID, streamID)
		return
	}
	var note string
	err := s.pool.QueryRow(r.Context(), `
		INSERT INTO account_stream_notes (account_id, stream_id, note)
		SELECT $1, id, $3 FROM streams WHERE id=$2 AND deleted_at IS NULL
		ON CONFLICT (account_id, stream_id) DO UPDATE SET note=EXCLUDED.note, updated_at=now()
		RETURNING note`, principal.AccountID, streamID, req.Note).Scan(&note)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "stream not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("save stream note: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "stream_id": streamID, "note": note})
}

func (s *Server) handleDashboardStreamNoteDelete(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok || principal.AccountID <= 0 {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	s.deleteDashboardStreamNote(w, r, principal.AccountID, streamID)
}

func (s *Server) deleteDashboardStreamNote(w http.ResponseWriter, r *http.Request, accountID, streamID int64) {
	if _, err := s.pool.Exec(r.Context(), `DELETE FROM account_stream_notes WHERE account_id=$1 AND stream_id=$2`, accountID, streamID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete stream note: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "stream_id": streamID, "note": ""})
}
