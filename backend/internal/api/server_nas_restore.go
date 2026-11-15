package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

// NAS restores are retired. Empty leases issue no capabilities. Reports refuse
// before verification, mismatch deletion, unpurge or durable row transitions.
const nasRestoreIdleRetrySec = 120

type nasRestoreTask struct {
	TaskID       int64                   `json:"task_id"`
	Attempt      int                     `json:"attempt"`
	ClipID       int64                   `json:"clip_id"`
	RelativePath string                  `json:"relative_path"`
	SizeBytes    int64                   `json:"size_bytes"`
	SHA256       string                  `json:"sha256"`
	ExpiresAt    time.Time               `json:"expires_at"`
	Put          nasRestoreSignedRequest `json:"put"`
}
type nasRestoreSignedRequest struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
}
type nasRestoreLeaseResponse struct {
	RetryAfterSec int              `json:"retry_after_sec"`
	Tasks         []nasRestoreTask `json:"tasks"`
}

// nasRestoreConnection resolves the caller's NAS pull connection without
// locking it: heartbeats update the same row every 30 seconds.
func (s *Server) nasRestoreConnection(ctx context.Context, w http.ResponseWriter, r *http.Request) (int64, bool) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return 0, false
	}
	if principal.APIKeyID == nil {
		util.WriteError(w, http.StatusForbidden, "operation requires a NAS pull key")
		return 0, false
	}
	var connectionID int64
	err := s.pool.QueryRow(ctx, `SELECT id FROM connections WHERE api_key_id=$1 AND account_id=$2 AND kind='nas_pull'`,
		*principal.APIKeyID, principal.AccountID).Scan(&connectionID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusForbidden, "no connection for this key")
		return 0, false
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load restore connection failed")
		return 0, false
	}
	return connectionID, true
}

// handleAccountConnectionRestoreLease returns the existing empty-lease protocol.
// Pending requests remain durable and no PUT capability is issued.
func (s *Server) handleAccountConnectionRestoreLease(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.nasRestoreConnection(r.Context(), w, r); !ok {
		return
	}
	util.WriteJSON(w, http.StatusOK, nasRestoreLeaseResponse{RetryAfterSec: nasRestoreIdleRetrySec, Tasks: []nasRestoreTask{}})
}

func (s *Server) handleAccountConnectionRestoreResult(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.nasRestoreConnection(r.Context(), w, r); !ok {
		return
	}
	util.WriteError(w, http.StatusConflict, "NAS restores are retired; durable requests and objects are preserved")
}
