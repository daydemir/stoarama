package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

// Upload probes are retired; historical rows and objects are preserved.
const nasUploadProbeDisabledSec = 3600

type nasUploadProbeResponse struct {
	Enabled       bool `json:"enabled"`
	Due           bool `json:"due"`
	RetryAfterSec int  `json:"retry_after_sec"`
}

func (s *Server) nasUploadProbeConnection(ctx context.Context, q pgx.Tx, w http.ResponseWriter, r *http.Request) (int64, bool) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return 0, false
	}
	if principal.APIKeyID == nil {
		util.WriteError(w, http.StatusForbidden, "upload probe requires a NAS pull key")
		return 0, false
	}
	var connectionID int64
	err := q.QueryRow(ctx, `SELECT id FROM connections WHERE api_key_id=$1 AND account_id=$2 AND kind='nas_pull' FOR UPDATE`,
		*principal.APIKeyID, principal.AccountID).Scan(&connectionID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusForbidden, "no connection for this key")
		return 0, false
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load upload probe connection failed")
		return 0, false
	}
	return connectionID, true
}

func (s *Server) handleAccountConnectionUploadProbe(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.nasRestoreConnection(r.Context(), w, r); !ok {
		return
	}
	util.WriteJSON(w, http.StatusOK, nasUploadProbeResponse{RetryAfterSec: nasUploadProbeDisabledSec})
}

func (s *Server) handleAccountConnectionUploadProbeResult(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.nasRestoreConnection(r.Context(), w, r); !ok {
		return
	}
	util.WriteError(w, http.StatusConflict, "NAS upload probes are retired; existing observations are preserved")
}
