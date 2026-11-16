package api

import (
	"context"
	"net/http"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
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
	return s.nasConnection(ctx, w, r, nasConnectionMetadata)
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
