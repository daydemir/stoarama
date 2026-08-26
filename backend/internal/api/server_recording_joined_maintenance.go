package api

import (
	"context"
	"net/http"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
)

const joinedAttemptReconcileInterval = time.Minute

type joinedAttemptReconcileRequest struct {
	BatchID string `json:"batch_id"`
}

type joinedAttemptReconcileResponse struct {
	BatchID               string `json:"batch_id"`
	Skipped               bool   `json:"skipped"`
	TerminalizedHours     int64  `json:"terminalized_hours"`
	TerminalizedArtifacts int64  `json:"terminalized_artifacts"`
}

func (s *Server) handleJoinedReconcileExpiredAttempts(w http.ResponseWriter, r *http.Request) {
	var req joinedAttemptReconcileRequest
	if err := util.DecodeJSON(r, &req); err != nil || req.BatchID != s.cfg.JoinedRecordingBatchID || !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusBadRequest, "invalid joined expired-attempt reconciliation")
		return
	}
	now := time.Now().UTC()
	s.joinedAttemptReconcileMu.Lock()
	if !s.joinedAttemptReconcileAt.IsZero() && now.Sub(s.joinedAttemptReconcileAt) < joinedAttemptReconcileInterval {
		s.joinedAttemptReconcileMu.Unlock()
		util.WriteJSON(w, http.StatusOK, joinedAttemptReconcileResponse{BatchID: req.BatchID, Skipped: true})
		return
	}
	s.joinedAttemptReconcileAt = now
	s.joinedAttemptReconcileMu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	hours, artifacts, err := s.recordJoinedExpiredAttemptEvidence(ctx, req.BatchID, s.joinedCanaryHourIDs(), s.joinedFrozenBatchScope())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "reconcile joined expired attempts")
		return
	}
	util.WriteJSON(w, http.StatusOK, joinedAttemptReconcileResponse{BatchID: req.BatchID,
		TerminalizedHours: hours, TerminalizedArtifacts: artifacts})
}
