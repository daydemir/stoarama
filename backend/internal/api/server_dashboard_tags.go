package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// Shared catalog tagging for any signed-in browser session (member or admin).
// These endpoints mutate the SHARED streams.tags column so every team member
// sees the same tags. Only single add (dedup + append) and single remove (drop
// one named tag) are exposed; no destructive replace-all is reachable here.

type dashboardStreamTagsAddRequest struct {
	Tags []string `json:"tags"`
}

type dashboardStreamTagsRemoveRequest struct {
	Tag string `json:"tag"`
}

// handleDashboardStreamTagsAdd dedups + appends one or more tags to the shared
// streams.tags array, computed from the current row inside a tx so a stale
// client cannot wipe the array.
func (s *Server) handleDashboardStreamTagsAdd(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req dashboardStreamTagsAddRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tagsToAdd := dedupeStrings(req.Tags)
	if len(tagsToAdd) == 0 {
		util.WriteError(w, http.StatusBadRequest, "tags must contain at least one tag")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin tag update tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	current, err := s.loadStreamForAssignmentTx(r.Context(), tx, streamID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "stream not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load stream: %v", err))
		return
	}
	updatedTags := dedupeStrings(append(current.Tags, tagsToAdd...))
	if _, err := tx.Exec(r.Context(), `
		UPDATE streams
		SET tags=$2, updated_at=now()
		WHERE id=$1
	`, streamID, updatedTags); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update stream tags: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit tag update tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"stream_id": streamID,
		"tags":      updatedTags,
	})
}

// handleDashboardStreamTagsRemove strips exactly one named tag from the shared
// streams.tags array. No-op-safe if the tag is absent.
func (s *Server) handleDashboardStreamTagsRemove(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req dashboardStreamTagsRemoveRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tagToRemove := strings.TrimSpace(req.Tag)
	if tagToRemove == "" {
		util.WriteError(w, http.StatusBadRequest, "tag is required")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin tag update tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	current, err := s.loadStreamForAssignmentTx(r.Context(), tx, streamID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "stream not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load stream: %v", err))
		return
	}
	kept := make([]string, 0, len(current.Tags))
	for _, tag := range current.Tags {
		if tag == tagToRemove {
			continue
		}
		kept = append(kept, tag)
	}
	updatedTags := dedupeStrings(kept)
	if _, err := tx.Exec(r.Context(), `
		UPDATE streams
		SET tags=$2, updated_at=now()
		WHERE id=$1
	`, streamID, updatedTags); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update stream tags: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit tag update tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"stream_id": streamID,
		"tags":      updatedTags,
	})
}
