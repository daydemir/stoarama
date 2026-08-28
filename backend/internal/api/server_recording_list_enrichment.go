package api

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const (
	recordingListEnrichmentTimeout = 5 * time.Second
	recordingJoinedProgressTimeout = 8 * time.Second
)

type recordingListEnrichmentItem struct {
	RecordingID       int64                    `json:"recording_id"`
	CaptureHealthBins []recordingHealthBin     `json:"capture_health_bins,omitempty"`
	TimelineHealth    *recordingTimelineHealth `json:"timeline_health,omitempty"`
}

type recordingJoinedProgressItem struct {
	RecordingID      int64 `json:"recording_id"`
	SourceDurationMS int64 `json:"source_duration_ms"`
	JoinedReadyMS    int64 `json:"joined_ready_ms"`
	Percent          *int  `json:"joined_percent"`
}

func requestedRecordingMetricID(r *http.Request) (int64, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("recording_id"))
	if raw == "" {
		return 0, nil
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("recording_id must be a positive integer")
	}
	return id, nil
}

func (s *Server) recordingMetricScopeIDs(ctx context.Context, accountID, recordingID int64, shared bool) ([]int64, error) {
	query := `SELECT id FROM recordings WHERE account_id=$1 AND status<>'canceled'`
	args := []any{accountID}
	if shared {
		query = `SELECT id FROM recordings WHERE account_id=$1 AND status IN ('active','paused','completed')`
	}
	if recordingID > 0 {
		query += ` AND id=$2`
		args = append(args, recordingID)
	}
	query += ` ORDER BY created_at DESC,id DESC`
	if shared && recordingID == 0 {
		query += ` LIMIT ` + strconv.Itoa(sharedRecordingsListLimit)
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if recordingID > 0 && len(ids) == 0 {
		return nil, pgx.ErrNoRows
	}
	return ids, nil
}

func writeRecordingMetricError(w http.ResponseWriter, err error, label string) {
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		w.Header().Set("Retry-After", "10")
		util.WriteError(w, http.StatusServiceUnavailable, label+" temporarily unavailable")
		return
	}
	util.WriteError(w, http.StatusInternalServerError, label)
}

func (s *Server) handleAccountRecordingListEnrichment(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	s.handleRecordingListEnrichment(w, r, principal.AccountID, false)
}

func (s *Server) handleSharedRecordingListEnrichment(w http.ResponseWriter, r *http.Request) {
	s.handleRecordingListEnrichment(w, r, s.cfg.SharedRecordingsAccountID, true)
}

func (s *Server) handleRecordingListEnrichment(w http.ResponseWriter, r *http.Request, accountID int64, shared bool) {
	recordingID, err := requestedRecordingMetricID(r)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	ids, err := s.recordingMetricScopeIDs(r.Context(), accountID, recordingID, shared)
	if err != nil {
		writeRecordingMetricError(w, err, "load recording metrics")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), recordingListEnrichmentTimeout)
	defer cancel()
	bins, err := s.recordingHealthBinsForAccount(ctx, accountID, ids)
	if err != nil {
		log.Printf("recording list enrichment failed account_id=%d: %v", accountID, err)
		writeRecordingMetricError(w, err, "load recording metrics")
		return
	}
	timeline, err := s.recordingTimelineHealthForAccount(ctx, accountID, ids)
	if err != nil {
		log.Printf("recording list timeline enrichment failed account_id=%d: %v", accountID, err)
		writeRecordingMetricError(w, err, "load recording metrics")
		return
	}
	items := make([]recordingListEnrichmentItem, 0, len(ids))
	for _, id := range ids {
		item := recordingListEnrichmentItem{RecordingID: id, CaptureHealthBins: bins[id]}
		if health, ok := timeline[id]; ok {
			item.TimelineHealth = &health
		}
		items = append(items, item)
	}
	w.Header().Set("Cache-Control", "private, no-store")
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleAccountRecordingJoinedProgress(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	s.handleRecordingJoinedProgress(w, r, principal.AccountID, false)
}

func (s *Server) handleSharedRecordingJoinedProgress(w http.ResponseWriter, r *http.Request) {
	s.handleRecordingJoinedProgress(w, r, s.cfg.SharedRecordingsAccountID, true)
}

func (s *Server) handleRecordingJoinedProgress(w http.ResponseWriter, r *http.Request, accountID int64, shared bool) {
	recordingID, err := requestedRecordingMetricID(r)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	ids, err := s.recordingMetricScopeIDs(r.Context(), accountID, recordingID, shared)
	if err != nil {
		writeRecordingMetricError(w, err, "load joined progress")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), recordingJoinedProgressTimeout)
	defer cancel()
	progress, err := s.recordingJoinedProgressForAccount(ctx, accountID, ids)
	if err != nil {
		log.Printf("recording joined progress failed account_id=%d: %v", accountID, err)
		writeRecordingMetricError(w, err, "load joined progress")
		return
	}
	items := make([]recordingJoinedProgressItem, 0, len(ids))
	for _, id := range ids {
		value := progress[id]
		items = append(items, recordingJoinedProgressItem{
			RecordingID: id, SourceDurationMS: value.SourceDurationMS,
			JoinedReadyMS: value.JoinedReadyMS, Percent: value.Percent,
		})
	}
	if direction := recordingJoinedSortDirection(r.URL.Query().Get("sort")); direction != 0 {
		sort.SliceStable(items, func(i, j int) bool {
			left := recordingJoinedProgress{SourceDurationMS: items[i].SourceDurationMS, JoinedReadyMS: items[i].JoinedReadyMS, Percent: items[i].Percent}
			right := recordingJoinedProgress{SourceDurationMS: items[j].SourceDurationMS, JoinedReadyMS: items[j].JoinedReadyMS, Percent: items[j].Percent}
			return recordingJoinedComesFirst(items[i].RecordingID, items[j].RecordingID, left, right, direction)
		})
	}
	w.Header().Set("Cache-Control", "private, no-store")
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}
