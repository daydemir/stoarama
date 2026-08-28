package api

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const (
	recordingListEnrichmentTimeout = 5 * time.Second
	recordingJoinedProgressTimeout = 8 * time.Second
	recordingEnrichmentCacheTTL    = 30 * time.Second
	recordingProgressCacheTTL      = 60 * time.Second
	recordingHealthPageCacheTTL    = 30 * time.Second
	recordingMetricFailureTTL      = 5 * time.Second
	recordingMetricConcurrency     = 2
)

type recordingMetricCacheKey struct {
	AccountID   int64
	RecordingID int64
	Shared      bool
	Scope       string
}

type recordingMetricCacheEntry[T any] struct {
	Value     T
	Err       error
	ExpiresAt time.Time
}

type recordingMetricFlight[T any] struct {
	Done  chan struct{}
	Value T
	Err   error
}

type recordingMetricCache[T any] struct {
	Mu      sync.Mutex
	Entries map[recordingMetricCacheKey]recordingMetricCacheEntry[T]
	Flights map[recordingMetricCacheKey]*recordingMetricFlight[T]
}

type recordingListEnrichmentResult struct {
	Bins     map[int64][]recordingHealthBin
	Timeline map[int64]recordingTimelineHealth
}

type recordingListEnrichmentItem struct {
	RecordingID       int64                    `json:"recording_id"`
	CaptureHealthBins []recordingHealthBin     `json:"capture_health_bins"`
	TimelineHealth    *recordingTimelineHealth `json:"timeline_health"`
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

func recordingMetricScopeSignature(ids []int64) string {
	var scope strings.Builder
	for i, id := range ids {
		if i > 0 {
			scope.WriteByte(',')
		}
		scope.WriteString(strconv.FormatInt(id, 10))
	}
	return scope.String()
}

func (s *Server) recordingMetricWorkSlots() chan struct{} {
	s.recordingMetricSlotsMu.Lock()
	defer s.recordingMetricSlotsMu.Unlock()
	if s.recordingMetricSlots == nil {
		s.recordingMetricSlots = make(chan struct{}, recordingMetricConcurrency)
	}
	return s.recordingMetricSlots
}

// loadRecordingMetricCached collapses identical requests and admits at most two
// expensive recording-metric loaders per API instance. Each caller can stop
// waiting independently, while the shared flight keeps the initiating request's
// absolute endpoint deadline and cannot be canceled by one disconnected client.
func loadRecordingMetricCached[T any](ctx context.Context, cache *recordingMetricCache[T], key recordingMetricCacheKey, loadBudget, successTTL, failureTTL time.Duration, slots chan struct{}, loader func(context.Context) (T, error)) (T, error) {
	now := time.Now()
	cache.Mu.Lock()
	if entry, ok := cache.Entries[key]; ok && now.Before(entry.ExpiresAt) {
		cache.Mu.Unlock()
		return entry.Value, entry.Err
	}
	if flight, ok := cache.Flights[key]; ok {
		cache.Mu.Unlock()
		return waitForRecordingMetricFlight(ctx, flight)
	}
	if cache.Entries == nil {
		cache.Entries = make(map[recordingMetricCacheKey]recordingMetricCacheEntry[T])
	}
	if cache.Flights == nil {
		cache.Flights = make(map[recordingMetricCacheKey]*recordingMetricFlight[T])
	}
	for staleKey, entry := range cache.Entries {
		if !now.Before(entry.ExpiresAt) {
			delete(cache.Entries, staleKey)
		}
	}
	flight := &recordingMetricFlight[T]{Done: make(chan struct{})}
	cache.Flights[key] = flight
	cache.Mu.Unlock()
	loadCtx, cancel := detachedRecordingMetricContext(ctx, loadBudget)
	go runRecordingMetricFlight(loadCtx, cancel, cache, key, flight, successTTL, failureTTL, slots, loader)
	return waitForRecordingMetricFlight(ctx, flight)
}

func waitForRecordingMetricFlight[T any](ctx context.Context, flight *recordingMetricFlight[T]) (T, error) {
	var zero T
	select {
	case <-flight.Done:
		return flight.Value, flight.Err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

func detachedRecordingMetricContext(ctx context.Context, loadBudget time.Duration) (context.Context, context.CancelFunc) {
	parent := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(parent, deadline)
	}
	return context.WithTimeout(parent, loadBudget)
}

func runRecordingMetricFlight[T any](ctx context.Context, cancel context.CancelFunc, cache *recordingMetricCache[T], key recordingMetricCacheKey, flight *recordingMetricFlight[T], successTTL, failureTTL time.Duration, slots chan struct{}, loader func(context.Context) (T, error)) {
	defer cancel()
	var value T
	var err error
	select {
	case slots <- struct{}{}:
		ctxErr := ctx.Err()
		if deadline, ok := ctx.Deadline(); ctxErr == nil && ok && !time.Now().Before(deadline) {
			ctxErr = context.DeadlineExceeded
		}
		if ctxErr != nil {
			err = ctxErr
		} else {
			value, err = loader(ctx)
		}
		<-slots
	case <-ctx.Done():
		err = ctx.Err()
	}

	cache.Mu.Lock()
	flight.Value = value
	flight.Err = err
	delete(cache.Flights, key)
	ttl := successTTL
	if err != nil {
		ttl = failureTTL
	}
	// A caller disconnect should not make a healthy metric key fail for every
	// other browser. Deadlines and database failures are briefly cached to stop
	// retry herds.
	if ttl > 0 && !errors.Is(err, context.Canceled) {
		cache.Entries[key] = recordingMetricCacheEntry[T]{Value: value, Err: err, ExpiresAt: time.Now().Add(ttl)}
	}
	close(flight.Done)
	cache.Mu.Unlock()
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
	ctx, cancel := context.WithTimeout(r.Context(), recordingListEnrichmentTimeout)
	defer cancel()
	ids, err := s.recordingMetricScopeIDs(ctx, accountID, recordingID, shared)
	if err != nil {
		writeRecordingMetricError(w, err, "load recording metrics")
		return
	}
	key := recordingMetricCacheKey{AccountID: accountID, RecordingID: recordingID, Shared: shared, Scope: recordingMetricScopeSignature(ids)}
	result, err := loadRecordingMetricCached(ctx, &s.recordingEnrichmentCache, key, recordingListEnrichmentTimeout, recordingEnrichmentCacheTTL, recordingMetricFailureTTL, s.recordingMetricWorkSlots(), func(loadCtx context.Context) (recordingListEnrichmentResult, error) {
		bins, loadErr := s.recordingHealthBinsForAccount(loadCtx, accountID, ids)
		if loadErr != nil {
			return recordingListEnrichmentResult{}, loadErr
		}
		if loadErr := s.populateRecordingListJoinedProgressBins(loadCtx, accountID, ids, bins); loadErr != nil {
			return recordingListEnrichmentResult{}, loadErr
		}
		timeline, loadErr := s.recordingTimelineHealthForAccount(loadCtx, accountID, ids)
		if loadErr != nil {
			return recordingListEnrichmentResult{}, loadErr
		}
		return recordingListEnrichmentResult{Bins: bins, Timeline: timeline}, nil
	})
	if err != nil {
		log.Printf("recording list enrichment failed account_id=%d: %v", accountID, err)
		writeRecordingMetricError(w, err, "load recording metrics")
		return
	}
	items := make([]recordingListEnrichmentItem, 0, len(ids))
	for _, id := range ids {
		bins := result.Bins[id]
		if bins == nil {
			bins = []recordingHealthBin{}
		}
		item := recordingListEnrichmentItem{RecordingID: id, CaptureHealthBins: bins}
		if health, ok := result.Timeline[id]; ok {
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
	ctx, cancel := context.WithTimeout(r.Context(), recordingJoinedProgressTimeout)
	defer cancel()
	ids, err := s.recordingMetricScopeIDs(ctx, accountID, recordingID, shared)
	if err != nil {
		writeRecordingMetricError(w, err, "load joined progress")
		return
	}
	key := recordingMetricCacheKey{AccountID: accountID, RecordingID: recordingID, Shared: shared, Scope: recordingMetricScopeSignature(ids)}
	progress, err := loadRecordingMetricCached(ctx, &s.recordingProgressCache, key, recordingJoinedProgressTimeout, recordingProgressCacheTTL, recordingMetricFailureTTL, s.recordingMetricWorkSlots(), func(loadCtx context.Context) (map[int64]recordingJoinedProgress, error) {
		return s.recordingJoinedProgressForAccount(loadCtx, accountID, ids)
	})
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

func (s *Server) populateRecordingListJoinedProgressBins(ctx context.Context, accountID int64, recordingIDs []int64, bins map[int64][]recordingHealthBin) error {
	type binRef struct {
		recordingID int64
		index       int
	}
	refs := make([]binRef, 0, len(recordingIDs)*recentHealthBinCount)
	binRecordingIDs := make([]int64, 0, cap(refs))
	starts := make([]time.Time, 0, cap(refs))
	ends := make([]time.Time, 0, cap(refs))
	for _, recordingID := range recordingIDs {
		for index, bin := range bins[recordingID] {
			refs = append(refs, binRef{recordingID: recordingID, index: index})
			binRecordingIDs = append(binRecordingIDs, recordingID)
			starts = append(starts, bin.Start)
			ends = append(ends, bin.End)
		}
	}
	if len(refs) == 0 {
		return nil
	}
	progress, err := s.recordingJoinedProgressForBins(ctx, accountID, binRecordingIDs, starts, ends)
	if err != nil {
		return err
	}
	if len(progress) != len(refs) {
		return errors.New("joined health bin count mismatch")
	}
	for index, ref := range refs {
		bin := &bins[ref.recordingID][ref.index]
		bin.SourceDurationMS = progress[index].SourceDurationMS
		bin.JoinedReadyMS = progress[index].JoinedReadyMS
	}
	return nil
}
