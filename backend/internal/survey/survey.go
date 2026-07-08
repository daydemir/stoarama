// Package survey captures one JPEG per stream per day and stores it cheaply in
// R2 under survey/stream/{id}/{YYYY-MM-DD}.jpg. Each frame is timestamped with
// the real grab time; the daily sweep is scheduled at a randomized time of day
// so imagery varies across days. It is decoupled from the recording-health
// pipeline: it does not touch stream_health, stream_capture_runtime, or
// capture_jobs. No detection, no tags.
package survey

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ErrorTag = "error"

	// confirmMinGap is the minimum age of a first-failure marker before a second
	// failure confirms (tags) the stream. The confirmation happens on a LATER
	// sweep, not by sleeping a worker: the first failure records a marker and
	// returns immediately, so a single failure never holds a worker slot. The
	// hourly cron cadence means the next sweep is ~1h later, comfortably past
	// this gap; it exists only to reject a pathological double-run confirming a
	// blip within seconds.
	confirmMinGap = 2 * time.Minute

	// backoffThreshold is the number of consecutive confirmed failures after
	// which a stream is skipped for a backoff window. Below it, a failing stream
	// is re-checked every sweep so recoveries are picked up fast.
	backoffThreshold = 3

	// backoffBase / backoffCap bound the skip window for chronically-failing
	// streams: window = backoffBase * 2^(consecutive_failures - backoffThreshold),
	// clamped to backoffCap. This keeps broken streams from dominating every
	// sweep while still re-checking them before too long (at most backoffCap).
	backoffBase = 1 * time.Hour
	backoffCap  = 6 * time.Hour
)

// Target is a stream selected for survey capture.
type Target struct {
	ID            int64
	Provider      string
	SourceURL     string
	SourcePageURL string
	CaptureType   string
	SourceFamily  string
	ExecClass     string
}

// ObjectKey returns the deterministic R2 key for a stream's survey frame on a
// given UTC date: one per stream per day, same-day overwrite OK.
func ObjectKey(streamID int64, day time.Time) string {
	d := day.UTC()
	return fmt.Sprintf("survey/stream/%d/%04d-%02d-%02d.jpg", streamID, d.Year(), int(d.Month()), d.Day())
}

// selectTargetsQuery selects survey targets, ordered so the streams that need
// attention are reached first every sweep, and skipping streams that are in a
// backoff window. It excludes YouTube (see below) and streams already surveyed
// today.
//
// The catalog is ~13k targets but only a few hundred frames land per hourly
// sweep at the bounded concurrency, so a blind ORDER BY id starved first-time
// captures and error recoveries behind the whole id range. Ordering fixes that:
//
//   - Streams already surveyed today are dropped (the sweep never re-captures a
//     done stream). This is the old HasFrameForDay skip, folded into the query.
//   - Streams in backoff (backoffThreshold+ consecutive failures whose backoff
//     window since last_attempt_at has not elapsed) are dropped, so chronically
//     broken streams stop consuming worker slots every sweep.
//   - Remaining rows are ordered needs-attention first: currently error-tagged
//     or previously-failing streams (a survey_stream_state row exists) ahead of
//     the rest, then never-surveyed (no survey frame ever) ahead of already-
//     surveyed-on-a-prior-day, then oldest last attempt, then id. So error
//     recoveries and first-time captures happen every sweep, and a recovered
//     stream clears its tag within the hour it recovers.
//
// $1 is now() (UTC); it is a parameter so tests can pin time deterministically.
const selectTargetsQuery = `
	SELECT s.id,
	       COALESCE(s.provider, ''),
	       COALESCE(s.source_url, ''),
	       COALESCE(s.source_page_url, ''),
	       COALESCE(s.capture_type, ''),
	       COALESCE(s.source_family, ''),
	       COALESCE(s.execution_class, '')
	FROM streams s
	LEFT JOIN survey_stream_state st ON st.stream_id = s.id
	WHERE s.deleted_at IS NULL
	  AND COALESCE(s.provider, '') NOT ILIKE 'youtube'
	  AND COALESCE(s.capture_type, '') NOT IN ('youtube_watch', 'youtube_relay')
	  AND lower(COALESCE(s.source_url, '')) NOT LIKE '%youtube.com%'
	  AND lower(COALESCE(s.source_url, '')) NOT LIKE '%youtu.be%'
	  AND lower(COALESCE(s.source_page_url, '')) NOT LIKE '%youtube.com%'
	  AND lower(COALESCE(s.source_page_url, '')) NOT LIKE '%youtu.be%'
	  -- already surveyed today: skip (idempotent, matches the old per-stream skip)
	  AND NOT EXISTS (
	      SELECT 1 FROM frames f
	      WHERE f.stream_id = s.id
	        AND f.source_kind = 'survey'
	        AND (f.captured_at AT TIME ZONE 'UTC')::date = ($1 AT TIME ZONE 'UTC')::date
	  )
	  -- in backoff: chronically failing and its backoff window has not elapsed
	  AND NOT (
	      COALESCE(st.consecutive_failures, 0) >= $2
	      AND st.last_attempt_at IS NOT NULL
	      AND st.last_attempt_at + LEAST(
	              $3::interval * power(2, COALESCE(st.consecutive_failures, 0) - $2),
	              $4::interval
	          ) > $1
	  )
	ORDER BY
	  -- needs attention first: error-tagged or previously-failing
	  (('` + ErrorTag + `' = ANY(COALESCE(s.tags, ARRAY[]::text[]))) OR st.stream_id IS NOT NULL) DESC,
	  -- never surveyed (no survey frame ever) before already-surveyed-earlier
	  (NOT EXISTS (
	      SELECT 1 FROM frames f2
	      WHERE f2.stream_id = s.id AND f2.source_kind = 'survey'
	  )) DESC,
	  st.last_attempt_at ASC NULLS FIRST,
	  s.id ASC
`

// SelectTargets returns the streams to survey this sweep, prioritized and with
// chronically-failing streams in backoff excluded. See selectTargetsQuery.
func SelectTargets(ctx context.Context, pool *pgxpool.Pool) ([]Target, error) {
	// YouTube streams are excluded: resolving them requires yt-dlp hitting
	// YouTube from the survey host, which gets the server IP blocked. They are
	// also not recordable by the recorder, so the survey has no reason to fetch
	// them. Excluded by provider, capture_type, and source host to be robust to
	// any single field being unset.
	now := time.Now().UTC()
	rows, err := pool.Query(ctx, selectTargetsQuery, now, backoffThreshold, backoffBase, backoffCap)
	if err != nil {
		return nil, fmt.Errorf("select survey targets: %w", err)
	}
	defer rows.Close()
	targets := make([]Target, 0, 1024)
	for rows.Next() {
		var t Target
		if err := rows.Scan(&t.ID, &t.Provider, &t.SourceURL, &t.SourcePageURL, &t.CaptureType, &t.SourceFamily, &t.ExecClass); err != nil {
			return nil, fmt.Errorf("scan survey target: %w", err)
		}
		targets = append(targets, t)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate survey targets: %w", rows.Err())
	}
	return targets, nil
}

// HasFrameForDay reports whether a survey frame already exists for the stream on
// the given UTC date.
func HasFrameForDay(ctx context.Context, pool *pgxpool.Pool, streamID int64, day time.Time) (bool, error) {
	d := day.UTC()
	dateStr := fmt.Sprintf("%04d-%02d-%02d", d.Year(), int(d.Month()), d.Day())
	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM frames
			WHERE stream_id = $1
			  AND source_kind = 'survey'
			  AND (captured_at AT TIME ZONE 'UTC')::date = $2::date
		)
	`, streamID, dateStr).Scan(&exists); err != nil {
		return false, fmt.Errorf("check survey frame for day: %w", err)
	}
	return exists, nil
}

// CaptureFrame resolves the stream and grabs a single frame using the same
// resolve+capture flow as the capture probe.
func CaptureFrame(ctx context.Context, registry *capture.Registry, t Target, resolveTimeout, captureTimeout time.Duration) (capture.Frame, error) {
	canonical, err := capture.DeriveCanonicalStreamFields(t.SourceURL, t.SourcePageURL, t.CaptureType, t.SourceFamily, t.ExecClass)
	if err != nil {
		return capture.Frame{}, fmt.Errorf("derive canonical fields: %w", err)
	}
	mode := capture.LegacyModeForStream(canonical.CaptureType, canonical.ExecutionClass)
	spec := capture.StreamSpec{
		ID:            t.ID,
		Provider:      t.Provider,
		StreamURL:     t.SourceURL,
		SourcePageURL: t.SourcePageURL,
		CaptureMode:   mode,
		TargetFPS:     1,
	}
	effective := capture.EffectiveMode(spec)
	if effective == capture.ModeUnsupported {
		return capture.Frame{}, fmt.Errorf("capture mode unsupported for stream %d", t.ID)
	}
	adapter, ok := registry.Get(effective)
	if !ok {
		return capture.Frame{}, fmt.Errorf("adapter not found for mode %s (stream %d)", effective, t.ID)
	}
	resolveCtx, cancelResolve := context.WithTimeout(ctx, resolveTimeout)
	resolved, err := adapter.Resolve(resolveCtx, spec)
	cancelResolve()
	if err != nil {
		return capture.Frame{}, fmt.Errorf("resolve capture source for stream %d: %w", t.ID, err)
	}
	capCtx, cancelCap := context.WithTimeout(ctx, captureTimeout)
	defer cancelCap()
	// Image sources (image_poll) are a direct JPEG fetch. Video sources
	// (hls_live / ffmpeg_direct, IsImage==false) must go through the recorder's
	// ffmpeg network-input args via CaptureSingleFrame: the bare ffmpeg in
	// capture.CaptureFrame segfaults on these network inputs, which is why every
	// hls/http_video survey was failing and writing no row.
	var frame capture.Frame
	if resolved.IsImage {
		frame, err = capture.CaptureFrame(capCtx, resolved.URL)
	} else {
		frame, err = capture.CaptureSingleFrameWithHeaders(capCtx, resolved.URL, "", resolved.InputHeaders)
	}
	if err != nil {
		return capture.Frame{}, fmt.Errorf("capture frame for stream %d: %w", t.ID, err)
	}
	return frame, nil
}

// DetectionCounts is the metrics-only per-class survivor counts for one surveyed
// frame (person + all vehicle classes; train excluded). No boxes are retained.
type DetectionCounts struct {
	Person     int
	Bicycle    int
	Car        int
	Motorcycle int
	Bus        int
	Truck      int
}

// Detector runs yolo11x detection on a survey JPEG and returns per-class counts.
// It is an interface so the survey package stays free of the ONNX/cgo dependency
// (its unit tests run without the model); the concrete detector is injected by the
// stoaramactl command. It is loaded ONCE and reused across the whole sweep.
type Detector interface {
	// Detect returns per-class counts and the inference wall-clock in ms.
	Detect(jpegBytes []byte) (DetectionCounts, int, error)
	PipelineVersion() string
	ConfThreshold() float64
	Imgsz() int
}

// DetectionResult is the fully-formed survey_detections row to insert alongside a
// frame. A nil *DetectionResult means the frame was not sampled for detection.
type DetectionResult struct {
	PipelineVersion string
	ConfThreshold   float64
	Imgsz           int
	Counts          DetectionCounts
	DetectMs        int
}

// Persist uploads the frame to R2 and inserts a survey frame row. It mirrors
// persistCaptureSuccess but uses the survey object key and source_kind='survey',
// and does NOT touch stream_health, stream_capture_runtime, or capture_jobs. When
// det is non-nil, the survey_detections row is inserted in the SAME transaction as
// the frame (keyed by the returned frame id), so a detection insert failure rolls
// the frame back cleanly (fail-fast, no partial).
func Persist(ctx context.Context, pool *pgxpool.Pool, r2c *r2.Client, streamID int64, day time.Time, capturedAt time.Time, frame capture.Frame, det *DetectionResult) error {
	objectKey := ObjectKey(streamID, day)
	etag, err := r2c.PutBytes(ctx, objectKey, frame.MIMEType, frame.Bytes)
	if err != nil {
		return fmt.Errorf("upload survey frame: %w", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin survey tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	mediaID, err := storage.UpsertMediaObject(ctx, tx, storage.MediaObjectInput{
		StorageProvider: "r2",
		Bucket:          r2c.Bucket(),
		ObjectKey:       objectKey,
		MIMEType:        frame.MIMEType,
		SizeBytes:       frame.SizeBytes,
		ETag:            etag,
		SHA256:          frame.SHA256,
		Width:           frame.Width,
		Height:          frame.Height,
	})
	if err != nil {
		return fmt.Errorf("upsert survey media object: %w", err)
	}
	var frameID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO frames (stream_id, capture_job_id, captured_at, raw_media_object_id, capture_status, capture_error, source_kind)
		VALUES ($1, NULL, $2, $3, 'success', NULL, 'survey')
		RETURNING id
	`, streamID, capturedAt, mediaID).Scan(&frameID); err != nil {
		return fmt.Errorf("insert survey frame: %w", err)
	}
	if det != nil {
		if _, err := tx.Exec(ctx, `
			INSERT INTO survey_detections (
				frame_id, stream_id, captured_at, pipeline_version, conf_threshold, imgsz,
				person_count, bicycle_count, car_count, motorcycle_count, bus_count, truck_count, detect_ms
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			ON CONFLICT (frame_id, pipeline_version) DO NOTHING
		`, frameID, streamID, capturedAt, det.PipelineVersion, det.ConfThreshold, det.Imgsz,
			det.Counts.Person, det.Counts.Bicycle, det.Counts.Car, det.Counts.Motorcycle, det.Counts.Bus, det.Counts.Truck, det.DetectMs); err != nil {
			return fmt.Errorf("insert survey detection for stream %d frame %d: %w", streamID, frameID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit survey tx: %w", err)
	}
	return nil
}

// surveyState is a stream's row in survey_stream_state (zero value = no row).
type surveyState struct {
	consecutiveFailures int
	firstFailureAt      time.Time // zero if no prior failure
}

// loadSurveyState reads the stream's survey_stream_state row. A missing row
// (never failed recently) returns the zero value and found=false.
func loadSurveyState(ctx context.Context, pool *pgxpool.Pool, streamID int64) (surveyState, bool, error) {
	var st surveyState
	var first *time.Time
	err := pool.QueryRow(ctx, `
		SELECT consecutive_failures, first_failure_at
		FROM survey_stream_state
		WHERE stream_id = $1
	`, streamID).Scan(&st.consecutiveFailures, &first)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return surveyState{}, false, nil
		}
		return surveyState{}, false, fmt.Errorf("load survey state for stream %d: %w", streamID, err)
	}
	if first != nil {
		st.firstFailureAt = *first
	}
	return st, true, nil
}

// shouldConfirmError reports whether a fresh failure should be confirmed (tagged
// + counted) now, given the prior state. The first failure in a series only
// records a marker; a subsequent failure at least confirmMinGap later confirms
// it. This moves the second-strike confirmation to a LATER sweep instead of
// sleeping a worker for the confirmation delay, so a single failure never holds
// a worker slot. It is pure so the two-strike timing is unit-tested without a DB.
func shouldConfirmError(prev surveyState, prevFound bool, now time.Time) bool {
	if !prevFound || prev.firstFailureAt.IsZero() {
		return false
	}
	return !now.Before(prev.firstFailureAt.Add(confirmMinGap))
}

// recordSurveyFailure upserts the stream's survey_stream_state after a capture
// failure. It never sleeps: it either records the first-failure marker (attempt
// 1, no tag) or, when confirm is true, increments consecutive_failures and tags
// the stream. last_attempt_at is always advanced so backoff is measured from the
// most recent attempt.
func recordSurveyFailure(ctx context.Context, pool *pgxpool.Pool, streamID int64, confirm bool, now time.Time) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin survey failure tx for stream %d: %w", streamID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if confirm {
		if _, err := tx.Exec(ctx, `
			INSERT INTO survey_stream_state (stream_id, consecutive_failures, first_failure_at, last_failure_at, last_attempt_at)
			VALUES ($1, 1, $2, $2, $2)
			ON CONFLICT (stream_id) DO UPDATE SET
			    consecutive_failures = survey_stream_state.consecutive_failures + 1,
			    first_failure_at = COALESCE(survey_stream_state.first_failure_at, EXCLUDED.first_failure_at),
			    last_failure_at = EXCLUDED.last_failure_at,
			    last_attempt_at = EXCLUDED.last_attempt_at
		`, streamID, now); err != nil {
			return fmt.Errorf("record confirmed survey failure for stream %d: %w", streamID, err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE streams
			SET tags = CASE
			        WHEN $2 = ANY(COALESCE(tags, ARRAY[]::text[])) THEN tags
			        ELSE array_append(COALESCE(tags, ARRAY[]::text[]), $2)
			    END,
			    updated_at = now()
			WHERE id = $1
		`, streamID, ErrorTag); err != nil {
			return fmt.Errorf("set survey error tag for stream %d: %w", streamID, err)
		}
	} else {
		// First failure: record the marker only, no tag, no worker sleep. The
		// confirmation happens on a later sweep via shouldConfirmError.
		if _, err := tx.Exec(ctx, `
			INSERT INTO survey_stream_state (stream_id, consecutive_failures, first_failure_at, last_failure_at, last_attempt_at)
			VALUES ($1, 0, $2, $2, $2)
			ON CONFLICT (stream_id) DO UPDATE SET
			    first_failure_at = COALESCE(survey_stream_state.first_failure_at, EXCLUDED.first_failure_at),
			    last_failure_at = EXCLUDED.last_failure_at,
			    last_attempt_at = EXCLUDED.last_attempt_at
		`, streamID, now); err != nil {
			return fmt.Errorf("record first survey failure for stream %d: %w", streamID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit survey failure for stream %d: %w", streamID, err)
	}
	return nil
}

// markSurveyHealthy clears the survey failure state on a successful capture: it
// deletes the survey_stream_state row and removes the 'error' tag. This is the
// un-flag-on-recovery path; a recovered stream clears within the sweep it
// recovers.
func markSurveyHealthy(ctx context.Context, pool *pgxpool.Pool, streamID int64) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin survey healthy tx for stream %d: %w", streamID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `DELETE FROM survey_stream_state WHERE stream_id = $1`, streamID); err != nil {
		return fmt.Errorf("clear survey state for stream %d: %w", streamID, err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE streams
		SET tags = array_remove(COALESCE(tags, ARRAY[]::text[]), $2),
		    updated_at = now()
		WHERE id = $1
		  AND $2 = ANY(COALESCE(tags, ARRAY[]::text[]))
	`, streamID, ErrorTag); err != nil {
		return fmt.Errorf("clear survey error tag for stream %d: %w", streamID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit survey healthy for stream %d: %w", streamID, err)
	}
	return nil
}

func RecordFailure(ctx context.Context, pool *pgxpool.Pool, streamID int64) error {
	prev, found, err := loadSurveyState(ctx, pool, streamID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	return recordSurveyFailure(ctx, pool, streamID, shouldConfirmError(prev, found, now), now)
}

func MarkHealthy(ctx context.Context, pool *pgxpool.Pool, streamID int64) error {
	return markSurveyHealthy(ctx, pool, streamID)
}

// CaptureAndPersist captures and persists one survey frame for the stream on the
// given UTC date. It is idempotent: if a survey frame already exists for the
// stream for that date it skips and returns skipped=true. A capture failure
// records/advances the stream's survey_stream_state (first failure = marker
// only; a confirmed second failure on a later sweep tags 'error') and returns
// immediately: a failure never holds a worker for a confirmation delay.
func CaptureAndPersist(ctx context.Context, pool *pgxpool.Pool, r2c *r2.Client, registry *capture.Registry, t Target, day time.Time, resolveTimeout, captureTimeout time.Duration, det Detector, sampleRate float64) (skipped bool, _ error) {
	exists, err := HasFrameForDay(ctx, pool, t.ID, day)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	frame, err := CaptureFrame(ctx, registry, t, resolveTimeout, captureTimeout)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		prev, found, loadErr := loadSurveyState(ctx, pool, t.ID)
		if loadErr != nil {
			return false, fmt.Errorf("%w; additionally failed to load survey state: %v", err, loadErr)
		}
		now := time.Now().UTC()
		confirm := shouldConfirmError(prev, found, now)
		if recErr := recordSurveyFailure(ctx, pool, t.ID, confirm, now); recErr != nil {
			return false, fmt.Errorf("%w; additionally failed to record survey failure: %v", err, recErr)
		}
		return false, fmt.Errorf("survey capture failure for stream %d (confirmed=%t): %w", t.ID, confirm, err)
	}
	// Per-stream randomized coverage gate: when a detector is configured, run
	// detection on this frame with probability sampleRate. The decision is per
	// capture, seeded by the process-global rand, so coverage is randomized across
	// streams and over time rather than always the same streams. A frame not
	// sampled still gets captured+persisted; only detection is skipped. Detection
	// runs BEFORE the persist tx (multi-second CPU inference must not hold a tx
	// open); a detect error fails the whole persist (fail-fast, no partial row).
	var detResult *DetectionResult
	if det != nil && rand.Float64() < sampleRate {
		counts, ms, derr := det.Detect(frame.Bytes)
		if derr != nil {
			return false, fmt.Errorf("survey detection failed for stream %d: %w", t.ID, derr)
		}
		detResult = &DetectionResult{
			PipelineVersion: det.PipelineVersion(),
			ConfThreshold:   det.ConfThreshold(),
			Imgsz:           det.Imgsz(),
			Counts:          counts,
			DetectMs:        ms,
		}
	}
	capturedAt := time.Now().UTC()
	if err := Persist(ctx, pool, r2c, t.ID, day, capturedAt, frame, detResult); err != nil {
		return false, err
	}
	if err := markSurveyHealthy(ctx, pool, t.ID); err != nil {
		return false, err
	}
	return false, nil
}

// RunResult summarizes a sweep over a set of targets.
type RunResult struct {
	Total   int
	Success int
	Skipped int
	Failed  int
}

// RunOnce sweeps the given targets once with bounded concurrency, capturing and
// persisting a survey frame for each on the given UTC date. onError, if set, is
// called for each per-stream failure (capture failures do not abort the sweep).
// det (nil = detection off) and sampleRate configure the per-stream randomized
// detection coverage gate applied inside CaptureAndPersist.
func RunOnce(ctx context.Context, pool *pgxpool.Pool, r2c *r2.Client, registry *capture.Registry, targets []Target, day time.Time, concurrency int, resolveTimeout, captureTimeout time.Duration, det Detector, sampleRate float64, onError func(streamID int64, err error)) RunResult {
	if concurrency < 1 {
		concurrency = 1
	}
	type outcome struct {
		skipped bool
		err     error
	}
	sem := make(chan struct{}, concurrency)
	results := make(chan outcome, len(targets))
	for _, t := range targets {
		select {
		case <-ctx.Done():
			results <- outcome{err: ctx.Err()}
			continue
		case sem <- struct{}{}:
		}
		go func(t Target) {
			defer func() { <-sem }()
			skipped, err := CaptureAndPersist(ctx, pool, r2c, registry, t, day, resolveTimeout, captureTimeout, det, sampleRate)
			if err != nil && onError != nil {
				onError(t.ID, err)
			}
			results <- outcome{skipped: skipped, err: err}
		}(t)
	}
	res := RunResult{Total: len(targets)}
	for range targets {
		o := <-results
		switch {
		case o.err != nil:
			res.Failed++
		case o.skipped:
			res.Skipped++
		default:
			res.Success++
		}
	}
	return res
}

// Coverage reports how many non-pruned streams have at least one survey frame
// ever and at least one for the given UTC date.
type Coverage struct {
	NonPrunedTotal  int64
	WithAnySurvey   int64
	WithTodaySurvey int64
}

func ComputeCoverage(ctx context.Context, pool *pgxpool.Pool, day time.Time) (Coverage, error) {
	d := day.UTC()
	dateStr := fmt.Sprintf("%04d-%02d-%02d", d.Year(), int(d.Month()), d.Day())
	var c Coverage
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM streams WHERE deleted_at IS NULL),
			(SELECT COUNT(*) FROM streams s
			   WHERE s.deleted_at IS NULL
			     AND EXISTS (SELECT 1 FROM frames f WHERE f.stream_id = s.id AND f.source_kind = 'survey')),
			(SELECT COUNT(*) FROM streams s
			   WHERE s.deleted_at IS NULL
			     AND EXISTS (SELECT 1 FROM frames f WHERE f.stream_id = s.id AND f.source_kind = 'survey'
			                   AND (f.captured_at AT TIME ZONE 'UTC')::date = $1::date))
	`, dateStr).Scan(&c.NonPrunedTotal, &c.WithAnySurvey, &c.WithTodaySurvey); err != nil {
		return Coverage{}, fmt.Errorf("compute survey coverage: %w", err)
	}
	return c, nil
}

// DeleteStreamCaptures removes a stream's survey objects from R2 and deletes the
// orphaned media_objects rows plus survey frame rows. A DB ON DELETE CASCADE
// removes frame rows but never deletes R2 bytes and orphans media_objects, so
// this is the explicit cleanup path.
func DeleteStreamCaptures(ctx context.Context, pool *pgxpool.Pool, r2c *r2.Client, streamID int64) (deletedObjects int, _ error) {
	prefix := fmt.Sprintf("survey/stream/%d/", streamID)
	keys := make([]string, 0, 512)
	if err := r2c.ListPrefix(ctx, prefix, 0, func(obj r2.ObjectInfo) error {
		keys = append(keys, obj.Key)
		return nil
	}); err != nil {
		return 0, fmt.Errorf("list survey objects for stream %d: %w", streamID, err)
	}
	if len(keys) > 0 {
		if err := r2c.DeleteObjects(ctx, keys); err != nil {
			return 0, fmt.Errorf("delete survey objects for stream %d: %w", streamID, err)
		}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin survey delete tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Delete survey frame rows for the stream, then the now-orphaned
	// media_objects rows for those survey keys.
	if _, err := tx.Exec(ctx, `
		DELETE FROM frames
		WHERE stream_id = $1 AND source_kind = 'survey'
	`, streamID); err != nil {
		return 0, fmt.Errorf("delete survey frames for stream %d: %w", streamID, err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM media_objects
		WHERE bucket = $1 AND object_key LIKE $2
		  AND NOT EXISTS (SELECT 1 FROM frames f WHERE f.raw_media_object_id = media_objects.id)
	`, r2c.Bucket(), prefix+"%"); err != nil {
		return 0, fmt.Errorf("delete orphaned survey media_objects for stream %d: %w", streamID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit survey delete tx: %w", err)
	}
	return len(keys), nil
}
