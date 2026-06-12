// Package survey captures one JPEG per stream per day and stores it cheaply in
// R2 under survey/stream/{id}/{YYYY-MM-DD}.jpg. Each frame is timestamped with
// the real grab time; the daily sweep is scheduled at a randomized time of day
// so imagery varies across days. It is decoupled from the recording-health
// pipeline: it does not touch stream_health, stream_capture_runtime, or
// capture_jobs. No detection, no tags.
package survey

import (
	"context"
	"fmt"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
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

// SelectTargets returns all streams to survey, independent of recording_state.
// Pruning is deferred, so no exclusion filter is applied yet.
func SelectTargets(ctx context.Context, pool *pgxpool.Pool) ([]Target, error) {
	rows, err := pool.Query(ctx, `
		SELECT id,
		       COALESCE(provider, ''),
		       COALESCE(source_url, ''),
		       COALESCE(source_page_url, ''),
		       COALESCE(capture_type, ''),
		       COALESCE(source_family, ''),
		       COALESCE(execution_class, '')
		FROM streams
		ORDER BY id
	`)
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
	frame, err := capture.CaptureFrame(capCtx, resolved.URL)
	if err != nil {
		return capture.Frame{}, fmt.Errorf("capture frame for stream %d: %w", t.ID, err)
	}
	return frame, nil
}

// Persist uploads the frame to R2 and inserts a survey frame row. It mirrors
// persistCaptureSuccess but uses the survey object key and source_kind='survey',
// and does NOT touch stream_health, stream_capture_runtime, or capture_jobs.
func Persist(ctx context.Context, pool *pgxpool.Pool, r2c *r2.Client, streamID int64, day time.Time, capturedAt time.Time, frame capture.Frame) error {
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
	if _, err := tx.Exec(ctx, `
		INSERT INTO frames (stream_id, capture_job_id, captured_at, raw_media_object_id, capture_status, capture_error, source_kind)
		VALUES ($1, NULL, $2, $3, 'success', NULL, 'survey')
	`, streamID, capturedAt, mediaID); err != nil {
		return fmt.Errorf("insert survey frame: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit survey tx: %w", err)
	}
	return nil
}

// CaptureAndPersist captures and persists one survey frame for the stream on the
// given UTC date. It is idempotent: if a survey frame already exists for the
// stream for that date it skips and returns skipped=true.
func CaptureAndPersist(ctx context.Context, pool *pgxpool.Pool, r2c *r2.Client, registry *capture.Registry, t Target, day time.Time, resolveTimeout, captureTimeout time.Duration) (skipped bool, _ error) {
	exists, err := HasFrameForDay(ctx, pool, t.ID, day)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	frame, err := CaptureFrame(ctx, registry, t, resolveTimeout, captureTimeout)
	if err != nil {
		return false, err
	}
	capturedAt := time.Now().UTC()
	if err := Persist(ctx, pool, r2c, t.ID, day, capturedAt, frame); err != nil {
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
func RunOnce(ctx context.Context, pool *pgxpool.Pool, r2c *r2.Client, registry *capture.Registry, targets []Target, day time.Time, concurrency int, resolveTimeout, captureTimeout time.Duration, onError func(streamID int64, err error)) RunResult {
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
			skipped, err := CaptureAndPersist(ctx, pool, r2c, registry, t, day, resolveTimeout, captureTimeout)
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
			(SELECT COUNT(*) FROM streams),
			(SELECT COUNT(*) FROM streams s
			   WHERE EXISTS (SELECT 1 FROM frames f WHERE f.stream_id = s.id AND f.source_kind = 'survey')),
			(SELECT COUNT(*) FROM streams s
			   WHERE EXISTS (SELECT 1 FROM frames f WHERE f.stream_id = s.id AND f.source_kind = 'survey'
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
