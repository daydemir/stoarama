package inferencebox

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	_ "image/png"
	"log"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/storage"
)

type ManagerConfig struct {
	WorkerID      string
	PollInterval  time.Duration
	MaxWorkers    int
	LeaseDuration time.Duration
	MaxAttempts   int
	RetryBase     time.Duration
	RetryMax      time.Duration
}

type Manager struct {
	pool *pgxpool.Pool
	r2   *r2.Client
	cfg  ManagerConfig
}

type boxJobClaim struct {
	JobID             int64
	InferenceResultID int64
	AttemptCount      int
	MaxAttempts       int
}

type boxJobContext struct {
	InferenceResultID int64
	PipelineID        string
	FrameID           int64
	StreamID          int64
	CapturedAt        time.Time
	Status            string
	RawObjectKey      string
}

type detection struct {
	ClassName  string
	Confidence float64
	X1         float64
	Y1         float64
	X2         float64
	Y2         float64
}

func NewManager(pool *pgxpool.Pool, r2c *r2.Client, cfg ManagerConfig) *Manager {
	return &Manager{pool: pool, r2: r2c, cfg: cfg}
}

func (m *Manager) Run(ctx context.Context) error {
	if strings.TrimSpace(m.cfg.WorkerID) == "" {
		return fmt.Errorf("inference-box manager: worker_id is required")
	}
	if m.cfg.PollInterval <= 0 {
		return fmt.Errorf("inference-box manager: poll interval must be > 0")
	}
	if m.cfg.MaxWorkers <= 0 {
		return fmt.Errorf("inference-box manager: max workers must be > 0")
	}
	if m.cfg.LeaseDuration <= 0 {
		return fmt.Errorf("inference-box manager: lease duration must be > 0")
	}
	if m.cfg.MaxAttempts <= 0 {
		return fmt.Errorf("inference-box manager: max attempts must be > 0")
	}
	if m.cfg.RetryBase <= 0 {
		return fmt.Errorf("inference-box manager: retry base must be > 0")
	}
	if m.cfg.RetryMax <= 0 {
		return fmt.Errorf("inference-box manager: retry max must be > 0")
	}
	if m.cfg.RetryMax < m.cfg.RetryBase {
		return fmt.Errorf("inference-box manager: retry max must be >= retry base")
	}

	log.Printf(
		"inference-box manager start worker_id=%s poll=%s max_workers=%d lease=%s max_attempts=%d retry_base=%s retry_max=%s",
		m.cfg.WorkerID, m.cfg.PollInterval, m.cfg.MaxWorkers, m.cfg.LeaseDuration, m.cfg.MaxAttempts, m.cfg.RetryBase, m.cfg.RetryMax,
	)

	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			log.Printf("inference-box manager stop worker_id=%s reason=%v", m.cfg.WorkerID, err)
			return nil
		}
		if err := m.expireLeases(ctx); err != nil {
			return fmt.Errorf("expire inference-box leases: %w", err)
		}
		claimed, err := m.processBatch(ctx)
		if err != nil {
			return fmt.Errorf("process inference-box batch: %w", err)
		}
		if claimed > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			log.Printf("inference-box manager stop worker_id=%s reason=%v", m.cfg.WorkerID, ctx.Err())
			return nil
		case <-ticker.C:
		}
	}
}

func (m *Manager) expireLeases(ctx context.Context) error {
	_, err := m.pool.Exec(ctx, `
		UPDATE inference_box_jobs
		SET status='pending', lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
		WHERE status='leased' AND lease_expires_at < now()
	`)
	return err
}

func (m *Manager) processBatch(ctx context.Context) (int, error) {
	jobs := make([]boxJobClaim, 0, m.cfg.MaxWorkers)
	for i := 0; i < m.cfg.MaxWorkers; i++ {
		job, ok, err := m.claimOne(ctx)
		if err != nil {
			return len(jobs), err
		}
		if !ok {
			break
		}
		jobs = append(jobs, job)
	}
	if len(jobs) == 0 {
		return 0, nil
	}

	var wg sync.WaitGroup
	for _, j := range jobs {
		job := j
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.processOne(ctx, job); err != nil {
				log.Printf("inference-box job_id=%d inference_result_id=%d process error: %v", job.JobID, job.InferenceResultID, err)
			}
		}()
	}
	wg.Wait()
	return len(jobs), nil
}

func (m *Manager) claimOne(ctx context.Context) (boxJobClaim, bool, error) {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return boxJobClaim{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	leaseSec := int(m.cfg.LeaseDuration / time.Second)
	if leaseSec <= 0 {
		leaseSec = 1
	}

	var out boxJobClaim
	err = tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT j.id, j.inference_result_id, j.attempt_count, j.max_attempts
			FROM inference_box_jobs j
			JOIN inference_results ir ON ir.id=j.inference_result_id
			WHERE (j.status='pending' OR j.status='error')
			  AND j.attempt_count < j.max_attempts
			  AND j.next_retry_at <= now()
			  AND ir.status='queued_boxed'
			ORDER BY j.next_retry_at ASC, j.id ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE inference_box_jobs j
			SET status='leased',
			    lease_owner=$1,
			    lease_expires_at=now() + make_interval(secs => $2),
			    updated_at=now()
			FROM candidate c
			WHERE j.id=c.id
			RETURNING j.id, j.inference_result_id, j.attempt_count, j.max_attempts
		)
		SELECT id, inference_result_id, attempt_count, max_attempts
		FROM claimed
	`, m.cfg.WorkerID, leaseSec).Scan(&out.JobID, &out.InferenceResultID, &out.AttemptCount, &out.MaxAttempts)
	if err != nil {
		if err == pgx.ErrNoRows {
			return boxJobClaim{}, false, nil
		}
		return boxJobClaim{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return boxJobClaim{}, false, err
	}
	return out, true, nil
}

func (m *Manager) processOne(ctx context.Context, job boxJobClaim) error {
	jobCtx, err := m.loadJobContext(ctx, job.InferenceResultID)
	if err != nil {
		return m.failJob(ctx, job, fmt.Errorf("load context: %w", err), true)
	}
	if jobCtx.Status != "queued_boxed" {
		return m.completeJobDoneOnly(ctx, job.JobID)
	}

	detections, err := m.loadDetections(ctx, job.InferenceResultID)
	if err != nil {
		return m.failJob(ctx, job, fmt.Errorf("load detections: %w", err), true)
	}
	if len(detections) == 0 {
		return m.completeJobNoBox(ctx, job)
	}

	rawBytes, err := m.r2.Get(ctx, jobCtx.RawObjectKey)
	if err != nil {
		return m.failJob(ctx, job, fmt.Errorf("download raw image %s: %w", jobCtx.RawObjectKey, err), false)
	}
	boxedJPEG, width, height, err := renderBoxedJPEG(rawBytes, detections)
	if err != nil {
		return m.failJob(ctx, job, fmt.Errorf("render boxed jpeg: %w", err), true)
	}

	objectKey := buildBoxedObjectKey(jobCtx.PipelineID, jobCtx.StreamID, jobCtx.CapturedAt, jobCtx.InferenceResultID)
	etag, err := m.r2.PutBytes(ctx, objectKey, "image/jpeg", boxedJPEG)
	if err != nil {
		return m.failJob(ctx, job, fmt.Errorf("upload boxed image: %w", err), false)
	}

	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return m.failJob(ctx, job, fmt.Errorf("begin success tx: %w", err), false)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	mediaID, err := storage.UpsertMediaObject(ctx, tx, storage.MediaObjectInput{
		StorageProvider: "r2",
		Bucket:          m.r2.Bucket(),
		ObjectKey:       objectKey,
		MIMEType:        "image/jpeg",
		SizeBytes:       int64(len(boxedJPEG)),
		ETag:            etag,
		Width:           width,
		Height:          height,
	})
	if err != nil {
		return m.failJob(ctx, job, fmt.Errorf("upsert boxed media object: %w", err), false)
	}

	cmd, err := tx.Exec(ctx, `
		UPDATE inference_results
		SET status='success',
		    boxed_media_object_id=$2,
		    error_text=NULL,
		    finished_at=COALESCE(finished_at, now())
		WHERE id=$1
		  AND status='queued_boxed'
	`, job.InferenceResultID, mediaID)
	if err != nil {
		return m.failJob(ctx, job, fmt.Errorf("mark inference result success: %w", err), false)
	}
	if cmd.RowsAffected() == 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE inference_box_jobs
			SET status='done', lease_owner=NULL, lease_expires_at=NULL, error_text=NULL, updated_at=now()
			WHERE id=$1
		`, job.JobID); err != nil {
			return m.failJob(ctx, job, fmt.Errorf("mark stale job done: %w", err), false)
		}
		if err := tx.Commit(ctx); err != nil {
			return m.failJob(ctx, job, fmt.Errorf("commit stale success tx: %w", err), false)
		}
		return nil
	}

	if _, err := tx.Exec(ctx, `
		UPDATE inference_box_jobs
		SET status='done', lease_owner=NULL, lease_expires_at=NULL, error_text=NULL, updated_at=now()
		WHERE id=$1
	`, job.JobID); err != nil {
		return m.failJob(ctx, job, fmt.Errorf("mark box job done: %w", err), false)
	}
	if err := tx.Commit(ctx); err != nil {
		return m.failJob(ctx, job, fmt.Errorf("commit success tx: %w", err), false)
	}

	log.Printf("inference-box job done job_id=%d inference_result_id=%d object_key=%s", job.JobID, job.InferenceResultID, objectKey)
	return nil
}

func (m *Manager) completeJobDoneOnly(ctx context.Context, jobID int64) error {
	_, err := m.pool.Exec(ctx, `
		UPDATE inference_box_jobs
		SET status='done', lease_owner=NULL, lease_expires_at=NULL, error_text=NULL, updated_at=now()
		WHERE id=$1
	`, jobID)
	return err
}

func (m *Manager) completeJobNoBox(ctx context.Context, job boxJobClaim) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return m.failJob(ctx, job, fmt.Errorf("begin no-box tx: %w", err), false)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE inference_results
		SET status='success', error_text=NULL, finished_at=COALESCE(finished_at, now())
		WHERE id=$1 AND status='queued_boxed'
	`, job.InferenceResultID); err != nil {
		return m.failJob(ctx, job, fmt.Errorf("set no-box success: %w", err), false)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE inference_box_jobs
		SET status='done', lease_owner=NULL, lease_expires_at=NULL, error_text='no detections', updated_at=now()
		WHERE id=$1
	`, job.JobID); err != nil {
		return m.failJob(ctx, job, fmt.Errorf("mark no-box job done: %w", err), false)
	}
	if err := tx.Commit(ctx); err != nil {
		return m.failJob(ctx, job, fmt.Errorf("commit no-box tx: %w", err), false)
	}
	return nil
}

func (m *Manager) failJob(ctx context.Context, job boxJobClaim, processErr error, terminal bool) error {
	errText := truncateErr("boxing_failed: " + strings.TrimSpace(processErr.Error()))
	nextAttempt := job.AttemptCount + 1
	isTerminal := terminal || nextAttempt >= job.MaxAttempts
	backoffSec := int(backoffForAttempt(nextAttempt, m.cfg.RetryBase, m.cfg.RetryMax) / time.Second)
	if backoffSec <= 0 {
		backoffSec = 1
	}

	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin fail tx: %w; original error: %v", err, processErr)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if isTerminal {
		if _, err := tx.Exec(ctx, `
			UPDATE inference_results
			SET status='error', error_text=$2
			WHERE id=$1 AND status='queued_boxed'
		`, job.InferenceResultID, errText); err != nil {
			return fmt.Errorf("mark inference result terminal error: %w; original error: %v", err, processErr)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE inference_box_jobs
			SET status='error',
			    attempt_count=$2,
			    error_text=$3,
			    lease_owner=NULL,
			    lease_expires_at=NULL,
			    next_retry_at=now(),
			    updated_at=now()
			WHERE id=$1
		`, job.JobID, nextAttempt, errText); err != nil {
			return fmt.Errorf("mark box job terminal error: %w; original error: %v", err, processErr)
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE inference_results
			SET error_text=$2
			WHERE id=$1 AND status='queued_boxed'
		`, job.InferenceResultID, errText); err != nil {
			return fmt.Errorf("mark inference result transient error: %w; original error: %v", err, processErr)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE inference_box_jobs
			SET status='error',
			    attempt_count=$2,
			    error_text=$3,
			    lease_owner=NULL,
			    lease_expires_at=NULL,
			    next_retry_at=now() + make_interval(secs => $4),
			    updated_at=now()
			WHERE id=$1
		`, job.JobID, nextAttempt, errText, backoffSec); err != nil {
			return fmt.Errorf("mark box job transient error: %w; original error: %v", err, processErr)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit fail tx: %w; original error: %v", err, processErr)
	}
	log.Printf(
		"inference-box job error job_id=%d inference_result_id=%d attempt=%d/%d terminal=%t err=%s",
		job.JobID, job.InferenceResultID, nextAttempt, job.MaxAttempts, isTerminal, errText,
	)
	return processErr
}

func (m *Manager) loadJobContext(ctx context.Context, inferenceResultID int64) (boxJobContext, error) {
	var out boxJobContext
	err := m.pool.QueryRow(ctx, `
		SELECT
			ir.id, ir.pipeline_id, ir.frame_id, ir.status,
			f.stream_id, f.captured_at, raw.object_key
		FROM inference_results ir
		JOIN frames f ON f.id=ir.frame_id
		JOIN media_objects raw ON raw.id=f.raw_media_object_id
		WHERE ir.id=$1
	`, inferenceResultID).Scan(
		&out.InferenceResultID,
		&out.PipelineID,
		&out.FrameID,
		&out.Status,
		&out.StreamID,
		&out.CapturedAt,
		&out.RawObjectKey,
	)
	if err != nil {
		return boxJobContext{}, err
	}
	return out, nil
}

func (m *Manager) loadDetections(ctx context.Context, inferenceResultID int64) ([]detection, error) {
	rows, err := m.pool.Query(ctx, `
		SELECT class_name, confidence, x1, y1, x2, y2
		FROM detections
		WHERE inference_result_id=$1
		ORDER BY id ASC
	`, inferenceResultID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]detection, 0, 32)
	for rows.Next() {
		var d detection
		if err := rows.Scan(&d.ClassName, &d.Confidence, &d.X1, &d.Y1, &d.X2, &d.Y2); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return out, nil
}

func renderBoxedJPEG(rawBytes []byte, detections []detection) ([]byte, int, int, error) {
	src, _, err := image.Decode(bytes.NewReader(rawBytes))
	if err != nil {
		return nil, 0, 0, err
	}
	b := src.Bounds()
	dst := image.NewRGBA(b)
	draw.Draw(dst, b, src, b.Min, draw.Src)

	boxColor := color.RGBA{R: 255, G: 40, B: 40, A: 255}
	for _, det := range detections {
		x1 := clampInt(int(det.X1), b.Min.X, b.Max.X-1)
		y1 := clampInt(int(det.Y1), b.Min.Y, b.Max.Y-1)
		x2 := clampInt(int(det.X2), b.Min.X, b.Max.X-1)
		y2 := clampInt(int(det.Y2), b.Min.Y, b.Max.Y-1)
		if x2 < x1 {
			x1, x2 = x2, x1
		}
		if y2 < y1 {
			y1, y2 = y2, y1
		}
		drawRect(dst, x1, y1, x2, y2, boxColor, 3)
	}

	var out bytes.Buffer
	if err := jpeg.Encode(&out, dst, &jpeg.Options{Quality: 92}); err != nil {
		return nil, 0, 0, err
	}
	return out.Bytes(), b.Dx(), b.Dy(), nil
}

func drawRect(dst draw.Image, x1, y1, x2, y2 int, c color.Color, width int) {
	if width <= 0 {
		width = 1
	}
	for w := 0; w < width; w++ {
		top := y1 + w
		bottom := y2 - w
		left := x1 + w
		right := x2 - w
		if left > right || top > bottom {
			return
		}
		for x := left; x <= right; x++ {
			dst.Set(x, top, c)
			dst.Set(x, bottom, c)
		}
		for y := top; y <= bottom; y++ {
			dst.Set(left, y, c)
			dst.Set(right, y, c)
		}
	}
}

func clampInt(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func buildBoxedObjectKey(pipelineID string, streamID int64, capturedAt time.Time, inferenceResultID int64) string {
	t := capturedAt.UTC()
	return fmt.Sprintf(
		"boxed/pipeline/%s/stream/%d/%04d/%02d/%02d/result-%d.jpg",
		sanitizeObjectKeyToken(pipelineID),
		streamID,
		t.Year(),
		int(t.Month()),
		t.Day(),
		inferenceResultID,
	)
}

func sanitizeObjectKeyToken(in string) string {
	s := strings.TrimSpace(strings.ToLower(in))
	if s == "" {
		return "unknown"
	}
	var b strings.Builder
	lastDash := false
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "unknown"
	}
	return out
}

func backoffForAttempt(attempt int, base, max time.Duration) time.Duration {
	if attempt <= 1 {
		return base
	}
	if base <= 0 {
		return time.Second
	}
	delay := base
	for i := 1; i < attempt; i++ {
		if delay >= max {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

func truncateErr(in string) string {
	s := strings.TrimSpace(in)
	if s == "" {
		return "boxing_failed: unknown"
	}
	const max = 1200
	if len(s) <= max {
		return s
	}
	return s[:max]
}
