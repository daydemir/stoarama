package api

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daydemir/stoarama/backend/internal/util"
)

type dayZipCreateRequest struct {
	Day string `json:"day"`
}

type dayZipSegmentRow struct {
	ID             int64
	SegmentStartAt time.Time
	DurationMs     int64
	ObjectKey      string
	MIMEType       string
	SizeBytes      int64
}

func dayZipKey(streamID int64, day string) string {
	return fmt.Sprintf("exports/stream/%d/day-%s-window30s.zip", streamID, day)
}

// dayZipWindowSegments queries the day's ~30s window clips (duration_ms < 60000),
// joining media_objects for object_key/mime/size, ordered oldest-first.
func dayZipWindowSegments(ctx context.Context, s *Server, streamID int64, dayStart, dayEnd time.Time) ([]dayZipSegmentRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			cs.id,
			cs.segment_start_at,
			cs.duration_ms,
			mo.object_key,
			mo.mime_type,
			mo.size_bytes
		FROM capture_segments cs
		JOIN media_objects mo ON mo.id = cs.media_object_id
		WHERE cs.stream_id = $1
		  AND cs.segment_start_at >= $2
		  AND cs.segment_start_at <  $3
		  AND cs.duration_ms < 60000
		  AND cs.capture_status = 'success'
		ORDER BY cs.segment_start_at ASC, cs.id ASC
	`, streamID, dayStart, dayEnd)
	if err != nil {
		return nil, fmt.Errorf("query day-zip segments: %w", err)
	}
	defer rows.Close()
	out := make([]dayZipSegmentRow, 0, 1024)
	for rows.Next() {
		var row dayZipSegmentRow
		if err := rows.Scan(&row.ID, &row.SegmentStartAt, &row.DurationMs, &row.ObjectKey, &row.MIMEType, &row.SizeBytes); err != nil {
			return nil, fmt.Errorf("scan day-zip segment: %w", err)
		}
		out = append(out, row)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate day-zip segments: %w", rows.Err())
	}
	return out, nil
}

func dayZipItemName(streamSlug string, streamID int64, segStart time.Time, segID int64, mimeType string) string {
	slug := strings.TrimSpace(streamSlug)
	if slug == "" {
		slug = fmt.Sprintf("stream-%d", streamID)
	}
	ext := fileExtensionFromMIME(mimeType)
	if ext == "" {
		ext = ".mp4"
	}
	return fmt.Sprintf("%s-%s-%d%s", slug, segStart.UTC().Format("20060102T150405Z"), segID, ext)
}

// errDayZipTooLarge is returned by buildDayZip when the bytes actually written
// to the archive exceed dayZipMaxBytes (enforced against real output, not the
// advisory DB-metadata pre-check).
var errDayZipTooLarge = errors.New("day too large")

// countingWriter wraps an io.Writer and tracks the total bytes written through it.
type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	written, err := cw.w.Write(p)
	cw.n += int64(written)
	return written, err
}

// buildDayZip streams each segment's media into a STORE zip written to w, buffering
// the manifest CSV in memory and writing it as a final entry so it is not interleaved
// with (and invalidated by) the media entries. It enforces dayZipMaxBytes against the
// bytes actually copied. onProgress, if non-nil, is called with the running processed
// count after each row. Returns the number of rows processed.
func buildDayZip(ctx context.Context, w io.Writer, rows []dayZipSegmentRow, streamSlug string, streamID int64, open func(context.Context, string) (io.ReadCloser, error), onProgress func(int)) (int, error) {
	zipWriter := zip.NewWriter(w)

	var manifestBuf bytes.Buffer
	csvWriter := csv.NewWriter(&manifestBuf)
	if err := csvWriter.Write([]string{"filename", "segment_start_at", "duration_ms", "bytes", "status"}); err != nil {
		_ = zipWriter.Close()
		return 0, fmt.Errorf("write manifest header: %w", err)
	}

	var totalWritten int64
	processed := 0
	for _, row := range rows {
		objectKey := strings.TrimSpace(row.ObjectKey)
		name := dayZipItemName(streamSlug, streamID, row.SegmentStartAt, row.ID, row.MIMEType)
		status := "ok"
		if objectKey == "" {
			status = "missing object_key"
		} else {
			entry, err := zipWriter.CreateHeader(&zip.FileHeader{
				Name:     name,
				Modified: row.SegmentStartAt.UTC(),
				Method:   zip.Store,
			})
			if err != nil {
				// A zip-structure failure is fatal: the archive is unusable.
				_ = zipWriter.Close()
				return 0, fmt.Errorf("create archive entry %s: %w", name, err)
			}
			body, err := open(ctx, objectKey)
			if err != nil {
				// Per-object failure: skip and record, do not fail the whole job.
				status = fmt.Sprintf("open error: %v", err)
			} else {
				cw := &countingWriter{w: entry}
				_, copyErr := io.Copy(cw, body)
				_ = body.Close()
				totalWritten += cw.n
				if totalWritten > dayZipMaxBytes {
					_ = zipWriter.Close()
					return 0, errDayZipTooLarge
				}
				if copyErr != nil {
					status = fmt.Sprintf("copy error: %v", copyErr)
				}
			}
		}
		if err := csvWriter.Write([]string{
			name,
			row.SegmentStartAt.UTC().Format(time.RFC3339Nano),
			strconv.FormatInt(row.DurationMs, 10),
			strconv.FormatInt(row.SizeBytes, 10),
			status,
		}); err != nil {
			_ = zipWriter.Close()
			return 0, fmt.Errorf("write manifest row: %w", err)
		}
		processed++
		if onProgress != nil {
			onProgress(processed)
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		_ = zipWriter.Close()
		return 0, fmt.Errorf("flush manifest: %w", err)
	}

	// Write the manifest as a final entry, after all media entries are done, so the
	// CSV writer is never invalidated by an intervening CreateHeader call.
	manifestWriter, err := zipWriter.Create("manifest.csv")
	if err != nil {
		_ = zipWriter.Close()
		return 0, fmt.Errorf("create manifest entry: %w", err)
	}
	if _, err := io.Copy(manifestWriter, &manifestBuf); err != nil {
		_ = zipWriter.Close()
		return 0, fmt.Errorf("write manifest: %w", err)
	}

	if err := zipWriter.Close(); err != nil {
		return 0, fmt.Errorf("close archive: %w", err)
	}
	return processed, nil
}

// dayZipJobMaxEntries caps how many job records are retained in memory as a hard
// backstop against unbounded growth on the public endpoint.
const dayZipJobMaxEntries = 256

func (s *Server) setDayZipJob(job *dayZipJob) {
	s.dayZipMu.Lock()
	defer s.dayZipMu.Unlock()
	s.reapDayZipJobsLocked()
	s.dayZips[job.ID] = job
}

// reapDayZipJobsLocked prunes finished (complete/error) job records: it evicts any
// whose FinishedAt is older than R2SignGetTTL, and, if the map is still over the
// hard cap, drops the oldest finished entries until under the cap. The caller must
// hold dayZipMu. In-flight (pending/running) jobs are never evicted.
func (s *Server) reapDayZipJobsLocked() {
	now := time.Now()
	for id, job := range s.dayZips {
		if job.FinishedAt != nil && now.Sub(*job.FinishedAt) > s.cfg.R2SignGetTTL {
			delete(s.dayZips, id)
		}
	}
	for len(s.dayZips) >= dayZipJobMaxEntries {
		var oldestID string
		var oldest *time.Time
		for id, job := range s.dayZips {
			if job.FinishedAt == nil {
				continue
			}
			if oldest == nil || job.FinishedAt.Before(*oldest) {
				oldest = job.FinishedAt
				oldestID = id
			}
		}
		if oldestID == "" {
			// All remaining entries are still in flight; nothing safe to drop.
			break
		}
		delete(s.dayZips, oldestID)
	}
}

func (s *Server) getDayZipJob(id string) (*dayZipJob, bool) {
	s.dayZipMu.Lock()
	defer s.dayZipMu.Unlock()
	job, ok := s.dayZips[id]
	if !ok {
		return nil, false
	}
	cp := *job
	return &cp, true
}

func (s *Server) updateDayZipJob(id string, fn func(*dayZipJob)) {
	s.dayZipMu.Lock()
	defer s.dayZipMu.Unlock()
	if job, ok := s.dayZips[id]; ok {
		fn(job)
	}
}

func (s *Server) handleDayZipCreate(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	stream, err := s.getStreamByID(r.Context(), streamID)
	if err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	var req dayZipCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	day := strings.TrimSpace(req.Day)
	dayStart, err := time.Parse("2006-01-02", day)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "day must be YYYY-MM-DD")
		return
	}
	dayStart = dayStart.UTC()
	dayEnd := dayStart.Add(24 * time.Hour)

	now := time.Now().UTC()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if dayStart.After(todayStart) {
		util.WriteError(w, http.StatusBadRequest, "day is in the future")
		return
	}

	zipKey := dayZipKey(streamID, day)

	// Cache hit: a strictly-past day whose zip already exists in R2.
	if dayStart.Before(todayStart) {
		if _, headErr := s.r2.Head(r.Context(), zipKey); headErr == nil {
			url, signErr := s.r2.PresignGet(r.Context(), zipKey, s.cfg.R2SignGetTTL)
			if signErr == nil {
				util.WriteJSON(w, http.StatusOK, map[string]any{
					"job_id":       zipKey,
					"status":       "complete",
					"download_url": url,
				})
				return
			}
		}
	}

	// Pre-check: count and total bytes for the day's window clips.
	var itemCount int64
	var totalBytes int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COUNT(*)::bigint, COALESCE(SUM(mo.size_bytes), 0)::bigint
		FROM capture_segments cs
		JOIN media_objects mo ON mo.id = cs.media_object_id
		WHERE cs.stream_id = $1
		  AND cs.segment_start_at >= $2
		  AND cs.segment_start_at <  $3
		  AND cs.duration_ms < 60000
		  AND cs.capture_status = 'success'
	`, streamID, dayStart, dayEnd).Scan(&itemCount, &totalBytes); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count day-zip segments: %v", err))
		return
	}
	if itemCount > dayZipMaxItems {
		util.WriteError(w, http.StatusRequestEntityTooLarge, "too many items")
		return
	}
	if totalBytes > dayZipMaxBytes {
		util.WriteError(w, http.StatusBadRequest, "day too large")
		return
	}

	// 1-job lock: non-blocking try-acquire.
	select {
	case s.dayZipSlot <- struct{}{}:
	default:
		util.WriteError(w, http.StatusConflict, "busy")
		return
	}

	jobID := uuid.NewString()
	s.setDayZipJob(&dayZipJob{
		ID:        jobID,
		StreamID:  streamID,
		Day:       day,
		Status:    "pending",
		ZipKey:    zipKey,
		ItemCount: int(itemCount),
		SizeBytes: totalBytes,
	})
	go s.runDayZipJob(jobID, stream.Slug, dayStart, dayEnd)

	util.WriteJSON(w, http.StatusAccepted, map[string]any{
		"job_id": jobID,
		"status": "pending",
	})
}

func (s *Server) handleDayZipGet(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(chi.URLParam(r, "jobId"))
	if jobID == "" {
		util.WriteError(w, http.StatusBadRequest, "invalid path jobId")
		return
	}
	job, ok := s.getDayZipJob(jobID)
	if !ok {
		util.WriteError(w, http.StatusNotFound, "job not found")
		return
	}
	resp := map[string]any{
		"status":    job.Status,
		"processed": job.Processed,
		"total":     job.ItemCount,
	}
	if job.Status == "complete" && strings.TrimSpace(job.ZipKey) != "" {
		// Re-presign on every poll so the URL is always within the GET TTL.
		if url, err := s.r2.PresignGet(r.Context(), job.ZipKey, s.cfg.R2SignGetTTL); err == nil {
			resp["download_url"] = url
		}
	}
	if job.Status == "error" && strings.TrimSpace(job.ErrorText) != "" {
		resp["error"] = job.ErrorText
	}
	util.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) runDayZipJob(jobID, streamSlug string, dayStart, dayEnd time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	defer func() { <-s.dayZipSlot }()

	startedAt := time.Now().UTC()
	s.updateDayZipJob(jobID, func(job *dayZipJob) {
		job.Status = "running"
		job.StartedAt = &startedAt
		job.ErrorText = ""
	})
	job, ok := s.getDayZipJob(jobID)
	if !ok {
		return
	}

	failJob := func(format string, args ...any) {
		finishedAt := time.Now().UTC()
		s.updateDayZipJob(jobID, func(job *dayZipJob) {
			job.Status = "error"
			job.ErrorText = fmt.Sprintf(format, args...)
			job.FinishedAt = &finishedAt
		})
	}

	rows, err := dayZipWindowSegments(ctx, s, job.StreamID, dayStart, dayEnd)
	if err != nil {
		failJob("query day-zip segments: %v", err)
		return
	}

	// Stream the archive directly from R2 (read) -> zip -> R2 (multipart write)
	// through a pipe. The whole zip is never materialized on this instance:
	// writing/reading a ~2.5GB temp file fills the container page cache and gets
	// the small (512MB) instance OOM-restarted. With the pipe + multipart
	// uploader, peak footprint is just the in-flight upload parts (~tens of MB).
	pr, pw := io.Pipe()
	buildDone := make(chan error, 1)
	go func() {
		_, buildErr := buildDayZip(ctx, pw, rows, streamSlug, job.StreamID, s.r2.Open, func(n int) {
			s.updateDayZipJob(jobID, func(j *dayZipJob) { j.Processed = n })
		})
		// Close the writer so the uploader's reads see EOF (success) or the error.
		_ = pw.CloseWithError(buildErr)
		buildDone <- buildErr
	}()

	_, upErr := s.r2.PutMultipart(ctx, job.ZipKey, "application/zip", pr)
	// If the upload stopped early, unblock any pending write in the builder.
	_ = pr.CloseWithError(upErr)
	buildErr := <-buildDone

	if buildErr != nil {
		failJob("%v", buildErr)
		return
	}
	if upErr != nil {
		failJob("upload archive: %v", upErr)
		return
	}

	finishedAt := time.Now().UTC()
	s.updateDayZipJob(jobID, func(job *dayZipJob) {
		job.Status = "complete"
		job.FinishedAt = &finishedAt
		job.ErrorText = ""
	})
}
