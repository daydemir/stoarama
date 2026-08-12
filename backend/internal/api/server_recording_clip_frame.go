package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/storage"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	clipFrameMaxClipBytes = int64(128 << 20)
	clipFrameTimeout      = 20 * time.Second
)

var clipFrameSlots = make(chan struct{}, 2)

type recordingClipFrameRequest struct {
	AccountID int64 `json:"account_id"`
}

type recordingClipFrameSource struct {
	accountID, recordingID, streamID, clipID int64
	clipStartAt                              time.Time
	clipEndAt                                *time.Time
	clipSHA, clipETag                        string
	dest                                     clipDestination
}

type clipFrameObject interface {
	Head(context.Context, string) (r2.ObjectHead, error)
	OpenExact(context.Context, string, string, string) (io.ReadCloser, error)
}

type clipFrameHTTPError struct {
	status int
	public string
}

func (e *clipFrameHTTPError) Error() string { return e.public }

func (s *Server) handleRecordingClipAuthoritativeFrame(w http.ResponseWriter, r *http.Request) {
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	clipID, ok := parseInt64Path(w, r, "clipId")
	if !ok {
		return
	}
	var req recordingClipFrameRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.AccountID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "account_id is required")
		return
	}
	select {
	case clipFrameSlots <- struct{}{}:
		defer func() { <-clipFrameSlots }()
	default:
		util.WriteError(w, http.StatusTooManyRequests, "authoritative clip-frame capacity is busy")
		return
	}

	frameID, frameSHA, err := s.createRecordingClipAuthoritativeFrame(r.Context(), req.AccountID, recordingID, clipID)
	if err != nil {
		var public *clipFrameHTTPError
		if errors.As(err, &public) {
			util.WriteError(w, public.status, public.public)
			return
		}
		log.Printf("recording clip authoritative frame failed account_id=%d recording_id=%d clip_id=%d: %v", req.AccountID, recordingID, clipID, err)
		util.WriteError(w, http.StatusInternalServerError, "create authoritative frame")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok": true, "frame_id": frameID, "frame_sha256": frameSHA,
		"recording_id": recordingID, "clip_id": clipID, "recording_heartbeat": false,
	})
}

func (s *Server) createRecordingClipAuthoritativeFrame(ctx context.Context, accountID, recordingID, clipID int64) (int64, string, error) {
	src, err := s.loadRecordingClipFrameSource(ctx, accountID, recordingID, clipID)
	if err != nil {
		return 0, "", err
	}
	if s.secrets == nil {
		return 0, "", &clipFrameHTTPError{http.StatusServiceUnavailable, "managed storage is unavailable"}
	}
	if err := validateStorageEndpointHTTPS(src.dest.endpoint); err != nil {
		return 0, "", &clipFrameHTTPError{http.StatusConflict, "managed clip destination is invalid"}
	}
	// Managed rows are a snapshot of the operator destination. Refuse a drifted
	// row instead of decrypting or forwarding credentials to another endpoint.
	if strings.TrimSpace(src.dest.endpoint) != strings.TrimSpace(s.cfg.R2Endpoint) || strings.TrimSpace(src.dest.bucket) != strings.TrimSpace(s.cfg.R2Bucket) {
		return 0, "", &clipFrameHTTPError{http.StatusConflict, "managed clip destination does not match operator storage"}
	}
	client, err := s.buildClipClientCtx(ctx, src.dest)
	if err != nil {
		return 0, "", fmt.Errorf("build managed clip client: %w", err)
	}
	frame, versionID, err := verifiedFrameFromClip(ctx, client, src, ffmpegFrameFromClip)
	if err != nil {
		return 0, "", err
	}
	return s.persistClipBackedAuthoritativeFrame(ctx, src, versionID, frame, s.r2.Bucket(), s.r2.PutBytes)
}

func (s *Server) loadRecordingClipFrameSource(ctx context.Context, accountID, recordingID, clipID int64) (recordingClipFrameSource, error) {
	var src recordingClipFrameSource
	var purgedAt *time.Time
	var managed bool
	err := s.pool.QueryRow(ctx, `
		SELECT r.account_id,r.id,r.stream_id,c.id,c.clip_start_at,c.clip_end_at,
		       lower(c.sha256),btrim(c.etag),c.object_key,c.size_bytes,
		       c.purged_at,sd.managed,sd.region,sd.bucket,sd.endpoint,sd.access_key_id,sd.secret_access_key_enc
		FROM recordings r JOIN recording_clips c ON c.recording_id=r.id
		JOIN storage_destinations sd ON sd.id=c.storage_destination_id
		WHERE r.account_id=$1 AND r.id=$2 AND r.status='active' AND c.id=$3
	`, accountID, recordingID, clipID).Scan(
		&src.accountID, &src.recordingID, &src.streamID, &src.clipID, &src.clipStartAt, &src.clipEndAt,
		&src.clipSHA, &src.clipETag, &src.dest.objectKey, &src.dest.sizeBytes,
		&purgedAt, &managed, &src.dest.region, &src.dest.bucket, &src.dest.endpoint, &src.dest.accessKeyID, &src.dest.secretEnc,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return src, &clipFrameHTTPError{http.StatusNotFound, "active recording clip not found"}
	}
	if err != nil {
		return src, fmt.Errorf("load recording clip: %w", err)
	}
	if purgedAt != nil || !managed {
		return src, &clipFrameHTTPError{http.StatusConflict, "clip is purged or not in managed storage"}
	}
	if src.dest.sizeBytes <= 0 || src.dest.sizeBytes > clipFrameMaxClipBytes {
		return src, &clipFrameHTTPError{http.StatusRequestEntityTooLarge, "clip exceeds authoritative-frame size limit"}
	}
	if len(src.clipSHA) != 64 || len(src.clipETag) == 0 || len(src.clipETag) > 256 {
		return src, &clipFrameHTTPError{http.StatusConflict, "clip object identity is incomplete"}
	}
	if _, err := hex.DecodeString(src.clipSHA); err != nil {
		return src, &clipFrameHTTPError{http.StatusConflict, "clip object identity is invalid"}
	}
	return src, nil
}

func verifiedFrameFromClip(ctx context.Context, obj clipFrameObject, src recordingClipFrameSource, decode func(context.Context, io.Reader) (capture.Frame, error)) (capture.Frame, string, error) {
	head, err := obj.Head(ctx, src.dest.objectKey)
	if err != nil {
		return capture.Frame{}, "", &clipFrameHTTPError{http.StatusConflict, "managed clip object is unavailable"}
	}
	if head.SizeBytes != src.dest.sizeBytes || !strings.EqualFold(strings.TrimSpace(head.ETag), src.clipETag) {
		return capture.Frame{}, "", &clipFrameHTTPError{http.StatusConflict, "managed clip object metadata mismatch"}
	}
	body, err := obj.OpenExact(ctx, src.dest.objectKey, head.ETag, head.VersionID)
	if err != nil {
		return capture.Frame{}, "", &clipFrameHTTPError{http.StatusConflict, "managed clip object generation changed"}
	}
	defer body.Close()

	tmp, err := os.CreateTemp("", "stoarama-clip-frame-*.media")
	if err != nil {
		return capture.Frame{}, "", fmt.Errorf("create bounded clip spool: %w", err)
	}
	path := tmp.Name()
	defer os.Remove(path)
	defer tmp.Close()
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(tmp, hash), io.LimitReader(body, src.dest.sizeBytes+1))
	if copyErr != nil || written != src.dest.sizeBytes {
		return capture.Frame{}, "", &clipFrameHTTPError{http.StatusConflict, "managed clip object size mismatch"}
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != src.clipSHA {
		return capture.Frame{}, "", &clipFrameHTTPError{http.StatusConflict, "managed clip object SHA-256 mismatch"}
	}
	headAfter, err := obj.Head(ctx, src.dest.objectKey)
	if err != nil || headAfter.SizeBytes != head.SizeBytes || !strings.EqualFold(headAfter.ETag, head.ETag) || headAfter.VersionID != head.VersionID {
		return capture.Frame{}, "", &clipFrameHTTPError{http.StatusConflict, "managed clip object changed during verification"}
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return capture.Frame{}, "", fmt.Errorf("rewind verified clip: %w", err)
	}
	frame, err := decode(ctx, tmp)
	if err != nil {
		return capture.Frame{}, "", &clipFrameHTTPError{http.StatusUnprocessableEntity, "verified clip does not contain a decodable video frame"}
	}
	return frame, head.VersionID, nil
}

type boundedWriter struct {
	b bytes.Buffer
	n int64
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if int64(w.b.Len())+int64(len(p)) > w.n {
		return 0, fmt.Errorf("output exceeds limit")
	}
	return w.b.Write(p)
}

func ffmpegFrameFromClip(parent context.Context, input io.Reader) (capture.Frame, error) {
	bin := strings.TrimSpace(os.Getenv("FFMPEG_BIN"))
	if bin == "" {
		return capture.Frame{}, fmt.Errorf("FFMPEG_BIN is required")
	}
	ctx, cancel := context.WithTimeout(parent, clipFrameTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, clipFrameFFmpegArgs()...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = time.Second
	cmd.Stdin = input
	out := &boundedWriter{n: authoritativeFrameMaxBytes}
	errOut := &boundedWriter{n: 32 << 10}
	cmd.Stdout, cmd.Stderr = out, errOut
	if err := cmd.Run(); err != nil {
		return capture.Frame{}, fmt.Errorf("ffmpeg frame decode failed")
	}
	jpegBytes := out.b.Bytes()
	cfg, format, err := image.DecodeConfig(bytes.NewReader(jpegBytes))
	if err != nil || format != "jpeg" || cfg.Width <= 0 || cfg.Height <= 0 || cfg.Width > 8192 || cfg.Height > 8192 || int64(cfg.Width)*int64(cfg.Height) > 33_554_432 {
		return capture.Frame{}, fmt.Errorf("invalid decoded JPEG")
	}
	return capture.BuildFrameFromBytes(jpegBytes, "image/jpeg", "authoritative_frame_refresh")
}

func clipFrameFFmpegArgs() []string {
	return []string{
		"-nostdin", "-v", "error",
		"-protocol_whitelist", "pipe,fd", "-protocol_blacklist", "file,http,https,tcp,tls,udp,rtp,ftp",
		"-i", "pipe:0", "-map", "0:v:0", "-frames:v", "1", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1",
	}
}

type clipFramePut func(context.Context, string, string, []byte) (string, error)

func (s *Server) persistClipBackedAuthoritativeFrame(ctx context.Context, src recordingClipFrameSource, versionID string, frame capture.Frame, bucket string, put clipFramePut) (int64, string, error) {
	capturedAt := src.clipStartAt.UTC()
	if src.clipEndAt != nil && src.clipEndAt.After(src.clipStartAt) {
		capturedAt = src.clipStartAt.Add(src.clipEndAt.Sub(src.clipStartAt) / 2).UTC()
	}
	objectKey := fmt.Sprintf("raw/stream/%d/%04d/%02d/%02d/authoritative-clip-%d-%s-%s.jpg", src.streamID, capturedAt.Year(), int(capturedAt.Month()), capturedAt.Day(), src.clipID, src.clipSHA, frame.SHA256)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, "", fmt.Errorf("begin clip frame tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	identity := fmt.Sprintf("authoritative-clip-frame:%d:%d:%d", src.accountID, src.recordingID, src.clipID)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, identity); err != nil {
		return 0, "", fmt.Errorf("lock clip frame identity: %w", err)
	}
	var currentSHA, currentETag string
	if err = tx.QueryRow(ctx, `SELECT lower(c.sha256),btrim(c.etag) FROM recordings r JOIN recording_clips c ON c.recording_id=r.id JOIN storage_destinations sd ON sd.id=c.storage_destination_id WHERE r.account_id=$1 AND r.id=$2 AND r.stream_id=$3 AND r.status='active' AND c.id=$4 AND c.purged_at IS NULL AND sd.managed FOR SHARE OF r,c,sd`, src.accountID, src.recordingID, src.streamID, src.clipID).Scan(&currentSHA, &currentETag); err != nil {
		return 0, "", err
	}
	if currentSHA != src.clipSHA || currentETag != src.clipETag {
		return 0, "", fmt.Errorf("clip identity changed before persistence")
	}
	var frameID int64
	var existingFrameSHA, existingClipSHA, existingETag, existingVersion string
	err = tx.QueryRow(ctx, `SELECT f.id,lower(m.sha256),f.source_recording_clip_sha256,f.source_recording_clip_etag,COALESCE(f.source_recording_clip_version_id,'') FROM frames f JOIN media_objects m ON m.id=f.raw_media_object_id WHERE f.source_recording_clip_id=$1`, src.clipID).Scan(&frameID, &existingFrameSHA, &existingClipSHA, &existingETag, &existingVersion)
	if err == nil {
		if existingFrameSHA != frame.SHA256 || existingClipSHA != src.clipSHA || existingETag != src.clipETag || existingVersion != versionID {
			return 0, "", fmt.Errorf("clip frame provenance conflict")
		}
		if err = tx.Commit(ctx); err != nil {
			return 0, "", fmt.Errorf("commit existing clip frame: %w", err)
		}
		return frameID, existingFrameSHA, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, "", fmt.Errorf("read clip frame identity: %w", err)
	}
	etag, err := put(ctx, objectKey, frame.MIMEType, frame.Bytes)
	if err != nil {
		return 0, "", fmt.Errorf("upload clip frame: %w", err)
	}
	mediaID, err := storage.UpsertMediaObject(ctx, tx, storage.MediaObjectInput{StorageProvider: "r2", Bucket: bucket, ObjectKey: objectKey, MIMEType: frame.MIMEType, SizeBytes: frame.SizeBytes, ETag: etag, SHA256: frame.SHA256, Width: frame.Width, Height: frame.Height})
	if err != nil {
		return 0, "", fmt.Errorf("upsert clip frame media: %w", err)
	}
	if err = tx.QueryRow(ctx, `INSERT INTO frames(stream_id,capture_job_id,captured_at,raw_media_object_id,capture_status,capture_error,source_kind,source_recording_clip_id,source_recording_clip_sha256,source_recording_clip_etag,source_recording_clip_version_id) VALUES($1,NULL,$2,$3,'success',NULL,'authoritative_frame_refresh',$4,$5,$6,NULLIF($7,'')) RETURNING id`, src.streamID, capturedAt, mediaID, src.clipID, src.clipSHA, src.clipETag, versionID).Scan(&frameID); err != nil {
		return 0, "", fmt.Errorf("insert clip frame: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO stream_health(stream_id,captures_total,captures_success,captures_error,last_capture_at,last_error_at,last_error_text) VALUES($1,1,1,0,$2,NULL,NULL) ON CONFLICT(stream_id) DO UPDATE SET captures_total=stream_health.captures_total+1,captures_success=stream_health.captures_success+1,last_capture_at=GREATEST(stream_health.last_capture_at,EXCLUDED.last_capture_at),updated_at=now()`, src.streamID, capturedAt); err != nil {
		return 0, "", fmt.Errorf("update clip frame health: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, "", fmt.Errorf("commit clip frame: %w", err)
	}
	return frameID, frame.SHA256, nil
}
