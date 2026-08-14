// Package recordability probes whether a stream can actually be recorded from a
// datacenter (DO) IP whose egress reputation matches the recorder droplet pool.
// It attempts a REAL ~600s recording to droplet-local temp, ffprobe-verifies the
// footage is a continuous valid video, classifies ok|blocked|source_unstable|
// inconclusive, records the verdict, then DELETES the footage. It NEVER uploads to
// R2 or user storage, NEVER creates recording_clips, and NEVER touches leases or
// billing. It shares only capture/netguard helpers with the recorder.
//
// Execution is gated by the caller behind STREAM_RECORDABILITY_PROBE_ENABLED
// (default off): with the flag off nothing here runs, so no droplet, no ffmpeg,
// zero spend, and both tables stay empty.
package recordability

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/netguard"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DefaultWindow is the ~10min real recording the probe attempts. Long enough to
	// catch the "resolves fine then dies after ~40s" datacenter-block pattern that a
	// resolve-only or short probe would miss.
	DefaultWindow = 600 * time.Second
	// DefaultSegment is the continuous segment length. ~10 segments over the window
	// let us measure decodable coverage and catch a mid-stream death.
	DefaultSegment = 60 * time.Second
	// TargetedFrameMaxBytes bounds the decoded JPEG proof transported from the
	// managed recorder to the API. The server independently decodes and hashes
	// these bytes; the worker-authored frame hash is never authoritative.
	TargetedFrameMaxBytes = 8 << 20
)

// Target is a stream selected for a recordability probe.
type Target struct {
	ID                int64  `json:"id"`
	Provider          string `json:"provider"`
	SourceURL         string `json:"source_url"`
	SourcePageURL     string `json:"source_page_url"`
	AttemptID         string `json:"attempt_id,omitempty"`
	Challenge         string `json:"challenge,omitempty"`
	MediaUploadURL    string `json:"media_upload_url,omitempty"`
	FrameUploadURL    string `json:"frame_upload_url,omitempty"`
	MediaMaxSizeBytes int64  `json:"media_max_size_bytes,omitempty"`
	FrameMaxSizeBytes int64  `json:"frame_max_size_bytes,omitempty"`
}

// TargetedEvidence is the immutable, actual-byte proof used by campaign
// admission. It is intentionally compact: media and decoded-frame hashes bind
// the observation without uploading probe footage.
type TargetedEvidence struct {
	ObservedAt            time.Time `json:"-"`
	Result                string    `json:"result"`
	Detail                string    `json:"detail"`
	ValidRatio            float64   `json:"valid_ratio"`
	DurationMs            int64     `json:"duration_ms"`
	SegmentCount          int       `json:"segment_count"`
	FrameSHA256           string    `json:"frame_sha256"`
	FrameBase64           string    `json:"frame_base64,omitempty"`
	MediaSHA256           string    `json:"media_sha256"`
	NativeSignatureSHA256 string    `json:"native_signature_sha256"`
	ChallengeProofSHA256  string    `json:"challenge_proof_sha256"`
	VideoCodec            string    `json:"video_codec"`
	AudioCodec            string    `json:"audio_codec"`
	AudioPresent          bool      `json:"audio_present"`
	VideoWidth            int       `json:"video_width"`
	VideoHeight           int       `json:"video_height"`
	ActualFPS             *float64  `json:"actual_fps"`
	// Local paths and cleanup root never cross the API. The worker streams these
	// exact files to server-created quarantine object intents, then removes them.
	MediaPath  string `json:"-"`
	FramePath  string `json:"-"`
	CleanupDir string `json:"-"`
}

func nativeSegmentSignature(seg capture.Segment) string {
	return targetedNativeSignature(seg.VideoCodec, seg.AudioCodec, seg.AudioPresent, seg.VideoWidth, seg.VideoHeight, seg.ActualFPS)
}

func targetedNativeSignature(videoCodec, audioCodec string, audioPresent bool, width, height int, actualFPS *float64) string {
	fps := ""
	if actualFPS != nil {
		fps = strconv.FormatFloat(*actualFPS, 'g', -1, 64)
	}
	return fmt.Sprintf("v1\nvideo=%s\naudio=%s\naudio_present=%t\nwidth=%d\nheight=%d\nfps=%s\n",
		strings.ToLower(strings.TrimSpace(videoCodec)), strings.ToLower(strings.TrimSpace(audioCodec)), audioPresent, width, height, fps)
}

// TargetedNativeSignatureSHA256 recomputes the compact native media contract
// from typed probe facts. The API ignores any caller-authored signature unless
// it exactly matches this value.
func TargetedNativeSignatureSHA256(e TargetedEvidence) string {
	return hashStrings([]string{targetedNativeSignature(e.VideoCodec, e.AudioCodec, e.AudioPresent, e.VideoWidth, e.VideoHeight, e.ActualFPS)})
}

// TargetedChallengeProofSHA256 binds one server-issued challenge to the exact
// media, decoded frame, and recomputed native signature for a single attempt.
func TargetedChallengeProofSHA256(challenge string, e TargetedEvidence) string {
	return hashStrings([]string{strings.TrimSpace(challenge), e.MediaSHA256, e.FrameSHA256, TargetedNativeSignatureSHA256(e)})
}

// TargetedMediaSHA256 binds the ordered exact segment generations sampled by a
// targeted probe. The server recomputes each segment hash after downloading the
// quarantined archive and derives this value without trusting the worker.
func TargetedMediaSHA256(segmentSHA256 []string) string {
	return hashStrings(segmentSHA256)
}

// TargetedObjectChallengeProofSHA256 binds the server-issued attempt and both
// exact object generations to the server-derived media contract.
func TargetedObjectChallengeProofSHA256(challenge, attemptID, mediaETag, mediaVersion, frameETag, frameVersion string, e TargetedEvidence) string {
	return hashStrings([]string{strings.TrimSpace(challenge), strings.TrimSpace(attemptID), strings.TrimSpace(mediaETag), strings.TrimSpace(mediaVersion), strings.TrimSpace(frameETag), strings.TrimSpace(frameVersion), e.MediaSHA256, e.FrameSHA256, TargetedNativeSignatureSHA256(e)})
}

func hashStrings(values []string) string {
	h := sha256.New()
	for _, value := range values {
		_, _ = fmt.Fprintf(h, "%d:", len(value))
		_, _ = h.Write([]byte(value))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// selectTargetsQuery selects untested / re-probeable non-YouTube video streams,
// prioritizing flagged providers, then never-probed, then oldest transient. 'ok'
// and 'blocked' rows are excluded (test-once memory: a stream seen to work or
// confirmed blocked is never re-probed). YouTube is excluded (already force-relay).
const selectTargetsQuery = `
	SELECT s.id, COALESCE(s.provider,''), COALESCE(s.source_url,''), COALESCE(s.source_page_url,'')
	FROM streams s
	LEFT JOIN stream_recordability sr ON sr.stream_id = s.id
	LEFT JOIN provider_recordability pr ON pr.provider = s.provider
	WHERE upper(COALESCE(s.provider,'')) NOT LIKE '%YOUTUBE%'
	  AND lower(COALESCE(s.source_url,'')) NOT LIKE '%youtube.com%'
	  AND lower(COALESCE(s.source_url,'')) NOT LIKE '%youtu.be%'
	  AND (sr.stream_id IS NULL OR sr.result IN ('source_unstable','inconclusive'))
	ORDER BY COALESCE(pr.needs_relay,false) DESC,
	         sr.last_probed_at ASC NULLS FIRST,
	         s.id
	LIMIT $1
`

// SelectTargets returns the next batch of streams to probe. batchSize is small by
// design (decision #3: slow background, one or very few at a time).
func SelectTargets(ctx context.Context, pool *pgxpool.Pool, batchSize int) ([]Target, error) {
	if batchSize < 1 {
		batchSize = 1
	}
	rows, err := pool.Query(ctx, selectTargetsQuery, batchSize)
	if err != nil {
		return nil, fmt.Errorf("select recordability targets: %w", err)
	}
	defer rows.Close()
	out := make([]Target, 0, batchSize)
	for rows.Next() {
		var t Target
		if err := rows.Scan(&t.ID, &t.Provider, &t.SourceURL, &t.SourcePageURL); err != nil {
			return nil, fmt.Errorf("scan recordability target: %w", err)
		}
		out = append(out, t)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate recordability targets: %w", rows.Err())
	}
	return out, nil
}

// ProbeStream runs one real recording attempt and classifies it. It resolves the
// same way the recorder does, re-runs the SSRF guard, records ~window seconds of
// continuous video to a droplet-local temp dir, ffprobe-verifies decodable
// coverage, then DELETES the temp dir. It never uploads or bills. detail carries
// the signature, valid_ratio, and error class for audit.
func ProbeStream(ctx context.Context, t Target, window, segment time.Duration) (result string, detail string) {
	if window <= 0 {
		window = DefaultWindow
	}
	if segment <= 0 {
		segment = DefaultSegment
	}

	resolveCtx, cancelResolve := context.WithTimeout(ctx, 30*time.Second)
	resolvedURL, isImage, inputHeaders, err := capture.ResolveCaptureInputWithHeaders(resolveCtx, t.Provider, t.SourceURL, t.SourcePageURL)
	cancelResolve()
	if err != nil {
		return Classify(Observation{OurErr: err.Error()}), fmt.Sprintf("resolve error: %v", err)
	}
	if isImage {
		return Classify(Observation{OurErr: "image source"}), "image source (not a video stream)"
	}
	if _, err := netguard.ValidatePublicURL(resolvedURL); err != nil {
		return Classify(Observation{OurErr: err.Error()}), fmt.Sprintf("ssrf guard rejected resolved url: %v", err)
	}

	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("recprobe-%d-", t.ID))
	if err != nil {
		return Classify(Observation{OurErr: err.Error()}), fmt.Sprintf("mktemp error: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Record for exactly the window: when captureCtx expires CaptureContinuous
	// SIGINTs ffmpeg, finalizes the last segment, and returns nil (clean). If ffmpeg
	// dies first (the block pattern) it returns the wrapped stderr, which we classify.
	captureCtx, cancelCapture := context.WithTimeout(ctx, window)
	defer cancelCapture()

	var validSeconds float64
	onSegment := func(seg capture.Segment) error {
		// A finalized segment counts its ffprobe duration toward decodable coverage
		// only when it carries a decoded video stream. Summing every valid segment
		// (not the longest contiguous run) means a single reconnect gap subtracts
		// only the gap seconds; the reconnect leaves that gap uncaptured, so the
		// surviving segments already exclude it.
		if strings.TrimSpace(seg.VideoCodec) != "" && seg.DurationMs > 0 {
			validSeconds += float64(seg.DurationMs) / 1000.0
		}
		return nil
	}

	capErr := capture.CaptureContinuousWithHeaders(captureCtx, resolvedURL, segment, "", nil, tmpDir, onSegment, inputHeaders)

	// Distinguish OUR cancellation (parent ctx cancelled = process shutdown) from the
	// window deadline (our own captureCtx timeout is expected and clean).
	if ctx.Err() != nil {
		return Classify(Observation{OurErr: "probe context cancelled"}), fmt.Sprintf("parent context cancelled: %v", ctx.Err())
	}

	ffmpegErr := ""
	// A capErr that is NOT our window deadline is a real ffmpeg exit (block/outage).
	if capErr != nil && captureCtx.Err() == nil {
		ffmpegErr = capErr.Error()
	}

	windowSeconds := window.Seconds()
	ratio := 0.0
	if windowSeconds > 0 {
		ratio = validSeconds / windowSeconds
	}
	obs := Observation{
		Started:    validSeconds > 0,
		ValidRatio: ratio,
		FFmpegErr:  ffmpegErr,
	}
	res := Classify(obs)
	sig := "none"
	switch classifySignature(ffmpegErr) {
	case sigNetworkCut:
		sig = "network_cut"
	case sigSourceDown:
		sig = "source_down"
	}
	detail = fmt.Sprintf("valid_ratio=%.3f started=%t signature=%s", ratio, obs.Started, sig)
	if ffmpegErr != "" {
		// Keep the audit detail bounded; the head carries the discriminating text.
		if len(ffmpegErr) > 300 {
			ffmpegErr = ffmpegErr[:300]
		}
		detail += " ffmpeg_err=" + ffmpegErr
	}
	return res, detail
}

// ProbeStreamTargeted performs the same no-upload continuous native capture as
// ProbeStream while deriving immutable evidence from finalized bytes. Evidence
// is complete only when at least two segments share one exact native signature
// and a decoded frame was extracted from the first accepted segment.
func ProbeStreamTargeted(ctx context.Context, t Target, window, segment time.Duration) TargetedEvidence {
	if window <= 0 {
		window = DefaultWindow
	}
	if segment <= 0 {
		segment = DefaultSegment
	}
	evidence := TargetedEvidence{ObservedAt: time.Now().UTC()}
	resolveCtx, cancelResolve := context.WithTimeout(ctx, 30*time.Second)
	resolvedURL, isImage, inputHeaders, err := capture.ResolveCaptureInputWithHeaders(resolveCtx, t.Provider, t.SourceURL, t.SourcePageURL)
	cancelResolve()
	if err != nil {
		evidence.Result = Classify(Observation{OurErr: err.Error()})
		evidence.Detail = "resolve_failed"
		return evidence
	}
	if isImage {
		evidence.Result = ResultInconclusive
		evidence.Detail = "image_source"
		return evidence
	}
	if _, err := netguard.ValidatePublicURL(resolvedURL); err != nil {
		evidence.Result = Classify(Observation{OurErr: err.Error()})
		evidence.Detail = "ssrf_guard_rejected"
		return evidence
	}
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("targeted-recprobe-%d-", t.ID))
	if err != nil {
		evidence.Result = ResultInconclusive
		evidence.Detail = "temporary_storage_unavailable"
		return evidence
	}
	keepProbeFiles := false
	defer func() {
		if !keepProbeFiles {
			_ = os.RemoveAll(tmpDir)
		}
	}()

	captureCtx, cancelCapture := context.WithTimeout(ctx, window)
	defer cancelCapture()
	var mediaHashes []string
	var signature string
	var invalidSignature bool
	var mediaPaths []string
	var firstFramePath string
	onSegment := func(seg capture.Segment) error {
		if strings.TrimSpace(seg.VideoCodec) == "" || seg.DurationMs <= 0 {
			return nil
		}
		candidateSignature := nativeSegmentSignature(seg)
		if signature == "" {
			signature = candidateSignature
			evidence.VideoCodec = strings.ToLower(strings.TrimSpace(seg.VideoCodec))
			evidence.AudioCodec = strings.ToLower(strings.TrimSpace(seg.AudioCodec))
			evidence.AudioPresent = seg.AudioPresent
			evidence.VideoWidth = seg.VideoWidth
			evidence.VideoHeight = seg.VideoHeight
			evidence.ActualFPS = seg.ActualFPS
			thumb, thumbErr := capture.ExtractSegmentThumbnail(captureCtx, seg.Path)
			if thumbErr == nil {
				frameBytes, readErr := os.ReadFile(thumb.Path)
				if readErr == nil && len(frameBytes) > 0 && len(frameBytes) <= TargetedFrameMaxBytes {
					evidence.FrameSHA256 = thumb.SHA256
					evidence.FrameBase64 = base64.StdEncoding.EncodeToString(frameBytes)
					firstFramePath = thumb.Path
				}
			}
		} else if signature != candidateSignature {
			invalidSignature = true
		}
		evidence.DurationMs += seg.DurationMs
		evidence.SegmentCount++
		mediaHashes = append(mediaHashes, seg.SHA256)
		if len(mediaPaths) < 2 {
			mediaPaths = append(mediaPaths, seg.Path)
		}
		return nil
	}
	capErr := capture.CaptureContinuousWithHeaders(captureCtx, resolvedURL, segment, "", nil, tmpDir, onSegment, inputHeaders)
	if ctx.Err() != nil {
		evidence.Result = ResultInconclusive
		evidence.Detail = "parent_context_cancelled"
		return evidence
	}
	ffmpegErr := ""
	if capErr != nil && captureCtx.Err() == nil {
		ffmpegErr = capErr.Error()
	}
	evidence.ValidRatio = float64(evidence.DurationMs) / float64(window.Milliseconds())
	evidence.Result = Classify(Observation{Started: evidence.DurationMs > 0, ValidRatio: evidence.ValidRatio, FFmpegErr: ffmpegErr})
	if evidence.Result == ResultOK && (evidence.SegmentCount < 2 || evidence.FrameSHA256 == "" || invalidSignature || signature == "") {
		evidence.Result = ResultInconclusive
	}
	if len(mediaHashes) > 0 {
		evidence.MediaSHA256 = hashStrings(mediaHashes)
	}
	if signature != "" && !invalidSignature {
		evidence.NativeSignatureSHA256 = hashStrings([]string{signature})
	}
	if evidence.MediaSHA256 != "" && evidence.FrameSHA256 != "" && evidence.NativeSignatureSHA256 != "" && strings.TrimSpace(t.Challenge) != "" {
		evidence.ChallengeProofSHA256 = hashStrings([]string{strings.TrimSpace(t.Challenge), evidence.MediaSHA256, evidence.FrameSHA256, evidence.NativeSignatureSHA256})
	}
	if evidence.Result == ResultOK && len(mediaPaths) == 2 && firstFramePath != "" {
		archivePath := filepath.Join(tmpDir, "media.zip")
		if err := writeTargetedMediaArchive(archivePath, mediaPaths); err != nil {
			evidence.Result = ResultInconclusive
			evidence.Detail = "temporary_storage_unavailable"
			return evidence
		}
		evidence.MediaPath = archivePath
		evidence.FramePath = firstFramePath
		evidence.CleanupDir = tmpDir
		keepProbeFiles = true
	}
	evidence.Detail = fmt.Sprintf("valid_ratio=%.3f segments=%d native_signature_stable=%t frame=%t", evidence.ValidRatio, evidence.SegmentCount, !invalidSignature && signature != "", evidence.FrameSHA256 != "")
	if ffmpegErr != "" {
		evidence.Detail += " capture_exit=" + targetedCaptureExitClass(ffmpegErr)
	}
	return evidence
}

func writeTargetedMediaArchive(archivePath string, paths []string) error {
	if len(paths) != 2 {
		return fmt.Errorf("targeted media archive requires exactly two segments")
	}
	out, err := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	zw := zip.NewWriter(out)
	for i, sourcePath := range paths {
		source, openErr := os.Open(sourcePath)
		if openErr != nil {
			_ = zw.Close()
			_ = out.Close()
			return openErr
		}
		entry, copyErr := zw.CreateHeader(&zip.FileHeader{Name: fmt.Sprintf("segment-%d.mp4", i+1), Method: zip.Store})
		if copyErr == nil {
			_, copyErr = io.Copy(entry, source)
		}
		_ = source.Close()
		if copyErr != nil {
			_ = zw.Close()
			_ = out.Close()
			return copyErr
		}
	}
	if err := zw.Close(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func targetedCaptureExitClass(raw string) string {
	switch classifySignature(raw) {
	case sigNetworkCut:
		return "network_cut"
	case sigSourceDown:
		return "source_down"
	default:
		return "other"
	}
}

// upsertResult writes the stream's own probe verdict (test-once memory).
func upsertResult(ctx context.Context, pool *pgxpool.Pool, streamID int64, result, detail, probeHost string) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO stream_recordability (stream_id, last_probed_at, result, detail, probe_host)
		VALUES ($1, now(), $2, $3, $4)
		ON CONFLICT (stream_id) DO UPDATE
		  SET last_probed_at = now(), result = EXCLUDED.result,
		      detail = EXCLUDED.detail, probe_host = EXCLUDED.probe_host
	`, streamID, result, detail, probeHost)
	if err != nil {
		return fmt.Errorf("upsert recordability result for stream %d: %w", streamID, err)
	}
	return nil
}

// flagProvider sticky-sets provider_recordability.needs_relay=true. Never
// auto-cleared (decision #4): a confirmed block keeps untested siblings safe.
func flagProvider(ctx context.Context, pool *pgxpool.Pool, provider string, streamID int64) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO provider_recordability (provider, needs_relay, set_by_stream_id, updated_at)
		VALUES ($1, true, $2, now())
		ON CONFLICT (provider) DO UPDATE
		  SET needs_relay = true, set_by_stream_id = EXCLUDED.set_by_stream_id, updated_at = now()
	`, provider, streamID)
	if err != nil {
		return fmt.Errorf("flag provider %q needs-relay: %w", provider, err)
	}
	return nil
}

// ProbeAndRecord probes one stream, persists its verdict, and applies the provider
// generalization with a CONFIRMATION GATE: a single 'blocked' observation could be
// a transient mid-stream reset, so before sticky-flagging the whole provider we
// re-probe the SAME stream once. Only two independent 'blocked' observations flag
// the provider. If the confirmation probe is not blocked, the second observation
// supersedes the stored verdict (the block was transient) and the provider is left
// alone.
func ProbeAndRecord(ctx context.Context, pool *pgxpool.Pool, t Target, window, segment time.Duration, probeHost string) (string, error) {
	result, detail := ProbeStream(ctx, t, window, segment)

	if result != ResultBlocked {
		if err := upsertResult(ctx, pool, t.ID, result, detail, probeHost); err != nil {
			return "", err
		}
		return result, nil
	}

	// First blocked observation. Confirm with one re-probe of the same stream before
	// the sticky provider flag (guards against a one-off connection-reset/EOF blip).
	confirmResult, confirmDetail := ProbeStream(ctx, t, window, segment)
	if confirmResult == ResultBlocked {
		mergedDetail := "confirmed block (2 observations): " + confirmDetail
		if err := upsertResult(ctx, pool, t.ID, ResultBlocked, mergedDetail, probeHost); err != nil {
			return "", err
		}
		if strings.TrimSpace(t.Provider) != "" {
			if err := flagProvider(ctx, pool, t.Provider, t.ID); err != nil {
				return "", err
			}
		}
		return ResultBlocked, nil
	}

	// Not confirmed: the second observation wins, provider left unflagged.
	mergedDetail := fmt.Sprintf("unconfirmed block (first=blocked second=%s): %s", confirmResult, confirmDetail)
	if err := upsertResult(ctx, pool, t.ID, confirmResult, mergedDetail, probeHost); err != nil {
		return "", err
	}
	return confirmResult, nil
}

// RunResult summarizes a probe sweep.
type RunResult struct {
	Total   int
	OK      int
	Blocked int
	Other   int
	Failed  int
}

// RunOnce probes a batch of targets sequentially (decision #3: one at a time, no
// rush). A per-stream error does not abort the sweep. tmpDir footage is deleted per
// stream inside ProbeStream; nothing is uploaded and nothing is billed.
func RunOnce(ctx context.Context, pool *pgxpool.Pool, targets []Target, window, segment time.Duration, probeHost string, onError func(streamID int64, err error)) RunResult {
	res := RunResult{Total: len(targets)}
	for _, t := range targets {
		if ctx.Err() != nil {
			break
		}
		verdict, err := ProbeAndRecord(ctx, pool, t, window, segment, probeHost)
		if err != nil {
			res.Failed++
			if onError != nil {
				onError(t.ID, err)
			}
			continue
		}
		switch verdict {
		case ResultOK:
			res.OK++
		case ResultBlocked:
			res.Blocked++
		default:
			res.Other++
		}
	}
	return res
}
