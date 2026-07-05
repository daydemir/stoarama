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
	"context"
	"fmt"
	"os"
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
)

// Target is a stream selected for a recordability probe.
type Target struct {
	ID            int64
	Provider      string
	SourceURL     string
	SourcePageURL string
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
	resolvedURL, isImage, err := capture.ResolveCaptureInput(resolveCtx, t.Provider, t.SourceURL, t.SourcePageURL)
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

	capErr := capture.CaptureContinuous(captureCtx, resolvedURL, segment, "", nil, tmpDir, onSegment)

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
