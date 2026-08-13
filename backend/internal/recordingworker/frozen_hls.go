package recordingworker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/netguard"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

const (
	frozenHLSManifestMaxBytes = 64 << 10
	frozenHLSSegmentMaxBytes  = 32 << 20
	frozenHLSMaxLines         = 512
	frozenHLSMaxSegments      = 200
	frozenHLSRequestTimeout   = 10 * time.Second
	frozenHLSForcedCaptureMax = 5 * time.Minute
	frozenHLSInitialMinSpan   = 90 * time.Second
)

type frozenHLSAllowlist map[string]map[int64]struct{}

func parseFrozenHLSAllowlist(raw string) (frozenHLSAllowlist, error) {
	out := make(frozenHLSAllowlist)
	if strings.TrimSpace(raw) == "" {
		return out, nil
	}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		workerID, recordingRaw, ok := strings.Cut(item, "/")
		if !ok || !validFrozenHLSWorkerID(workerID) {
			return nil, fmt.Errorf("invalid frozen HLS allowlist entry %q: want worker-id/recording-id", item)
		}
		recordingID, err := strconv.ParseInt(recordingRaw, 10, 64)
		if err != nil || recordingID <= 0 || strconv.FormatInt(recordingID, 10) != recordingRaw {
			return nil, fmt.Errorf("invalid frozen HLS recording id in %q", item)
		}
		if out[workerID] == nil {
			out[workerID] = make(map[int64]struct{})
		}
		if _, duplicate := out[workerID][recordingID]; duplicate {
			return nil, fmt.Errorf("duplicate frozen HLS allowlist entry %q", item)
		}
		out[workerID][recordingID] = struct{}{}
	}
	return out, nil
}

func validFrozenHLSWorkerID(workerID string) bool {
	if workerID == "" || len(workerID) > 128 {
		return false
	}
	for _, r := range workerID {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' || r == ':' {
			continue
		}
		return false
	}
	return true
}

func (a frozenHLSAllowlist) allows(workerID string, recordingID int64) bool {
	_, ok := a[workerID][recordingID]
	return ok
}

type frozenHLSSnapshot struct {
	ResolvedURLSHA256    string
	InputHeadersSHA256   string
	ManifestSHA256       string
	MediaSequence        uint64
	DiscontinuitySeq     uint64
	TargetDuration       time.Duration
	TimelineSHA256       string
	LastSegmentURLSHA256 string
	LastSegmentFinalSHA  string
	LastSegmentSHA256    string
	LastSegmentSize      int64
}

func (s frozenHLSSnapshot) equal(other frozenHLSSnapshot) bool {
	return s == other
}

type frozenHLSObserveFunc func(context.Context, string, string) (frozenHLSSnapshot, error)
type frozenHLSObserveCurrentFunc func(context.Context, recordingapi.RecordingJob) (frozenHLSSnapshot, error)

type frozenHLSObservationResult struct {
	snapshot frozenHLSSnapshot
	err      error
}

type frozenHLSJobState struct {
	baseline   frozenHLSSnapshot
	classified bool
}

type frozenHLSCycleResult uint8

const (
	frozenHLSCycleUnclassified frozenHLSCycleResult = iota
	frozenHLSCycleResumeCapture
)

var errFrozenHLSSelfUpdate = errors.New("frozen HLS watcher interrupted for self-update")

func (w *Worker) classifyFrozenHLS(
	ctx context.Context,
	job recordingapi.RecordingJob,
	resolvedURL, inputHeaders string,
	forcedLaunchDeadline time.Time,
) (frozenHLSSnapshot, bool, error) {
	if !w.frozenHLSAllowlist.allows(w.cfg.WorkerID, job.RecordingID) ||
		w.cfg.ContinuousNoProgressTimeout <= 0 || !isDirectHLSURL(resolvedURL) {
		return frozenHLSSnapshot{}, false, nil
	}
	if !time.Now().Before(forcedLaunchDeadline) {
		return frozenHLSSnapshot{}, false, nil
	}
	// Three observations must span at least 90 seconds even before the first
	// target duration is known. If that cannot fit, do not make a request that
	// could delay the unchanged ordinary no-progress surrender.
	minimumSpan := w.initialFrozenHLSProofSpan(time.Second)
	if time.Until(forcedLaunchDeadline) <= minimumSpan {
		return frozenHLSSnapshot{}, false, nil
	}
	classificationCtx, cancel := context.WithDeadline(ctx, forcedLaunchDeadline)
	defer cancel()
	first, err := w.observeFrozenHLSWithSafety(classificationCtx, func(observeCtx context.Context) (frozenHLSSnapshot, error) {
		return w.frozenHLSObserve(observeCtx, resolvedURL, inputHeaders)
	})
	if err != nil {
		if errors.Is(err, errDiskPressure) || errors.Is(err, errFrozenHLSSelfUpdate) || classificationCtx.Err() != nil {
			return frozenHLSSnapshot{}, false, err
		}
		return frozenHLSSnapshot{}, false, nil
	}
	proofSpan := w.initialFrozenHLSProofSpan(first.TargetDuration)
	separation := proofSpan / 2
	if separation <= 0 || !time.Now().Add(proofSpan).Before(forcedLaunchDeadline) {
		return frozenHLSSnapshot{}, false, nil
	}
	if err := w.waitInitialFrozenHLS(classificationCtx, separation); err != nil {
		return frozenHLSSnapshot{}, false, err
	}
	second, err := w.observeFrozenHLSWithSafety(classificationCtx, func(observeCtx context.Context) (frozenHLSSnapshot, error) {
		return w.observeCurrentFrozenHLS(observeCtx, job)
	})
	if err != nil && (errors.Is(err, errDiskPressure) || errors.Is(err, errFrozenHLSSelfUpdate) || classificationCtx.Err() != nil) {
		return frozenHLSSnapshot{}, false, err
	}
	if err != nil || !first.equal(second) {
		return frozenHLSSnapshot{}, false, nil
	}
	if err := w.waitInitialFrozenHLS(classificationCtx, separation); err != nil {
		return frozenHLSSnapshot{}, false, err
	}
	third, err := w.observeFrozenHLSWithSafety(classificationCtx, func(observeCtx context.Context) (frozenHLSSnapshot, error) {
		return w.observeCurrentFrozenHLS(observeCtx, job)
	})
	if err != nil && (errors.Is(err, errDiskPressure) || errors.Is(err, errFrozenHLSSelfUpdate) || classificationCtx.Err() != nil) {
		return frozenHLSSnapshot{}, false, err
	}
	if err != nil || !first.equal(third) {
		return frozenHLSSnapshot{}, false, nil
	}
	return third, true, nil
}

func initialFrozenHLSProofSpan(targetDuration time.Duration) time.Duration {
	return max(6*targetDuration, frozenHLSInitialMinSpan)
}

func (w *Worker) initialFrozenHLSProofSpan(targetDuration time.Duration) time.Duration {
	if w.frozenHLSProofSpan != nil {
		return w.frozenHLSProofSpan(targetDuration)
	}
	return initialFrozenHLSProofSpan(targetDuration)
}

func (w *Worker) waitInitialFrozenHLS(ctx context.Context, delay time.Duration) error {
	if w.frozenHLSWait != nil {
		return w.frozenHLSWait(ctx, delay)
	}
	return w.waitFrozenHLS(ctx, delay)
}

// handleFrozenHLSCycle retains the already-heartbeating job lease between forced
// ordinary capture launches. The first cycle requires three separated strict
// observations. Later no-output cycles reconfirm the persisted exact baseline;
// an ambiguity or change clears it and immediately resumes capture. The caller
// creates forcedLaunchDeadline at FFmpeg exit, so proof plus waiting can never
// postpone the next launch beyond the same absolute five-minute ceiling.
func (w *Worker) handleFrozenHLSCycle(
	ctx context.Context,
	job recordingapi.RecordingJob,
	resolvedURL, inputHeaders string,
	state *frozenHLSJobState,
	forcedLaunchDeadline time.Time,
) (frozenHLSCycleResult, error) {
	if !state.classified {
		baseline, frozen, err := w.classifyFrozenHLS(ctx, job, resolvedURL, inputHeaders, forcedLaunchDeadline)
		if err != nil || !frozen {
			return frozenHLSCycleUnclassified, err
		}
		state.baseline = baseline
		state.classified = true
	} else {
		reconfirmCtx, cancel := context.WithDeadline(ctx, forcedLaunchDeadline)
		observed, err := w.observeFrozenHLSWithSafety(reconfirmCtx, func(observeCtx context.Context) (frozenHLSSnapshot, error) {
			return w.observeCurrentFrozenHLS(observeCtx, job)
		})
		reconfirmDeadlineReached := reconfirmCtx.Err() != nil && ctx.Err() == nil
		cancel()
		if errors.Is(err, errDiskPressure) || errors.Is(err, errFrozenHLSSelfUpdate) || (err != nil && ctx.Err() != nil) {
			return frozenHLSCycleResumeCapture, err
		}
		if reconfirmDeadlineReached {
			return frozenHLSCycleResumeCapture, nil
		}
		if err != nil || !state.baseline.equal(observed) {
			*state = frozenHLSJobState{}
			return frozenHLSCycleResumeCapture, nil
		}
	}
	return frozenHLSCycleResumeCapture, w.watchFrozenHLSUntil(ctx, job, state, forcedLaunchDeadline)
}

func frozenHLSInitialDeadline(ffmpegExit, lastUniqueIngest time.Time, forcedCeiling, noProgressTimeout time.Duration) time.Time {
	forcedDeadline := ffmpegExit.Add(min(forcedCeiling, frozenHLSForcedCaptureMax))
	progressDeadline := lastUniqueIngest.Add(noProgressTimeout)
	if progressDeadline.Before(forcedDeadline) {
		return progressDeadline
	}
	return forcedDeadline
}

func (w *Worker) watchFrozenHLSUntil(ctx context.Context, job recordingapi.RecordingJob, state *frozenHLSJobState, forcedLaunchDeadline time.Time) error {
	watchCtx, cancel := context.WithDeadline(ctx, forcedLaunchDeadline)
	defer cancel()
	pollEvery := state.baseline.TargetDuration
	if pollEvery > w.frozenHLSPollMax {
		pollEvery = w.frozenHLSPollMax
	}
	if pollEvery <= 0 {
		return nil
	}
	poll := time.NewTicker(pollEvery)
	defer poll.Stop()
	safety := time.NewTicker(w.frozenHLSSafetyInterval)
	defer safety.Stop()
	w.cfg.RelayDiagnostics.Stage(job.JobID, "continuous_hls_frozen_wait")
	for {
		select {
		case <-watchCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		case <-safety.C:
			if err := w.frozenHLSSafetyError(); err != nil {
				return err
			}
		case <-poll.C:
			observed, err := w.observeFrozenHLSWithSafety(watchCtx, func(observeCtx context.Context) (frozenHLSSnapshot, error) {
				return w.observeCurrentFrozenHLS(observeCtx, job)
			})
			if watchCtx.Err() != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return nil
			}
			if errors.Is(err, errDiskPressure) || errors.Is(err, errFrozenHLSSelfUpdate) {
				return err
			}
			if err != nil || !state.baseline.equal(observed) {
				*state = frozenHLSJobState{}
				return nil
			}
		}
	}
}

func (w *Worker) observeFrozenHLSWithSafety(ctx context.Context, observe func(context.Context) (frozenHLSSnapshot, error)) (frozenHLSSnapshot, error) {
	observeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan frozenHLSObservationResult, 1)
	go func() {
		snapshot, err := observe(observeCtx)
		result <- frozenHLSObservationResult{snapshot: snapshot, err: err}
	}()
	ticker := time.NewTicker(w.frozenHLSSafetyInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return frozenHLSSnapshot{}, ctx.Err()
		case observed := <-result:
			return observed.snapshot, observed.err
		case <-ticker.C:
			if err := w.frozenHLSSafetyError(); err != nil {
				return frozenHLSSnapshot{}, err
			}
		}
	}
}

func (w *Worker) waitFrozenHLS(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	safety := time.NewTicker(w.frozenHLSSafetyInterval)
	defer safety.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		case <-safety.C:
			if err := w.frozenHLSSafetyError(); err != nil {
				return err
			}
		}
	}
}

func (w *Worker) frozenHLSSafetyError() error {
	if w.cfg.DrainForUpdate != nil && w.cfg.DrainForUpdate.Load() {
		return errFrozenHLSSelfUpdate
	}
	if !w.diskHasSpace(w.cfg.MinActiveFreeBytes) {
		return errDiskPressure
	}
	return nil
}

func (w *Worker) observeCurrentFrozenHLS(ctx context.Context, job recordingapi.RecordingJob) (frozenHLSSnapshot, error) {
	if w.frozenHLSObserveCurrent != nil {
		return w.frozenHLSObserveCurrent(ctx, job)
	}
	resolveCtx, cancel := context.WithTimeout(ctx, frozenHLSRequestTimeout)
	resolved, isImage, inputHeaders, err := capture.ResolveCaptureInputWithHeaders(resolveCtx, job.StreamProvider, job.SourceURL, job.SourcePageURL)
	cancel()
	if err != nil || isImage || !isDirectHLSURL(resolved) {
		return frozenHLSSnapshot{}, fmt.Errorf("source no longer has an unambiguous direct HLS identity")
	}
	if _, err := netguard.ValidatePublicURL(resolved); err != nil {
		return frozenHLSSnapshot{}, fmt.Errorf("resolved source rejected")
	}
	return w.frozenHLSObserve(ctx, resolved, inputHeaders)
}

func isDirectHLSURL(rawURL string) bool {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	if u.User != nil || u.Hostname() == "" || u.Fragment != "" || (scheme != "http" && scheme != "https") {
		return false
	}
	return strings.EqualFold(pathExtension(u.Path), ".m3u8")
}

func pathExtension(path string) string {
	lastSlash := strings.LastIndex(path, "/")
	lastDot := strings.LastIndex(path, ".")
	if lastDot <= lastSlash {
		return ""
	}
	return path[lastDot:]
}

type parsedFrozenHLS struct {
	mediaSequence        uint64
	discontinuitySeq     uint64
	targetDuration       time.Duration
	timelineSHA256       string
	lastSegmentURL       string
	lastSegmentURLSHA256 string
}

func observeFrozenHLS(ctx context.Context, resolvedURL, inputHeaders string) (frozenHLSSnapshot, error) {
	if _, err := netguard.ValidatePublicURL(resolvedURL); err != nil {
		return frozenHLSSnapshot{}, fmt.Errorf("manifest URL rejected: %w", err)
	}
	origin, err := frozenHLSOrigin(resolvedURL)
	if err != nil {
		return frozenHLSSnapshot{}, err
	}
	headers, err := parseFrozenHLSInputHeaders(inputHeaders)
	if err != nil {
		return frozenHLSSnapshot{}, err
	}
	transport := &http.Transport{
		DisableCompression: true,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 10 * time.Second,
			Control:   netguard.ControlReject,
		}).DialContext,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Timeout:   frozenHLSRequestTimeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return fmt.Errorf("too many frozen HLS redirects")
			}
			if _, err := netguard.ValidatePublicURL(req.URL.String()); err != nil {
				return fmt.Errorf("redirect rejected: %w", err)
			}
			if got, err := frozenHLSOrigin(req.URL.String()); err != nil || got != origin {
				return fmt.Errorf("cross-origin frozen HLS redirect rejected")
			}
			return nil
		},
	}

	body, finalURL, err := frozenHLSGet(ctx, client, resolvedURL, headers, frozenHLSManifestMaxBytes, "application/vnd.apple.mpegurl,application/x-mpegURL,*/*")
	if err != nil {
		return frozenHLSSnapshot{}, err
	}
	if got, err := frozenHLSOrigin(finalURL); err != nil || got != origin {
		return frozenHLSSnapshot{}, fmt.Errorf("manifest response changed origin")
	}
	parsed, err := parseFrozenHLSMediaPlaylist(body, finalURL)
	if err != nil {
		return frozenHLSSnapshot{}, err
	}
	if got, err := frozenHLSOrigin(parsed.lastSegmentURL); err != nil || got != origin {
		return frozenHLSSnapshot{}, fmt.Errorf("last segment is not same-origin")
	}
	if _, err := netguard.ValidatePublicURL(parsed.lastSegmentURL); err != nil {
		return frozenHLSSnapshot{}, fmt.Errorf("last segment URL rejected: %w", err)
	}
	segment, segmentFinalURL, err := frozenHLSGet(ctx, client, parsed.lastSegmentURL, headers, frozenHLSSegmentMaxBytes, "*/*")
	if err != nil {
		return frozenHLSSnapshot{}, fmt.Errorf("read last segment: %w", err)
	}
	if got, err := frozenHLSOrigin(segmentFinalURL); err != nil || got != origin {
		return frozenHLSSnapshot{}, fmt.Errorf("last segment response changed origin")
	}
	if len(segment) == 0 {
		return frozenHLSSnapshot{}, fmt.Errorf("last segment is empty")
	}
	segmentSHA := sha256.Sum256(segment)
	manifestSHA := sha256.Sum256(body)
	resolvedSHA := sha256.Sum256([]byte(strings.TrimSpace(resolvedURL)))
	headerSHA := sha256.Sum256([]byte(inputHeaders))
	return frozenHLSSnapshot{
		ResolvedURLSHA256:    hex.EncodeToString(resolvedSHA[:]),
		InputHeadersSHA256:   hex.EncodeToString(headerSHA[:]),
		ManifestSHA256:       hex.EncodeToString(manifestSHA[:]),
		MediaSequence:        parsed.mediaSequence,
		DiscontinuitySeq:     parsed.discontinuitySeq,
		TargetDuration:       parsed.targetDuration,
		TimelineSHA256:       parsed.timelineSHA256,
		LastSegmentURLSHA256: parsed.lastSegmentURLSHA256,
		LastSegmentFinalSHA:  digestFrozenHLSString(segmentFinalURL),
		LastSegmentSHA256:    hex.EncodeToString(segmentSHA[:]),
		LastSegmentSize:      int64(len(segment)),
	}, nil
}

func frozenHLSGet(ctx context.Context, client *http.Client, rawURL string, headers http.Header, maxBytes int64, accept string) ([]byte, string, error) {
	requestCtx, cancel := context.WithTimeout(ctx, frozenHLSRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("build frozen HLS request: %w", err)
	}
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("frozen HLS request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("frozen HLS response status=%d", resp.StatusCode)
	}
	if strings.TrimSpace(resp.Header.Get("Content-Encoding")) != "" {
		return nil, "", fmt.Errorf("frozen HLS response uses content encoding")
	}
	if err := rejectFrozenHLSCacheAmbiguity(resp.Header); err != nil {
		return nil, "", err
	}
	if resp.ContentLength > maxBytes {
		return nil, "", fmt.Errorf("frozen HLS response exceeds %d bytes", maxBytes)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("read frozen HLS response: %w", err)
	}
	if int64(len(body)) > maxBytes {
		return nil, "", fmt.Errorf("frozen HLS response exceeds %d bytes", maxBytes)
	}
	return body, resp.Request.URL.String(), nil
}

func rejectFrozenHLSCacheAmbiguity(header http.Header) error {
	if len(header.Values("Warning")) != 0 {
		return fmt.Errorf("cached response carries Warning header")
	}
	if raw := strings.TrimSpace(header.Get("Age")); raw != "" {
		age, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || age != 0 {
			return fmt.Errorf("cached response age is ambiguous")
		}
	}
	for _, name := range []string{"X-Cache", "X-Proxy-Cache"} {
		if strings.TrimSpace(frozenHLSHeaderValue(header, name)) != "" {
			return fmt.Errorf("cached response carries ambiguous cache status")
		}
	}
	if raw := strings.ToUpper(strings.TrimSpace(frozenHLSHeaderValue(header, "CF-Cache-Status"))); raw != "" && raw != "DYNAMIC" && raw != "BYPASS" && raw != "MISS" {
		return fmt.Errorf("cached response carries ambiguous cache status")
	}
	return nil
}

func frozenHLSHeaderValue(header http.Header, name string) string {
	for key, values := range header {
		if strings.EqualFold(key, name) {
			return strings.Join(values, ",")
		}
	}
	return ""
}

func parseFrozenHLSInputHeaders(raw string) (http.Header, error) {
	headers := make(http.Header)
	if raw == "" {
		return headers, nil
	}
	trimmed := strings.TrimSuffix(raw, "\r\n")
	if strings.Contains(strings.ReplaceAll(trimmed, "\r\n", ""), "\r") || strings.Contains(strings.ReplaceAll(trimmed, "\r\n", ""), "\n") {
		return nil, fmt.Errorf("input headers use invalid framing")
	}
	lines := strings.Split(trimmed, "\r\n")
	if len(lines) > 16 {
		return nil, fmt.Errorf("too many input headers")
	}
	for _, line := range lines {
		name, value, ok := strings.Cut(line, ":")
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)
		if !ok || name == "" || len(name) > 128 || len(value) > 4096 || !validFrozenHLSHeaderValue(value) {
			return nil, fmt.Errorf("invalid input header")
		}
		switch strings.ToLower(name) {
		case "host", "content-length", "transfer-encoding", "connection", "range", "accept", "cache-control", "pragma":
			return nil, fmt.Errorf("input header %q is reserved", name)
		}
		headers.Add(name, value)
	}
	return headers, nil
}

func validFrozenHLSHeaderValue(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func frozenHLSOrigin(rawURL string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || u.Hostname() == "" {
		return "", fmt.Errorf("invalid frozen HLS URL")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" || u.User != nil {
		return "", fmt.Errorf("invalid frozen HLS URL authority")
	}
	port := u.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return scheme + "://" + strings.ToLower(u.Hostname()) + ":" + port, nil
}

func parseFrozenHLSMediaPlaylist(body []byte, baseURL string) (parsedFrozenHLS, error) {
	if len(body) == 0 || len(body) > frozenHLSManifestMaxBytes || bytes.ContainsRune(body, '\r') {
		return parsedFrozenHLS{}, fmt.Errorf("manifest has invalid framing or size")
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return parsedFrozenHLS{}, fmt.Errorf("parse manifest base URL: %w", err)
	}
	type segmentIdentity struct {
		duration      string
		urlSHA256     string
		discontinuity uint64
		mapSHA256     string
		keySHA256     string
	}
	var (
		lineCount, segmentCount int
		targetDuration          uint64
		mediaSequence           uint64
		discontinuitySeq        uint64
		sawTarget, sawSequence  bool
		sawDiscontinuitySeq     bool
		pendingDuration         string
		currentMap, currentKey  string
		segments                []segmentIdentity
		lastURL                 string
	)
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 2048), 2048)
	for scanner.Scan() {
		lineCount++
		if lineCount > frozenHLSMaxLines {
			return parsedFrozenHLS{}, fmt.Errorf("manifest has too many lines")
		}
		rawLine := scanner.Text()
		line := strings.TrimSpace(rawLine)
		if line != rawLine {
			return parsedFrozenHLS{}, fmt.Errorf("manifest line is not canonical")
		}
		if lineCount == 1 && line != "#EXTM3U" {
			return parsedFrozenHLS{}, fmt.Errorf("manifest lacks canonical EXTM3U header")
		}
		switch {
		case line == "", line == "#EXTM3U":
			continue
		case line == "#EXT-X-ENDLIST":
			return parsedFrozenHLS{}, fmt.Errorf("manifest is not live")
		case strings.HasPrefix(line, "#EXT-X-STREAM-INF"), strings.HasPrefix(line, "#EXT-X-I-FRAME-STREAM-INF"):
			return parsedFrozenHLS{}, fmt.Errorf("master playlists are not classified")
		case strings.HasPrefix(line, "#EXT-X-PART"), strings.HasPrefix(line, "#EXT-X-PRELOAD-HINT"), strings.HasPrefix(line, "#EXT-X-SKIP"):
			return parsedFrozenHLS{}, fmt.Errorf("low-latency playlists are not classified")
		case strings.HasPrefix(line, "#EXT-X-BYTERANGE"):
			return parsedFrozenHLS{}, fmt.Errorf("byterange playlists are not classified")
		case strings.HasPrefix(line, "#EXT-X-TARGETDURATION:"):
			if sawTarget {
				return parsedFrozenHLS{}, fmt.Errorf("duplicate target duration")
			}
			targetDuration, err = parseFrozenHLSUint(strings.TrimPrefix(line, "#EXT-X-TARGETDURATION:"))
			if err != nil || targetDuration == 0 || targetDuration > 300 {
				return parsedFrozenHLS{}, fmt.Errorf("invalid target duration")
			}
			sawTarget = true
		case strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"):
			if sawSequence {
				return parsedFrozenHLS{}, fmt.Errorf("duplicate media sequence")
			}
			mediaSequence, err = parseFrozenHLSUint(strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"))
			if err != nil {
				return parsedFrozenHLS{}, fmt.Errorf("invalid media sequence")
			}
			sawSequence = true
		case strings.HasPrefix(line, "#EXT-X-DISCONTINUITY-SEQUENCE:"):
			if sawDiscontinuitySeq {
				return parsedFrozenHLS{}, fmt.Errorf("duplicate discontinuity sequence")
			}
			discontinuitySeq, err = parseFrozenHLSUint(strings.TrimPrefix(line, "#EXT-X-DISCONTINUITY-SEQUENCE:"))
			if err != nil {
				return parsedFrozenHLS{}, fmt.Errorf("invalid discontinuity sequence")
			}
			sawDiscontinuitySeq = true
		case line == "#EXT-X-DISCONTINUITY":
			discontinuitySeq++
		case strings.HasPrefix(line, "#EXT-X-MAP:"):
			currentMap = digestFrozenHLSString(line)
		case strings.HasPrefix(line, "#EXT-X-KEY:"):
			currentKey = digestFrozenHLSString(line)
		case strings.HasPrefix(line, "#EXTINF:"):
			if pendingDuration != "" {
				return parsedFrozenHLS{}, fmt.Errorf("segment duration missing URI")
			}
			durationRaw := strings.SplitN(strings.TrimPrefix(line, "#EXTINF:"), ",", 2)[0]
			duration, parseErr := strconv.ParseFloat(durationRaw, 64)
			if parseErr != nil || math.IsNaN(duration) || math.IsInf(duration, 0) || duration <= 0 {
				return parsedFrozenHLS{}, fmt.Errorf("invalid segment duration")
			}
			pendingDuration = durationRaw
		case strings.HasPrefix(line, "#EXT-X-VERSION:"), strings.HasPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:"):
			// These tags do not change the segment byte identity, but must remain
			// syntactically visible rather than falling through as an unknown tag.
			continue
		case strings.HasPrefix(line, "#EXT-X-"):
			return parsedFrozenHLS{}, fmt.Errorf("unsupported media-playlist tag")
		case strings.HasPrefix(line, "#"):
			continue
		default:
			if pendingDuration == "" {
				return parsedFrozenHLS{}, fmt.Errorf("segment URI lacks duration")
			}
			ref, parseErr := url.Parse(line)
			if parseErr != nil {
				return parsedFrozenHLS{}, fmt.Errorf("invalid segment URI")
			}
			resolved := base.ResolveReference(ref)
			if resolved.User != nil || resolved.Fragment != "" || (resolved.Scheme != "http" && resolved.Scheme != "https") {
				return parsedFrozenHLS{}, fmt.Errorf("invalid segment URL authority")
			}
			lastURL = resolved.String()
			segments = append(segments, segmentIdentity{
				duration: pendingDuration, urlSHA256: digestFrozenHLSString(lastURL),
				discontinuity: discontinuitySeq, mapSHA256: currentMap, keySHA256: currentKey,
			})
			pendingDuration = ""
			segmentCount++
			if segmentCount > frozenHLSMaxSegments {
				return parsedFrozenHLS{}, fmt.Errorf("manifest has too many segments")
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return parsedFrozenHLS{}, fmt.Errorf("scan manifest: %w", err)
	}
	if !sawTarget || !sawSequence || pendingDuration != "" || len(segments) == 0 {
		return parsedFrozenHLS{}, fmt.Errorf("manifest is missing required media facts")
	}
	var canonical bytes.Buffer
	writeFrozenHLSField(&canonical, "stoarama-frozen-hls-media-v1")
	writeFrozenHLSField(&canonical, strconv.FormatUint(mediaSequence, 10))
	writeFrozenHLSField(&canonical, strconv.FormatUint(uint64(len(segments)), 10))
	for _, segment := range segments {
		writeFrozenHLSField(&canonical, segment.duration)
		writeFrozenHLSField(&canonical, segment.urlSHA256)
		writeFrozenHLSField(&canonical, strconv.FormatUint(segment.discontinuity, 10))
		writeFrozenHLSField(&canonical, segment.mapSHA256)
		writeFrozenHLSField(&canonical, segment.keySHA256)
	}
	timelineSHA := sha256.Sum256(canonical.Bytes())
	return parsedFrozenHLS{
		mediaSequence: mediaSequence, discontinuitySeq: discontinuitySeq,
		targetDuration: time.Duration(targetDuration) * time.Second,
		timelineSHA256: hex.EncodeToString(timelineSHA[:]), lastSegmentURL: lastURL,
		lastSegmentURLSHA256: segments[len(segments)-1].urlSHA256,
	}, nil
}

func parseFrozenHLSUint(raw string) (uint64, error) {
	if raw == "" || (len(raw) > 1 && raw[0] == '0') {
		return 0, fmt.Errorf("non-canonical unsigned integer")
	}
	return strconv.ParseUint(raw, 10, 64)
}

func digestFrozenHLSString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func writeFrozenHLSField(out *bytes.Buffer, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	out.Write(length[:])
	out.WriteString(value)
}
