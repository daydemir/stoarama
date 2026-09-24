package capture

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestYouTubeLiveSoak is an opt-in, networked soak of the relay's YouTube
// capture path against real live streams: resolve (yt-dlp, explicit HLS format,
// split audio/video), continuous FFmpeg capture, and the worker's re-resolve on
// an expired-fragment return. It never runs in CI.
//
//	STOARAMA_LIVE_YOUTUBE_SOAK_URLS=https://www.youtube.com/watch?v=...,...
//	STOARAMA_LIVE_YOUTUBE_SOAK_DURATION=20m   (default 20m)
//	YT_DLP_BIN=/path/to/one-directory/yt-dlp_macos
//	go test ./internal/capture -run TestYouTubeLiveSoak -v -timeout 40m
func TestYouTubeLiveSoak(t *testing.T) {
	rawURLs := strings.TrimSpace(os.Getenv("STOARAMA_LIVE_YOUTUBE_SOAK_URLS"))
	if rawURLs == "" {
		t.Skip("set STOARAMA_LIVE_YOUTUBE_SOAK_URLS to run the live YouTube soak")
	}
	duration := 20 * time.Minute
	if raw := os.Getenv("STOARAMA_LIVE_YOUTUBE_SOAK_DURATION"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatal(err)
		}
		duration = parsed
	}
	tempRoot := t.TempDir()
	t.Setenv(YTDLPRuntimeTempRootEnv, tempRoot)
	outRoot := t.TempDir()
	systemTemp := os.TempDir()
	meiBefore := countMEIDirs(systemTemp)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	var wg sync.WaitGroup
	for index, watchURL := range strings.Split(rawURLs, ",") {
		watchURL = strings.TrimSpace(watchURL)
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats := soakOneStream(ctx, t, watchURL, filepath.Join(outRoot, strconv.Itoa(index)))
			t.Logf("SOAK %s", stats)
		}()
	}
	wg.Wait()

	leftovers, _ := os.ReadDir(tempRoot)
	t.Logf("SOAK private_temp_leftovers=%d system_MEI_before=%d system_MEI_after=%d", len(leftovers), meiBefore, countMEIDirs(systemTemp))
	if len(leftovers) != 0 {
		t.Errorf("yt-dlp private temp leaked %d entries", len(leftovers))
	}
}

type soakStats struct {
	url                string
	resolves           int
	resolveFailures    int
	split              int
	muxed              int
	resolveDurations   []time.Duration
	expired403         int
	otherAttemptErrors []string
	segments           int
	segmentsWithAudio  int
	segmentDurationsMs []int64
	seamGaps           []time.Duration
	lastEnd            time.Time
}

func (s *soakStats) String() string {
	median := func(values []time.Duration) time.Duration {
		if len(values) == 0 {
			return 0
		}
		sorted := append([]time.Duration(nil), values...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		return sorted[len(sorted)/2]
	}
	maxOf := func(values []time.Duration) time.Duration {
		var m time.Duration
		for _, v := range values {
			m = max(m, v)
		}
		return m
	}
	var durations []string
	for _, ms := range s.segmentDurationsMs {
		durations = append(durations, strconv.FormatInt(ms, 10))
	}
	var gaps []string
	for _, gap := range s.seamGaps {
		gaps = append(gaps, gap.Round(10*time.Millisecond).String())
	}
	return strings.Join([]string{
		"url=" + s.url,
		"resolves=" + strconv.Itoa(s.resolves),
		"resolve_failures=" + strconv.Itoa(s.resolveFailures),
		"split=" + strconv.Itoa(s.split),
		"muxed=" + strconv.Itoa(s.muxed),
		"resolve_median=" + median(s.resolveDurations).Round(10*time.Millisecond).String(),
		"resolve_max=" + maxOf(s.resolveDurations).Round(10*time.Millisecond).String(),
		"expired_403_restarts=" + strconv.Itoa(s.expired403),
		"other_attempt_errors=" + strconv.Itoa(len(s.otherAttemptErrors)),
		"segments=" + strconv.Itoa(s.segments),
		"segments_with_audio=" + strconv.Itoa(s.segmentsWithAudio),
		"segment_ms=[" + strings.Join(durations, ",") + "]",
		"seam_gaps=[" + strings.Join(gaps, ",") + "]",
		"errors=" + strings.Join(s.otherAttemptErrors, " | "),
	}, " ")
}

func soakOneStream(ctx context.Context, t *testing.T, watchURL, outRoot string) *soakStats {
	stats := &soakStats{url: watchURL}
	for ctx.Err() == nil {
		started := time.Now()
		resolveCtx, cancelResolve := context.WithTimeout(ctx, 30*time.Second)
		input, err := ResolveCapture(resolveCtx, "YOUTUBE", watchURL, "")
		cancelResolve()
		stats.resolves++
		stats.resolveDurations = append(stats.resolveDurations, time.Since(started))
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			stats.resolveFailures++
			stats.otherAttemptErrors = append(stats.otherAttemptErrors, "resolve: "+firstLine(err.Error()))
			time.Sleep(2 * time.Second)
			continue
		}
		if input.AudioURL != "" {
			stats.split++
		} else {
			stats.muxed++
		}
		outDir := filepath.Join(outRoot, strconv.Itoa(stats.resolves))
		if err := os.MkdirAll(outDir, 0o700); err != nil {
			t.Error(err)
			return stats
		}
		firstInAttempt := true
		err = CaptureContinuousInput(ctx, input, 60*time.Second, "", nil, outDir, func(seg Segment) error {
			stats.segments++
			if seg.AudioPresent {
				stats.segmentsWithAudio++
			}
			stats.segmentDurationsMs = append(stats.segmentDurationsMs, seg.DurationMs)
			if firstInAttempt && !stats.lastEnd.IsZero() {
				stats.seamGaps = append(stats.seamGaps, seg.StartAt.Sub(stats.lastEnd))
			}
			firstInAttempt = false
			stats.lastEnd = seg.EndAt
			RemoveSegmentFile(seg)
			return nil
		})
		_ = os.RemoveAll(outDir)
		switch {
		case ctx.Err() != nil:
		case errors.Is(err, ErrContinuousExpiredGooglevideoFragment):
			stats.expired403++
		case err != nil:
			stats.otherAttemptErrors = append(stats.otherAttemptErrors, "capture: "+firstLine(err.Error()))
		default:
			stats.otherAttemptErrors = append(stats.otherAttemptErrors, "capture: clean exit")
		}
	}
	return stats
}

func firstLine(s string) string {
	s = diagnosticURLPattern.ReplaceAllString(s, "[url]")
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

func countMEIDirs(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return -1
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "_MEI") {
			count++
		}
	}
	return count
}

var diagnosticURLPattern = regexp.MustCompile(`https?://\S+`)
