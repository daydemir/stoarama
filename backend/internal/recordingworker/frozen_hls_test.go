package recordingworker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

type frozenHLSRoundTripFunc func(*http.Request) (*http.Response, error)

func (f frozenHLSRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

const validFrozenHLSPlaylist = "#EXTM3U\n" +
	"#EXT-X-VERSION:3\n" +
	"#EXT-X-TARGETDURATION:8\n" +
	"#EXT-X-MEDIA-SEQUENCE:5971\n" +
	"#EXTINF:8.0,\n" +
	"segment-5971.ts\n" +
	"#EXTINF:8.0,\n" +
	"segment-5972.ts\n"

func TestFrozenHLSAllowlistIsExactAndDefaultOff(t *testing.T) {
	empty, err := parseFrozenHLSAllowlist("")
	if err != nil {
		t.Fatal(err)
	}
	if empty.allows("worker-1", 437) {
		t.Fatal("empty allowlist admitted a pair")
	}
	allowlist, err := parseFrozenHLSAllowlist("worker-1/437,worker-2/438")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		workerID    string
		recordingID int64
		want        bool
	}{
		{workerID: "worker-1", recordingID: 437, want: true},
		{workerID: "worker-2", recordingID: 438, want: true},
		{workerID: "worker-1", recordingID: 438},
		{workerID: "worker-10", recordingID: 437},
	} {
		if got := allowlist.allows(test.workerID, test.recordingID); got != test.want {
			t.Fatalf("allows(%q,%d)=%v want %v", test.workerID, test.recordingID, got, test.want)
		}
	}
	for _, raw := range []string{
		"*/437", "worker-1/*", "worker-1/0", "worker-1/0437", "worker-1/-1",
		"worker-1", "worker-1/437/1", "worker 1/437", "worker-1/437,worker-1/437",
	} {
		if _, err := parseFrozenHLSAllowlist(raw); err == nil {
			t.Fatalf("parseFrozenHLSAllowlist(%q) succeeded", raw)
		}
	}
}

func TestNewWorkerRejectsMalformedFrozenHLSAllowlist(t *testing.T) {
	worker, err := NewWorker(Config{Client: &recordingapi.Client{}, WorkerID: "worker-1"})
	if err != nil {
		t.Fatal(err)
	}
	if worker.frozenHLSAllowlist.allows("worker-1", 437) {
		t.Fatal("unset configuration enabled frozen HLS observation")
	}
	if _, err := NewWorker(Config{
		Client: &recordingapi.Client{}, WorkerID: "worker-1", FrozenHLSQuiescenceAllowlist: "worker-1/*",
	}); err == nil {
		t.Fatal("malformed frozen HLS allowlist was accepted")
	}
}

func TestParseFrozenHLSMediaPlaylistCanonicalFacts(t *testing.T) {
	got, err := parseFrozenHLSMediaPlaylist([]byte(validFrozenHLSPlaylist), "https://media.example/live/index.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	if got.mediaSequence != 5971 || got.discontinuitySeq != 0 || got.targetDuration != 8*time.Second {
		t.Fatalf("unexpected facts: %+v", got)
	}
	if got.lastSegmentURL != "https://media.example/live/segment-5972.ts" || len(got.timelineSHA256) != 64 || len(got.lastSegmentURLSHA256) != 64 {
		t.Fatalf("unexpected identities: %+v", got)
	}
	changed, err := parseFrozenHLSMediaPlaylist([]byte(strings.Replace(validFrozenHLSPlaylist, "segment-5972.ts", "segment-new.ts", 1)), "https://media.example/live/index.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	if changed.timelineSHA256 == got.timelineSHA256 || changed.lastSegmentURLSHA256 == got.lastSegmentURLSHA256 {
		t.Fatal("changed segment identity preserved playlist digest")
	}
}

func TestParseFrozenHLSMediaPlaylistFailsClosed(t *testing.T) {
	tests := map[string]string{
		"crlf":                    strings.ReplaceAll(validFrozenHLSPlaylist, "\n", "\r\n"),
		"master":                  "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nchild.m3u8\n",
		"ended":                   validFrozenHLSPlaylist + "#EXT-X-ENDLIST\n",
		"low latency part":        strings.Replace(validFrozenHLSPlaylist, "#EXTINF:8.0,", "#EXT-X-PART:DURATION=1,URI=\"part.ts\"\n#EXTINF:8.0,", 1),
		"byterange":               strings.Replace(validFrozenHLSPlaylist, "#EXTINF:8.0,", "#EXT-X-BYTERANGE:100@0\n#EXTINF:8.0,", 1),
		"playlist type":           strings.Replace(validFrozenHLSPlaylist, "#EXT-X-VERSION:3", "#EXT-X-PLAYLIST-TYPE:EVENT", 1),
		"duplicate target":        strings.Replace(validFrozenHLSPlaylist, "#EXT-X-TARGETDURATION:8", "#EXT-X-TARGETDURATION:8\n#EXT-X-TARGETDURATION:8", 1),
		"duplicate sequence":      strings.Replace(validFrozenHLSPlaylist, "#EXT-X-MEDIA-SEQUENCE:5971", "#EXT-X-MEDIA-SEQUENCE:5971\n#EXT-X-MEDIA-SEQUENCE:5971", 1),
		"duplicate discontinuity": strings.Replace(validFrozenHLSPlaylist, "#EXT-X-MEDIA-SEQUENCE:5971", "#EXT-X-MEDIA-SEQUENCE:5971\n#EXT-X-DISCONTINUITY-SEQUENCE:1\n#EXT-X-DISCONTINUITY-SEQUENCE:1", 1),
		"noncanonical sequence":   strings.Replace(validFrozenHLSPlaylist, "5971", "05971", 1),
		"missing duration":        strings.Replace(validFrozenHLSPlaylist, "#EXTINF:8.0,\n", "", 1),
		"dangling duration":       strings.TrimSuffix(validFrozenHLSPlaylist, "segment-5972.ts\n"),
		"userinfo segment":        strings.Replace(validFrozenHLSPlaylist, "segment-5972.ts", "https://u:p@media.example/seg.ts", 1),
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parseFrozenHLSMediaPlaylist([]byte(body), "https://media.example/live/index.m3u8"); err == nil {
				t.Fatal("ambiguous playlist was accepted")
			}
		})
	}
	var tooManyLines strings.Builder
	tooManyLines.WriteString(validFrozenHLSPlaylist)
	for range frozenHLSMaxLines {
		tooManyLines.WriteString("# comment\n")
	}
	if _, err := parseFrozenHLSMediaPlaylist([]byte(tooManyLines.String()), "https://media.example/live/index.m3u8"); err == nil {
		t.Fatal("playlist with too many lines was accepted")
	}
	var tooManySegments strings.Builder
	tooManySegments.WriteString("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n")
	for i := range frozenHLSMaxSegments + 1 {
		fmt.Fprintf(&tooManySegments, "#EXTINF:1,\nsegment-%d.ts\n", i)
	}
	if _, err := parseFrozenHLSMediaPlaylist([]byte(tooManySegments.String()), "https://media.example/live/index.m3u8"); err == nil {
		t.Fatal("playlist with too many segments was accepted")
	}
	oversize := append([]byte(validFrozenHLSPlaylist), make([]byte, frozenHLSManifestMaxBytes)...)
	if _, err := parseFrozenHLSMediaPlaylist(oversize, "https://media.example/live/index.m3u8"); err == nil {
		t.Fatal("oversized playlist was accepted")
	}
}

func TestFrozenHLSGetRejectsEncodedAndOversizedResponses(t *testing.T) {
	for _, test := range []struct {
		name   string
		header http.Header
		body   string
	}{
		{name: "encoded", header: http.Header{"Content-Encoding": []string{"gzip"}}, body: "ok"},
		{name: "oversized", header: make(http.Header), body: "toolong"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Transport: frozenHLSRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: 200, Header: test.header, Body: io.NopCloser(strings.NewReader(test.body)), Request: req,
				}, nil
			})}
			maxBytes := int64(64)
			if test.name == "oversized" {
				maxBytes = 3
			}
			if _, _, err := frozenHLSGet(context.Background(), client, "https://media.example/live.m3u8", nil, maxBytes, "*/*"); err == nil {
				t.Fatal("unsafe response was accepted")
			}
		})
	}
}

func TestFrozenHLSInputHeadersAndCacheFailClosed(t *testing.T) {
	headers, err := parseFrozenHLSInputHeaders("Authorization: Bearer opaque\r\nUser-Agent: recorder\r\n")
	if err != nil {
		t.Fatal(err)
	}
	if headers.Get("Authorization") != "Bearer opaque" || headers.Get("User-Agent") != "recorder" {
		t.Fatalf("unexpected headers: %v", headers)
	}
	for _, raw := range []string{
		"Host: attacker.example\r\n", "Range: bytes=0-1\r\n", "Cache-Control: max-age=999\r\n",
		"X-Test: ok\nInjected: yes", "X-Test: bad\x00value\r\n",
	} {
		if _, err := parseFrozenHLSInputHeaders(raw); err == nil {
			t.Fatalf("unsafe headers %q accepted", raw)
		}
	}
	for _, header := range []http.Header{
		{"Age": []string{"1"}}, {"Age": []string{"invalid"}}, {"Warning": []string{"110 stale"}},
		{"X-Cache": []string{"HIT"}}, {"CF-Cache-Status": []string{"HIT"}},
	} {
		if err := rejectFrozenHLSCacheAmbiguity(header); err == nil {
			t.Fatalf("cache ambiguity accepted: %v", header)
		}
	}
	if err := rejectFrozenHLSCacheAmbiguity(http.Header{"Age": []string{"0"}}); err != nil {
		t.Fatal(err)
	}
	if err := rejectFrozenHLSCacheAmbiguity(http.Header{"CF-Cache-Status": []string{"DYNAMIC"}}); err != nil {
		t.Fatal(err)
	}
}

func TestFrozenHLSSnapshotBindsEveryObservationIdentity(t *testing.T) {
	base := frozenHLSSnapshot{
		ResolvedURLSHA256: "a", InputHeadersSHA256: "b", ManifestSHA256: "m", MediaSequence: 1,
		DiscontinuitySeq: 2, TargetDuration: time.Second, TimelineSHA256: "c",
		LastSegmentURLSHA256: "d", LastSegmentFinalSHA: "f", LastSegmentSHA256: "e", LastSegmentSize: 7,
	}
	if !base.equal(base) {
		t.Fatal("snapshot not equal to itself")
	}
	mutations := []frozenHLSSnapshot{base, base, base, base, base, base, base, base, base, base}
	mutations[0].ResolvedURLSHA256 = "x"
	mutations[1].InputHeadersSHA256 = "x"
	mutations[2].ManifestSHA256 = "x"
	mutations[3].MediaSequence++
	mutations[4].DiscontinuitySeq++
	mutations[5].TimelineSHA256 = "x"
	mutations[6].LastSegmentURLSHA256 = "x"
	mutations[7].LastSegmentFinalSHA = "x"
	mutations[8].LastSegmentSHA256 = "x"
	mutations[9].LastSegmentSize++
	for i, mutation := range mutations {
		if base.equal(mutation) {
			t.Fatalf("mutation %d was not detected", i)
		}
	}
}

func newFrozenHLSTestWorker() *Worker {
	allowlist, err := parseFrozenHLSAllowlist("worker-1/437")
	if err != nil {
		panic(err)
	}
	return &Worker{
		cfg: Config{
			WorkerID:                    "worker-1",
			ContinuousNoProgressTimeout: 200 * time.Millisecond,
		},
		frozenHLSAllowlist:      allowlist,
		frozenHLSPollMax:        5 * time.Millisecond,
		frozenHLSForceCapture:   25 * time.Millisecond,
		frozenHLSSafetyInterval: time.Millisecond,
		frozenHLSWait:           func(context.Context, time.Duration) error { return nil },
		frozenHLSProofSpan:      func(time.Duration) time.Duration { return 4 * time.Millisecond },
	}
}

func TestClassifyFrozenHLSRequiresThreeExactSeparatedObservations(t *testing.T) {
	w := newFrozenHLSTestWorker()
	job := recordingapi.RecordingJob{RecordingID: 437}
	baseline := frozenHLSSnapshot{TargetDuration: 5 * time.Millisecond, MediaSequence: 1, TimelineSHA256: "timeline", LastSegmentSHA256: "segment"}
	w.frozenHLSObserve = func(context.Context, string, string) (frozenHLSSnapshot, error) { return baseline, nil }
	var currentCalls atomic.Int64
	w.frozenHLSObserveCurrent = func(context.Context, recordingapi.RecordingJob) (frozenHLSSnapshot, error) {
		currentCalls.Add(1)
		return baseline, nil
	}
	got, frozen, err := w.classifyFrozenHLS(context.Background(), job, "https://media.example/live.m3u8", "", time.Now().Add(5*time.Minute))
	if err != nil || !frozen || !got.equal(baseline) || currentCalls.Load() != 2 {
		t.Fatalf("got=%+v frozen=%v calls=%d err=%v", got, frozen, currentCalls.Load(), err)
	}

	changed := baseline
	changed.MediaSequence++
	w.frozenHLSObserveCurrent = func(context.Context, recordingapi.RecordingJob) (frozenHLSSnapshot, error) { return changed, nil }
	if _, frozen, err := w.classifyFrozenHLS(context.Background(), job, "https://media.example/live.m3u8", "", time.Now().Add(5*time.Minute)); err != nil || frozen {
		t.Fatalf("changed playlist frozen=%v err=%v", frozen, err)
	}
	w.frozenHLSObserveCurrent = func(context.Context, recordingapi.RecordingJob) (frozenHLSSnapshot, error) {
		return frozenHLSSnapshot{}, errors.New("ambiguous")
	}
	if _, frozen, err := w.classifyFrozenHLS(context.Background(), job, "https://media.example/live.m3u8", "", time.Now().Add(5*time.Minute)); err != nil || frozen {
		t.Fatalf("ambiguous observation frozen=%v err=%v", frozen, err)
	}
}

func TestInitialFrozenHLSProofSpansSixTargetsAndAtLeastNinetySeconds(t *testing.T) {
	for _, test := range []struct {
		name   string
		target time.Duration
		want   time.Duration
	}{
		{name: "eight second target", target: 8 * time.Second, want: 90 * time.Second},
		{name: "high target", target: 30 * time.Second, want: 180 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := initialFrozenHLSProofSpan(test.target); got != test.want {
				t.Fatalf("proof span=%s want %s", got, test.want)
			}
			w := newFrozenHLSTestWorker()
			w.frozenHLSProofSpan = nil
			var waits []time.Duration
			w.frozenHLSWait = func(_ context.Context, delay time.Duration) error {
				waits = append(waits, delay)
				return nil
			}
			baseline := frozenHLSSnapshot{TargetDuration: test.target, MediaSequence: 1, ManifestSHA256: "manifest", LastSegmentSHA256: "segment"}
			w.frozenHLSObserve = func(context.Context, string, string) (frozenHLSSnapshot, error) { return baseline, nil }
			w.frozenHLSObserveCurrent = func(context.Context, recordingapi.RecordingJob) (frozenHLSSnapshot, error) { return baseline, nil }
			_, frozen, err := w.classifyFrozenHLS(
				context.Background(), recordingapi.RecordingJob{RecordingID: 437},
				"https://media.example/live.m3u8", "", time.Now().Add(5*time.Minute),
			)
			if err != nil || !frozen {
				t.Fatalf("frozen=%v err=%v", frozen, err)
			}
			if len(waits) != 2 || waits[0]+waits[1] < test.want || waits[0] != test.want/2 || waits[1] != test.want/2 {
				t.Fatalf("waits=%v want two halves spanning %s", waits, test.want)
			}
		})
	}
}

func TestClassifyFrozenHLSIsDefaultOffAndDeadlineBounded(t *testing.T) {
	w := newFrozenHLSTestWorker()
	var calls atomic.Int64
	w.frozenHLSObserve = func(context.Context, string, string) (frozenHLSSnapshot, error) {
		calls.Add(1)
		return frozenHLSSnapshot{TargetDuration: 5 * time.Millisecond}, nil
	}
	for _, test := range []struct {
		name     string
		job      recordingapi.RecordingJob
		url      string
		deadline time.Time
	}{
		{name: "wrong recording", job: recordingapi.RecordingJob{RecordingID: 438}, url: "https://media.example/live.m3u8", deadline: time.Now().Add(time.Second)},
		{name: "not direct hls", job: recordingapi.RecordingJob{RecordingID: 437}, url: "https://media.example/live.mp4", deadline: time.Now().Add(time.Second)},
		{name: "malformed URL", job: recordingapi.RecordingJob{RecordingID: 437}, url: "https://media.example:port/live.m3u8", deadline: time.Now().Add(time.Second)},
		{name: "deadline elapsed", job: recordingapi.RecordingJob{RecordingID: 437}, url: "https://media.example/live.m3u8", deadline: time.Now().Add(-time.Second)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, frozen, err := w.classifyFrozenHLS(context.Background(), test.job, test.url, "", test.deadline); err != nil || frozen {
				t.Fatalf("frozen=%v err=%v", frozen, err)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("observer called %d times for ineligible work", calls.Load())
	}
}

func TestFrozenHLSLateInitialNoOutputCannotExtendOrdinarySurrender(t *testing.T) {
	w := newFrozenHLSTestWorker()
	w.frozenHLSProofSpan = nil
	w.cfg.ContinuousNoProgressTimeout = 50 * time.Millisecond
	w.frozenHLSForceCapture = 5 * time.Minute
	var calls atomic.Int64
	w.frozenHLSObserve = func(context.Context, string, string) (frozenHLSSnapshot, error) {
		calls.Add(1)
		return frozenHLSSnapshot{TargetDuration: 5 * time.Millisecond}, nil
	}
	w.frozenHLSObserveCurrent = func(context.Context, recordingapi.RecordingJob) (frozenHLSSnapshot, error) {
		calls.Add(1)
		return frozenHLSSnapshot{TargetDuration: 5 * time.Millisecond}, nil
	}
	lastUnique := time.Now().Add(-49 * time.Millisecond)
	ffmpegExit := time.Now()
	deadline := frozenHLSInitialDeadline(ffmpegExit, lastUnique, w.frozenHLSForceCapture, w.cfg.ContinuousNoProgressTimeout)
	if !deadline.Before(ffmpegExit.Add(5 * time.Millisecond)) {
		t.Fatalf("initial deadline=%s extended stale progress", deadline.Sub(ffmpegExit))
	}
	state := frozenHLSJobState{}
	result, err := w.handleFrozenHLSCycle(
		context.Background(), recordingapi.RecordingJob{RecordingID: 437},
		"https://media.example/live.m3u8", "", &state, deadline,
	)
	if err != nil || result != frozenHLSCycleUnclassified || state.classified {
		t.Fatalf("result=%v state=%+v err=%v", result, state, err)
	}
	if calls.Load() != 0 {
		t.Fatalf("late classification made %d observations", calls.Load())
	}
	if !continuousNoProgressExpired(lastUnique, deadline, w.cfg.ContinuousNoProgressTimeout) {
		t.Fatal("ordinary no-progress surrender was not due at the preserved deadline")
	}

	state.classified = true
	state.baseline = frozenHLSSnapshot{TargetDuration: 5 * time.Millisecond}
	classifiedDeadline := frozenHLSInitialDeadline(ffmpegExit, ffmpegExit, w.frozenHLSForceCapture, w.cfg.ContinuousNoProgressTimeout)
	if classifiedDeadline.Sub(ffmpegExit) != w.cfg.ContinuousNoProgressTimeout {
		t.Fatalf("initial helper deadline=%s", classifiedDeadline.Sub(ffmpegExit))
	}
	// Production deliberately bypasses frozenHLSInitialDeadline after the state
	// is classified; the subsequent per-cycle deadline remains exit+forced.
	if got := ffmpegExit.Add(min(w.frozenHLSForceCapture, frozenHLSForcedCaptureMax)); got.Sub(ffmpegExit) != frozenHLSForcedCaptureMax {
		t.Fatalf("classified forced deadline=%s", got.Sub(ffmpegExit))
	}
}

func TestWatchFrozenHLSResumesOnChangeAmbiguityAndAbsoluteCeiling(t *testing.T) {
	baseline := frozenHLSSnapshot{TargetDuration: 2 * time.Millisecond, MediaSequence: 1}
	for _, test := range []struct {
		name    string
		observe frozenHLSObserveCurrentFunc
	}{
		{name: "changed", observe: func(context.Context, recordingapi.RecordingJob) (frozenHLSSnapshot, error) {
			changed := baseline
			changed.MediaSequence++
			return changed, nil
		}},
		{name: "ambiguous", observe: func(context.Context, recordingapi.RecordingJob) (frozenHLSSnapshot, error) {
			return frozenHLSSnapshot{}, errors.New("ambiguous")
		}},
		{name: "forced", observe: func(context.Context, recordingapi.RecordingJob) (frozenHLSSnapshot, error) { return baseline, nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := newFrozenHLSTestWorker()
			w.frozenHLSObserveCurrent = test.observe
			state := frozenHLSJobState{baseline: baseline, classified: true}
			if err := w.watchFrozenHLSUntil(context.Background(), recordingapi.RecordingJob{JobID: 1, RecordingID: 437}, &state, time.Now().Add(100*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFrozenHLSJobStatePersistsAcrossForcedCyclesWithHeartbeat(t *testing.T) {
	var heartbeats atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		heartbeats.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"cancel":false,"lease_expires_at":%q}`, time.Now().Add(time.Second).Format(time.RFC3339Nano))
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	w := newFrozenHLSTestWorker()
	w.cfg.Client = client
	w.heartbeatInt = 2 * time.Millisecond
	w.leaseSafetyMargin = time.Millisecond
	baseline := frozenHLSSnapshot{TargetDuration: 2 * time.Millisecond, MediaSequence: 1, ManifestSHA256: "manifest", LastSegmentSHA256: "segment"}
	var firstCalls, currentCalls atomic.Int64
	var changed atomic.Bool
	w.frozenHLSObserve = func(context.Context, string, string) (frozenHLSSnapshot, error) {
		firstCalls.Add(1)
		return baseline, nil
	}
	w.frozenHLSObserveCurrent = func(context.Context, recordingapi.RecordingJob) (frozenHLSSnapshot, error) {
		currentCalls.Add(1)
		observed := baseline
		if changed.Load() {
			observed.MediaSequence++
		}
		return observed, nil
	}
	job := recordingapi.RecordingJob{JobID: 437, RecordingID: 437, LeaseExpiresAt: time.Now().Add(time.Second)}
	jobCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	heartbeatCanceled := w.startHeartbeat(jobCtx, cancel, job.JobID, "lease", job.LeaseExpiresAt)
	var state frozenHLSJobState
	for cycle := 0; cycle < 2; cycle++ {
		deadline := time.Now().Add(200 * time.Millisecond)
		result, err := w.handleFrozenHLSCycle(jobCtx, job, "https://media.example/live.m3u8", "", &state, deadline)
		if err != nil || result != frozenHLSCycleResumeCapture || !state.classified {
			t.Fatalf("cycle=%d result=%v state=%+v err=%v", cycle, result, state, err)
		}
	}
	if firstCalls.Load() != 1 {
		t.Fatalf("persistent baseline reclassified %d times", firstCalls.Load())
	}
	if currentCalls.Load() < 3 {
		t.Fatalf("insufficient strict observations: %d", currentCalls.Load())
	}
	if heartbeatCanceled() || jobCtx.Err() != nil || heartbeats.Load() < 3 {
		t.Fatalf("lease did not remain heartbeating: canceled=%v err=%v heartbeats=%d", heartbeatCanceled(), jobCtx.Err(), heartbeats.Load())
	}

	changed.Store(true)
	result, err := w.handleFrozenHLSCycle(jobCtx, job, "https://media.example/live.m3u8", "", &state, time.Now().Add(200*time.Millisecond))
	if err != nil || result != frozenHLSCycleResumeCapture || state.classified {
		t.Fatalf("manifest change did not clear state: result=%v state=%+v err=%v", result, state, err)
	}

	state = frozenHLSJobState{baseline: baseline, classified: true}
	changed.Store(false)
	var drain atomic.Bool
	drain.Store(true)
	w.cfg.DrainForUpdate = &drain
	if _, err := w.handleFrozenHLSCycle(jobCtx, job, "https://media.example/live.m3u8", "", &state, time.Now().Add(200*time.Millisecond)); !errors.Is(err, errFrozenHLSSelfUpdate) {
		t.Fatalf("safety cancellation err=%v", err)
	}
	drain.Store(false)
	closedCtx, closeWindow := context.WithCancel(jobCtx)
	closeWindow()
	if _, err := w.handleFrozenHLSCycle(closedCtx, job, "https://media.example/live.m3u8", "", &state, time.Now().Add(200*time.Millisecond)); !errors.Is(err, context.Canceled) {
		t.Fatalf("window cancellation err=%v", err)
	}
}

func TestContinuousJobRetainsLeaseAcrossFrozenHLSForcedCaptureCycles(t *testing.T) {
	var heartbeats, surrenders, completes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			heartbeats.Add(1)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"cancel":false,"lease_expires_at":%q}`, time.Now().Add(time.Second).Format(time.RFC3339Nano))
		case strings.HasSuffix(r.URL.Path, "/surrender"):
			surrenders.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "/complete"):
			completes.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected API path %q", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	w := newFrozenHLSTestWorker()
	w.cfg.Client = client
	w.cfg.CaptureTempDir = t.TempDir()
	w.cfg.HeartbeatSec = 1
	w.cfg.UploadWorkers = 1
	w.heartbeatInt = 2 * time.Millisecond
	w.leaseSafetyMargin = time.Millisecond
	w.frozenHLSForceCapture = 100 * time.Millisecond
	baseline := frozenHLSSnapshot{TargetDuration: 2 * time.Millisecond, MediaSequence: 1, ManifestSHA256: "manifest", LastSegmentSHA256: "segment"}
	var captureCalls atomic.Int64
	var changed atomic.Bool
	jobCtx, cancelJob := context.WithCancel(context.Background())
	defer cancelJob()
	w.continuousCapture = func(context.Context, string, time.Duration, string, *int, string, func(capture.Segment) error, string) error {
		call := captureCalls.Add(1)
		if call == 3 {
			changed.Store(true)
		}
		if call == 4 {
			cancelJob()
			return context.Canceled
		}
		return capture.ErrContinuousNoOutput
	}
	w.frozenHLSObserve = func(context.Context, string, string) (frozenHLSSnapshot, error) { return baseline, nil }
	w.frozenHLSObserveCurrent = func(context.Context, recordingapi.RecordingJob) (frozenHLSSnapshot, error) {
		observed := baseline
		if changed.Load() {
			observed.MediaSequence++
		}
		return observed, nil
	}
	windowEnd := time.Now().Add(time.Second)
	job := recordingapi.RecordingJob{
		JobID: 437, RecordingID: 437, SourceURL: "https://example.com/live.m3u8",
		ClipDurationSec: 30, Kind: "continuous_window", WindowEndAt: &windowEnd,
		LeaseToken: "lease", LeaseExpiresAt: time.Now().Add(time.Second),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.processContinuousJob(jobCtx, job)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("continuous lifecycle did not finish")
	}
	if captureCalls.Load() != 4 {
		t.Fatalf("capture launches=%d want 4", captureCalls.Load())
	}
	if surrenders.Load() != 0 || completes.Load() != 0 {
		t.Fatalf("surrenders=%d completes=%d", surrenders.Load(), completes.Load())
	}
	if heartbeats.Load() < 3 {
		t.Fatalf("lease heartbeat count=%d", heartbeats.Load())
	}
}

func TestFrozenHLSWaitHonorsSafetySignals(t *testing.T) {
	for _, test := range []struct {
		name string
		set  func(*Worker)
		want error
	}{
		{name: "self update", set: func(w *Worker) {
			var drain atomic.Bool
			drain.Store(true)
			w.cfg.DrainForUpdate = &drain
		}, want: errFrozenHLSSelfUpdate},
		{name: "disk pressure", set: func(w *Worker) {
			w.cfg.MinActiveFreeBytes = 100
			w.cfg.DiskFreeBytes = func() (uint64, error) { return 99, nil }
		}, want: errDiskPressure},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := newFrozenHLSTestWorker()
			test.set(w)
			err := w.waitFrozenHLS(context.Background(), time.Second)
			if !errors.Is(err, test.want) {
				t.Fatalf("err=%v want %v", err, test.want)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := newFrozenHLSTestWorker().waitFrozenHLS(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait err=%v", err)
	}
}

func TestFrozenHLSObservationIsPreemptedBySafetySignal(t *testing.T) {
	w := newFrozenHLSTestWorker()
	w.cfg.MinActiveFreeBytes = 100
	var pressured atomic.Bool
	w.cfg.DiskFreeBytes = func() (uint64, error) {
		if pressured.Load() {
			return 99, nil
		}
		return 101, nil
	}
	started := make(chan struct{})
	w.frozenHLSObserve = func(ctx context.Context, _, _ string) (frozenHLSSnapshot, error) {
		close(started)
		<-ctx.Done()
		return frozenHLSSnapshot{}, ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := w.classifyFrozenHLS(context.Background(), recordingapi.RecordingJob{RecordingID: 437}, "https://media.example/live.m3u8", "", time.Now().Add(5*time.Minute))
		done <- err
	}()
	<-started
	pressured.Store(true)
	select {
	case err := <-done:
		if !errors.Is(err, errDiskPressure) {
			t.Fatalf("err=%v want disk pressure", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("blocked observation ignored disk safety signal")
	}
}

func TestFrozenHLSObservationRequiresCleanDeliveryAndPreservesPartialSpool(t *testing.T) {
	noOutput := fmtNoOutput()
	if !frozenHLSCanObserve(false, nil, false, noOutput) {
		t.Fatal("clean no-output attempt was not eligible")
	}
	for _, test := range []struct {
		windowClosed bool
		deliveryErr  error
		pending      bool
		captureErr   error
	}{
		{windowClosed: true, captureErr: noOutput},
		{deliveryErr: errors.New("upload failed"), captureErr: noOutput},
		{pending: true, captureErr: noOutput},
		{captureErr: errors.New("ordinary ffmpeg error")},
		{captureErr: errors.Join(noOutput, errors.New("partial spool finalization failed"))},
	} {
		if frozenHLSCanObserve(test.windowClosed, test.deliveryErr, test.pending, test.captureErr) {
			t.Fatalf("unsafe attempt became eligible: %+v", test)
		}
	}
	if shouldCleanupContinuousAttempt(true, noOutput, false) {
		t.Fatal("no-output classification would remove an unacknowledged partial spool")
	}
}

func fmtNoOutput() error {
	return capture.ErrContinuousNoOutput
}
