package capture

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeRefusingCDN replays KBS's loomex CDN (kbscctv-cache.loomex.net): a
// Wowza origin whose token lives in the chunklist path, so the variant URL
// never changes, fronted by a CDN that answers a bare 403 to bursts of
// requests. refuse decides, per request, whether the CDN refuses it.
type fakeRefusingCDN struct {
	publishEvery time.Duration
	window       int
	segments     [][]byte
	started      time.Time

	mu     sync.Mutex
	refuse func(path string, n int64) bool

	requests          atomic.Int64
	masterRequests    atomic.Int64
	forbiddenRequests atomic.Int64
	requestedSeqs     sync.Map
}

const fakeCDNToken = "tkdfaketoken"

func newFakeRefusingCDN(t *testing.T, publishEvery time.Duration, segments [][]byte, refuse func(path string, n int64) bool) (*fakeRefusingCDN, *httptest.Server) {
	t.Helper()
	cdn := &fakeRefusingCDN{publishEvery: publishEvery, window: 3, segments: segments, started: time.Now(), refuse: refuse}
	server := httptest.NewServer(cdn)
	t.Cleanup(server.Close)
	return cdn, server
}

func (c *fakeRefusingCDN) masterURL(server *httptest.Server) string {
	return server.URL + "/lowStream/_definst_/9996_low.stream/playlist.m3u8?wowzatokenhash=abc"
}

func (c *fakeRefusingCDN) setRefuse(refuse func(path string, n int64) bool) {
	c.mu.Lock()
	c.refuse = refuse
	c.mu.Unlock()
}

func (c *fakeRefusingCDN) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := c.requests.Add(1)
	path := strings.TrimPrefix(r.URL.Path, "/lowStream/_definst_/9996_low.stream/")
	c.mu.Lock()
	refuse, started := c.refuse, c.started
	c.mu.Unlock()
	if refuse != nil && refuse(path, n) {
		c.forbiddenRequests.Add(1)
		w.WriteHeader(http.StatusForbidden) // the CDN's refusal has no body
		return
	}
	edge := int64(time.Since(started) / c.publishEvery)
	switch {
	case path == "playlist.m3u8":
		c.masterRequests.Add(1)
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprintf(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-STREAM-INF:BANDWIDTH=1724207,CODECS=\"avc1.42c020\",RESOLUTION=720x406\nchunklist_%s.m3u8\n", fakeCDNToken)
	case path == "chunklist_"+fakeCDNToken+".m3u8":
		first := max(edge-int64(c.window)+1, 0)
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprintf(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:%d\n#EXT-X-DISCONTINUITY-SEQUENCE:0\n", first)
		for seq := first; seq <= edge; seq++ {
			fmt.Fprintf(w, "#EXTINF:1.0,\nmedia-uit6zw6aq_%s_%d.ts\n", fakeCDNToken, seq)
		}
	case strings.HasPrefix(path, "media-uit6zw6aq_"+fakeCDNToken+"_"):
		var seq int64
		if _, err := fmt.Sscanf(strings.TrimSuffix(strings.TrimPrefix(path, "media-uit6zw6aq_"+fakeCDNToken+"_"), ".ts"), "%d", &seq); err != nil {
			http.NotFound(w, r)
			return
		}
		c.requestedSeqs.Store(seq, true)
		w.Header().Set("Content-Type", "video/mp2t")
		body := []byte(fmt.Sprintf("segment-%d", seq))
		if len(c.segments) > 0 {
			body = c.segments[int(seq)%len(c.segments)]
		}
		_, _ = w.Write(body)
	default:
		http.NotFound(w, r)
	}
}

func fastHLSSessionRetries(t *testing.T, delays ...time.Duration) {
	t.Helper()
	previous := hlsSessionRetryDelays
	hlsSessionRetryDelays = delays
	t.Cleanup(func() { hlsSessionRetryDelays = previous })
}

// refuseNext refuses the next count requests whose path has prefix.
func refuseNext(prefix string, count int64) func(string, int64) bool {
	var refused atomic.Int64
	return func(path string, _ int64) bool {
		return strings.HasPrefix(path, prefix) && refused.Add(1) <= count
	}
}

// A burst of CDN refusals on the chunklist is retried, and the stable,
// path-token variant URL never looks like a new session.
func TestHLSSessionProxyRetriesRefusedChunklist(t *testing.T) {
	allowLoopbackHLSSessionOrigin(t)
	fastHLSSessionRetries(t, 5*time.Millisecond, 5*time.Millisecond, 5*time.Millisecond)
	cdn, server := newFakeRefusingCDN(t, time.Hour, nil, nil)
	proxy, err := startHLSSessionProxy(CaptureInput{URL: cdn.masterURL(server)})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	if status, body := fetchProxyPlaylist(t, proxy.URL()); status != http.StatusOK {
		t.Fatalf("initial proxy playlist status=%d body=%q", status, body)
	}
	cdn.setRefuse(refuseNext("chunklist_", 2))
	status, body := fetchProxyPlaylist(t, proxy.URL())
	if status != http.StatusOK {
		t.Fatalf("proxy playlist after refused chunklist status=%d", status)
	}
	proxyPlaylistSequence(t, body)
	if cdn.forbiddenRequests.Load() != 2 {
		t.Fatalf("forbidden=%d, want the two refusals to have been retried", cdn.forbiddenRequests.Load())
	}
	proxy.mu.Lock()
	generation, epoch := proxy.generation, proxy.epoch
	proxy.mu.Unlock()
	if generation != 1 || epoch != 0 {
		t.Fatalf("generation=%d epoch=%d, want an unchanged session for a stable variant URL", generation, epoch)
	}
}

// When the CDN keeps refusing past the retry budget, the proxy replays the
// last good chunklist. FFmpeg ends the process on a failed playlist reload;
// an unchanged playlist just makes it poll again.
func TestHLSSessionProxyReplaysLastGoodChunklistWhileRefused(t *testing.T) {
	allowLoopbackHLSSessionOrigin(t)
	fastHLSSessionRetries(t, 5*time.Millisecond)
	cdn, server := newFakeRefusingCDN(t, time.Hour, nil, nil)
	proxy, err := startHLSSessionProxy(CaptureInput{URL: cdn.masterURL(server)})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	status, before := fetchProxyPlaylist(t, proxy.URL())
	if status != http.StatusOK {
		t.Fatalf("initial proxy playlist status=%d", status)
	}
	cdn.setRefuse(func(string, int64) bool { return true })
	status, during := fetchProxyPlaylist(t, proxy.URL())
	if status != http.StatusOK || during != before {
		t.Fatalf("refused reload status=%d body=%q, want the last good playlist %q", status, during, before)
	}
}

// Without any good chunklist yet, a refusal is still reported to FFmpeg.
func TestHLSSessionProxyReportsRefusalWithoutLastGood(t *testing.T) {
	allowLoopbackHLSSessionOrigin(t)
	fastHLSSessionRetries(t, 5*time.Millisecond)
	cdn, server := newFakeRefusingCDN(t, time.Hour, nil, func(path string, _ int64) bool { return strings.HasPrefix(path, "chunklist_") })
	proxy, err := startHLSSessionProxy(CaptureInput{URL: cdn.masterURL(server)})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if status, _ := fetchProxyPlaylist(t, proxy.URL()); status != http.StatusForbidden {
		t.Fatalf("status=%d, want the CDN's 403 when no playlist was ever served", status)
	}
}

// A refused segment is retried rather than handed to FFmpeg as a 403, which
// FFmpeg would skip (a 10-second hole in the clip).
func TestHLSSessionProxyRetriesRefusedSegment(t *testing.T) {
	allowLoopbackHLSSessionOrigin(t)
	fastHLSSessionRetries(t, 5*time.Millisecond, 5*time.Millisecond, 5*time.Millisecond)
	cdn, server := newFakeRefusingCDN(t, time.Hour, nil, nil)
	proxy, err := startHLSSessionProxy(CaptureInput{URL: cdn.masterURL(server)})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	status, body := fetchProxyPlaylist(t, proxy.URL())
	if status != http.StatusOK {
		t.Fatalf("proxy playlist status=%d", status)
	}
	_, uris := proxyPlaylistSequence(t, body)
	cdn.setRefuse(refuseNext("media-", 3))
	resp, err := http.Get("http://" + proxy.listener.Addr().String() + uris[0])
	if err != nil {
		t.Fatal(err)
	}
	segment, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(segment) != "segment-0" {
		t.Fatalf("refused segment status=%d body=%q, want segment-0 after retrying", resp.StatusCode, segment)
	}
	if cdn.forbiddenRequests.Load() != 3 {
		t.Fatalf("forbidden=%d want 3", cdn.forbiddenRequests.Load())
	}
	// One lookup confirms the session URL is unchanged; later attempts retry
	// the segment alone rather than re-reading the master playlist each time.
	if got := cdn.masterRequests.Load(); got != 2 {
		t.Fatalf("master requests=%d, want 2 (initial + one confirming lookup)", got)
	}
}

// A new session URL invalidates the last good playlist: replaying it would
// point FFmpeg at the expired session's segment URLs.
func TestHLSSessionProxyDoesNotReplayPlaylistAcrossSessions(t *testing.T) {
	allowLoopbackHLSSessionOrigin(t)
	fastHLSSessionRetries(t, 5*time.Millisecond)
	origin, server := newFakeWowzaOrigin(t, 200*time.Millisecond, time.Hour, nil)
	proxy, err := startHLSSessionProxy(CaptureInput{URL: server.URL + "/live/7_Bell.stream/playlist.m3u8"})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if status, _ := fetchProxyPlaylist(t, proxy.URL()); status != http.StatusOK {
		t.Fatalf("initial proxy playlist status=%d", status)
	}
	// Expire the session, and make every new session expire immediately, so
	// the proxy obtains new session URLs but never a playlist for them.
	origin.mu.Lock()
	origin.sessionTTL = 0
	origin.mu.Unlock()
	time.Sleep(250 * time.Millisecond)
	status, body := fetchProxyPlaylist(t, proxy.URL())
	if status == http.StatusOK {
		t.Fatalf("proxy replayed an old session's playlist after a session change:\n%s", body)
	}
	if origin.masterRequests.Load() < 2 {
		t.Fatalf("master requests=%d, want a refresh to a new session", origin.masterRequests.Load())
	}
}

// End to end against the KBS CDN shape: one persistent FFmpeg records through
// repeated 403 bursts on chunklist and segment requests without exiting, and
// consumes every published segment. Before the proxy covered this origin,
// FFmpeg exited on the first refused playlist reload.
func TestContinuousCaptureSurvivesCDNRefusalBursts(t *testing.T) {
	ffmpeg, err := exec.LookPath(ffmpegBin())
	if err != nil {
		t.Skipf("ffmpeg unavailable: %v", err)
	}
	allowLoopbackHLSSessionOrigin(t)
	fastHLSSessionRetries(t, 20*time.Millisecond, 50*time.Millisecond, 100*time.Millisecond, 200*time.Millisecond)
	segments := generateHLSFixtureSegments(t, ffmpeg, t.TempDir(), 30)
	// Refuse every request in a 3-request burst out of each 8, so both
	// chunklist reloads and segment fetches hit consecutive 403s.
	cdn, server := newFakeRefusingCDN(t, time.Second, segments, func(path string, n int64) bool {
		return path != "playlist.m3u8" && n > 4 && n%8 < 3
	})

	var delivered atomic.Int64
	captureCtx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	err = CaptureContinuousInput(captureCtx, CaptureInput{URL: cdn.masterURL(server)}, 2*time.Second, "", nil, t.TempDir(), func(Segment) error {
		delivered.Add(1)
		return nil
	})
	if captureCtx.Err() == nil {
		t.Fatalf("FFmpeg exited before the window closed (err=%v); forbidden=%d", err, cdn.forbiddenRequests.Load())
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("capture returned %v", err)
	}
	if cdn.forbiddenRequests.Load() < 3 {
		t.Fatalf("forbidden=%d, the fake CDN never refused a burst", cdn.forbiddenRequests.Load())
	}
	if delivered.Load() < 2 {
		t.Fatalf("delivered %d segments, want at least 2", delivered.Load())
	}
	var lowest, highest int64 = -1, -1
	cdn.requestedSeqs.Range(func(key, _ any) bool {
		seq := key.(int64)
		if lowest < 0 || seq < lowest {
			lowest = seq
		}
		highest = max(highest, seq)
		return true
	})
	for seq := lowest; seq <= highest; seq++ {
		if _, ok := cdn.requestedSeqs.Load(seq); !ok {
			t.Fatalf("FFmpeg skipped media sequence %d (served %d..%d)", seq, lowest, highest)
		}
	}
	if highest-lowest < 5 {
		t.Fatalf("FFmpeg consumed only sequences %d..%d", lowest, highest)
	}
}

// Once a fresh session supplies the replacement URL for an expired segment,
// a transient failure on that replacement is retried on the replacement, not
// on the expired URL.
func TestHLSSessionProxyRetriesReplacementSegmentURL(t *testing.T) {
	allowLoopbackHLSSessionOrigin(t)
	fastHLSSessionRetries(t, 5*time.Millisecond, 5*time.Millisecond)
	origin := &fakeWowzaOrigin{
		t: t, sessionTTL: 300 * time.Millisecond, publishEvery: time.Hour, window: 3,
		started: time.Now(), sessions: map[string]time.Time{}, nextSession: 1000,
	}
	var expiredFetches, replacementFailures atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "media_w1001_"):
			expiredFetches.Add(1)
		case strings.Contains(r.URL.Path, "media_w1002_") && replacementFailures.Add(1) == 1:
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		origin.ServeHTTP(w, r)
	}))
	defer server.Close()
	proxy, err := startHLSSessionProxy(CaptureInput{URL: server.URL + "/live/7_Bell.stream/playlist.m3u8"})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	status, body := fetchProxyPlaylist(t, proxy.URL())
	if status != http.StatusOK {
		t.Fatalf("proxy playlist status=%d", status)
	}
	_, uris := proxyPlaylistSequence(t, body)
	time.Sleep(400 * time.Millisecond) // session 1001 is now expired

	resp, err := http.Get("http://" + proxy.listener.Addr().String() + uris[0])
	if err != nil {
		t.Fatal(err)
	}
	segment, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(segment) != "segment-0" {
		t.Fatalf("segment status=%d body=%q, want segment-0 from the replacement session", resp.StatusCode, segment)
	}
	if got := expiredFetches.Load(); got != 1 {
		t.Fatalf("expired session URL fetched %d times, want 1 (retries must use the replacement)", got)
	}
}

// A stale CDN copy of the same session's chunklist whose sequence went
// backwards is not served to FFmpeg, and does not replace the newer playlist
// kept for replay.
func TestHLSSessionProxyServesNewestPlaylistOverStaleCDNCopy(t *testing.T) {
	allowLoopbackHLSSessionOrigin(t)
	fastHLSSessionRetries(t, 5*time.Millisecond)
	cdn, server := newFakeRefusingCDN(t, 50*time.Millisecond, nil, nil)
	time.Sleep(300 * time.Millisecond) // the live edge is now past sequence 5
	proxy, err := startHLSSessionProxy(CaptureInput{URL: cdn.masterURL(server)})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	_, newest := fetchProxyPlaylist(t, proxy.URL())
	newestFirst, _ := proxyPlaylistSequence(t, newest)
	cdn.mu.Lock()
	cdn.started = cdn.started.Add(250 * time.Millisecond) // serve an older window
	cdn.mu.Unlock()
	_, served := fetchProxyPlaylist(t, proxy.URL())
	if servedFirst, _ := proxyPlaylistSequence(t, served); servedFirst < newestFirst {
		t.Fatalf("proxy served a regressed media sequence %d after %d", servedFirst, newestFirst)
	}
	cdn.setRefuse(func(string, int64) bool { return true })
	_, replayed := fetchProxyPlaylist(t, proxy.URL())
	if replayFirst, _ := proxyPlaylistSequence(t, replayed); replayFirst < newestFirst {
		t.Fatalf("proxy replayed a regressed media sequence %d after %d", replayFirst, newestFirst)
	}
}
