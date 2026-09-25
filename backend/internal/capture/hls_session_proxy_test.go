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
	"syscall"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/netguard"
)

// fakeWowzaOrigin replays Seattle DOT's StreamLock behavior: the master
// playlist issues a per-viewer session, and that session's chunklist and
// segments return 403 once the session is older than sessionTTL. The live
// edge advances one segment per publishEvery; restartSequence makes a fresh
// session renumber from 1, as a restarted camera stream does.
type fakeWowzaOrigin struct {
	t            *testing.T
	sessionTTL   time.Duration
	publishEvery time.Duration
	window       int
	segments     [][]byte

	started time.Time

	mu              sync.Mutex
	sessions        map[string]time.Time
	nextSession     int
	restartSequence bool
	sequenceBase    int64

	masterRequests    atomic.Int64
	forbiddenRequests atomic.Int64
	requestedSeqs     sync.Map
}

func newFakeWowzaOrigin(t *testing.T, sessionTTL, publishEvery time.Duration, segments [][]byte) (*fakeWowzaOrigin, *httptest.Server) {
	t.Helper()
	origin := &fakeWowzaOrigin{
		t:            t,
		sessionTTL:   sessionTTL,
		publishEvery: publishEvery,
		window:       3,
		segments:     segments,
		started:      time.Now(),
		sessions:     map[string]time.Time{},
		nextSession:  1000,
	}
	server := httptest.NewServer(origin)
	t.Cleanup(server.Close)
	return origin, server
}

func (o *fakeWowzaOrigin) liveEdge() int64 {
	return o.sequenceBase + int64(time.Since(o.started)/o.publishEvery)
}

func (o *fakeWowzaOrigin) sessionAlive(id string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	created, ok := o.sessions[id]
	return ok && time.Since(created) < o.sessionTTL
}

func (o *fakeWowzaOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/live/7_Bell.stream/")
	switch {
	case path == "playlist.m3u8":
		o.masterRequests.Add(1)
		o.mu.Lock()
		o.nextSession++
		id := fmt.Sprintf("%d", o.nextSession)
		o.sessions[id] = time.Now()
		if o.restartSequence {
			o.restartSequence = false
			o.sequenceBase = 1 - int64(time.Since(o.started)/o.publishEvery)
		}
		o.mu.Unlock()
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprintf(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-STREAM-INF:BANDWIDTH=4053280,CODECS=\"avc1.640028\",RESOLUTION=1920x1080\nchunklist_w%s.m3u8\n", id)
	case strings.HasPrefix(path, "chunklist_w"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "chunklist_w"), ".m3u8")
		if !o.sessionAlive(id) {
			o.forbiddenRequests.Add(1)
			http.Error(w, "session expired", http.StatusForbidden)
			return
		}
		o.mu.Lock()
		edge := o.liveEdge()
		o.mu.Unlock()
		first := max(edge-int64(o.window)+1, 0)
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprintf(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:%d\n#EXT-X-DISCONTINUITY-SEQUENCE:0\n", first)
		for seq := first; seq <= edge; seq++ {
			fmt.Fprintf(w, "#EXTINF:1.0,\nmedia_w%s_%d.ts\n", id, seq)
		}
	case strings.HasPrefix(path, "media_w"):
		var id string
		var seq int64
		name := strings.TrimSuffix(strings.TrimPrefix(path, "media_w"), ".ts")
		if cut := strings.LastIndex(name, "_"); cut > 0 {
			id = name[:cut]
			if _, err := fmt.Sscanf(name[cut+1:], "%d", &seq); err != nil {
				http.NotFound(w, r)
				return
			}
		}
		if !o.sessionAlive(id) {
			o.forbiddenRequests.Add(1)
			http.Error(w, "session expired", http.StatusForbidden)
			return
		}
		o.requestedSeqs.Store(seq, true)
		w.Header().Set("Content-Type", "video/mp2t")
		body := []byte(fmt.Sprintf("segment-%d", seq))
		if len(o.segments) > 0 {
			body = o.segments[int(seq)%len(o.segments)]
		}
		_, _ = w.Write(body)
	default:
		http.NotFound(w, r)
	}
}

func allowLoopbackHLSSessionOrigin(t *testing.T) {
	t.Helper()
	hlsSessionRefreshHost = func(host string) bool { return host == "127.0.0.1" }
	hlsSessionDialControl = func(string, string, syscall.RawConn) error { return nil }
	t.Cleanup(func() {
		hlsSessionRefreshHost = func(host string) bool {
			return strings.EqualFold(strings.TrimSuffix(host, "."), seattleStreamLockHost)
		}
		hlsSessionDialControl = netguard.ControlReject
	})
}

func fetchProxyPlaylist(t *testing.T, proxyURL string) (int, string) {
	t.Helper()
	resp, err := http.Get(proxyURL)
	if err != nil {
		t.Fatalf("fetch proxy playlist: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func proxyPlaylistSequence(t *testing.T, body string) (int64, []string) {
	t.Helper()
	var first int64 = -1
	var uris []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:") {
			fmt.Sscanf(strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"), "%d", &first)
		} else if line != "" && !strings.HasPrefix(line, "#") {
			uris = append(uris, line)
		}
	}
	if first < 0 || len(uris) == 0 {
		t.Fatalf("proxy playlist has no media sequence or segments:\n%s", body)
	}
	return first, uris
}

func TestHLSSessionRefreshAppliesOnlyToSeattleStreamLock(t *testing.T) {
	cases := []struct {
		name    string
		input   CaptureInput
		pinHost string
		want    bool
	}{
		{"sdot master", CaptureInput{URL: "https://61e0c5d388c2e.streamlock.net/live/7_Bell.stream/playlist.m3u8"}, "", true},
		{"sdot legacy port", CaptureInput{URL: "https://61e0c5d388c2e.streamlock.net:443/live/7_Bell.stream/playlist.m3u8"}, "", true},
		{"other wowza", CaptureInput{URL: "https://example.streamlock.net/live/x.stream/playlist.m3u8"}, "", false},
		{"googlevideo", CaptureInput{URL: "https://manifest.googlevideo.com/api/manifest/hls_playlist/index.m3u8"}, "", false},
		{"sdot image", CaptureInput{URL: "https://61e0c5d388c2e.streamlock.net/live/7_Bell.jpg"}, "", false},
		{"split audio", CaptureInput{URL: "https://61e0c5d388c2e.streamlock.net/live/a/playlist.m3u8", AudioURL: "https://61e0c5d388c2e.streamlock.net/live/b/playlist.m3u8"}, "", false},
		{"pinned host", CaptureInput{URL: "https://61e0c5d388c2e.streamlock.net/live/a/playlist.m3u8"}, "origin.example", false},
	}
	for _, tc := range cases {
		if got := hlsSessionRefreshApplies(tc.input, tc.pinHost); got != tc.want {
			t.Errorf("%s: hlsSessionRefreshApplies=%v want %v", tc.name, got, tc.want)
		}
	}
}

// The chunklist's 403 on an expired session must be answered by re-reading
// the master playlist, and FFmpeg's view of the media sequence must continue.
func TestHLSSessionProxyRefreshesExpiredChunklist(t *testing.T) {
	allowLoopbackHLSSessionOrigin(t)
	origin, server := newFakeWowzaOrigin(t, 300*time.Millisecond, 100*time.Millisecond, nil)
	proxy, err := startHLSSessionProxy(CaptureInput{URL: server.URL + "/live/7_Bell.stream/playlist.m3u8"})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	status, body := fetchProxyPlaylist(t, proxy.URL())
	if status != http.StatusOK {
		t.Fatalf("initial proxy playlist status=%d", status)
	}
	firstSeq, _ := proxyPlaylistSequence(t, body)

	time.Sleep(450 * time.Millisecond) // the first session is now expired
	status, body = fetchProxyPlaylist(t, proxy.URL())
	if status != http.StatusOK {
		t.Fatalf("proxy playlist after session expiry status=%d body=%q", status, body)
	}
	nextSeq, uris := proxyPlaylistSequence(t, body)
	if origin.forbiddenRequests.Load() == 0 {
		t.Fatal("fake origin never expired the session; the test did not exercise a 403")
	}
	if got := origin.masterRequests.Load(); got != 2 {
		t.Fatalf("master playlist requests=%d want 2 (initial + one refresh)", got)
	}
	if nextSeq <= firstSeq {
		t.Fatalf("media sequence did not advance across the session refresh: %d -> %d", firstSeq, nextSeq)
	}

	// Every rewritten segment is served through the proxy from the fresh session.
	resp, err := http.Get("http://" + proxy.listener.Addr().String() + uris[len(uris)-1])
	if err != nil {
		t.Fatal(err)
	}
	segment, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(string(segment), "segment-") {
		t.Fatalf("proxied segment status=%d body=%q", resp.StatusCode, segment)
	}
}

// A segment whose session expired between the playlist read and the segment
// fetch is re-found by media sequence in a fresh session, not dropped.
func TestHLSSessionProxyRefetchesExpiredSegmentFromFreshSession(t *testing.T) {
	allowLoopbackHLSSessionOrigin(t)
	origin, server := newFakeWowzaOrigin(t, 300*time.Millisecond, time.Hour, nil)
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
	time.Sleep(400 * time.Millisecond)

	resp, err := http.Get("http://" + proxy.listener.Addr().String() + uris[0])
	if err != nil {
		t.Fatal(err)
	}
	segment, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(segment) != "segment-0" {
		t.Fatalf("expired segment status=%d body=%q, want segment-0 from a fresh session", resp.StatusCode, segment)
	}
	if origin.forbiddenRequests.Load() == 0 || origin.masterRequests.Load() != 2 {
		t.Fatalf("forbidden=%d master=%d, want a 403 then one master refresh", origin.forbiddenRequests.Load(), origin.masterRequests.Load())
	}
}

// When the camera stream restarts, the fresh session renumbers from 1. FFmpeg
// must still see a monotonic sequence or it would wait for numbers it already
// consumed.
func TestHLSSessionProxyKeepsSequenceMonotonicAcrossStreamRestart(t *testing.T) {
	allowLoopbackHLSSessionOrigin(t)
	origin, server := newFakeWowzaOrigin(t, 300*time.Millisecond, 20*time.Millisecond, nil)
	proxy, err := startHLSSessionProxy(CaptureInput{URL: server.URL + "/live/7_Bell.stream/playlist.m3u8"})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	_, _ = fetchProxyPlaylist(t, proxy.URL())
	time.Sleep(400 * time.Millisecond) // the live edge is now ~20
	_, body := fetchProxyPlaylist(t, proxy.URL())
	before, uris := proxyPlaylistSequence(t, body)
	lastBefore := before + int64(len(uris)) - 1
	if lastBefore < 10 {
		t.Fatalf("live edge before restart=%d, want the stream well past sequence 1", lastBefore)
	}

	origin.mu.Lock()
	origin.restartSequence = true
	origin.mu.Unlock()
	time.Sleep(400 * time.Millisecond) // expire the session; the next one renumbers from 1
	status, body := fetchProxyPlaylist(t, proxy.URL())
	if status != http.StatusOK {
		t.Fatalf("proxy playlist after restart status=%d", status)
	}
	after, _ := proxyPlaylistSequence(t, body)
	if after <= lastBefore {
		t.Fatalf("media sequence after stream restart=%d, want > %d (last sequence FFmpeg saw)", after, lastBefore)
	}
	if !strings.Contains(body, "seq=1&") {
		t.Fatalf("restarted stream segments should keep their upstream numbering in the proxy URL:\n%s", body)
	}
}

func TestHLSSessionProxyRejectsOffOriginSegment(t *testing.T) {
	allowLoopbackHLSSessionOrigin(t)
	_, server := newFakeWowzaOrigin(t, time.Hour, time.Hour, nil)
	proxy, err := startHLSSessionProxy(CaptureInput{URL: server.URL + "/live/7_Bell.stream/playlist.m3u8"})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	resp, err := http.Get("http://" + proxy.listener.Addr().String() + "/segment?seq=0&gen=1&epoch=0&u=http%3A%2F%2F169.254.169.254%2Flatest")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("off-origin segment status=%d want 400", resp.StatusCode)
	}
}

// End to end: one persistent FFmpeg process records across several session
// expiries without exiting, and consumes every published segment in order.
func TestContinuousCaptureSurvivesWowzaSessionExpiry(t *testing.T) {
	ffmpeg, err := exec.LookPath(ffmpegBin())
	if err != nil {
		t.Skipf("ffmpeg unavailable: %v", err)
	}
	allowLoopbackHLSSessionOrigin(t)
	segments := generateHLSFixtureSegments(t, ffmpeg, t.TempDir(), 30)
	origin, server := newFakeWowzaOrigin(t, 2500*time.Millisecond, time.Second, segments)

	outDir := t.TempDir()
	var delivered atomic.Int64
	captureCtx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	err = CaptureContinuousInput(captureCtx, CaptureInput{URL: server.URL + "/live/7_Bell.stream/playlist.m3u8"}, 2*time.Second, "", nil, outDir, func(Segment) error {
		delivered.Add(1)
		return nil
	})
	if captureCtx.Err() == nil {
		t.Fatalf("FFmpeg exited before the window closed (err=%v); master=%d forbidden=%d", err, origin.masterRequests.Load(), origin.forbiddenRequests.Load())
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("capture returned %v", err)
	}
	if origin.forbiddenRequests.Load() == 0 || origin.masterRequests.Load() < 3 {
		t.Fatalf("expected repeated session expiry: master=%d forbidden=%d", origin.masterRequests.Load(), origin.forbiddenRequests.Load())
	}
	if delivered.Load() < 2 {
		t.Fatalf("delivered %d segments across session expiries, want at least 2", delivered.Load())
	}
	var lowest, highest int64 = -1, -1
	origin.requestedSeqs.Range(func(key, _ any) bool {
		seq := key.(int64)
		if lowest < 0 || seq < lowest {
			lowest = seq
		}
		if seq > highest {
			highest = seq
		}
		return true
	})
	for seq := lowest; seq <= highest; seq++ {
		if _, ok := origin.requestedSeqs.Load(seq); !ok {
			t.Fatalf("FFmpeg skipped media sequence %d (fetched %d..%d)", seq, lowest, highest)
		}
	}
	if highest-lowest < 5 {
		t.Fatalf("FFmpeg consumed only sequences %d..%d", lowest, highest)
	}
}
