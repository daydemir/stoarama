package capture

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestContinuousHLSExpiredFragmentFailsFast replays the Googlevideo failure
// shape seen by relays: FFmpeg consumes media, then a signed child fragment
// returns 403. The worker can re-resolve a fresh manifest only after FFmpeg
// exits. FFmpeg skips the failed fragment, then normally holds the unchanged
// live playlist until our 30-second watchdog fires. This test requires that
// stale-manifest wait to end in a few seconds instead.
func TestContinuousHLSExpiredFragmentFailsFast(t *testing.T) {
	ffmpeg, err := exec.LookPath(ffmpegBin())
	if err != nil {
		t.Skipf("ffmpeg unavailable: %v", err)
	}

	temp := t.TempDir()
	segment := generateHLSFixtureSegment(t, ffmpeg, temp)

	var forbiddenRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/live.m3u8":
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			fmt.Fprint(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:1,\n/segment.ts\n#EXTINF:1,\n/expired.ts\n")
		case "/segment.ts":
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(segment)
		case "/expired.ts":
			forbiddenRequests.Add(1)
			http.Error(w, "expired", http.StatusForbidden)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	outPattern := filepath.Join(temp, "seg-%Y%m%d-%H%M%S.mp4")
	args := buildFFmpegContinuousArgs(server.URL+"/live.m3u8", outPattern, time.Second, "manifest.googlevideo.com", nil)
	captureCtx, captureCancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer captureCancel()
	started := time.Now()
	cmd := exec.CommandContext(captureCtx, ffmpeg, args...)
	output, _ := cmd.CombinedOutput()
	elapsed := time.Since(started)
	if captureCtx.Err() == context.DeadlineExceeded {
		t.Fatalf("FFmpeg held an unchanged HLS playlist past 4s after an expired fragment; requests=%d stderr=%s", forbiddenRequests.Load(), strings.TrimSpace(string(output)))
	}
	if elapsed > 3*time.Second {
		t.Fatalf("expired HLS fragment took %s to fail; requests=%d", elapsed, forbiddenRequests.Load())
	}
	if forbiddenRequests.Load() != 1 {
		t.Fatalf("expired HLS fragment requests=%d want=1", forbiddenRequests.Load())
	}
}

func TestContinuousGooglevideoHLSToleratesOneStaleReload(t *testing.T) {
	ffmpeg, err := exec.LookPath(ffmpegBin())
	if err != nil {
		t.Skipf("ffmpeg unavailable: %v", err)
	}
	temp := t.TempDir()
	segment := generateHLSFixtureSegment(t, ffmpeg, temp)

	var playlistRequests atomic.Int64
	freshRequested := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/live.m3u8":
			request := playlistRequests.Add(1)
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			if request <= 2 {
				fmt.Fprint(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:1,\n/segment.ts\n")
				return
			}
			fmt.Fprint(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1,\n/fresh.ts\n")
		case "/segment.ts":
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(segment)
		case "/fresh.ts":
			select {
			case freshRequested <- struct{}{}:
			default:
			}
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(segment)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	captureCtx, captureCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer captureCancel()
	args := buildFFmpegContinuousArgs(server.URL+"/live.m3u8", filepath.Join(temp, "seg-%Y%m%d-%H%M%S.mp4"), time.Second, "manifest.googlevideo.com", nil)
	cmd := exec.CommandContext(captureCtx, ffmpeg, args...)
	var output strings.Builder
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()
	select {
	case <-freshRequested:
		captureCancel()
		<-waitErr
	case err := <-waitErr:
		t.Fatalf("FFmpeg exited before the playlist advanced after one stale reload: %v (%s)", err, strings.TrimSpace(output.String()))
	case <-captureCtx.Done():
		<-waitErr
		t.Fatalf("FFmpeg never consumed the advanced playlist; reloads=%d stderr=%s", playlistRequests.Load(), strings.TrimSpace(output.String()))
	}
}

func generateHLSFixtureSegment(t *testing.T, ffmpeg, temp string) []byte {
	t.Helper()
	segmentPath := filepath.Join(temp, "segment.ts")
	generateCtx, generateCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer generateCancel()
	generate := exec.CommandContext(generateCtx, ffmpeg,
		"-nostdin", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=size=32x32:rate=10",
		"-t", "1", "-c:v", "mpeg2video", "-g", "10",
		"-f", "mpegts", segmentPath,
	)
	if output, err := generate.CombinedOutput(); err != nil {
		t.Fatalf("generate HLS fixture: %v (%s)", err, strings.TrimSpace(string(output)))
	}
	segment, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	return segment
}
