package capture

import (
	"context"
	"errors"
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
	if elapsed > 3500*time.Millisecond {
		t.Fatalf("expired HLS fragment took %s to fail; requests=%d", elapsed, forbiddenRequests.Load())
	}
	if forbiddenRequests.Load() != 1 {
		t.Fatalf("expired HLS fragment requests=%d want=1", forbiddenRequests.Load())
	}
}

// TestContinuousGooglevideoHLSAdvancingManifestExpiredFragments replays the
// production shape that the stale-playlist counter cannot catch: the manifest
// sequence keeps advancing, but its signed child fragments have already expired.
// FFmpeg logs each 403 and remains alive while producing no media, so the relay
// must return to the worker's fresh resolver without waiting for the watchdog.
func TestContinuousGooglevideoHLSAdvancingManifestExpiredFragments(t *testing.T) {
	ffmpeg, err := exec.LookPath(ffmpegBin())
	if err != nil {
		t.Skipf("ffmpeg unavailable: %v", err)
	}

	temp := t.TempDir()
	segment := generateHLSFixtureSegment(t, ffmpeg, temp)

	var playlistRequests atomic.Int64
	var forbiddenRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/live.m3u8":
			sequence := playlistRequests.Add(1) - 1
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			fmt.Fprintf(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:%d\n", sequence)
			if sequence == 0 {
				fmt.Fprint(w, "#EXTINF:1,\n/segment.ts\n")
				return
			}
			fmt.Fprintf(w, "#EXTINF:1,\n/expired-%d.ts\n", sequence)
		case r.URL.Path == "/segment.ts":
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(segment)
		case strings.HasPrefix(r.URL.Path, "/expired-"):
			forbiddenRequests.Add(1)
			http.Error(w, "expired", http.StatusForbidden)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	captureCtx, captureCancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer captureCancel()
	started := time.Now()
	err = captureContinuousWithHeaders(
		captureCtx,
		server.URL+"/live.m3u8",
		time.Second,
		"manifest.googlevideo.com",
		nil,
		temp,
		func(Segment) error { return nil },
		"",
		10*time.Second,
		10*time.Second,
	)
	elapsed := time.Since(started)
	if captureCtx.Err() == context.DeadlineExceeded {
		t.Fatalf("capture held advancing manifest with expired fragments past 4s; playlists=%d forbidden=%d", playlistRequests.Load(), forbiddenRequests.Load())
	}
	if !errors.Is(err, ErrContinuousExpiredGooglevideoFragment) {
		t.Fatalf("capture error=%v, want expired Googlevideo HLS fragment", err)
	}
	if elapsed > 3500*time.Millisecond {
		t.Fatalf("advancing manifest with expired fragments took %s to reconnect", elapsed)
	}
	if forbiddenRequests.Load() == 0 {
		t.Fatal("fixture never served an expired fragment")
	}
}

// TestContinuousGooglevideoHLSTransientForbiddenRecoversWithoutRestart covers
// the healthy production shape that a first-403 trigger misclassifies. FFmpeg
// may see one unavailable child fragment while the manifest keeps publishing
// valid media. CaptureContinuous must keep the child when output resumes.
func TestContinuousGooglevideoHLSTransientForbiddenRecoversWithoutRestart(t *testing.T) {
	ffmpeg, err := exec.LookPath(ffmpegBin())
	if err != nil {
		t.Skipf("ffmpeg unavailable: %v", err)
	}

	temp := t.TempDir()
	segments := generateHLSFixtureSegments(t, ffmpeg, temp, 7)

	var playlistRequests atomic.Int64
	var forbiddenRequests atomic.Int64
	var freshRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/live.m3u8":
			request := playlistRequests.Add(1)
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			if request == 1 {
				fmt.Fprint(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:1,\n/segment.ts\n#EXTINF:1,\n/transient.ts\n")
				return
			}
			sequence := request
			fmt.Fprintf(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:%d\n#EXTINF:1,\n/fresh-%d.ts\n", sequence, sequence)
			if request >= 4 {
				fmt.Fprint(w, "#EXT-X-ENDLIST\n")
			}
		case r.URL.Path == "/segment.ts":
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(segments[0])
		case r.URL.Path == "/transient.ts":
			forbiddenRequests.Add(1)
			http.Error(w, "temporarily unavailable", http.StatusForbidden)
		case strings.HasPrefix(r.URL.Path, "/fresh-"):
			freshRequests.Add(1)
			var index int
			if _, err := fmt.Sscanf(r.URL.Path, "/fresh-%d.ts", &index); err != nil || index >= len(segments) {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(segments[index])
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	captureCtx, captureCancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer captureCancel()
	result := make(chan error, 1)
	go func() {
		result <- captureContinuousWithHeaders(
			captureCtx,
			server.URL+"/live.m3u8",
			time.Second,
			"manifest.googlevideo.com",
			nil,
			temp,
			func(Segment) error { return nil },
			"",
			10*time.Second,
			10*time.Second,
		)
	}()

	deadline := time.NewTimer(6 * time.Second)
	defer deadline.Stop()
	for forbiddenRequests.Load() == 0 || freshRequests.Load() < 2 {
		select {
		case err := <-result:
			t.Fatalf("capture restarted after a transient 403 before media recovered: %v; playlists=%d forbidden=%d fresh=%d", err, playlistRequests.Load(), forbiddenRequests.Load(), freshRequests.Load())
		case <-deadline.C:
			t.Fatalf("fixture did not recover after transient 403; playlists=%d forbidden=%d fresh=%d", playlistRequests.Load(), forbiddenRequests.Load(), freshRequests.Load())
		case <-time.After(25 * time.Millisecond):
		}
	}

	// The finite fixture ends after recovered media. A stale 403 classification
	// must not turn that later clean FFmpeg exit into a resolver sentinel.
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("capture misclassified a clean exit after recovered media: %v; playlists=%d forbidden=%d fresh=%d", err, playlistRequests.Load(), forbiddenRequests.Load(), freshRequests.Load())
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("capture did not finish the recovered finite fixture; playlists=%d forbidden=%d fresh=%d", playlistRequests.Load(), forbiddenRequests.Load(), freshRequests.Load())
	}
}

func TestContinuousGooglevideoHLSSurvivesSustainedHealthyPublication(t *testing.T) {
	ffmpeg, err := exec.LookPath(ffmpegBin())
	if err != nil {
		t.Skipf("ffmpeg unavailable: %v", err)
	}
	temp := t.TempDir()
	segment := generateHLSFixtureSegment(t, ffmpeg, temp)

	started := time.Now()
	var highestSegmentRequested atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			fmt.Fprint(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n")
			published := 1 + int(time.Since(started)/(1100*time.Millisecond))
			for i := 0; i < published; i++ {
				fmt.Fprintf(w, "#EXTINF:1,\n/segment-%d.ts\n", i)
			}
			return
		}
		var index int64
		if _, err := fmt.Sscanf(r.URL.Path, "/segment-%d.ts", &index); err == nil {
			for {
				prior := highestSegmentRequested.Load()
				if index <= prior || highestSegmentRequested.CompareAndSwap(prior, index) {
					break
				}
			}
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(segment)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	captureCtx, captureCancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer captureCancel()
	args := []string{"-nostdin", "-loglevel", "error"}
	args = appendGooglevideoHLSRecoveryInputArgs(args, "https://manifest.googlevideo.com/live.m3u8", "")
	args = append(args,
		"-i", server.URL+"/live.m3u8",
		"-map", "0:v:0", "-c", "copy", "-f", "null", "-",
	)
	cmd := exec.CommandContext(captureCtx, ffmpeg, args...)
	output, runErr := cmd.CombinedOutput()
	if captureCtx.Err() != context.DeadlineExceeded {
		t.Fatalf("FFmpeg exited during a healthy advancing playlist; elapsed=%s highest_segment=%d err=%v stderr=%s", time.Since(started), highestSegmentRequested.Load(), runErr, strings.TrimSpace(string(output)))
	}
	if highestSegmentRequested.Load() < 2 {
		t.Fatalf("healthy advancing playlist reached only segment %d", highestSegmentRequested.Load())
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

func generateHLSFixtureSegments(t *testing.T, ffmpeg, temp string, count int) [][]byte {
	t.Helper()
	segmentPattern := filepath.Join(temp, "fixture-%d.ts")
	manifestPath := filepath.Join(temp, "fixture.m3u8")
	generateCtx, generateCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer generateCancel()
	generate := exec.CommandContext(generateCtx, ffmpeg,
		"-nostdin", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=size=32x32:rate=10",
		"-t", fmt.Sprintf("%d", count), "-c:v", "mpeg2video", "-g", "10",
		"-f", "hls", "-hls_time", "1", "-hls_list_size", "0",
		"-hls_segment_filename", segmentPattern, manifestPath,
	)
	if output, err := generate.CombinedOutput(); err != nil {
		t.Fatalf("generate HLS fixtures: %v (%s)", err, strings.TrimSpace(string(output)))
	}
	segments := make([][]byte, count)
	for i := range count {
		body, err := os.ReadFile(filepath.Join(temp, fmt.Sprintf("fixture-%d.ts", i)))
		if err != nil {
			t.Fatalf("read HLS fixture %d: %v", i, err)
		}
		segments[i] = body
	}
	return segments
}
