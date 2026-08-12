package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecordingCanaryIsReservedNativeLocalAndCleaned(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PATH", os.Getenv("PATH"))
	binDir := filepath.Join(home, ".stoarama", "bin")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	argsLog := filepath.Join(home, "ffmpeg-args.log")
	ffmpeg := `#!/bin/sh
printf '%s\n' "$*" >> '` + argsLog + `'
case " $* " in
  *" -f null "*) exit 0 ;;
esac
sleep 2
eval "out=\${$#}"
printf 'native-media' > "$out"
`
	ffprobe := `#!/bin/sh
printf '%s\n' '{"format":{"duration":"15.0"},"streams":[{"codec_type":"video","codec_name":"h264","avg_frame_rate":"30/1","r_frame_rate":"30/1","width":1280,"height":720},{"codec_type":"audio","codec_name":"aac"}]}'
`
	for name, body := range map[string]string{"ffmpeg": ffmpeg, "ffprobe": ffprobe} {
		if err := os.WriteFile(filepath.Join(binDir, name), []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	var starts, checks, finishes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/canary-reservations"):
			starts.Add(1)
			writeCanaryTestSpec(w)
		case strings.HasSuffix(r.URL.Path, "/check"):
			checks.Add(1)
			writeCanaryTestSpec(w)
		case strings.HasSuffix(r.URL.Path, "/finish"):
			finishes.Add(1)
			_, _ = w.Write([]byte(`{"finished":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	config := relayConfig{NodeID: 150, NodeToken: "node-secret", APIURL: server.URL, InstalledAt: time.Now()}
	configBytes, _ := json.Marshal(config)
	if err := os.WriteFile(filepath.Join(home, ".stoarama", "config.json"), configBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := runRecordingCanary(context.Background(), []string{"--recording-id", "445"}); err != nil {
		t.Fatal(err)
	}
	if starts.Load() != 1 || checks.Load() < 1 || finishes.Load() != 1 {
		t.Fatalf("reservation calls start=%d check=%d finish=%d", starts.Load(), checks.Load(), finishes.Load())
	}
	logged, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(logged)), "\n")
	if len(lines) != 2 {
		t.Fatalf("ffmpeg calls=%d want capture+decode only: %q", len(lines), logged)
	}
	if !strings.Contains(lines[0], "-c copy") || !strings.Contains(lines[1], "-f null") {
		t.Fatalf("unexpected ffmpeg calls: %q", logged)
	}
	for _, forbidden := range []string{"libx264", "-vf", "thumbnail", "-c:v"} {
		if strings.Contains(string(logged), forbidden) {
			t.Fatalf("canary re-encoded with %q: %q", forbidden, logged)
		}
	}
	outputPath := strings.Fields(lines[0])[len(strings.Fields(lines[0]))-1]
	if _, err := os.Stat(filepath.Dir(filepath.Dir(outputPath))); !os.IsNotExist(err) {
		t.Fatalf("private canary root was not removed: %s err=%v", outputPath, err)
	}
}

func TestRecordingCanaryRejectsMismatchedNodeAndShortReservation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		nodeID    int64
		safeUntil time.Time
	}{
		{name: "wrong node", nodeID: 151, safeUntil: time.Now().Add(3 * time.Minute)},
		{name: "short reservation", nodeID: 150, safeUntil: time.Now().Add(20 * time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			binDir := filepath.Join(home, ".stoarama", "bin")
			if err := os.MkdirAll(binDir, 0o700); err != nil {
				t.Fatal(err)
			}
			argsLog := filepath.Join(home, "ffmpeg-args.log")
			if err := os.WriteFile(filepath.Join(binDir, "ffmpeg"), []byte("#!/bin/sh\nprintf x >> '"+argsLog+"'\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/canary-reservations"):
					writeCanaryTestSpecValues(w, tc.nodeID, tc.safeUntil)
				case strings.HasSuffix(r.URL.Path, "/finish"):
					_, _ = w.Write([]byte(`{"finished":true}`))
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			configBytes, _ := json.Marshal(relayConfig{NodeID: 150, NodeToken: "node-secret", APIURL: server.URL, InstalledAt: time.Now()})
			if err := os.WriteFile(filepath.Join(home, ".stoarama", "config.json"), configBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := runRecordingCanary(context.Background(), []string{"--recording-id", "445"}); err == nil {
				t.Fatal("unsafe reservation was accepted")
			}
			if _, err := os.Stat(argsLog); !os.IsNotExist(err) {
				t.Fatalf("FFmpeg ran for rejected reservation: %v", err)
			}
		})
	}
}

func TestRecordingCanaryCancelsAndCleansWhenReservationIsLost(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PATH", os.Getenv("PATH"))
	canaryTmp := filepath.Join(home, "canary-tmp")
	if err := os.MkdirAll(canaryTmp, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", canaryTmp)
	binDir := filepath.Join(home, ".stoarama", "bin")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	argsLog := filepath.Join(home, "ffmpeg-args.log")
	ffmpeg := `#!/bin/sh
printf '%s\n' "$*" >> '` + argsLog + `'
exec sleep 20
`
	if err := os.WriteFile(filepath.Join(binDir, "ffmpeg"), []byte(ffmpeg), 0o700); err != nil {
		t.Fatal(err)
	}

	var starts, checks, finishes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/canary-reservations"):
			starts.Add(1)
			writeCanaryTestSpec(w)
		case strings.HasSuffix(r.URL.Path, "/check"):
			if checks.Add(1) > 1 {
				http.Error(w, `{"error":"reservation lost"}`, http.StatusConflict)
				return
			}
			writeCanaryTestSpec(w)
		case strings.HasSuffix(r.URL.Path, "/finish"):
			finishes.Add(1)
			_, _ = w.Write([]byte(`{"finished":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	configBytes, _ := json.Marshal(relayConfig{NodeID: 150, NodeToken: "node-secret", APIURL: server.URL, InstalledAt: time.Now()})
	if err := os.WriteFile(filepath.Join(home, ".stoarama", "config.json"), configBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	err := runRecordingCanary(context.Background(), []string{"--recording-id", "445"})
	if err == nil || !strings.Contains(err.Error(), "production safety") {
		t.Fatalf("err=%v want production safety cancellation", err)
	}
	if time.Since(started) > 15*time.Second {
		t.Fatalf("reservation loss did not cancel promptly: %s", time.Since(started))
	}
	if starts.Load() != 1 || checks.Load() < 2 || finishes.Load() != 1 {
		t.Fatalf("reservation calls start=%d check=%d finish=%d", starts.Load(), checks.Load(), finishes.Load())
	}
	entries, readErr := os.ReadDir(canaryTmp)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("canceled canary left private temp entries: %v", entries)
	}
}

func TestRecordingCanaryKeepsSafetyWatcherThroughDecode(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PATH", os.Getenv("PATH"))
	binDir := filepath.Join(home, ".stoarama", "bin")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	ffmpeg := `#!/bin/sh
case " $* " in
  *" -f null "*) exec sleep 20 ;;
esac
eval "out=\${$#}"
printf 'native-media' > "$out"
`
	ffprobe := `#!/bin/sh
printf '%s\n' '{"format":{"duration":"15.0"},"streams":[{"codec_type":"video","codec_name":"h264","avg_frame_rate":"30/1","r_frame_rate":"30/1","width":1280,"height":720}]}'
`
	for name, body := range map[string]string{"ffmpeg": ffmpeg, "ffprobe": ffprobe} {
		if err := os.WriteFile(filepath.Join(binDir, name), []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	var checks, finishes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/canary-reservations"):
			writeCanaryTestSpec(w)
		case strings.HasSuffix(r.URL.Path, "/check"):
			if checks.Add(1) > 1 {
				http.Error(w, `{"error":"reservation lost"}`, http.StatusConflict)
				return
			}
			writeCanaryTestSpec(w)
		case strings.HasSuffix(r.URL.Path, "/finish"):
			finishes.Add(1)
			_, _ = w.Write([]byte(`{"finished":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	configBytes, _ := json.Marshal(relayConfig{NodeID: 150, NodeToken: "node-secret", APIURL: server.URL, InstalledAt: time.Now()})
	if err := os.WriteFile(filepath.Join(home, ".stoarama", "config.json"), configBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	err := runRecordingCanary(context.Background(), []string{"--recording-id", "445"})
	if err == nil || !strings.Contains(err.Error(), "production safety") {
		t.Fatalf("err=%v want decode-stage production safety cancellation", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("decode-stage reservation loss did not cancel promptly: %s", elapsed)
	}
	if checks.Load() < 2 || finishes.Load() != 1 {
		t.Fatalf("reservation calls checks=%d finish=%d", checks.Load(), finishes.Load())
	}
}

func writeCanaryTestSpec(w http.ResponseWriter) {
	writeCanaryTestSpecValues(w, 150, time.Now().Add(3*time.Minute).UTC())
}

func writeCanaryTestSpecValues(w http.ResponseWriter, nodeID int64, safeUntil time.Time) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"reservation_id": "123e4567-e89b-12d3-a456-426614174000",
		"recording_id":   445, "node_id": nodeID, "stream_id": 17342,
		"provider": "TEST", "source_url": "https://203.0.113.10/live.m3u8",
		"safe_until": safeUntil.UTC(),
	})
}
