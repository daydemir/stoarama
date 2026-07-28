package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type failingMonitorWriter struct{}

func (failingMonitorWriter) Write([]byte) (int, error) {
	return 0, errors.New("output closed")
}

func TestValidateSourceMonitorConfig(t *testing.T) {
	valid := sourceMonitorConfig{
		SourceURL: "https://example.com/live/playlist.m3u8",
		Duration:  time.Hour,
		Clip:      time.Minute,
		FFmpeg:    "ffmpeg",
	}
	if err := validateSourceMonitorConfig(valid); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	tests := []sourceMonitorConfig{
		{SourceURL: "file:///etc/passwd", Duration: time.Hour, Clip: time.Minute, FFmpeg: "ffmpeg"},
		{SourceURL: valid.SourceURL, Duration: 0, Clip: time.Minute, FFmpeg: "ffmpeg"},
		{SourceURL: valid.SourceURL, Duration: time.Hour, Clip: time.Second, FFmpeg: "ffmpeg"},
		{SourceURL: valid.SourceURL, Duration: time.Second, Clip: time.Minute, FFmpeg: "ffmpeg"},
		{SourceURL: valid.SourceURL, Duration: time.Hour, Clip: time.Minute},
	}
	for i, test := range tests {
		if err := validateSourceMonitorConfig(test); err == nil {
			t.Fatalf("case %d unexpectedly valid", i)
		}
	}
}

func TestConsumeCompletedMonitorSegmentsKeepsActiveNewest(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"segment-000000000.ts", "segment-000000001.ts"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("video"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var output bytes.Buffer
	count, err := consumeCompletedMonitorSegments(dir, false, &output)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("consumed=%d want 1", count)
	}
	if _, err := os.Stat(filepath.Join(dir, "segment-000000001.ts")); err != nil {
		t.Fatalf("newest segment must remain: %v", err)
	}
	var event sourceMonitorEvent
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	if event.Kind != sourceMonitorSegment || event.Bytes != 5 {
		t.Fatalf("event=%+v", event)
	}
}

func TestConsumeCompletedMonitorSegmentsRejectsEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "segment-000000000.ts")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := consumeCompletedMonitorSegments(dir, true, &bytes.Buffer{}); err == nil {
		t.Fatal("empty segment unexpectedly accepted")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("failed segment must remain for inspection: %v", err)
	}
}

func TestMonitorSourceCompletesAndDeletesSegments(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"segment-000000000.ts", "segment-000000001.ts"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("video"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	script := filepath.Join(dir, "fake-ffmpeg")
	body := "#!/bin/sh\nsleep 5\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err := monitorSource(context.Background(), sourceMonitorConfig{
		SourceURL: "https://example.com/live.m3u8",
		Duration:  100 * time.Millisecond,
		Clip:      10 * time.Millisecond,
		OutputDir: dir,
		FFmpeg:    script,
	}, &output)
	if err != nil {
		t.Fatalf("monitor source: %v\n%s", err, output.String())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".ts" {
			t.Fatalf("segment was not deleted: %s", entry.Name())
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	kinds := make([]sourceMonitorEventKind, 0, 4)
	for {
		var event sourceMonitorEvent
		if err := decoder.Decode(&event); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		kinds = append(kinds, event.Kind)
	}
	if len(kinds) != 4 || kinds[0] != sourceMonitorStarted || kinds[3] != sourceMonitorCompleted {
		t.Fatalf("event kinds=%v", kinds)
	}
}

func TestMonitorSourceEmitsFailedLifecycleOnStartError(t *testing.T) {
	var output bytes.Buffer
	err := monitorSource(context.Background(), sourceMonitorConfig{
		SourceURL: "https://example.com/live.m3u8",
		Duration:  time.Second,
		Clip:      10 * time.Millisecond,
		OutputDir: t.TempDir(),
		FFmpeg:    filepath.Join(t.TempDir(), "missing-ffmpeg"),
	}, &output)
	if err == nil {
		t.Fatal("missing ffmpeg unexpectedly succeeded")
	}
	events := decodeSourceMonitorEvents(t, output.Bytes())
	if len(events) != 2 || events[0].Kind != sourceMonitorStarted || events[1].Kind != sourceMonitorFailed {
		t.Fatalf("events=%+v", events)
	}
}

func TestMonitorSourceReturnsOutputError(t *testing.T) {
	err := monitorSource(context.Background(), sourceMonitorConfig{
		SourceURL: "https://example.com/live.m3u8",
		Duration:  time.Second,
		Clip:      10 * time.Millisecond,
		OutputDir: t.TempDir(),
		FFmpeg:    "unused",
	}, failingMonitorWriter{})
	if err == nil || !strings.Contains(err.Error(), "write source-monitor event") {
		t.Fatalf("error=%v", err)
	}
}

func TestMonitorSourceCancellationReturnsPromptlyAndEmitsFailed(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-ffmpeg")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	var output bytes.Buffer
	started := time.Now()
	err := monitorSource(ctx, sourceMonitorConfig{
		SourceURL: "https://example.com/live.m3u8",
		Duration:  time.Second,
		Clip:      10 * time.Millisecond,
		OutputDir: dir,
		FFmpeg:    script,
	}, &output)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want canceled", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("cancellation took %s", elapsed)
	}
	events := decodeSourceMonitorEvents(t, output.Bytes())
	if events[len(events)-1].Kind != sourceMonitorFailed {
		t.Fatalf("events=%+v", events)
	}
}

func TestMonitorSourceCancellationAfterCaptureEmitsCompleted(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-ffmpeg")
	source := "#!/bin/sh\nfor output_path do :; done\nprintf video > \"$output_path\"\nsleep 30\n"
	if err := os.WriteFile(script, []byte(source), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			info, err := os.Stat(filepath.Join(dir, "segment-%09d.ts"))
			if err == nil && info.Size() > 0 {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	defer cancel()
	var output bytes.Buffer
	err := monitorSource(ctx, sourceMonitorConfig{
		SourceURL: "https://example.com/live.m3u8",
		Duration:  3 * time.Second,
		Clip:      10 * time.Millisecond,
		OutputDir: dir,
		FFmpeg:    script,
	}, &output)
	if err != nil {
		t.Fatalf("error=%v", err)
	}
	events := decodeSourceMonitorEvents(t, output.Bytes())
	last := events[len(events)-1]
	if last.Kind != sourceMonitorCompleted || last.Segments != 1 {
		t.Fatalf("events=%+v", events)
	}
}

func TestMonitorSourceCancellationDiscardsEmptyNewestSegment(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-ffmpeg")
	source := "#!/bin/sh\nfor output_path do :; done\nfirst=${output_path%-%09d.ts}-000000000.ts\nsecond=${output_path%-%09d.ts}-000000001.ts\nprintf video > \"$first\"\n: > \"$second\"\nsleep 30\n"
	if err := os.WriteFile(script, []byte(source), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		second := filepath.Join(dir, "segment-000000001.ts")
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(second); err == nil {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	defer cancel()
	var output bytes.Buffer
	err := monitorSource(ctx, sourceMonitorConfig{
		SourceURL: "https://example.com/live.m3u8",
		Duration:  3 * time.Second,
		Clip:      10 * time.Millisecond,
		OutputDir: dir,
		FFmpeg:    script,
	}, &output)
	if err != nil {
		t.Fatalf("error=%v", err)
	}
	events := decodeSourceMonitorEvents(t, output.Bytes())
	last := events[len(events)-1]
	if last.Kind != sourceMonitorCompleted || last.Segments != 1 {
		t.Fatalf("events=%+v", events)
	}
}

func TestMonitorSourceFailedEventIncludesSegmentsConsumedBeforeScanError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "segment-000000000.ts"), []byte("video"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "segment-000000001.ts"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "segment-000000002.ts"), []byte("active"), 0o600); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, "fake-ffmpeg")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err := monitorSource(context.Background(), sourceMonitorConfig{
		SourceURL: "https://example.com/live.m3u8",
		Duration:  2 * time.Second,
		Clip:      10 * time.Millisecond,
		OutputDir: dir,
		FFmpeg:    script,
	}, &output)
	if err == nil {
		t.Fatal("empty finalized segment unexpectedly accepted")
	}
	events := decodeSourceMonitorEvents(t, output.Bytes())
	failed := events[len(events)-1]
	if failed.Kind != sourceMonitorFailed || failed.Segments != 1 {
		t.Fatalf("failed event=%+v want one consumed segment", failed)
	}
}

func TestBoundedSourceMonitorOutputKeepsNewestBytes(t *testing.T) {
	var output boundedSourceMonitorOutput
	prefix := bytes.Repeat([]byte("a"), sourceMonitorOutputLimit)
	if n, err := output.Write(prefix); err != nil || n != len(prefix) {
		t.Fatalf("write prefix=(%d,%v)", n, err)
	}
	if n, err := output.Write([]byte("tail")); err != nil || n != 4 {
		t.Fatalf("write tail=(%d,%v)", n, err)
	}
	got := output.String()
	if len(got) != sourceMonitorOutputLimit {
		t.Fatalf("len=%d want %d", len(got), sourceMonitorOutputLimit)
	}
	if !strings.HasSuffix(got, "tail") {
		t.Fatalf("output does not retain newest bytes")
	}

	oversized := append(bytes.Repeat([]byte("b"), sourceMonitorOutputLimit), []byte("newest")...)
	if n, err := output.Write(oversized); err != nil || n != len(oversized) {
		t.Fatalf("write oversized=(%d,%v)", n, err)
	}
	got = output.String()
	if len(got) != sourceMonitorOutputLimit || !strings.HasSuffix(got, "newest") {
		t.Fatalf("oversized write retained wrong tail")
	}
}

func decodeSourceMonitorEvents(t *testing.T, body []byte) []sourceMonitorEvent {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(body))
	var events []sourceMonitorEvent
	for {
		var event sourceMonitorEvent
		if err := decoder.Decode(&event); errors.Is(err, io.EOF) {
			return events
		} else if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
}
