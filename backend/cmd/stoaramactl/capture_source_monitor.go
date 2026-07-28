package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const sourceMonitorOutputLimit = 64 << 10

type sourceMonitorEventKind string

const (
	sourceMonitorStarted   sourceMonitorEventKind = "started"
	sourceMonitorSegment   sourceMonitorEventKind = "segment"
	sourceMonitorCompleted sourceMonitorEventKind = "completed"
	sourceMonitorFailed    sourceMonitorEventKind = "failed"
)

type sourceMonitorEvent struct {
	Kind       sourceMonitorEventKind `json:"kind"`
	At         time.Time              `json:"at"`
	Segment    string                 `json:"segment,omitempty"`
	Bytes      int64                  `json:"bytes,omitempty"`
	Segments   int                    `json:"segments,omitempty"`
	DurationMS int64                  `json:"duration_ms,omitempty"`
	Error      string                 `json:"error,omitempty"`
}

type sourceMonitorConfig struct {
	SourceURL string
	Duration  time.Duration
	Clip      time.Duration
	OutputDir string
	FFmpeg    string
}

type boundedSourceMonitorOutput struct {
	mu   sync.Mutex
	data []byte
}

func (b *boundedSourceMonitorOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) >= sourceMonitorOutputLimit {
		b.data = append(b.data[:0], p[len(p)-sourceMonitorOutputLimit:]...)
		return len(p), nil
	}
	overflow := len(b.data) + len(p) - sourceMonitorOutputLimit
	if overflow > 0 {
		copy(b.data, b.data[overflow:])
		b.data = b.data[:len(b.data)-overflow]
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *boundedSourceMonitorOutput) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data)
}

func runCaptureSourceMonitor(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("capture source-monitor", flag.ExitOnError)
	sourceURL := fs.String("source-url", "", "direct HTTP(S) stream URL")
	duration := fs.Duration("duration", 4*time.Hour, "monitor duration")
	clip := fs.Duration("clip", time.Minute, "segment duration")
	outputDir := fs.String("output-dir", "", "directory for transient segments")
	ffmpeg := fs.String("ffmpeg", "ffmpeg", "ffmpeg binary")
	_ = fs.Parse(args)

	cfg := sourceMonitorConfig{
		SourceURL: strings.TrimSpace(*sourceURL),
		Duration:  *duration,
		Clip:      *clip,
		OutputDir: strings.TrimSpace(*outputDir),
		FFmpeg:    strings.TrimSpace(*ffmpeg),
	}
	if err := validateSourceMonitorConfig(cfg); err != nil {
		log.Fatal(err)
	}
	if cfg.OutputDir == "" {
		dir, err := os.MkdirTemp("", "stoarama-source-monitor-*")
		if err != nil {
			log.Fatalf("create monitor directory: %v", err)
		}
		cfg.OutputDir = dir
		err = func() error {
			defer os.RemoveAll(dir)
			if err := os.MkdirAll(cfg.OutputDir, 0o700); err != nil {
				return fmt.Errorf("create --output-dir: %w", err)
			}
			return monitorSource(ctx, cfg, os.Stdout)
		}()
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o700); err != nil {
		log.Fatalf("create --output-dir: %v", err)
	}
	if err := monitorSource(ctx, cfg, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func validateSourceMonitorConfig(cfg sourceMonitorConfig) error {
	u, err := url.Parse(cfg.SourceURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("--source-url must be an absolute HTTP(S) URL")
	}
	if cfg.Duration <= 0 {
		return fmt.Errorf("--duration must be positive")
	}
	if cfg.Clip < 5*time.Second || cfg.Clip > 15*time.Minute {
		return fmt.Errorf("--clip must be between 5s and 15m")
	}
	if cfg.Duration < cfg.Clip {
		return fmt.Errorf("--duration must be at least --clip")
	}
	if cfg.FFmpeg == "" {
		return fmt.Errorf("--ffmpeg is required")
	}
	return nil
}

func monitorSource(ctx context.Context, cfg sourceMonitorConfig, output io.Writer) (runErr error) {
	started := time.Now().UTC()
	segments := 0
	defer func() {
		if runErr == nil {
			return
		}
		_ = emitSourceMonitorEvent(output, sourceMonitorEvent{
			Kind:       sourceMonitorFailed,
			At:         time.Now().UTC(),
			Segments:   segments,
			DurationMS: time.Since(started).Milliseconds(),
			Error:      runErr.Error(),
		})
	}()
	if err := emitSourceMonitorEvent(output, sourceMonitorEvent{Kind: sourceMonitorStarted, At: started}); err != nil {
		return err
	}

	runCtx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()
	pattern := filepath.Join(cfg.OutputDir, "segment-%09d.ts")
	cmd := exec.CommandContext(runCtx, cfg.FFmpeg,
		"-hide_banner", "-loglevel", "warning", "-nostdin",
		"-i", cfg.SourceURL, "-map", "0:v:0", "-map", "0:a:0?",
		"-c", "copy", "-f", "segment",
		"-segment_time", fmt.Sprintf("%.3f", cfg.Clip.Seconds()),
		"-reset_timestamps", "1", pattern,
	)
	var ffmpegOutput boundedSourceMonitorOutput
	cmd.Stdout = &ffmpegOutput
	cmd.Stderr = &ffmpegOutput
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start ffmpeg: %w", err)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	parentDone := ctx.Done()

	for {
		select {
		case <-ticker.C:
			count, err := consumeCompletedMonitorSegments(cfg.OutputDir, false, output)
			segments += count
			if err != nil {
				cancel()
				<-done
				return err
			}
		case err := <-done:
			if runCtx.Err() != nil {
				if removeErr := removeEmptyNewestMonitorSegment(cfg.OutputDir); removeErr != nil {
					return removeErr
				}
			}
			count, consumeErr := consumeCompletedMonitorSegments(cfg.OutputDir, true, output)
			segments += count
			if consumeErr != nil {
				return consumeErr
			}
			elapsed := time.Since(started)
			if (errors.Is(runCtx.Err(), context.DeadlineExceeded) || errors.Is(runCtx.Err(), context.Canceled)) && segments > 0 {
				return emitSourceMonitorEvent(output, sourceMonitorEvent{Kind: sourceMonitorCompleted, At: time.Now().UTC(), Segments: segments, DurationMS: elapsed.Milliseconds()})
			}
			if runCtx.Err() != nil {
				return runCtx.Err()
			}
			detail := strings.TrimSpace(ffmpegOutput.String())
			if err == nil {
				err = fmt.Errorf("ffmpeg exited before monitor duration")
			}
			if detail != "" {
				err = fmt.Errorf("%w: %s", err, detail)
			}
			return err
		case <-parentDone:
			cancel()
			parentDone = nil
		}
	}
}

func removeEmptyNewestMonitorSegment(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read monitor directory: %w", err)
	}
	var newest string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "segment-") && strings.HasSuffix(entry.Name(), ".ts") && entry.Name() > newest {
			newest = entry.Name()
		}
	}
	if newest == "" {
		return nil
	}
	path := filepath.Join(dir, newest)
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("stat newest monitor segment: %w", err)
	}
	if info.Size() > 0 {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove empty newest monitor segment: %w", err)
	}
	return nil
}

func consumeCompletedMonitorSegments(dir string, includeNewest bool, output io.Writer) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("read monitor directory: %w", err)
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "segment-") && strings.HasSuffix(entry.Name(), ".ts") {
			paths = append(paths, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(paths)
	if !includeNewest && len(paths) > 0 {
		paths = paths[:len(paths)-1]
	}
	consumed := 0
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return consumed, fmt.Errorf("stat segment: %w", err)
		}
		if info.Size() == 0 {
			return consumed, fmt.Errorf("empty segment %s", filepath.Base(path))
		}
		if err := emitSourceMonitorEvent(output, sourceMonitorEvent{Kind: sourceMonitorSegment, At: time.Now().UTC(), Segment: filepath.Base(path), Bytes: info.Size()}); err != nil {
			return consumed, err
		}
		if err := os.Remove(path); err != nil {
			return consumed, fmt.Errorf("remove consumed segment: %w", err)
		}
		consumed++
	}
	return consumed, nil
}

func emitSourceMonitorEvent(output io.Writer, event sourceMonitorEvent) error {
	if err := json.NewEncoder(output).Encode(event); err != nil {
		return fmt.Errorf("write source-monitor event: %w", err)
	}
	return nil
}
