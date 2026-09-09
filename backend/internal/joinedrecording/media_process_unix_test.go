//go:build linux || darwin

package joinedrecording

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	mediaProcessHelperMode = "STOARAMA_TEST_MEDIA_PROCESS_MODE"
	mediaProcessPIDFile    = "STOARAMA_TEST_MEDIA_PROCESS_PID_FILE"
	mediaProcessReadyFile  = "STOARAMA_TEST_MEDIA_PROCESS_READY_FILE"
)

func TestBoundedMediaProcessHelper(t *testing.T) {
	mode := os.Getenv(mediaProcessHelperMode)
	if mode == "" {
		return
	}
	switch mode {
	case "stubborn-leader", "mixed-leader":
		if mode == "stubborn-leader" {
			signalIgnoreTERM()
		}
		grandchild := exec.Command(os.Args[0], "-test.run=^TestBoundedMediaProcessHelper$")
		grandchild.Env = replaceMediaProcessTestEnv(os.Environ(), mediaProcessHelperMode, "stubborn-grandchild")
		grandchild.Env = replaceMediaProcessTestEnv(grandchild.Env, mediaProcessReadyFile, "")
		grandchild.Stdout, grandchild.Stderr = os.Stdout, os.Stderr
		if err := grandchild.Start(); err != nil {
			os.Exit(71)
		}
		mustWriteMediaProcessFile(mediaProcessPIDFile, strconv.Itoa(grandchild.Process.Pid))
		mustWriteMediaProcessFile(mediaProcessReadyFile, "ready")
		for {
			time.Sleep(time.Hour)
		}
	case "stubborn-grandchild", "sibling":
		signalIgnoreTERM()
		mustWriteMediaProcessFile(mediaProcessReadyFile, "ready")
		for {
			time.Sleep(time.Hour)
		}
	default:
		os.Exit(72)
	}
}

func TestBoundedMediaProcessKillsDescendantsAndPreservesDeadline(t *testing.T) {
	dir := t.TempDir()
	siblingReady := filepath.Join(dir, "sibling.ready")
	sibling := exec.Command(os.Args[0], "-test.run=^TestBoundedMediaProcessHelper$")
	sibling.Env = replaceMediaProcessTestEnv(os.Environ(), mediaProcessHelperMode, "sibling")
	sibling.Env = replaceMediaProcessTestEnv(sibling.Env, mediaProcessReadyFile, siblingReady)
	if err := sibling.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sibling.Process.Kill()
		_ = sibling.Wait()
	})
	waitForMediaProcessFile(t, siblingReady)

	for _, mode := range []string{"stubborn-leader", "mixed-leader"} {
		t.Run(mode, func(t *testing.T) {
			caseDir := t.TempDir()
			grandchildPIDFile := filepath.Join(caseDir, "grandchild.pid")
			readyFile := filepath.Join(caseDir, "leader.ready")
			ctx := newMediaProcessDeadlineContext()
			process := newBoundedMediaProcess(ctx, os.Args[0], "-test.run=^TestBoundedMediaProcessHelper$")
			process.cmd.Env = replaceMediaProcessTestEnv(os.Environ(), mediaProcessHelperMode, mode)
			process.cmd.Env = replaceMediaProcessTestEnv(process.cmd.Env, mediaProcessPIDFile, grandchildPIDFile)
			process.cmd.Env = replaceMediaProcessTestEnv(process.cmd.Env, mediaProcessReadyFile, readyFile)
			var processOutput bytes.Buffer
			process.cmd.Stdout, process.cmd.Stderr = &processOutput, &processOutput
			if err := process.Start(); err != nil {
				t.Fatal(err)
			}
			leaderPID := process.cmd.Process.Pid
			waitForMediaProcessFile(t, readyFile)
			grandchildPID := readMediaProcessPID(t, grandchildPIDFile)

			started := time.Now()
			ctx.expire()
			err := process.Wait()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline was not preserved: %v output=%q", err, processOutput.String())
			}
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				t.Fatalf("canceled media process leaked an exit error into recovery classification: %v", err)
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("process group exceeded kill bound: %s", elapsed)
			}
			waitForMediaProcessGone(t, leaderPID)
			waitForMediaProcessGone(t, grandchildPID)
			if err := sibling.Process.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("bounded group cancellation killed sibling pid=%d: %v", sibling.Process.Pid, err)
			}
		})
	}
}

func TestBoundedMediaProcessCancelNaturalExitRacePreservesUnrelatedGroup(t *testing.T) {
	dir := t.TempDir()
	siblingReady := filepath.Join(dir, "sibling.ready")
	sibling := exec.Command(os.Args[0], "-test.run=^TestBoundedMediaProcessHelper$")
	sibling.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	sibling.Env = replaceMediaProcessTestEnv(os.Environ(), mediaProcessHelperMode, "sibling")
	sibling.Env = replaceMediaProcessTestEnv(sibling.Env, mediaProcessReadyFile, siblingReady)
	if err := sibling.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(-sibling.Process.Pid, syscall.SIGKILL)
		_ = sibling.Wait()
	})
	waitForMediaProcessFile(t, siblingReady)

	for i := 0; i < 250; i++ {
		ctx := newMediaProcessDeadlineContext()
		process := newBoundedMediaProcess(ctx, "/bin/sh", "-c", "exit 0")
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
		go ctx.expire()
		err := process.Wait()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("iteration=%d wait err=%v", i, err)
		}
		if err := syscall.Kill(-sibling.Process.Pid, syscall.Signal(0)); err != nil {
			t.Fatalf("iteration=%d unrelated process group was lost: %v", i, err)
		}
	}
}

func TestVerifyJoinedMediaPreservesDecodedFallbackDeadline(t *testing.T) {
	dir := t.TempDir()
	first := makeMediaClip(t, dir, "one.mp4", 440, false)
	second := makeMediaClip(t, dir, "two.mp4", 880, false)
	manifestPath := filepath.Join(dir, "concat.txt")
	manifest := "file '" + first.Path + "'\nfile '" + second.Path + "'\n"
	if err := os.WriteFile(manifestPath, []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(dir, "normalized-timebase.mp4")
	cmd := exec.Command(ffmpegBinary(), "-nostdin", "-v", "error", "-f", "concat", "-safe", "0", "-i", manifestPath, "-map", "0:v:0", "-c", "copy", "-video_track_timescale", "90000", outputPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("make normalized-timebase fixture: %v (%s)", err, output)
	}

	readyFile := filepath.Join(dir, "ffmpeg.ready")
	fakeFFmpeg := filepath.Join(dir, "ffmpeg")
	if err := os.WriteFile(fakeFFmpeg, []byte("#!/bin/sh\n: > \"$"+mediaProcessReadyFile+"\"\nexec sleep 60\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(mediaProcessReadyFile, readyFile)
	t.Setenv("FFMPEG_BIN", fakeFFmpeg)
	ctx := newMediaProcessDeadlineContext()
	errCh := make(chan error, 1)
	go func() {
		_, err := VerifyJoinedMedia(ctx, []LocalSource{first, second}, outputPath)
		errCh <- err
	}()
	waitForMediaProcessFile(t, readyFile)
	ctx.expire()
	err := <-errCh
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "bind rejected stream-copy decoded evidence") {
		t.Fatalf("decoded fallback deadline was flattened: %v", err)
	}
}

func TestVerifyJoinedMediaOverlapsOutputProbeWithSerialSources(t *testing.T) {
	dir := t.TempDir()
	source := makeMediaClip(t, dir, "source.mp4", 440, false)
	outputPath := filepath.Join(dir, "output.mp4")
	body, err := os.ReadFile(source.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outputPath, body, 0600); err != nil {
		t.Fatal(err)
	}
	realFFprobe, err := exec.LookPath(ffprobeBinary())
	if err != nil {
		t.Skip("ffprobe unavailable")
	}
	fakeFFprobe := filepath.Join(dir, "ffprobe")
	script := `#!/bin/sh
packets=false
last=
for arg do
	last=$arg
	if [ "$arg" = "-show_packets" ]; then packets=true; fi
done
if [ "$packets" = true ]; then
	case "${last##*/}" in
	source.mp4)
		: > "$STOARAMA_TEST_PROBE_DIR/source.ready"
		while [ ! -e "$STOARAMA_TEST_PROBE_DIR/output.ready" ]; do sleep 0.01; done
		;;
	output.mp4)
		: > "$STOARAMA_TEST_PROBE_DIR/output.ready"
		while [ ! -e "$STOARAMA_TEST_PROBE_DIR/source.ready" ]; do sleep 0.01; done
		;;
	esac
fi
exec "$STOARAMA_TEST_REAL_FFPROBE" "$@"
`
	if err := os.WriteFile(fakeFFprobe, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("STOARAMA_TEST_PROBE_DIR", dir)
	t.Setenv("STOARAMA_TEST_REAL_FFPROBE", realFFprobe)
	t.Setenv("FFPROBE_BIN", fakeFFprobe)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	verification, err := VerifyJoinedMedia(ctx, []LocalSource{source}, outputPath)
	if err != nil {
		t.Fatalf("overlapped verification failed: %v", err)
	}
	if verification.Status != "passed" {
		t.Fatalf("verification status=%q want=passed", verification.Status)
	}
}

func TestCompareDecodedEquivalentOverlapsOutputWithSerialSources(t *testing.T) {
	dir := t.TempDir()
	useDecodedIdentityFFmpeg(t, dir, "overlap")
	want, got := decodedEquivalentFingerprints(2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sha, err := compareDecodedEquivalent(ctx, []LocalSource{
		{Path: filepath.Join(dir, "source-one.mp4")},
		{Path: filepath.Join(dir, "source-two.mp4")},
	}, filepath.Join(dir, "output.mp4"), want, got)
	if err != nil || !lowerHex64(sha) {
		t.Fatalf("decoded equivalence did not overlap output with serial sources: sha=%q err=%v", sha, err)
	}
}

func TestCompareDecodedEquivalentSourceErrorCancelsAndReapsOutput(t *testing.T) {
	dir := t.TempDir()
	useDecodedIdentityFFmpeg(t, dir, "source-error")
	want, got := decodedEquivalentFingerprints(1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := compareDecodedEquivalent(ctx, []LocalSource{{Path: filepath.Join(dir, "source-one.mp4")}}, filepath.Join(dir, "output.mp4"), want, got)
	if err == nil || !strings.Contains(err.Error(), "decode source frame sequence") || !strings.Contains(err.Error(), "source decode failed") {
		t.Fatalf("source error lost precedence: %v", err)
	}
	outputPID := readMediaProcessPID(t, filepath.Join(dir, "output.pid"))
	waitForMediaProcessGone(t, outputPID)
}

func TestCompareDecodedEquivalentEarlyOutputErrorIsReaped(t *testing.T) {
	dir := t.TempDir()
	useDecodedIdentityFFmpeg(t, dir, "output-error")
	want, got := decodedEquivalentFingerprints(1)

	_, err := compareDecodedEquivalent(context.Background(), []LocalSource{{Path: filepath.Join(dir, "source-one.mp4")}}, filepath.Join(dir, "output.mp4"), want, got)
	if err == nil || !strings.Contains(err.Error(), "decode joined frame sequence") || !strings.Contains(err.Error(), "output decode failed") {
		t.Fatalf("output error was not preserved: %v", err)
	}
	outputPID := readMediaProcessPID(t, filepath.Join(dir, "output.pid"))
	waitForMediaProcessGone(t, outputPID)
}

func TestCompareDecodedEquivalentMalformedOutputReapsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	useDecodedIdentityFFmpeg(t, dir, "malformed-output")
	cleanupDecodedIdentityProcessGroups(t, dir, "output")
	want, got := decodedEquivalentFingerprints(1)

	_, err := compareDecodedEquivalent(context.Background(), []LocalSource{{Path: filepath.Join(dir, "source-one.mp4")}}, filepath.Join(dir, "output.mp4"), want, got)
	if err == nil || !strings.Contains(err.Error(), "decode joined frame sequence") || !strings.Contains(err.Error(), "invalid decoded frame duration") {
		t.Fatalf("malformed output error was not preserved: %v", err)
	}
	assertDecodedIdentityProcessGroupsGone(t, dir, "output")
}

func TestCompareDecodedEquivalentMalformedSourceWinsAndReapsBothGroups(t *testing.T) {
	dir := t.TempDir()
	useDecodedIdentityFFmpeg(t, dir, "malformed-source")
	cleanupDecodedIdentityProcessGroups(t, dir, "source", "output")
	want, got := decodedEquivalentFingerprints(1)

	_, err := compareDecodedEquivalent(context.Background(), []LocalSource{{Path: filepath.Join(dir, "source-one.mp4")}}, filepath.Join(dir, "output.mp4"), want, got)
	if err == nil || !strings.Contains(err.Error(), "decode source frame sequence") || !strings.Contains(err.Error(), "invalid decoded frame identity") || strings.Contains(err.Error(), "decode joined frame sequence") {
		t.Fatalf("malformed source error lost precedence: %v", err)
	}
	assertDecodedIdentityProcessGroupsGone(t, dir, "source", "output")
}

func TestCompareDecodedEquivalentParentCancelReapsBothBranches(t *testing.T) {
	dir := t.TempDir()
	useDecodedIdentityFFmpeg(t, dir, "cancel")
	want, got := decodedEquivalentFingerprints(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := compareDecodedEquivalent(ctx, []LocalSource{{Path: filepath.Join(dir, "source-one.mp4")}}, filepath.Join(dir, "output.mp4"), want, got)
		errCh <- err
	}()
	waitForMediaProcessFile(t, filepath.Join(dir, "source-one.mp4.started"))
	waitForMediaProcessFile(t, filepath.Join(dir, "output.mp4.started"))
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("parent cancellation was not preserved: %v", err)
	}
	for _, name := range []string{"source-one.mp4", "output.mp4"} {
		pid := readMediaProcessPID(t, filepath.Join(dir, name+".pid"))
		waitForMediaProcessGone(t, pid)
	}
}

func decodedEquivalentFingerprints(frames int64) (MediaFingerprint, MediaFingerprint) {
	packetSHA := strings.Repeat("a", 64)
	want := MediaFingerprint{DurationSeconds: float64(frames), Tracks: map[string]*TrackFingerprint{
		"video": {TimestampStatus: "source_clips_independent", PacketCount: frames, PacketChainSHA256: packetSHA, DecodedFrames: frames},
	}}
	got := MediaFingerprint{DurationSeconds: float64(frames), Tracks: map[string]*TrackFingerprint{
		"video": {TimestampStatus: "monotonic", PacketCount: frames, PacketChainSHA256: packetSHA, DecodedFrames: frames},
	}}
	return want, got
}

func useDecodedIdentityFFmpeg(t *testing.T, dir, mode string) {
	t.Helper()
	fake := filepath.Join(dir, "ffmpeg")
	script := `#!/bin/sh
input=
while [ "$#" -gt 0 ]; do
	if [ "$1" = "-i" ]; then
		shift
		input=$1
		break
	fi
	shift
done
name=${input##*/}
case "$STOARAMA_TEST_DECODE_IDENTITY_MODE:$name" in
	overlap:output.mp4)
		: > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.started"
		while [ ! -e "$STOARAMA_TEST_DECODE_IDENTITY_DIR/source.started" ]; do sleep 0.01; done
		printf '0,0,0,1,4,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n0,0,1,1,4,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n'
		;;
	overlap:source-one.mp4)
		: > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/source.started"
		while [ ! -e "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.started" ]; do sleep 0.01; done
		printf '0,0,0,1,4,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n'
		: > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/source-one.done"
		;;
	overlap:source-two.mp4)
		[ -e "$STOARAMA_TEST_DECODE_IDENTITY_DIR/source-one.done" ] || exit 42
		printf '0,0,1,1,4,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n'
		;;
	source-error:output.mp4)
		printf '%s' $$ > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.pid"
		: > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.started"
		exec sleep 60
		;;
	source-error:source-one.mp4)
		while [ ! -e "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.started" ]; do sleep 0.01; done
		echo 'source decode failed' >&2
		exit 23
		;;
	output-error:output.mp4)
		printf '%s' $$ > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.pid"
		: > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.started"
		echo 'output decode failed' >&2
		exit 24
		;;
	output-error:source-one.mp4)
		while [ ! -e "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.started" ]; do sleep 0.01; done
		printf '0,0,0,1,4,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n'
		;;
	malformed-output:output.mp4)
		printf '%s' $$ > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.pid"
		sleep 60 &
		printf '%s' $! > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.child.pid"
		: > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.started"
		printf '0,0,0,bad-duration,4,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n'
		wait
		;;
	malformed-output:source-one.mp4)
		while [ ! -e "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.started" ]; do sleep 0.01; done
		printf '0,0,0,1,4,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n'
		;;
	malformed-source:output.mp4)
		printf '%s' $$ > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.pid"
		sleep 60 &
		printf '%s' $! > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.child.pid"
		: > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.started"
		wait
		;;
	malformed-source:source-one.mp4)
		while [ ! -e "$STOARAMA_TEST_DECODE_IDENTITY_DIR/output.started" ]; do sleep 0.01; done
		printf '%s' $$ > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/source.pid"
		sleep 60 &
		printf '%s' $! > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/source.child.pid"
		printf '0,0,0,1,bad-size,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n'
		wait
		;;
	cancel:*)
		printf '%s' $$ > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/$name.pid"
		: > "$STOARAMA_TEST_DECODE_IDENTITY_DIR/$name.started"
		exec sleep 60
		;;
	*)
		exit 44
		;;
esac
`
	if err := os.WriteFile(fake, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FFMPEG_BIN", fake)
	t.Setenv("STOARAMA_TEST_DECODE_IDENTITY_DIR", dir)
	t.Setenv("STOARAMA_TEST_DECODE_IDENTITY_MODE", mode)
}

func cleanupDecodedIdentityProcessGroups(t *testing.T, dir string, names ...string) {
	t.Helper()
	t.Cleanup(func() {
		for _, name := range names {
			raw, err := os.ReadFile(filepath.Join(dir, name+".pid"))
			if err != nil {
				continue
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
			if err != nil || pid <= 0 {
				continue
			}
			if pgid, err := syscall.Getpgid(pid); err == nil && pgid == pid {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
			}
		}
	})
}

func assertDecodedIdentityProcessGroupsGone(t *testing.T, dir string, names ...string) {
	t.Helper()
	for _, name := range names {
		for _, suffix := range []string{".pid", ".child.pid"} {
			pid := readMediaProcessPID(t, filepath.Join(dir, name+suffix))
			waitForMediaProcessGone(t, pid)
		}
	}
}

func signalIgnoreTERM() {
	signalIgnore(syscall.SIGTERM)
}

func mustWriteMediaProcessFile(envName, value string) {
	path := os.Getenv(envName)
	if path == "" {
		return
	}
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		os.Exit(73)
	}
}

func waitForMediaProcessFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("helper did not create %s", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func readMediaProcessPID(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(raw))
	if err != nil || pid <= 0 {
		t.Fatalf("invalid helper pid %q: %v", raw, err)
	}
	return pid
}

func waitForMediaProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		err := syscall.Kill(pid, syscall.Signal(0))
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if err != nil {
			t.Fatalf("inspect pid=%d: %v", pid, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("orphan media process remains pid=%d", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func replaceMediaProcessTestEnv(env []string, key, value string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if len(entry) >= len(prefix) && entry[:len(prefix)] == prefix {
			continue
		}
		out = append(out, entry)
	}
	return append(out, prefix+value)
}

type mediaProcessDeadlineContext struct {
	context.Context
	done chan struct{}
	once sync.Once
}

func newMediaProcessDeadlineContext() *mediaProcessDeadlineContext {
	return &mediaProcessDeadlineContext{Context: context.Background(), done: make(chan struct{})}
}

func (c *mediaProcessDeadlineContext) Done() <-chan struct{} { return c.done }

func (c *mediaProcessDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (c *mediaProcessDeadlineContext) expire() { c.once.Do(func() { close(c.done) }) }

func signalIgnore(sig syscall.Signal) {
	// The helper process owns its signal policy; production code never changes
	// process-wide signal handlers.
	signal.Ignore(sig)
}
