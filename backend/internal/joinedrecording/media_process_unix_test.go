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
