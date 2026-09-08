package capture

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestRunYTDLPCommandCleansPrivateTempAfterExitAndTimeout(t *testing.T) {
	for _, test := range []struct {
		name     string
		behavior string
		timeout  time.Duration
	}{
		{name: "success", behavior: "success", timeout: 5 * time.Second},
		{name: "error", behavior: "error", timeout: 5 * time.Second},
		{name: "timeout", behavior: "timeout", timeout: 100 * time.Millisecond},
		{name: "stubborn grandchild", behavior: "stubborn", timeout: 100 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			mediaSibling := filepath.Join(root, "recording.mp4")
			if err := os.WriteFile(mediaSibling, []byte("preserve"), 0o600); err != nil {
				t.Fatal(err)
			}
			parentTemp := t.TempDir()
			t.Setenv("TMPDIR", parentTemp)
			t.Setenv(YTDLPRuntimeTempRootEnv, root)
			t.Setenv(YTDLPPrivateTempEnv, "1")
			t.Setenv("STOARAMA_YTDLP_COMMAND_HELPER", test.behavior)
			ctx, cancel := context.WithTimeout(context.Background(), test.timeout)
			defer cancel()
			output, err := RunYTDLPCommand(ctx, os.Args[0], "-test.run=TestYTDLPCommandHelperProcess")
			switch test.behavior {
			case "success":
				if err != nil {
					t.Fatal(err)
				}
			case "error":
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) || exitErr.ExitCode() != 7 {
					t.Fatalf("error=%v", err)
				}
			case "timeout", "stubborn":
				if !errors.Is(ctx.Err(), context.DeadlineExceeded) || err == nil {
					t.Fatalf("ctx=%v err=%v", ctx.Err(), err)
				}
			}
			if test.behavior == "stubborn" {
				lines := strings.Fields(string(output))
				if len(lines) < 2 {
					t.Fatalf("stubborn helper output=%q", output)
				}
				if err := waitForProcessExit(lines[len(lines)-1]); err != nil {
					t.Fatal(err)
				}
			}
			entries, readErr := os.ReadDir(root)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 1 || entries[0].Name() != "recording.mp4" {
				t.Fatalf("private temp cleanup crossed invocation boundary: %v", entries)
			}
			if got := os.Getenv("TMPDIR"); got != parentTemp {
				t.Fatalf("parent TMPDIR changed to %q", got)
			}
		})
	}
}

func TestRunYTDLPCommandCleanupFailurePreservesSuccessfulResult(t *testing.T) {
	root := t.TempDir()
	t.Setenv(YTDLPPrivateTempEnv, "1")
	t.Setenv(YTDLPRuntimeTempRootEnv, root)
	t.Setenv("STOARAMA_YTDLP_COMMAND_HELPER", "swap")

	output, err := RunYTDLPCommand(context.Background(), os.Args[0], "-test.run=TestYTDLPCommandHelperProcess")
	if err != nil {
		t.Fatalf("cleanup failure replaced successful child result: %v", err)
	}
	if !strings.Contains(string(output), "stoarama-ytdlp-") {
		t.Fatalf("child output changed: %q", output)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("failed-closed cleanup did not retain audit paths: %v", entries)
	}
}

func TestRunYTDLPCommandDefaultOffPreservesParentEnvironment(t *testing.T) {
	parentTemp := t.TempDir()
	t.Setenv("TMPDIR", parentTemp)
	t.Setenv(YTDLPPrivateTempEnv, "0")
	t.Setenv(YTDLPRuntimeTempRootEnv, "")
	t.Setenv("STOARAMA_YTDLP_COMMAND_HELPER", "report")

	output, err := RunYTDLPCommand(context.Background(), os.Args[0], "-test.run=TestYTDLPCommandHelperProcess")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(output)); got != parentTemp {
		t.Fatalf("child TMPDIR=%q want parent value %q", got, parentTemp)
	}
	if got := os.Getenv("TMPDIR"); got != parentTemp {
		t.Fatalf("parent TMPDIR changed to %q", got)
	}
}

func TestRunYTDLPCommandOutputPreservesStdoutOnly(t *testing.T) {
	for _, gate := range []string{"0", "1"} {
		t.Run(gate, func(t *testing.T) {
			root := t.TempDir()
			t.Setenv(YTDLPPrivateTempEnv, gate)
			t.Setenv(YTDLPRuntimeTempRootEnv, root)
			t.Setenv("STOARAMA_YTDLP_COMMAND_HELPER", "split")
			output, err := RunYTDLPCommandOutput(context.Background(), os.Args[0], "-test.run=TestYTDLPCommandHelperProcess")
			if err != nil {
				t.Fatal(err)
			}
			if got := string(output); got != "stdout\n" {
				t.Fatalf("output=%q", got)
			}
		})
	}
}

func TestRunYTDLPCommandConcurrentInvocationsAreIsolated(t *testing.T) {
	root := t.TempDir()
	t.Setenv(YTDLPPrivateTempEnv, "1")
	t.Setenv(YTDLPRuntimeTempRootEnv, root)
	t.Setenv("STOARAMA_YTDLP_COMMAND_HELPER", "success")
	const count = 8
	paths := make(chan string, count)
	errs := make(chan error, count)
	var group sync.WaitGroup
	for i := 0; i < count; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			output, err := RunYTDLPCommand(context.Background(), os.Args[0], "-test.run=TestYTDLPCommandHelperProcess")
			if err != nil {
				errs <- err
				return
			}
			paths <- strings.TrimSpace(string(output))
		}()
	}
	group.Wait()
	close(paths)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := map[string]struct{}{}
	for path := range paths {
		seen[path] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("unique private dirs=%d want %d: %#v", len(seen), count, seen)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("concurrent cleanup left entries: %v", entries)
	}
}

func TestYTDLPInvocationCleanupFailsClosedOnSymlinkAndInodeSwap(t *testing.T) {
	for _, test := range []string{"symlink", "inode"} {
		t.Run(test, func(t *testing.T) {
			root := t.TempDir()
			invocation, err := newYTDLPInvocationTemp(root)
			if err != nil {
				t.Fatal(err)
			}
			original := invocation.path + "-original"
			if err := os.Rename(invocation.path, original); err != nil {
				t.Fatal(err)
			}
			switch test {
			case "symlink":
				if err := os.Symlink(original, invocation.path); err != nil {
					t.Fatal(err)
				}
			case "inode":
				if err := os.Mkdir(invocation.path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := invocation.remove(); err == nil {
				t.Fatal("changed invocation directory was removed")
			}
			for _, path := range []string{original, invocation.path} {
				if _, err := os.Lstat(path); err != nil {
					t.Fatalf("%s was not preserved: %v", path, err)
				}
			}
		})
	}
}

func TestResolveYouTubeStreamURLCleansPrivateTemp(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(t.TempDir(), "yt-dlp")
	contents := "#!/bin/sh\nprintf temporary > \"$TMPDIR/marker\"\nprintf 'https://example.com/live.m3u8\\n'\n"
	if err := os.WriteFile(script, []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("YT_DLP_BIN", script)
	t.Setenv(YTDLPPrivateTempEnv, "1")
	t.Setenv(YTDLPRuntimeTempRootEnv, root)

	got, err := resolveYouTubeStreamURL(context.Background(), "https://www.youtube.com/watch?v=test")
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://example.com/live.m3u8" {
		t.Fatalf("url=%q", got)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("normal resolution left private temp: %v", entries)
	}
}

func TestRunYTDLPCommandRefusesSymlinkTempRoot(t *testing.T) {
	realRoot := t.TempDir()
	link := filepath.Join(t.TempDir(), "root")
	if err := os.Symlink(realRoot, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv(YTDLPRuntimeTempRootEnv, link)
	t.Setenv(YTDLPPrivateTempEnv, "1")
	if _, err := RunYTDLPCommand(context.Background(), os.Args[0], "-test.run=TestYTDLPCommandHelperProcess"); err == nil {
		t.Fatal("symlink temp root accepted")
	}
}

func TestYTDLPCommandHelperProcess(t *testing.T) {
	behavior := os.Getenv("STOARAMA_YTDLP_COMMAND_HELPER")
	if behavior == "" {
		return
	}
	temp := os.Getenv("TMPDIR")
	if behavior == "report" {
		fmt.Println(temp)
		os.Exit(0)
	}
	if behavior == "split" {
		fmt.Println("stdout")
		fmt.Fprintln(os.Stderr, "stderr")
		os.Exit(0)
	}
	root := os.Getenv(YTDLPRuntimeTempRootEnv)
	realRoot, rootErr := filepath.EvalSymlinks(root)
	realTemp, tempErr := filepath.EvalSymlinks(temp)
	relative, err := filepath.Rel(realRoot, realTemp)
	if rootErr != nil || tempErr != nil || err != nil || filepath.Dir(relative) != "." || !strings.HasPrefix(relative, "stoarama-ytdlp-") {
		os.Exit(91)
	}
	if err := os.WriteFile(filepath.Join(temp, "marker"), []byte("temporary"), 0o600); err != nil {
		os.Exit(92)
	}
	if info, err := os.Stat(temp); err != nil || info.Mode().Perm() != 0o700 {
		os.Exit(90)
	}
	fmt.Println(temp)
	switch behavior {
	case "success":
		os.Exit(0)
	case "error":
		os.Exit(7)
	case "timeout":
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "stubborn":
		child := exec.Command("/bin/sh", "-c", `trap '' TERM; sleep 30`)
		if err := child.Start(); err != nil {
			os.Exit(94)
		}
		fmt.Println(child.Process.Pid)
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "swap":
		saved := temp + "-saved"
		if err := os.Rename(temp, saved); err != nil {
			os.Exit(95)
		}
		if err := os.Symlink(saved, temp); err != nil {
			os.Exit(96)
		}
		os.Exit(0)
	default:
		os.Exit(93)
	}
}

func waitForProcessExit(pidText string) error {
	pid, err := strconv.Atoi(strings.TrimSpace(pidText))
	if err != nil {
		return err
	}
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("process %d remains", pid)
}
