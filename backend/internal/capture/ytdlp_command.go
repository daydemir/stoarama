package capture

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

const (
	YTDLPPrivateTempEnv     = "STOARAMA_YTDLP_PRIVATE_TMP"
	YTDLPRuntimeTempRootEnv = "STOARAMA_YTDLP_RUNTIME_TMP_ROOT"
)

type ytdlpInvocationTemp struct {
	rootDevice uint64
	rootInode  uint64
	device     uint64
	inode      uint64
	root       string
	path       string
}

// RunYTDLPCommand gives each opted-in relay invocation its own private temp
// directory and removes that exact directory only after the child is reaped.
// Cleanup failure is logged but never replaces the resolver's output or error.
func RunYTDLPCommand(ctx context.Context, bin string, args ...string) ([]byte, error) {
	return runYTDLPCommand(ctx, true, bin, args...)
}

// RunYTDLPCommandOutput is the stdout-only form used where the prior yt-dlp
// call deliberately ignored stderr.
func RunYTDLPCommandOutput(ctx context.Context, bin string, args ...string) ([]byte, error) {
	return runYTDLPCommand(ctx, false, bin, args...)
}

func runYTDLPCommand(ctx context.Context, combined bool, bin string, args ...string) ([]byte, error) {
	if os.Getenv(YTDLPPrivateTempEnv) != "1" {
		return commandOutput(exec.CommandContext(ctx, bin, args...), combined)
	}
	invocation, err := newYTDLPInvocationTemp(os.Getenv(YTDLPRuntimeTempRootEnv))
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = replaceCommandEnvironment(os.Environ(), "TMPDIR", invocation.path)
	configureYTDLPProcessGroup(cmd)
	output, runErr := commandOutput(cmd, combined)
	if cleanupErr := invocation.remove(); cleanupErr != nil {
		log.Printf("yt-dlp private temp cleanup failed; owned invocation retained")
	}
	return output, runErr
}

func commandOutput(cmd *exec.Cmd, combined bool) ([]byte, error) {
	if combined {
		return cmd.CombinedOutput()
	}
	return cmd.Output()
}

func newYTDLPInvocationTemp(root string) (ytdlpInvocationTemp, error) {
	root, rootStat, err := ownedRealDirectory(root)
	if err != nil {
		return ytdlpInvocationTemp{}, fmt.Errorf("unsafe yt-dlp temp root: %w", err)
	}
	path, err := os.MkdirTemp(root, "stoarama-ytdlp-")
	if err != nil {
		return ytdlpInvocationTemp{}, fmt.Errorf("create private yt-dlp temp: %w", err)
	}
	path, pathStat, err := ownedRealDirectory(path)
	if err != nil {
		return ytdlpInvocationTemp{}, fmt.Errorf("verify private yt-dlp temp: %w", err)
	}
	return ytdlpInvocationTemp{
		rootDevice: uint64(rootStat.Dev),
		rootInode:  rootStat.Ino,
		device:     uint64(pathStat.Dev),
		inode:      pathStat.Ino,
		root:       root,
		path:       path,
	}, nil
}

func (invocation ytdlpInvocationTemp) remove() error {
	root, rootStat, err := ownedRealDirectory(invocation.root)
	if err != nil || root != invocation.root || uint64(rootStat.Dev) != invocation.rootDevice || rootStat.Ino != invocation.rootInode {
		return errors.New("private temp parent changed; retained for audit")
	}
	relative, err := filepath.Rel(root, invocation.path)
	if err != nil || filepath.Dir(relative) != "." || !strings.HasPrefix(relative, "stoarama-ytdlp-") {
		return errors.New("private temp path escaped parent; retained for audit")
	}
	path, pathStat, err := ownedRealDirectory(invocation.path)
	if err != nil || path != invocation.path || uint64(pathStat.Dev) != invocation.device || pathStat.Ino != invocation.inode {
		return errors.New("private temp directory changed; retained for audit")
	}
	if err := os.RemoveAll(invocation.path); err != nil {
		return fmt.Errorf("remove owned private temp: %w", err)
	}
	return nil
}

func ownedRealDirectory(path string) (string, *syscall.Stat_t, error) {
	if !filepath.IsAbs(path) {
		return "", nil, errors.New("path is not absolute")
	}
	clean := filepath.Clean(path)
	info, err := os.Lstat(clean)
	if err != nil {
		return "", nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !ok || stat.Uid != uint32(os.Getuid()) {
		return "", nil, errors.New("path is not a real directory owned by the relay user")
	}
	return clean, stat, nil
}

func replaceCommandEnvironment(environment []string, key, value string) []string {
	prefix := key + "="
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return append(result, prefix+value)
}
