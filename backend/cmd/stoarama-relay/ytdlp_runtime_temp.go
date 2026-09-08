package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

func privateYTDLPTempEnabled() bool {
	return os.Getenv(capture.YTDLPPrivateTempEnv) == "1"
}

func prepareYTDLPPrivateTemp(command string) error {
	if !privateYTDLPTempEnabled() {
		return nil
	}
	switch command {
	case "run", "link-youtube", "canary":
	default:
		return nil
	}
	home, err := stoaramaHome()
	if err != nil {
		return err
	}
	root := filepath.Join(home, "tmp", "yt-dlp-runtime")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create private yt-dlp temp root: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("inspect private yt-dlp temp root: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !ok || stat.Uid != uint32(os.Getuid()) {
		return errors.New("private yt-dlp temp root is not a real directory owned by the relay user")
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return fmt.Errorf("secure private yt-dlp temp root: %w", err)
	}
	return os.Setenv(capture.YTDLPRuntimeTempRootEnv, root)
}
