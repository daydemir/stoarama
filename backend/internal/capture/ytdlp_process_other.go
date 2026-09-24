//go:build !darwin && !linux

package capture

import (
	"os/exec"
	"time"
)

func configureYTDLPProcessGroup(cmd *exec.Cmd, grace time.Duration) { cmd.WaitDelay = grace }

func killYTDLPProcessGroup(cmd *exec.Cmd) {}

func waitForYTDLPProcessGroupExit(cmd *exec.Cmd) error { return nil }
