//go:build !darwin && !linux

package capture

import "os/exec"

func configureYTDLPProcessGroup(cmd *exec.Cmd) {}

func stopYTDLPProcessGroup(cmd *exec.Cmd) error { return nil }
