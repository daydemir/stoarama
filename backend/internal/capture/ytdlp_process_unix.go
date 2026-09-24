//go:build darwin || linux

package capture

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// configureYTDLPProcessGroup puts yt-dlp in its own process group and makes
// context cancellation a graceful group SIGTERM. exec escalates to SIGKILL of the
// leader after grace; runYTDLPCommand then kills any surviving group member.
func configureYTDLPProcessGroup(cmd *exec.Cmd, grace time.Duration) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = grace
}

func killYTDLPProcessGroup(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}

func waitForYTDLPProcessGroupExit(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	group := -cmd.Process.Pid
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if err := syscall.Kill(group, 0); errors.Is(err, syscall.ESRCH) {
			return nil
		} else if err != nil {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("yt-dlp process group %d remains", -group)
}
