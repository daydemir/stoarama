//go:build !linux && !darwin

package joinedrecording

import (
	"context"
	"errors"
	"os/exec"
)

type boundedMediaProcess struct {
	cmd *exec.Cmd
}

func newBoundedMediaProcess(ctx context.Context, name string, args ...string) *boundedMediaProcess {
	if ctx == nil {
		ctx = context.Background()
	}
	return &boundedMediaProcess{cmd: exec.CommandContext(ctx, name, args...)}
}

func (p *boundedMediaProcess) Start() error {
	if p == nil || p.cmd == nil {
		return errors.New("bounded media process configuration is required")
	}
	return p.cmd.Start()
}

func (p *boundedMediaProcess) Wait() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return errors.New("bounded media process was not started")
	}
	return p.cmd.Wait()
}

func (p *boundedMediaProcess) Kill() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return errors.New("bounded media process was not started")
	}
	return p.cmd.Process.Kill()
}
