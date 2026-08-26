//go:build linux || darwin

package joinedrecording

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
)

// boundedMediaProcess owns one media-tool process group. Wait is the only
// process reaper. Cancellation kills the complete owned group immediately so
// a TERM-responsive leader cannot exit while leaving a TERM-ignoring child
// alive with worker pipes or CPU.
type boundedMediaProcess struct {
	cmd *exec.Cmd
	ctx context.Context

	exited    chan struct{}
	watchDone chan struct{}
	waitOnce  sync.Once
	waitErr   error
	reaped    atomic.Bool
	lifecycle sync.Mutex

	mu        sync.Mutex
	cancelErr error
	signalErr error
}

func newBoundedMediaProcess(ctx context.Context, name string, args ...string) *boundedMediaProcess {
	if ctx == nil {
		ctx = context.Background()
	}
	cmd := exec.Command(name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return &boundedMediaProcess{cmd: cmd, ctx: ctx}
}

func (p *boundedMediaProcess) Start() error {
	if p == nil || p.cmd == nil {
		return errors.New("bounded media process configuration is required")
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	if err := p.cmd.Start(); err != nil {
		return err
	}
	p.exited = make(chan struct{})
	p.watchDone = make(chan struct{})
	go p.watchCancellation()
	return nil
}

func (p *boundedMediaProcess) Wait() error {
	if p == nil || p.cmd == nil || p.exited == nil || p.watchDone == nil {
		return errors.New("bounded media process was not started")
	}
	p.waitOnce.Do(func() {
		commandErr := p.cmd.Wait()
		p.lifecycle.Lock()
		p.reaped.Store(true)
		close(p.exited)
		p.lifecycle.Unlock()
		<-p.watchDone

		p.mu.Lock()
		cancelErr, signalErr := p.cancelErr, p.signalErr
		p.mu.Unlock()
		if cancelErr != nil {
			p.waitErr = errors.Join(fmt.Errorf("bounded media process canceled: %w", cancelErr), signalErr)
			return
		}
		p.waitErr = errors.Join(commandErr, signalErr)
	})
	return p.waitErr
}

func (p *boundedMediaProcess) Kill() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return errors.New("bounded media process was not started")
	}
	return p.signalOwnedProcessGroup(syscall.SIGKILL)
}

func (p *boundedMediaProcess) watchCancellation() {
	defer close(p.watchDone)
	select {
	case <-p.exited:
		return
	case <-p.ctx.Done():
	}

	p.mu.Lock()
	p.cancelErr = p.ctx.Err()
	p.mu.Unlock()
	if err := p.signalOwnedProcessGroup(syscall.SIGKILL); err != nil {
		p.recordSignalError(fmt.Errorf("kill media process group: %w", err))
	}
}

func (p *boundedMediaProcess) signalOwnedProcessGroup(signal syscall.Signal) error {
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	if p.reaped.Load() {
		return nil
	}
	pid := p.cmd.Process.Pid
	if pid <= 0 {
		return errors.New("media process has no process group")
	}
	pgid, err := syscall.Getpgid(pid)
	if errors.Is(err, syscall.ESRCH) || p.reaped.Load() {
		return nil
	}
	if err != nil {
		return err
	}
	if pgid != pid {
		return fmt.Errorf("media process group ownership changed pid=%d pgid=%d", pid, pgid)
	}
	if p.reaped.Load() {
		return nil
	}
	return signalMediaProcessGroup(pid, signal)
}

func (p *boundedMediaProcess) recordSignalError(err error) {
	if err == nil {
		return
	}
	p.mu.Lock()
	p.signalErr = errors.Join(p.signalErr, err)
	p.mu.Unlock()
}

func signalMediaProcessGroup(pid int, signal syscall.Signal) error {
	if pid <= 0 {
		return errors.New("media process has no process group")
	}
	err := syscall.Kill(-pid, signal)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
