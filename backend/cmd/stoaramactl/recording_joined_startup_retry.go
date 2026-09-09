package main

import (
	"context"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func checkJoinedWorkerRemoteStartupWithRetry(ctx context.Context, cfg config.Config, idlePoll time.Duration, check func(context.Context) error) error {
	scope, err := cfg.JoinedWorkScope()
	if err != nil {
		return err
	}
	if scope != config.JoinedWorkScopeFrozenBatch {
		return check(ctx)
	}
	if idlePoll <= 0 {
		idlePoll = joinedWorkerIdlePoll
	}
	var retries joinedAdmissionRetryState
	for {
		err = check(ctx)
		if err == nil {
			return nil
		}
		delay, ok := retries.next(err, idlePoll, time.Now())
		if !ok {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}
