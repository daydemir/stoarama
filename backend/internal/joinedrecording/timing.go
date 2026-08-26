package joinedrecording

import (
	"context"
	"time"
)

// StageTimingEvent reports one bounded worker stage without exposing storage
// capabilities, object keys, or credentials.
type StageTimingEvent struct {
	Stage     string
	ElapsedMS int64
	Outcome   string
}

type stageTimingObserverKey struct{}

// WithStageTimingObserver installs process-local timing observation. It does
// not change the joined-recording protocol or persist timing data.
func WithStageTimingObserver(ctx context.Context, observer func(StageTimingEvent)) context.Context {
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, stageTimingObserverKey{}, observer)
}

func emitStageTiming(ctx context.Context, stage string, elapsed time.Duration, err error) {
	observer, _ := ctx.Value(stageTimingObserverKey{}).(func(StageTimingEvent))
	if observer == nil {
		return
	}
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	observer(StageTimingEvent{Stage: stage, ElapsedMS: elapsed.Milliseconds(), Outcome: outcome})
}
