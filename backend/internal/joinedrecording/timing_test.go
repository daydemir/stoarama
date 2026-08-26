package joinedrecording

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestStageTimingObserverReceivesSafeDeterministicFields(t *testing.T) {
	var got []StageTimingEvent
	ctx := WithStageTimingObserver(context.Background(), func(event StageTimingEvent) { got = append(got, event) })
	emitStageTiming(ctx, "download", 1234*time.Millisecond, nil)
	emitStageTiming(ctx, "upload_verify", 7*time.Millisecond, errors.New("signed URL must not escape"))
	want := []StageTimingEvent{
		{Stage: "download", ElapsedMS: 1234, Outcome: "ok"},
		{Stage: "upload_verify", ElapsedMS: 7, Outcome: "error"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events=%+v want=%+v", got, want)
	}
}

func TestNilStageTimingObserverLeavesContextUsable(t *testing.T) {
	ctx := WithStageTimingObserver(context.Background(), nil)
	emitStageTiming(ctx, "download", time.Second, nil)
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
}
