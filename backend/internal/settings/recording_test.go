package settings

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i := range dest {
		switch d := dest[i].(type) {
		case *int:
			*d = r.values[i].(int)
		case *time.Time:
			*d = r.values[i].(time.Time)
		default:
			panic("unsupported scan destination")
		}
	}
	return nil
}

type fakeQueryRower struct {
	row fakeRow
}

func (f fakeQueryRower) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return f.row
}

func TestGetRecordingSettings(t *testing.T) {
	now := time.Now().UTC()
	got, err := GetRecordingSettings(context.Background(), fakeQueryRower{
		row: fakeRow{values: []any{30, 240, 480, 300, now}},
	})
	if err != nil {
		t.Fatalf("GetRecordingSettings error: %v", err)
	}
	if got.ClipDurationSec != 30 || got.SampleIntervalMinSec != 240 || got.SampleIntervalMaxSec != 480 || got.StaleGraceSec != 300 {
		t.Fatalf("settings=%+v", got)
	}
	if !got.UpdatedAt.Equal(now) {
		t.Fatalf("updated_at mismatch")
	}
}

func TestGetRecordingSettingsRejectsInvalidClipDuration(t *testing.T) {
	_, err := GetRecordingSettings(context.Background(), fakeQueryRower{
		row: fakeRow{values: []any{60, 240, 480, 300, time.Now().UTC()}},
	})
	if err == nil || !strings.Contains(err.Error(), "invalid clip duration") {
		t.Fatalf("expected invalid clip duration error, got: %v", err)
	}
}

func TestGetRecordingSettingsAllowsNinetySecondClipDuration(t *testing.T) {
	now := time.Now().UTC()
	got, err := GetRecordingSettings(context.Background(), fakeQueryRower{
		row: fakeRow{values: []any{90, 240, 480, 300, now}},
	})
	if err != nil {
		t.Fatalf("GetRecordingSettings error: %v", err)
	}
	if got.ClipDurationSec != 90 {
		t.Fatalf("clip_duration_sec=%d want=90", got.ClipDurationSec)
	}
}

func TestSetRecordingSamplingPolicyValidatesInput(t *testing.T) {
	_, err := SetRecordingSamplingPolicy(context.Background(), fakeQueryRower{}, RecordingSettings{
		ClipDurationSec:      30,
		SampleIntervalMinSec: 240,
		SampleIntervalMaxSec: 120,
		StaleGraceSec:        300,
	})
	if err == nil || !strings.Contains(err.Error(), "invalid sample interval max") {
		t.Fatalf("expected validation error, got: %v", err)
	}
}

func TestSetRecordingSamplingPolicyRejectsChangedSampleCadence(t *testing.T) {
	_, err := SetRecordingSamplingPolicy(context.Background(), fakeQueryRower{}, RecordingSettings{
		ClipDurationSec:      90,
		SampleIntervalMinSec: 120,
		SampleIntervalMaxSec: 480,
		StaleGraceSec:        300,
	})
	if err == nil || !strings.Contains(err.Error(), "invalid sample interval min") {
		t.Fatalf("expected invalid sample interval min error, got: %v", err)
	}
}

func TestSetRecordingSamplingPolicyRejectsUnsupportedClipDuration(t *testing.T) {
	_, err := SetRecordingSamplingPolicy(context.Background(), fakeQueryRower{}, RecordingSettings{
		ClipDurationSec:      45,
		SampleIntervalMinSec: 240,
		SampleIntervalMaxSec: 480,
		StaleGraceSec:        300,
	})
	if err == nil || !strings.Contains(err.Error(), "invalid clip duration") {
		t.Fatalf("expected invalid clip duration error, got: %v", err)
	}
}
