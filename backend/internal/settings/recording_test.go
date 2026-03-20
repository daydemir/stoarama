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
		row: fakeRow{values: []any{2, now}},
	})
	if err != nil {
		t.Fatalf("GetRecordingSettings error: %v", err)
	}
	if got.CaptureIntervalSec != 2 {
		t.Fatalf("interval=%d want=2", got.CaptureIntervalSec)
	}
	if !got.UpdatedAt.Equal(now) {
		t.Fatalf("updated_at mismatch")
	}
}

func TestGetRecordingSettingsRejectsInvalidInterval(t *testing.T) {
	_, err := GetRecordingSettings(context.Background(), fakeQueryRower{
		row: fakeRow{values: []any{0, time.Now().UTC()}},
	})
	if err == nil || !strings.Contains(err.Error(), "invalid recording interval") {
		t.Fatalf("expected invalid interval error, got: %v", err)
	}
}

func TestSetRecordingIntervalSecValidatesInput(t *testing.T) {
	_, err := SetRecordingIntervalSec(context.Background(), fakeQueryRower{}, 0)
	if err == nil || !strings.Contains(err.Error(), "interval_sec must be > 0") {
		t.Fatalf("expected validation error, got: %v", err)
	}
}
