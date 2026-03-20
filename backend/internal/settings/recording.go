package settings

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const DefaultRecordingIntervalSec = 1

type RecordingSettings struct {
	CaptureIntervalSec int
	UpdatedAt          time.Time
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func GetRecordingSettings(ctx context.Context, q queryRower) (RecordingSettings, error) {
	if q == nil {
		return RecordingSettings{}, fmt.Errorf("recording settings query target is nil")
	}
	var out RecordingSettings
	if err := q.QueryRow(ctx, `
		SELECT capture_interval_sec, updated_at
		FROM recording_settings
		WHERE id=true
	`).Scan(&out.CaptureIntervalSec, &out.UpdatedAt); err != nil {
		return RecordingSettings{}, fmt.Errorf("load recording settings: %w", err)
	}
	if out.CaptureIntervalSec <= 0 {
		return RecordingSettings{}, fmt.Errorf("invalid recording interval in DB: %d", out.CaptureIntervalSec)
	}
	return out, nil
}

func SetRecordingIntervalSec(ctx context.Context, q queryRower, intervalSec int) (RecordingSettings, error) {
	if q == nil {
		return RecordingSettings{}, fmt.Errorf("recording settings query target is nil")
	}
	if intervalSec <= 0 {
		return RecordingSettings{}, fmt.Errorf("interval_sec must be > 0")
	}
	var out RecordingSettings
	if err := q.QueryRow(ctx, `
		INSERT INTO recording_settings (id, capture_interval_sec, updated_at)
		VALUES (true, $1, now())
		ON CONFLICT (id)
		DO UPDATE SET capture_interval_sec=EXCLUDED.capture_interval_sec, updated_at=now()
		RETURNING capture_interval_sec, updated_at
	`, intervalSec).Scan(&out.CaptureIntervalSec, &out.UpdatedAt); err != nil {
		return RecordingSettings{}, fmt.Errorf("upsert recording settings: %w", err)
	}
	if out.CaptureIntervalSec <= 0 {
		return RecordingSettings{}, fmt.Errorf("invalid recording interval after update: %d", out.CaptureIntervalSec)
	}
	return out, nil
}
