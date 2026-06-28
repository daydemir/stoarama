package settings

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	DefaultRecordingIntervalSec = 1
	DefaultClipDurationSec      = 30
	ExtendedClipDurationSec     = 90
	DefaultSampleIntervalMinSec = 4 * 60
	DefaultSampleIntervalMaxSec = 8 * 60
	DefaultSampleStaleGraceSec  = 5 * 60
	DefaultSampleStaleWindowSec = DefaultSampleIntervalMaxSec + DefaultSampleStaleGraceSec
)

type RecordingSettings struct {
	ClipDurationSec      int
	SampleIntervalMinSec int
	SampleIntervalMaxSec int
	StaleGraceSec        int
	UpdatedAt            time.Time
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
		SELECT clip_duration_sec, sample_interval_min_sec, sample_interval_max_sec, stale_grace_sec, updated_at
		FROM recording_settings
		WHERE id=true
	`).Scan(&out.ClipDurationSec, &out.SampleIntervalMinSec, &out.SampleIntervalMaxSec, &out.StaleGraceSec, &out.UpdatedAt); err != nil {
		return RecordingSettings{}, fmt.Errorf("load recording settings: %w", err)
	}
	if err := out.Validate(); err != nil {
		return RecordingSettings{}, err
	}
	return out, nil
}

func (s RecordingSettings) Validate() error {
	if !IsAllowedClipDurationSec(s.ClipDurationSec) {
		return fmt.Errorf("invalid clip duration in DB: %d", s.ClipDurationSec)
	}
	if s.SampleIntervalMinSec != DefaultSampleIntervalMinSec {
		return fmt.Errorf("invalid sample interval min in DB: %d", s.SampleIntervalMinSec)
	}
	if s.SampleIntervalMaxSec != DefaultSampleIntervalMaxSec {
		return fmt.Errorf("invalid sample interval max in DB: %d", s.SampleIntervalMaxSec)
	}
	if s.StaleGraceSec != DefaultSampleStaleGraceSec {
		return fmt.Errorf("invalid stale grace in DB: %d", s.StaleGraceSec)
	}
	return nil
}

func IsAllowedClipDurationSec(v int) bool {
	return v == DefaultClipDurationSec || v == ExtendedClipDurationSec
}
