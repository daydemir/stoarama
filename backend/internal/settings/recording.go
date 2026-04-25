package settings

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	DefaultRecordingIntervalSec  = 1
	DefaultClipDurationSec       = 30
	DefaultSampleIntervalMinSec  = 4 * 60
	DefaultSampleIntervalMaxSec  = 8 * 60
	DefaultSampleStaleGraceSec   = 5 * 60
	DefaultSampleStaleWindowSec  = DefaultSampleIntervalMaxSec + DefaultSampleStaleGraceSec
	DefaultSampleExpectedPerHour = 3600 / DefaultSampleIntervalMaxSec
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
	if s.ClipDurationSec != DefaultClipDurationSec {
		return fmt.Errorf("invalid clip duration in DB: %d", s.ClipDurationSec)
	}
	if s.SampleIntervalMinSec <= 0 {
		return fmt.Errorf("invalid sample interval min in DB: %d", s.SampleIntervalMinSec)
	}
	if s.SampleIntervalMaxSec < s.SampleIntervalMinSec {
		return fmt.Errorf("invalid sample interval max in DB: %d < %d", s.SampleIntervalMaxSec, s.SampleIntervalMinSec)
	}
	if s.StaleGraceSec < 0 {
		return fmt.Errorf("invalid stale grace in DB: %d", s.StaleGraceSec)
	}
	return nil
}

func SetRecordingSamplingPolicy(ctx context.Context, q queryRower, policy RecordingSettings) (RecordingSettings, error) {
	if q == nil {
		return RecordingSettings{}, fmt.Errorf("recording settings query target is nil")
	}
	if err := policy.Validate(); err != nil {
		return RecordingSettings{}, err
	}
	var out RecordingSettings
	if err := q.QueryRow(ctx, `
		INSERT INTO recording_settings (
			id, capture_interval_sec, clip_duration_sec, sample_interval_min_sec, sample_interval_max_sec, stale_grace_sec, updated_at
		)
		VALUES (true, 1, $1, $2, $3, $4, now())
		ON CONFLICT (id)
		DO UPDATE SET
			capture_interval_sec=1,
			clip_duration_sec=EXCLUDED.clip_duration_sec,
			sample_interval_min_sec=EXCLUDED.sample_interval_min_sec,
			sample_interval_max_sec=EXCLUDED.sample_interval_max_sec,
			stale_grace_sec=EXCLUDED.stale_grace_sec,
			updated_at=now()
		RETURNING clip_duration_sec, sample_interval_min_sec, sample_interval_max_sec, stale_grace_sec, updated_at
	`, policy.ClipDurationSec, policy.SampleIntervalMinSec, policy.SampleIntervalMaxSec, policy.StaleGraceSec).Scan(
		&out.ClipDurationSec, &out.SampleIntervalMinSec, &out.SampleIntervalMaxSec, &out.StaleGraceSec, &out.UpdatedAt,
	); err != nil {
		return RecordingSettings{}, fmt.Errorf("upsert recording settings: %w", err)
	}
	if err := out.Validate(); err != nil {
		return RecordingSettings{}, err
	}
	return out, nil
}
