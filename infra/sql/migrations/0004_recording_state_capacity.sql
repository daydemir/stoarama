BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'recording_state_enum') THEN
    CREATE TYPE recording_state_enum AS ENUM ('off', 'on', 'failed');
  END IF;
END
$$;

ALTER TABLE streams
  ADD COLUMN IF NOT EXISTS recording_state recording_state_enum NOT NULL DEFAULT 'off',
  ADD COLUMN IF NOT EXISTS recording_failed_reason TEXT,
  ADD COLUMN IF NOT EXISTS recording_failed_at TIMESTAMPTZ;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'streams' AND column_name = 'recording_enabled'
  ) THEN
    EXECUTE $sql$
      UPDATE streams
      SET recording_state = CASE
        WHEN recording_enabled THEN 'on'::recording_state_enum
        ELSE 'off'::recording_state_enum
      END
    $sql$;
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS recording_mode_capacity (
  mode TEXT PRIMARY KEY CHECK (mode IN ('youtube_live', 'hls_live', 'image_poll', 'ffmpeg_direct')),
  max_active INTEGER NOT NULL CHECK (max_active > 0),
  enabled BOOLEAN NOT NULL DEFAULT true,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO recording_mode_capacity (mode, max_active, enabled)
VALUES
  ('youtube_live', 8, true),
  ('hls_live', 12, true),
  ('image_poll', 12, true),
  ('ffmpeg_direct', 12, true)
ON CONFLICT (mode) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_streams_recording_state
ON streams(recording_state);

DROP VIEW IF EXISTS v_stream_overview;

CREATE VIEW v_stream_overview AS
SELECT
  s.id,
  s.provider,
  s.external_id,
  s.name,
  s.slug,
  s.recording_state,
  s.capture_mode,
  s.tags,
  lf.frame_id AS latest_frame_id,
  lf.captured_at AS latest_captured_at,
  lf.capture_status AS latest_capture_status,
  lf.capture_error AS latest_capture_error,
  sh.captures_total,
  sh.captures_success,
  sh.captures_error,
  CASE
    WHEN COALESCE(sh.captures_total, 0) = 0 THEN NULL
    ELSE (sh.captures_success::DOUBLE PRECISION / sh.captures_total::DOUBLE PRECISION)
  END AS capture_success_rate
FROM streams s
LEFT JOIN v_stream_latest_frame lf ON lf.stream_id = s.id
LEFT JOIN stream_health sh ON sh.stream_id = s.id;

ALTER TABLE streams DROP COLUMN IF EXISTS recording_enabled;
ALTER TABLE streams DROP COLUMN IF EXISTS capture_interval_sec;
ALTER TABLE streams DROP COLUMN IF EXISTS priority;
ALTER TABLE streams DROP COLUMN IF EXISTS excluded_flag;

COMMIT;
