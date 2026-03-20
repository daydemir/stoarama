UPDATE streams
SET
  recording_state = 'off',
  recording_failed_reason = NULL,
  recording_failed_at = NULL,
  updated_at = now()
WHERE recording_state::text IN ('pending', 'failed');

UPDATE recording_state_events
SET prev_state = 'off'
WHERE prev_state::text IN ('pending', 'failed');

UPDATE recording_state_events
SET next_state = 'off'
WHERE next_state::text IN ('pending', 'failed');

DROP VIEW IF EXISTS v_stream_overview;
DROP TRIGGER IF EXISTS trg_streams_recording_state_events ON streams;

CREATE TYPE recording_state_enum_v2 AS ENUM ('off', 'on');

ALTER TABLE streams
  ALTER COLUMN recording_state DROP DEFAULT;

ALTER TABLE recording_state_events
  ALTER COLUMN prev_state TYPE recording_state_enum_v2
  USING (
    CASE
      WHEN prev_state IS NULL THEN NULL
      WHEN prev_state::text = 'on' THEN 'on'::recording_state_enum_v2
      ELSE 'off'::recording_state_enum_v2
    END
  );

ALTER TABLE recording_state_events
  ALTER COLUMN next_state TYPE recording_state_enum_v2
  USING (
    CASE
      WHEN next_state::text = 'on' THEN 'on'::recording_state_enum_v2
      ELSE 'off'::recording_state_enum_v2
    END
  );

ALTER TABLE streams
  ALTER COLUMN recording_state TYPE recording_state_enum_v2
  USING (
    CASE
      WHEN recording_state::text = 'on' THEN 'on'::recording_state_enum_v2
      ELSE 'off'::recording_state_enum_v2
    END
  );

DROP TYPE recording_state_enum;
ALTER TYPE recording_state_enum_v2 RENAME TO recording_state_enum;

ALTER TABLE streams
  ALTER COLUMN recording_state SET DEFAULT 'off'::recording_state_enum;

CREATE TRIGGER trg_streams_recording_state_events
AFTER UPDATE OF recording_state, recording_failed_reason, recording_failed_at ON streams
FOR EACH ROW
EXECUTE FUNCTION log_recording_state_change();

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
