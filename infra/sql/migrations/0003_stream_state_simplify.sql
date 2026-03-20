BEGIN;

DROP VIEW IF EXISTS v_stream_overview;

CREATE VIEW v_stream_overview AS
SELECT
  s.id,
  s.provider,
  s.external_id,
  s.name,
  s.slug,
  s.recording_enabled,
  s.excluded_flag,
  s.capture_interval_sec,
  s.priority,
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

ALTER TABLE streams DROP COLUMN IF EXISTS capture_disabled_reason;
ALTER TABLE streams DROP COLUMN IF EXISTS enabled;
ALTER TABLE streams DROP COLUMN IF EXISTS shortlist_flag;

COMMIT;
