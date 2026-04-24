UPDATE streams
SET expected_fps = 30,
    execution_config_jsonb = COALESCE(execution_config_jsonb, '{}'::jsonb) - 'target_fps' - 'expected_fps',
    updated_at = now()
WHERE capture_family = 'continuous_video';

UPDATE capture_segments
SET target_fps = 30
WHERE target_fps IS DISTINCT FROM 30;

ALTER TABLE capture_segments
  DROP CONSTRAINT IF EXISTS capture_segments_target_fps_positive,
  DROP CONSTRAINT IF EXISTS capture_segments_target_fps_30;

ALTER TABLE capture_segments
  ADD CONSTRAINT capture_segments_target_fps_30 CHECK (target_fps = 30);

ALTER TABLE streams
  DROP CONSTRAINT IF EXISTS streams_capture_cadence_check,
  DROP CONSTRAINT IF EXISTS streams_capture_family_cadence_check,
  DROP CONSTRAINT IF EXISTS streams_continuous_video_expected_fps_30;

ALTER TABLE streams
  ADD CONSTRAINT streams_continuous_video_expected_fps_30 CHECK (
    (
      capture_family = 'continuous_video'
      AND expected_fps = 30
      AND expected_image_interval_sec IS NULL
    )
    OR (
      capture_family = 'snapshot_image'
      AND expected_image_interval_sec IS NOT NULL
      AND expected_image_interval_sec > 0
      AND expected_fps IS NULL
    )
  );
