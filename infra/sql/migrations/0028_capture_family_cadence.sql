ALTER TABLE streams
  ADD COLUMN IF NOT EXISTS capture_family TEXT,
  ADD COLUMN IF NOT EXISTS expected_fps DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS expected_image_interval_sec INTEGER;

UPDATE streams
SET capture_family = CASE
    WHEN capture_type = 'still_image' THEN 'snapshot_image'
    ELSE 'continuous_video'
  END
WHERE capture_family IS NULL;

UPDATE streams
SET expected_fps = CASE
    WHEN capture_family = 'continuous_video' THEN GREATEST(
      1,
      COALESCE(NULLIF(execution_config_jsonb->>'expected_fps', '')::integer, NULLIF(execution_config_jsonb->>'target_fps', '')::integer, 1)
    )::double precision
    ELSE NULL
  END,
  expected_image_interval_sec = CASE
    WHEN capture_family = 'snapshot_image' THEN GREATEST(
      1,
      COALESCE(
        NULLIF(execution_config_jsonb->>'expected_image_interval_sec', '')::integer,
        CASE
          WHEN provider = 'SDOT'
            OR source_url ILIKE '%seattle.gov/trafficcams/images/%'
            OR source_page_url ILIKE '%seattle.gov/trafficcams/%'
            THEN 300
          WHEN COALESCE(NULLIF(execution_config_jsonb->>'poll_interval_sec', '')::integer, 0) > 1
            THEN NULLIF(execution_config_jsonb->>'poll_interval_sec', '')::integer
          ELSE 60
        END
      )
    )
    ELSE NULL
  END;

ALTER TABLE streams
  ALTER COLUMN capture_family SET NOT NULL;

ALTER TABLE streams
  DROP CONSTRAINT IF EXISTS streams_capture_family_check;
ALTER TABLE streams
  ADD CONSTRAINT streams_capture_family_check
  CHECK (capture_family IN ('continuous_video', 'snapshot_image'));

ALTER TABLE streams
  DROP CONSTRAINT IF EXISTS streams_capture_cadence_check;
ALTER TABLE streams
  ADD CONSTRAINT streams_capture_cadence_check
  CHECK (
    (capture_family = 'continuous_video' AND expected_fps IS NOT NULL AND expected_fps > 0 AND expected_image_interval_sec IS NULL)
    OR
    (capture_family = 'snapshot_image' AND expected_image_interval_sec IS NOT NULL AND expected_image_interval_sec > 0 AND expected_fps IS NULL)
  );
