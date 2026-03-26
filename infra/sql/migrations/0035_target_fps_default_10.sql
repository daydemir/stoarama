UPDATE streams
SET
  execution_config_jsonb = jsonb_set(
    COALESCE(execution_config_jsonb, '{}'::jsonb),
    '{target_fps}',
    to_jsonb(10),
    true
  ),
  expected_fps = 10
WHERE capture_family = 'continuous_video'
  AND COALESCE(
    CASE
      WHEN COALESCE(execution_config_jsonb->>'target_fps', '') ~ '^-?[0-9]+$'
        THEN (execution_config_jsonb->>'target_fps')::integer
      ELSE NULL
    END,
    0
  ) <= 1;
