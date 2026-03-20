BEGIN;

CREATE TABLE IF NOT EXISTS stream_inference_stats (
  stream_id BIGINT PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
  inferenced_captures BIGINT NOT NULL DEFAULT 0,
  person_detections_total BIGINT NOT NULL DEFAULT 0,
  avg_people_per_inferenced_capture DOUBLE PRECISION NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stream_inference_stats_avg_people_desc
ON stream_inference_stats (avg_people_per_inferenced_capture DESC, stream_id);

CREATE INDEX IF NOT EXISTS idx_streams_tags_gin
ON streams USING GIN (tags);

CREATE INDEX IF NOT EXISTS idx_inference_results_frame_status_active
ON inference_results (frame_id)
WHERE status IN ('success', 'queued_boxed');

CREATE INDEX IF NOT EXISTS idx_detections_person_result
ON detections (inference_result_id)
WHERE class_name = 'person';

CREATE OR REPLACE FUNCTION refresh_stream_inference_stats(p_stream_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  v_inf BIGINT := 0;
  v_people BIGINT := 0;
  v_avg DOUBLE PRECISION := 0;
BEGIN
  IF p_stream_id IS NULL OR p_stream_id <= 0 THEN
    RETURN;
  END IF;

  SELECT COUNT(*)::bigint
  INTO v_inf
  FROM frames f
  JOIN inference_results ir ON ir.frame_id = f.id
  WHERE f.stream_id = p_stream_id
    AND ir.status IN ('success', 'queued_boxed');

  SELECT COUNT(*)::bigint
  INTO v_people
  FROM frames f
  JOIN inference_results ir ON ir.frame_id = f.id
  JOIN detections d ON d.inference_result_id = ir.id
  WHERE f.stream_id = p_stream_id
    AND ir.status IN ('success', 'queued_boxed')
    AND d.class_name = 'person';

  IF v_inf > 0 THEN
    v_avg := v_people::double precision / v_inf::double precision;
  ELSE
    v_avg := 0;
  END IF;

  INSERT INTO stream_inference_stats (
    stream_id,
    inferenced_captures,
    person_detections_total,
    avg_people_per_inferenced_capture,
    updated_at
  )
  VALUES (
    p_stream_id,
    v_inf,
    v_people,
    v_avg,
    now()
  )
  ON CONFLICT (stream_id) DO UPDATE SET
    inferenced_captures = EXCLUDED.inferenced_captures,
    person_detections_total = EXCLUDED.person_detections_total,
    avg_people_per_inferenced_capture = EXCLUDED.avg_people_per_inferenced_capture,
    updated_at = now();
END;
$$;

CREATE OR REPLACE FUNCTION trg_refresh_stream_inference_stats_from_inference_results()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_stream_old BIGINT;
  v_stream_new BIGINT;
BEGIN
  IF TG_OP = 'INSERT' THEN
    SELECT stream_id INTO v_stream_new FROM frames WHERE id = NEW.frame_id;
    PERFORM refresh_stream_inference_stats(v_stream_new);
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    SELECT stream_id INTO v_stream_old FROM frames WHERE id = OLD.frame_id;
    SELECT stream_id INTO v_stream_new FROM frames WHERE id = NEW.frame_id;
    PERFORM refresh_stream_inference_stats(v_stream_old);
    IF v_stream_new IS DISTINCT FROM v_stream_old THEN
      PERFORM refresh_stream_inference_stats(v_stream_new);
    END IF;
    RETURN NEW;
  ELSE
    SELECT stream_id INTO v_stream_old FROM frames WHERE id = OLD.frame_id;
    PERFORM refresh_stream_inference_stats(v_stream_old);
    RETURN OLD;
  END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_refresh_stream_inference_stats_on_inference_results ON inference_results;
CREATE TRIGGER trg_refresh_stream_inference_stats_on_inference_results
AFTER INSERT OR UPDATE OF frame_id, status OR DELETE ON inference_results
FOR EACH ROW
EXECUTE FUNCTION trg_refresh_stream_inference_stats_from_inference_results();

CREATE OR REPLACE FUNCTION trg_refresh_stream_inference_stats_from_detections()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_stream_old BIGINT;
  v_stream_new BIGINT;
BEGIN
  IF TG_OP = 'INSERT' THEN
    SELECT f.stream_id INTO v_stream_new
    FROM inference_results ir
    JOIN frames f ON f.id = ir.frame_id
    WHERE ir.id = NEW.inference_result_id;
    PERFORM refresh_stream_inference_stats(v_stream_new);
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    SELECT f.stream_id INTO v_stream_old
    FROM inference_results ir
    JOIN frames f ON f.id = ir.frame_id
    WHERE ir.id = OLD.inference_result_id;
    SELECT f.stream_id INTO v_stream_new
    FROM inference_results ir
    JOIN frames f ON f.id = ir.frame_id
    WHERE ir.id = NEW.inference_result_id;
    PERFORM refresh_stream_inference_stats(v_stream_old);
    IF v_stream_new IS DISTINCT FROM v_stream_old THEN
      PERFORM refresh_stream_inference_stats(v_stream_new);
    END IF;
    RETURN NEW;
  ELSE
    SELECT f.stream_id INTO v_stream_old
    FROM inference_results ir
    JOIN frames f ON f.id = ir.frame_id
    WHERE ir.id = OLD.inference_result_id;
    PERFORM refresh_stream_inference_stats(v_stream_old);
    RETURN OLD;
  END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_refresh_stream_inference_stats_on_detections ON detections;
CREATE TRIGGER trg_refresh_stream_inference_stats_on_detections
AFTER INSERT OR UPDATE OF inference_result_id, class_name OR DELETE ON detections
FOR EACH ROW
EXECUTE FUNCTION trg_refresh_stream_inference_stats_from_detections();

WITH inf AS (
  SELECT
    f.stream_id,
    COUNT(*)::bigint AS inferenced_captures
  FROM frames f
  JOIN inference_results ir ON ir.frame_id = f.id
  WHERE ir.status IN ('success', 'queued_boxed')
  GROUP BY f.stream_id
),
det AS (
  SELECT
    f.stream_id,
    COUNT(*)::bigint AS person_detections_total
  FROM frames f
  JOIN inference_results ir ON ir.frame_id = f.id
  JOIN detections d ON d.inference_result_id = ir.id
  WHERE ir.status IN ('success', 'queued_boxed')
    AND d.class_name = 'person'
  GROUP BY f.stream_id
)
INSERT INTO stream_inference_stats (
  stream_id,
  inferenced_captures,
  person_detections_total,
  avg_people_per_inferenced_capture,
  updated_at
)
SELECT
  s.id AS stream_id,
  COALESCE(inf.inferenced_captures, 0) AS inferenced_captures,
  COALESCE(det.person_detections_total, 0) AS person_detections_total,
  CASE
    WHEN COALESCE(inf.inferenced_captures, 0) > 0
      THEN COALESCE(det.person_detections_total, 0)::double precision / inf.inferenced_captures::double precision
    ELSE 0::double precision
  END AS avg_people_per_inferenced_capture,
  now() AS updated_at
FROM streams s
LEFT JOIN inf ON inf.stream_id = s.id
LEFT JOIN det ON det.stream_id = s.id
ON CONFLICT (stream_id) DO UPDATE SET
  inferenced_captures = EXCLUDED.inferenced_captures,
  person_detections_total = EXCLUDED.person_detections_total,
  avg_people_per_inferenced_capture = EXCLUDED.avg_people_per_inferenced_capture,
  updated_at = now();

COMMIT;
