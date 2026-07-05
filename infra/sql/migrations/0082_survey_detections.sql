BEGIN;

-- Survey detection metrics (#47). ONE row per surveyed frame that was sampled for
-- yolo11x detection: per-class COUNTS only (no boxes, no crops, no footage). The
-- table starts EMPTY and is only written by the survey sweep run with --detect on
-- the unified survey+detection droplet, so nothing changes until that runs.
--
-- The migration runner applies each file in ONE transaction (see 0080/0081), so
-- this is plain in-txn DDL: no CREATE INDEX CONCURRENTLY. Additive, AUTO_MIGRATE-safe.
--
-- It does NOT touch detections / inference_results / stream_inference_stats (the
-- external inference_node box path); this is a separate metrics-only lane keyed to
-- the shared survey `frames` row.
CREATE TABLE IF NOT EXISTS survey_detections (
  id               BIGSERIAL PRIMARY KEY,
  frame_id         BIGINT NOT NULL REFERENCES frames(id) ON DELETE CASCADE,
  stream_id        BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  captured_at      TIMESTAMPTZ NOT NULL,
  pipeline_version TEXT NOT NULL,            -- e.g. yolo11x-img1600-conf010-notile-v1
  conf_threshold   DOUBLE PRECISION NOT NULL,
  imgsz            INT NOT NULL,
  person_count     INT NOT NULL DEFAULT 0,
  bicycle_count    INT NOT NULL DEFAULT 0,
  car_count        INT NOT NULL DEFAULT 0,
  motorcycle_count INT NOT NULL DEFAULT 0,
  bus_count        INT NOT NULL DEFAULT 0,
  truck_count      INT NOT NULL DEFAULT 0,
  detect_ms        INT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Re-running the same pipeline over a frame is idempotent; a config bump
  -- (new pipeline_version) is a distinct row.
  UNIQUE (frame_id, pipeline_version)
);

CREATE INDEX IF NOT EXISTS survey_detections_stream_captured_idx
  ON survey_detections (stream_id, captured_at DESC);

COMMIT;
