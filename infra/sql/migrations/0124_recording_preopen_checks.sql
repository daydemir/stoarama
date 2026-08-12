BEGIN;
CREATE TABLE IF NOT EXISTS recording_preopen_checks (
  recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
  window_start_at TIMESTAMPTZ NOT NULL,
  stage TEXT NOT NULL CHECK (stage IN ('early','confirm')),
  checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  result TEXT NOT NULL CHECK (result IN ('pass','fail','unknown')),
  method TEXT NOT NULL CHECK (method IN ('media_probe','catalog_frame')),
  detail TEXT NOT NULL DEFAULT '' CHECK (length(detail)<=500),
  attempt_count INTEGER NOT NULL DEFAULT 1 CHECK (attempt_count BETWEEN 1 AND 3),
  next_retry_at TIMESTAMPTZ,
  PRIMARY KEY(recording_id,window_start_at,stage)
);
CREATE INDEX IF NOT EXISTS recording_preopen_checks_due_idx
  ON recording_preopen_checks(window_start_at,result,next_retry_at);
COMMIT;
