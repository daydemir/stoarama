BEGIN;

CREATE TABLE IF NOT EXISTS recording_settings (
  id BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
  capture_interval_sec INTEGER NOT NULL CHECK (capture_interval_sec > 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO recording_settings (id, capture_interval_sec, updated_at)
VALUES (true, 1, now())
ON CONFLICT (id) DO NOTHING;

COMMIT;
