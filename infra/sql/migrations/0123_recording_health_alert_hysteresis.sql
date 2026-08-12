BEGIN;

-- Detection remains immediate in the operator ledger, but ordinary continuous
-- no-progress email waits for a sustained episode. Existing rows start a new
-- observation interval on deploy so migration cannot trigger an email burst.
ALTER TABLE recorder_health_alerts
  ADD COLUMN IF NOT EXISTS episode_started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS last_detected_at TIMESTAMPTZ;

COMMIT;
