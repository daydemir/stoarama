BEGIN;

-- A failed or ambiguous Resend call must stay retryable without making the
-- five-minute live-health cadence send the same email on every run. Successful
-- acknowledgement remains separate in last_alerted_at.
ALTER TABLE recorder_health_alerts
  ADD COLUMN IF NOT EXISTS last_delivery_attempt_at TIMESTAMPTZ;

COMMIT;
