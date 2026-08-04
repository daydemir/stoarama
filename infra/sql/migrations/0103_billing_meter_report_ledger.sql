BEGIN;

-- Durable outbox guard for Stripe meter events. Stripe only guarantees meter
-- event identifier deduplication for a rolling window, so the monthly cursor
-- alone cannot safely distinguish "request accepted, DB update failed" from
-- "request never reached Stripe" after a process crash. A pending row is an
-- intentionally fail-closed ambiguous state: it is never sent automatically a
-- second time and must be reconciled against Stripe before being marked reported.
CREATE TABLE billing_meter_reports (
  id             BIGSERIAL PRIMARY KEY,
  account_id     BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  period_end     TIMESTAMPTZ NOT NULL,
  meter_kind     TEXT NOT NULL CHECK (meter_kind IN ('recording_hour','stream_hour_month')),
  expected_value TEXT NOT NULL,
  identifier     TEXT NOT NULL,
  status         TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','reported')),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  reported_at    TIMESTAMPTZ,
  UNIQUE (account_id, period_end, meter_kind),
  UNIQUE (meter_kind, identifier)
);

CREATE INDEX idx_billing_meter_reports_pending
ON billing_meter_reports (created_at)
WHERE status = 'pending';

COMMIT;
