-- Persist Stripe period bounds before they close so recorder-control downtime,
-- deploys, or Stripe read failures around the close cannot make a period vanish
-- when Stripe advances the subscription to its next period.
CREATE TABLE billing_meter_periods (
  account_id      BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  stripe_customer_id TEXT NOT NULL,
  stripe_subscription_id TEXT NOT NULL,
  period_start    TIMESTAMPTZ NOT NULL,
  period_end      TIMESTAMPTZ NOT NULL,
  discovered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  stripe_invoice_id TEXT,
  invoice_verified_at TIMESTAMPTZ,
  recording_amount_cents BIGINT,
  storage_amount_cents BIGINT,
  metered_at      TIMESTAMPTZ,
  PRIMARY KEY(account_id, period_end),
  CONSTRAINT chk_billing_meter_period_bounds CHECK (period_start < period_end)
);

CREATE INDEX idx_billing_meter_periods_due
  ON billing_meter_periods(period_end, account_id)
  WHERE metered_at IS NULL;

-- The timestamp is billing-critical evidence: reports for a closed period use
-- an instant inside that period rather than Stripe's default of "now".
ALTER TABLE billing_meter_reports
  ADD COLUMN event_timestamp TIMESTAMPTZ;

-- Reconstructable, immutable daily storage facts. These are derived from clip
-- lifecycle timestamps after a day closes, so downtime on the snapshot job does
-- not permanently erase billable history.
CREATE TABLE billing_storage_daily_facts (
  account_id          BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  usage_date          DATE NOT NULL,
  stream_hours_stored DOUBLE PRECISION NOT NULL CHECK (stream_hours_stored >= 0),
  computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(account_id, usage_date)
);

CREATE TABLE billing_storage_fact_config (
  singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),
  -- The first complete UTC day after this migration is the earliest day whose
  -- frozen classification is trustworthy for automatic storage billing.
  eligible_from DATE NOT NULL DEFAULT ((now() AT TIME ZONE 'UTC')::date + 1)
);
INSERT INTO billing_storage_fact_config DEFAULT VALUES;

CREATE TABLE clip_storage_billing_contracts (
  clip_id BIGINT PRIMARY KEY REFERENCES recording_clips(id) ON DELETE CASCADE,
  mode TEXT NOT NULL CHECK(mode IN ('excluded','managed_monthly','nas_pull_monthly')),
  authoritative BOOLEAN NOT NULL
);

CREATE FUNCTION freeze_clip_storage_billing() RETURNS trigger AS $$
BEGIN
  INSERT INTO clip_storage_billing_contracts(clip_id,mode,authoritative)
  SELECT NEW.id, CASE
      WHEN sd.managed AND r.storage_retention_tier <> 'yearly_prepaid' AND r.delivery='nas_pull' THEN 'nas_pull_monthly'
      WHEN sd.managed AND r.storage_retention_tier <> 'yearly_prepaid' THEN 'managed_monthly'
      ELSE 'excluded'
    END, true
  FROM recordings r
  LEFT JOIN storage_destinations sd ON sd.id=NEW.storage_destination_id
  WHERE r.id=NEW.recording_id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_recording_clips_freeze_storage_billing
AFTER INSERT ON recording_clips
FOR EACH ROW EXECUTE FUNCTION freeze_clip_storage_billing();
