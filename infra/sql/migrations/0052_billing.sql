BEGIN;

-- One Stripe subscription per account; quantity = absolute live recording count.
-- This is the single source of paid/dunning truth for the recorder feature.
CREATE TABLE IF NOT EXISTS account_billing (
  account_id                   BIGINT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
  stripe_customer_id           TEXT UNIQUE,
  stripe_subscription_id       TEXT UNIQUE,
  stripe_subscription_item_id  TEXT,                            -- the single $8 price line item
  subscription_status          TEXT NOT NULL DEFAULT 'none'
    CHECK (subscription_status IN ('none','incomplete','trialing','active','past_due','canceled','unpaid','incomplete_expired')),
  paid_quantity                INTEGER NOT NULL DEFAULT 0 CHECK (paid_quantity >= 0),
  current_period_end           TIMESTAMPTZ,
  cancel_at_period_end         BOOLEAN NOT NULL DEFAULT false,
  last_payment_failed_at       TIMESTAMPTZ,
  -- event-ordering guard: ignore stale out-of-order webhook events.
  last_event_at                TIMESTAMPTZ,
  created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at                   TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_account_billing_updated_at ON account_billing;
CREATE TRIGGER trg_account_billing_updated_at BEFORE UPDATE ON account_billing
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Webhook dedup ledger: each Stripe event id is processed at most once.
CREATE TABLE IF NOT EXISTS stripe_webhook_events (
  id                BIGSERIAL PRIMARY KEY,
  provider_event_id TEXT NOT NULL,
  event_type        TEXT NOT NULL,
  payload_jsonb     JSONB NOT NULL,
  processed_at      TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_stripe_webhook_events_event_id
  ON stripe_webhook_events (provider_event_id);

-- The single billing gate. A recording is billable when:
--   it is one of the account's first paid_quantity ACTIVE/PAUSED recordings by creation order
--   AND the subscription grants access (active/trialing, or past_due still inside the period).
-- Says NOTHING about capture: the enqueue/lease gate ANDs status='active' separately.
CREATE OR REPLACE VIEW recording_billing_state AS
SELECT
  r.id AS recording_id,
  r.account_id,
  (
    b.subscription_status IN ('active','trialing','past_due')
    AND (b.subscription_status <> 'past_due' OR b.current_period_end > now())
    AND row_number() OVER (
          PARTITION BY r.account_id ORDER BY r.created_at ASC, r.id ASC
        ) <= COALESCE(b.paid_quantity, 0)
  ) AS billable
FROM recordings r
LEFT JOIN account_billing b ON b.account_id = r.account_id
WHERE r.status <> 'canceled';

COMMIT;
