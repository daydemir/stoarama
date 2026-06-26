BEGIN;

-- Usage-billing pivot: replace the per-seat model (Stripe quantity = active-recording
-- count) with metered recording-days. Drop the seat-rank gate view; it has no role in
-- usage billing. Replaced below by recording_billing_days.
DROP VIEW IF EXISTS recording_billing_state;

-- Card-on-file + metering cursor. Card capture happens via Checkout (mode=subscription,
-- payment_method_collection=always); starting a recording does not charge.
ALTER TABLE account_billing ADD COLUMN has_payment_method BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE account_billing ADD COLUMN stripe_default_payment_method_id TEXT;
ALTER TABLE account_billing ADD COLUMN last_metered_period_end DATE;  -- metering-job idempotency guard

-- Retire the seat/subscription-quantity model. KEEP stripe_customer_id and
-- stripe_subscription_id; KEEP last_payment_failed_at, last_event_at, created_at, updated_at.
ALTER TABLE account_billing DROP COLUMN subscription_status;
ALTER TABLE account_billing DROP COLUMN paid_quantity;
ALTER TABLE account_billing DROP COLUMN current_period_end;
ALTER TABLE account_billing DROP COLUMN cancel_at_period_end;
ALTER TABLE account_billing DROP COLUMN stripe_subscription_item_id;

-- Billable recording-days: DISTINCT (recording, UTC day) with at least one successful
-- clip, inside the recording's [start_at, end_at) window. A recording_clips row exists
-- only after a verified upload, so row-presence IS "successful capture" (there is no
-- capture_status column). The clip's day column is clip_start_at.
CREATE VIEW recording_billing_days AS
SELECT DISTINCT
  c.recording_id,
  r.account_id,
  date(c.clip_start_at AT TIME ZONE 'UTC') AS rec_day
FROM recording_clips c
JOIN recordings r ON r.id = c.recording_id
WHERE c.clip_start_at >= r.start_at
  AND c.clip_start_at <  COALESCE(r.end_at, 'infinity'::timestamptz);

COMMIT;
