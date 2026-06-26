BEGIN;

-- Pause = not billed. The billing gate now considers ONLY active recordings:
-- a paused recording is neither billed nor captured. Both the seat-rank
-- (row_number) and the billable predicate count active recordings only, so the
-- Stripe quantity (countLiveRecordings, also active-only) and this view agree.
--   A recording is billable when:
--     it is one of the account's first paid_quantity ACTIVE recordings by creation order
--     AND the subscription grants access (active/trialing, or past_due still inside the period).
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
WHERE r.status = 'active';

COMMIT;
