BEGIN;

-- Phase 4 yearly-prepaid retention. A recording's storage_retention_tier decides
-- HOW its managed-storage footage is billed:
--
--   'monthly'        (default) the existing metered model: the stream_hour_month
--                    meter bills the average stored stream-hours in arrears each
--                    Stripe period. Every pre-existing recording is 'monthly' so
--                    this migration changes no existing billing.
--   'yearly_prepaid' the account prepays 12 months of storage for that footage up
--                    front (a standalone invoice), and on invoice.paid a Stripe
--                    billing credit grant (scoped to the storage price only, +12mo
--                    expiry) nets the monthly stream_hour_month line to $0. The
--                    stream_hour_month meter is still reported unchanged; the credit
--                    is what makes the net monthly charge zero for prepaid footage.
--
-- yearly_prepaid is only selectable when the recording's capture destination is the
-- account's managed destination AND the org has a card on file (enforced in the API,
-- not the DB, because managed-ness is a storage_destinations property).
ALTER TABLE recordings
  ADD COLUMN storage_retention_tier TEXT NOT NULL DEFAULT 'monthly'
    CHECK (storage_retention_tier IN ('monthly', 'yearly_prepaid'));

-- prepaid_storage_batches: the ledger of prepay charges + their resulting credit
-- grants, one row per (account, month) monthly-aggregated prepay pass or per
-- retroactive monthly->yearly switch. batch_key is the idempotency anchor shared by
-- the DB UNIQUE constraint AND the Stripe idempotency key on both the invoice-item
-- and the invoice, so a re-run of the monthly pass (or a webhook redelivery) can
-- never double-charge. Lifecycle: pending -> charged (invoice created) -> granted
-- (invoice.paid webhook created the credit grant). 'failed' records a charge that
-- errored so the next pass does not silently re-attempt under the same key.
--
--   stream_hours   the aggregated yearly-tier footage (stream-hours) this batch
--                  prepaid, from recording_clips (purged_at IS NULL AND
--                  released_at IS NULL), computed directly (account_storage_snapshots
--                  is account-aggregate and has no recording_id).
--   charged_cents  round(stream_hours * 12 * 5): 12 months x $0.05 per stream-hour-month.
--   recording_id   set for a retroactive per-recording switch; NULL for the
--                  monthly per-account aggregate pass (which spans all the account's
--                  yearly recordings).
--   expires_at     the +12mo credit-grant expiry, stamped when the grant is created.
CREATE TABLE prepaid_storage_batches (
  id                     BIGSERIAL PRIMARY KEY,
  batch_key              TEXT NOT NULL UNIQUE,
  account_id             BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  recording_id           BIGINT REFERENCES recordings(id) ON DELETE SET NULL,
  stream_hours           DOUBLE PRECISION NOT NULL,
  charged_cents          BIGINT NOT NULL,
  stripe_invoice_id      TEXT,
  stripe_invoice_item_id TEXT,
  stripe_credit_grant_id TEXT,
  status                 TEXT NOT NULL DEFAULT 'pending'
                           CHECK (status IN ('pending', 'charged', 'granted', 'failed')),
  charged_at             TIMESTAMPTZ,
  granted_at             TIMESTAMPTZ,
  expires_at             TIMESTAMPTZ,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_prepaid_storage_batches_updated_at ON prepaid_storage_batches;
CREATE TRIGGER trg_prepaid_storage_batches_updated_at BEFORE UPDATE ON prepaid_storage_batches
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- The invoice.paid webhook resolves a paid invoice to its ledger row by
-- stripe_invoice_id; the metering pass scans by status. Both are hot lookups.
CREATE INDEX idx_prepaid_storage_batches_status ON prepaid_storage_batches (status);
CREATE INDEX idx_prepaid_storage_batches_invoice ON prepaid_storage_batches (stripe_invoice_id);

COMMIT;
