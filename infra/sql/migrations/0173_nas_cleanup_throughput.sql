-- NAS cleanup throughput (2026-11). Grants no deletion authority; every gate
-- of 0167-0172 is unchanged. Additive only: old binaries ignore the new
-- columns and table.
--
-- 1. Per-item copies of immutable plan facts. New plans derive item/size_bytes
--    from their own stored manifest in the same INSERT (never from client
--    input), so an item row never needs to detoast a multi-MB manifest again.
--    Legacy rows keep NULL and fall back to the manifest. Both columns are
--    write-once: no UPDATE may change them (including NULL -> value).
ALTER TABLE nas_cleanup_items
 ADD COLUMN item jsonb,
 ADD COLUMN size_bytes bigint,
 -- Evidence of the dry run's streamed-SHA R2 verification: the exact object
 -- generation (ETag/version/size) whose bytes hashed to the catalog SHA.
 ADD COLUMN recovery_evidence jsonb,
 -- Expiry of the one deletion permit issued for this item (idempotent replay
 -- of a lost response returns the same permit; it is never extended).
 ADD COLUMN permit_expires_at timestamptz,
 -- Protocol-2 lease request that leased the item: a retried lease request
 -- gets the same still-leased tasks back instead of leasing more work.
 ADD COLUMN lease_request_id text;
-- Checks on the new (all-NULL) columns are added NOT VALID: metadata-only, no
-- table scan under the ACCESS EXCLUSIVE lock, still enforced for every write.
ALTER TABLE nas_cleanup_items
 ADD CONSTRAINT nas_cleanup_items_size_bytes_positive CHECK (size_bytes IS NULL OR size_bytes > 0) NOT VALID,
 ADD CONSTRAINT nas_cleanup_items_lease_request_hex CHECK (lease_request_id IS NULL OR lease_request_id ~ '^[a-f0-9]{32}$') NOT VALID;
CREATE INDEX nas_cleanup_items_lease_request ON nas_cleanup_items(lease_request_id) WHERE state='leased' AND lease_request_id IS NOT NULL;

CREATE OR REPLACE FUNCTION guard_nas_cleanup_item() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='DELETE' OR (OLD.delete_ever_issued AND NOT NEW.delete_ever_issued) OR ROW(NEW.plan_id,NEW.ordinal) IS DISTINCT FROM ROW(OLD.plan_id,OLD.ordinal)
  OR NEW.item IS DISTINCT FROM OLD.item OR NEW.size_bytes IS DISTINCT FROM OLD.size_bytes
  OR (OLD.permit_expires_at IS NOT NULL AND NEW.permit_expires_at IS DISTINCT FROM OLD.permit_expires_at) THEN
  RAISE EXCEPTION 'cleanup item identity is immutable'; END IF;
 RETURN NEW;
END $$;

-- 2. Best-14 replay result computed once when a plan is activated for
--    execution: {"<stream_id>":{"best_start":..,"fingerprint":..,"at":..}}.
--    Per-item checks compare a cheap fingerprint of the replay's exact input
--    rows; any difference falls back to the full live replay.
ALTER TABLE nas_cleanup_plans ADD COLUMN execution_rankings jsonb;

-- 3. Batch approval: the owner approves an ordered list of plans once, bound
--    to a digest over (policy, plan id, plan digest) for every plan. Exactly
--    one plan is ever enabled; the next plan is enabled only after the current
--    one finished cleanly (every item deleted) and only after passing every
--    per-plan enable gate again. Any anomaly pauses the plan and the batch.
CREATE TABLE nas_cleanup_batches (
 id bigserial PRIMARY KEY,
 account_id bigint NOT NULL REFERENCES accounts(id),
 connection_id bigint NOT NULL REFERENCES connections(id),
 batch_sha256 text NOT NULL CHECK (batch_sha256 ~ '^[a-f0-9]{64}$'),
 report_sha256 text NOT NULL CHECK (report_sha256 ~ '^[a-f0-9]{64}$'),
 plan_ids bigint[] NOT NULL CHECK (cardinality(plan_ids) BETWEEN 1 AND 500),
 plan_sha256s text[] NOT NULL CHECK (cardinality(plan_sha256s) = cardinality(plan_ids)),
 mode text NOT NULL CHECK (mode IN ('enabled','paused','completed')),
 approval_reason text NOT NULL DEFAULT '',
 approved_until timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(),
 CHECK (mode <> 'enabled' OR (length(approval_reason) > 0 AND approved_until IS NOT NULL))
);
CREATE UNIQUE INDEX nas_cleanup_batches_one_enabled ON nas_cleanup_batches ((true)) WHERE mode = 'enabled';
CREATE FUNCTION guard_nas_cleanup_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.account_id,NEW.connection_id,NEW.batch_sha256,NEW.report_sha256,NEW.plan_ids,NEW.plan_sha256s,NEW.created_at)
  IS DISTINCT FROM ROW(OLD.account_id,OLD.connection_id,OLD.batch_sha256,OLD.report_sha256,OLD.plan_ids,OLD.plan_sha256s,OLD.created_at)
  OR (OLD.mode <> 'enabled' AND NEW.mode = 'enabled') THEN
  RAISE EXCEPTION 'cleanup batch identity is immutable'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_batch_identity BEFORE UPDATE OR DELETE ON nas_cleanup_batches FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_batch();
