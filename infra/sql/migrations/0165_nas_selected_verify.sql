-- Verify-only selected-file manifests. No copy, overwrite, delete, release,
-- purge, or cursor operations are represented by this schema.
CREATE TABLE IF NOT EXISTS nas_selected_verify_plans (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
  label TEXT NOT NULL UNIQUE CHECK (label ~ '^[a-z0-9][a-z0-9._-]{0,79}$'),
  manifest JSONB NOT NULL,
  plan_sha256 TEXT NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (jsonb_typeof(manifest)='array')
);

CREATE TABLE IF NOT EXISTS nas_selected_verify_items (
  id BIGSERIAL PRIMARY KEY,
  plan_id BIGINT NOT NULL REFERENCES nas_selected_verify_plans(id) ON DELETE RESTRICT,
  ordinal INTEGER NOT NULL CHECK (ordinal > 0),
  kind TEXT NOT NULL CHECK (kind IN ('raw','collated','joined')),
  source_id BIGINT NOT NULL CHECK (source_id > 0),
  relative_path TEXT NOT NULL CHECK (relative_path <> '' AND left(relative_path,1) <> '/'),
  size_bytes BIGINT NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 34359738368),
  sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
  state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','leased','match','missing','conflict','unverified')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0 AND attempts <= 3),
  leased_at TIMESTAMPTZ,
  lease_expires_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  outcome_error TEXT NOT NULL DEFAULT '' CHECK (octet_length(outcome_error) <= 1024),
  duration_ms BIGINT CHECK (duration_ms >= 0),
  client_version TEXT NOT NULL DEFAULT '',
  UNIQUE(plan_id,ordinal), UNIQUE(plan_id,kind,source_id), UNIQUE(plan_id,relative_path),
  CHECK ((state='leased')=(lease_expires_at IS NOT NULL)),
  CHECK ((state IN ('match','missing','conflict','unverified'))=(completed_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS nas_selected_verify_queue_idx
  ON nas_selected_verify_items(plan_id,id) WHERE state IN ('pending','leased');

-- A plan is an immutable evidence object once inserted.
CREATE OR REPLACE FUNCTION reject_nas_selected_verify_plan_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'NAS selected verify plans are immutable'; END $$;
DROP TRIGGER IF EXISTS nas_selected_verify_plans_immutable ON nas_selected_verify_plans;
CREATE TRIGGER nas_selected_verify_plans_immutable BEFORE UPDATE OR DELETE ON nas_selected_verify_plans
FOR EACH ROW EXECUTE FUNCTION reject_nas_selected_verify_plan_mutation();

CREATE OR REPLACE FUNCTION guard_nas_selected_verify_item_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN
  IF TG_OP='DELETE' OR ROW(NEW.plan_id,NEW.ordinal,NEW.kind,NEW.source_id,NEW.relative_path,NEW.size_bytes,NEW.sha256)
    IS DISTINCT FROM ROW(OLD.plan_id,OLD.ordinal,OLD.kind,OLD.source_id,OLD.relative_path,OLD.size_bytes,OLD.sha256)
  THEN RAISE EXCEPTION 'NAS selected verify item identity is immutable'; END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS nas_selected_verify_items_identity_immutable ON nas_selected_verify_items;
CREATE TRIGGER nas_selected_verify_items_identity_immutable BEFORE UPDATE OR DELETE ON nas_selected_verify_items
FOR EACH ROW EXECUTE FUNCTION guard_nas_selected_verify_item_mutation();
