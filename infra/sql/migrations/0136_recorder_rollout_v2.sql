BEGIN;

-- Recorder rollout v2 separates desired deployment identity from the legacy
-- worker-authored build_sha.  Existing readers keep build_sha unchanged; new
-- rollout code requires the typed desired/observed fields before a worker can
-- enter the lease-admission set.
ALTER TABLE recorder_droplets
  ADD COLUMN IF NOT EXISTS desired_build_sha TEXT,
  ADD COLUMN IF NOT EXISTS desired_common_policy_sha256 TEXT,
  ADD COLUMN IF NOT EXISTS desired_runtime_config_sha256 TEXT,
  ADD COLUMN IF NOT EXISTS observed_build_sha TEXT,
  ADD COLUMN IF NOT EXISTS observed_common_policy_sha256 TEXT,
  ADD COLUMN IF NOT EXISTS observed_runtime_config_sha256 TEXT,
  ADD COLUMN IF NOT EXISTS firewall_fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS firewall_verified_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS rollout_generation BIGINT,
  ADD COLUMN IF NOT EXISTS replacement_for_id BIGINT REFERENCES recorder_droplets(id),
  ADD COLUMN IF NOT EXISTS rollout_provider_read_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS rollout_last_provider_facts_sha256 TEXT,
  ADD COLUMN IF NOT EXISTS rollout_last_provider_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS lease_admission_enabled BOOLEAN NOT NULL DEFAULT false;

UPDATE recorder_droplets
SET observed_build_sha=NULLIF(build_sha,''),
    desired_build_sha=NULLIF(build_sha,''),
    lease_admission_enabled=(state='active')
WHERE observed_build_sha IS NULL;

ALTER TABLE recorder_droplets DROP CONSTRAINT IF EXISTS recorder_droplets_state_check;
ALTER TABLE recorder_droplets ADD CONSTRAINT recorder_droplets_state_check
  CHECK (state IN ('provisioning','standby_ready','active','rollout_draining','draining','destroying','destroyed','failed'));
ALTER TABLE recorder_droplets ADD CONSTRAINT recorder_droplets_rollout_sha_shape CHECK (
  (desired_build_sha IS NULL OR desired_build_sha ~ '^[0-9a-f]{40}$') AND
  (observed_build_sha IS NULL OR observed_build_sha ~ '^[0-9a-f]{40,64}$') AND
  (desired_common_policy_sha256 IS NULL OR desired_common_policy_sha256 ~ '^[0-9a-f]{64}$') AND
  (desired_runtime_config_sha256 IS NULL OR desired_runtime_config_sha256 ~ '^[0-9a-f]{64}$') AND
  (observed_common_policy_sha256 IS NULL OR observed_common_policy_sha256 ~ '^[0-9a-f]{64}$') AND
  (observed_runtime_config_sha256 IS NULL OR observed_runtime_config_sha256 ~ '^[0-9a-f]{64}$') AND
  (firewall_fingerprint IS NULL OR firewall_fingerprint ~ '^[0-9a-f]{64}$')
  AND (rollout_last_provider_facts_sha256 IS NULL OR rollout_last_provider_facts_sha256 ~ '^[0-9a-f]{64}$')
  AND rollout_provider_read_count BETWEEN 0 AND 2
);
ALTER TABLE recorder_droplets ADD CONSTRAINT recorder_droplets_observed_build_alias CHECK (
  observed_build_sha IS NULL OR build_sha='' OR build_sha=observed_build_sha
);
ALTER TABLE recorder_droplets ADD CONSTRAINT recorder_droplets_admission_shape CHECK (
  NOT lease_admission_enabled OR state='active'
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_recorder_droplets_rollout_replacement
  ON recorder_droplets(rollout_generation) WHERE rollout_generation IS NOT NULL AND state IN ('provisioning','standby_ready');
CREATE INDEX IF NOT EXISTS idx_recorder_droplets_lease_admission
  ON recorder_droplets(name,node_id) WHERE state='active' AND lease_admission_enabled;

CREATE TABLE recorder_pool_rollouts (
  generation BIGSERIAL PRIMARY KEY,
  status TEXT NOT NULL CHECK(status IN ('planned','running','stopped','complete','failed')),
  baseline_worker_set_sha256 TEXT NOT NULL CHECK(baseline_worker_set_sha256 ~ '^[0-9a-f]{64}$'),
  to_build_sha TEXT NOT NULL CHECK(to_build_sha ~ '^[0-9a-f]{40}$'),
  common_policy_sha256 TEXT NOT NULL CHECK(common_policy_sha256 ~ '^[0-9a-f]{64}$'),
  firewall_id TEXT NOT NULL CHECK(length(trim(firewall_id)) BETWEEN 1 AND 128),
  firewall_rules_sha256 TEXT NOT NULL CHECK(firewall_rules_sha256 ~ '^[0-9a-f]{64}$'),
  firewall_tag TEXT NOT NULL CHECK(firewall_tag='fleet:recorder-pool-v1'),
  baseline_min INTEGER NOT NULL CHECK(baseline_min > 0),
  capacity INTEGER NOT NULL CHECK(capacity > 0),
  max_surge INTEGER NOT NULL CHECK(max_surge=1),
  account_droplet_limit INTEGER NOT NULL CHECK(account_droplet_limit > 0),
  baseline_account_droplets INTEGER NOT NULL CHECK(baseline_account_droplets >= 0),
  provider_observed_at TIMESTAMPTZ NOT NULL,
  provider_facts_sha256 TEXT NOT NULL CHECK(provider_facts_sha256 ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX uq_recorder_pool_rollouts_running
  ON recorder_pool_rollouts((true)) WHERE status IN ('planned','running');
CREATE TRIGGER trg_recorder_pool_rollouts_updated_at BEFORE UPDATE ON recorder_pool_rollouts
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE OR REPLACE FUNCTION validate_recorder_rollout_capacity() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE r recorder_pool_rollouts%ROWTYPE; claimable_count INTEGER; claimable_capacity INTEGER; live_count INTEGER; bad_id BIGINT;
BEGIN
  SELECT * INTO r FROM recorder_pool_rollouts WHERE status IN ('planned','running');
  IF NOT FOUND THEN RETURN NULL; END IF;
  SELECT count(*),COALESCE(sum(capacity),0) INTO claimable_count,claimable_capacity
  FROM recorder_droplets WHERE state='active' AND lease_admission_enabled;
  SELECT count(*) INTO live_count FROM recorder_droplets
  WHERE state IN ('provisioning','standby_ready','active','rollout_draining','draining','destroying');
  IF claimable_count<>r.baseline_min OR claimable_capacity<>r.baseline_min*r.capacity THEN
    RAISE EXCEPTION 'recorder rollout claimable capacity must remain exact';
  END IF;
  IF live_count>r.baseline_min+r.max_surge THEN
    RAISE EXCEPTION 'recorder rollout live set exceeds bounded surge';
  END IF;
  SELECT id INTO bad_id FROM recorder_droplets d
  WHERE (state IN ('provisioning','standby_ready','rollout_draining','draining','destroying') AND lease_admission_enabled)
     OR (rollout_generation IS NOT NULL AND state='active' AND (
          NOT lease_admission_enabled OR observed_build_sha<>desired_build_sha
          OR observed_common_policy_sha256<>desired_common_policy_sha256
          OR observed_runtime_config_sha256<>desired_runtime_config_sha256
          OR firewall_fingerprint IS DISTINCT FROM (SELECT firewall_rules_sha256 FROM recorder_pool_rollouts WHERE generation=d.rollout_generation)
     ))
  ORDER BY id LIMIT 1;
  IF bad_id IS NOT NULL THEN
    RAISE EXCEPTION 'recorder rollout worker % has incoherent admission identity',bad_id;
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER trg_recorder_droplets_rollout_capacity
AFTER INSERT OR UPDATE OR DELETE ON recorder_droplets DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_recorder_rollout_capacity();
CREATE CONSTRAINT TRIGGER trg_recorder_pool_rollouts_capacity
AFTER INSERT OR UPDATE OR DELETE ON recorder_pool_rollouts DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_recorder_rollout_capacity();

-- This table is intentionally write-less in R0.  A later reviewed operator
-- workflow may insert one exact future worker/recording plan.  Merely deploying
-- this migration leaves every runtime config empty and non-production-eligible.
CREATE TABLE recorder_cloud_config_plans (
  id BIGSERIAL PRIMARY KEY,
  revision BIGINT NOT NULL UNIQUE CHECK(revision > 0),
  worker_id TEXT NOT NULL CHECK(worker_id ~ '^stoarama-rec-[A-Za-z0-9._-]+$'),
  recording_id BIGINT NOT NULL REFERENCES recordings(id),
  common_policy_sha256 TEXT NOT NULL CHECK(common_policy_sha256 ~ '^[0-9a-f]{64}$'),
  runtime_config_sha256 TEXT NOT NULL CHECK(runtime_config_sha256 ~ '^[0-9a-f]{64}$'),
  plan_sha256 TEXT NOT NULL UNIQUE CHECK(plan_sha256 ~ '^[0-9a-f]{64}$'),
  expires_at TIMESTAMPTZ NOT NULL,
  production_eligible BOOLEAN NOT NULL DEFAULT false CHECK(NOT production_eligible),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION reject_recorder_rollout_truncate() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'recorder rollout authority is append-preserved'; END $$;
CREATE TRIGGER trg_recorder_pool_rollouts_no_truncate BEFORE TRUNCATE ON recorder_pool_rollouts
FOR EACH STATEMENT EXECUTE FUNCTION reject_recorder_rollout_truncate();
CREATE TRIGGER trg_recorder_cloud_config_plans_no_truncate BEFORE TRUNCATE ON recorder_cloud_config_plans
FOR EACH STATEMENT EXECUTE FUNCTION reject_recorder_rollout_truncate();

CREATE OR REPLACE FUNCTION reject_recorder_cloud_config_plan_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'recorder cloud config plans are immutable'; END $$;
CREATE TRIGGER trg_recorder_cloud_config_plans_no_update_delete
BEFORE UPDATE OR DELETE ON recorder_cloud_config_plans
FOR EACH ROW EXECUTE FUNCTION reject_recorder_cloud_config_plan_mutation();

-- A plan is authority over one exact future worker/recording pair. Recompute its
-- typed, domain-separated identity in PostgreSQL so a direct SQL writer cannot
-- provide a self-consistent but semantically unrelated plan hash.
CREATE OR REPLACE FUNCTION recorder_rollout_typed_field(v TEXT) RETURNS BYTEA
LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT int8send(octet_length(convert_to(v,'UTF8'))::bigint) || convert_to(v,'UTF8')
$$;
CREATE OR REPLACE FUNCTION validate_recorder_cloud_config_plan() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE expected TEXT;
BEGIN
  expected := encode(digest(
    recorder_rollout_typed_field('stoarama-cloud-recorder-runtime-config-v1') ||
    recorder_rollout_typed_field(NEW.common_policy_sha256) ||
    recorder_rollout_typed_field(NEW.worker_id) ||
    recorder_rollout_typed_field('1') ||
    recorder_rollout_typed_field(NEW.recording_id::text),
    'sha256'), 'hex');
  IF NEW.runtime_config_sha256 <> expected THEN
    RAISE EXCEPTION 'recorder cloud runtime config digest mismatch';
  END IF;
  expected := encode(digest(
    recorder_rollout_typed_field('stoarama-cloud-recorder-config-plan-v1') ||
    recorder_rollout_typed_field(NEW.worker_id) ||
    recorder_rollout_typed_field(NEW.recording_id::text) ||
    recorder_rollout_typed_field(NEW.common_policy_sha256) ||
    recorder_rollout_typed_field(NEW.runtime_config_sha256) ||
    recorder_rollout_typed_field(to_char(NEW.expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')),
    'sha256'), 'hex');
  IF NEW.plan_sha256 <> expected THEN
    RAISE EXCEPTION 'recorder cloud config plan digest mismatch';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER trg_recorder_cloud_config_plans_validate
BEFORE INSERT ON recorder_cloud_config_plans
FOR EACH ROW EXECUTE FUNCTION validate_recorder_cloud_config_plan();

COMMIT;
