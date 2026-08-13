BEGIN;

-- R0 owns build convergence only. It has no recording/canary authority and is
-- structurally disabled until the controller rollout switch is explicitly on.
ALTER TABLE recorder_droplets
  ADD COLUMN IF NOT EXISTS desired_build_sha TEXT,
  ADD COLUMN IF NOT EXISTS desired_common_policy_sha256 TEXT,
  ADD COLUMN IF NOT EXISTS observed_build_sha TEXT,
  ADD COLUMN IF NOT EXISTS observed_common_policy_sha256 TEXT,
  ADD COLUMN IF NOT EXISTS firewall_fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS firewall_verified_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS rollout_generation BIGINT,
  ADD COLUMN IF NOT EXISTS rollout_ordinal INTEGER,
  ADD COLUMN IF NOT EXISTS replacement_for_id BIGINT REFERENCES recorder_droplets(id),
  ADD COLUMN IF NOT EXISTS lease_admission_enabled BOOLEAN NOT NULL DEFAULT false;

UPDATE recorder_droplets SET
  observed_build_sha=NULLIF(build_sha,''),
  desired_build_sha=NULLIF(build_sha,''),
  lease_admission_enabled=(state='active')
WHERE observed_build_sha IS NULL;

ALTER TABLE recorder_droplets DROP CONSTRAINT IF EXISTS recorder_droplets_state_check;
ALTER TABLE recorder_droplets ADD CONSTRAINT recorder_droplets_state_check CHECK
  (state IN ('provisioning','standby_ready','active','rollout_draining','draining','destroying','destroyed','failed'));
ALTER TABLE recorder_droplets ADD CONSTRAINT recorder_droplets_rollout_shape CHECK (
  (desired_build_sha IS NULL OR desired_build_sha ~ '^[0-9a-f]{40}$') AND
  (observed_build_sha IS NULL OR observed_build_sha ~ '^[0-9a-f]{40}$') AND
  (desired_common_policy_sha256 IS NULL OR desired_common_policy_sha256 ~ '^[0-9a-f]{64}$') AND
  (observed_common_policy_sha256 IS NULL OR observed_common_policy_sha256 ~ '^[0-9a-f]{64}$') AND
  (rollout_generation IS NULL)=(rollout_ordinal IS NULL) AND
  (rollout_ordinal IS NULL OR rollout_ordinal>=0) AND
  (NOT lease_admission_enabled OR state='active') AND
  (observed_build_sha IS NULL OR build_sha='' OR build_sha=observed_build_sha)
);
CREATE UNIQUE INDEX uq_recorder_droplets_rollout_ordinal
  ON recorder_droplets(rollout_generation,rollout_ordinal) WHERE rollout_generation IS NOT NULL AND rollout_ordinal>0;
CREATE INDEX idx_recorder_droplets_lease_admission
  ON recorder_droplets(name,node_id) WHERE state='active' AND lease_admission_enabled;

CREATE TABLE recorder_pool_rollouts (
  generation BIGSERIAL PRIMARY KEY,
  status TEXT NOT NULL CHECK(status IN ('running','stopping','stopped','complete','failed')),
  stop_reason TEXT CHECK(stop_reason IS NULL OR stop_reason IN
    ('disabled','forecast_exceeds_capacity','provider_drift','duplicate_provider_object','target_mismatch','operator_stop')),
  baseline_worker_set_sha256 TEXT NOT NULL CHECK(baseline_worker_set_sha256 ~ '^[0-9a-f]{64}$'),
  frozen_nonpool_set_sha256 TEXT NOT NULL CHECK(frozen_nonpool_set_sha256 ~ '^[0-9a-f]{64}$'),
  target_sha256 TEXT NOT NULL CHECK(target_sha256 ~ '^[0-9a-f]{64}$'),
  to_build_sha TEXT NOT NULL CHECK(to_build_sha ~ '^[0-9a-f]{40}$'),
  common_policy_sha256 TEXT NOT NULL CHECK(common_policy_sha256 ~ '^[0-9a-f]{64}$'),
  host_firewall_script_sha256 TEXT NOT NULL CHECK(host_firewall_script_sha256 ~ '^[0-9a-f]{64}$'),
  kernel_firewall_policy_sha256 TEXT NOT NULL CHECK(kernel_firewall_policy_sha256 ~ '^[0-9a-f]{64}$'),
  firewall_id TEXT NOT NULL CHECK(length(trim(firewall_id)) BETWEEN 1 AND 128),
  firewall_rules_sha256 TEXT NOT NULL CHECK(firewall_rules_sha256 ~ '^[0-9a-f]{64}$'),
  firewall_tag TEXT NOT NULL CHECK(firewall_tag='fleet:recorder-pool-v1'),
  region TEXT NOT NULL CHECK(length(trim(region)) BETWEEN 1 AND 64),
  size_slug TEXT NOT NULL CHECK(length(trim(size_slug)) BETWEEN 1 AND 64),
  project_id TEXT NOT NULL CHECK(length(trim(project_id)) BETWEEN 1 AND 128),
  steady_workers INTEGER NOT NULL CHECK(steady_workers=8),
  worker_capacity INTEGER NOT NULL CHECK(worker_capacity=5),
  heartbeat_sec INTEGER NOT NULL CHECK(heartbeat_sec BETWEEN 1 AND 300),
  poll_sec INTEGER NOT NULL CHECK(poll_sec BETWEEN 1 AND 300),
  backend_api_origin TEXT NOT NULL CHECK(length(backend_api_origin) BETWEEN 9 AND 512),
  max_surge INTEGER NOT NULL CHECK(max_surge=1),
  steady_account_droplets INTEGER NOT NULL CHECK(steady_account_droplets=10),
  surge_account_droplets INTEGER NOT NULL CHECK(surge_account_droplets=11),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  stopping_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  CHECK ((status='running' AND stopping_at IS NULL AND completed_at IS NULL) OR
         (status='stopping' AND stopping_at IS NOT NULL AND completed_at IS NULL AND stop_reason IS NOT NULL) OR
         (status IN ('stopped','complete','failed') AND completed_at IS NOT NULL))
);
CREATE UNIQUE INDEX uq_recorder_pool_rollouts_live ON recorder_pool_rollouts((true))
  WHERE status IN ('running','stopping');
CREATE UNIQUE INDEX uq_recorder_pool_rollouts_target_complete ON recorder_pool_rollouts(target_sha256)
  WHERE status='complete';

CREATE TABLE recorder_rollout_provider_observations (
  id BIGSERIAL PRIMARY KEY,
  rollout_generation BIGINT NOT NULL REFERENCES recorder_pool_rollouts(generation),
  phase TEXT NOT NULL CHECK(phase IN ('baseline','reconcile','pre_switch','pre_delete','post_delete')),
  account_droplet_limit INTEGER NOT NULL CHECK(account_droplet_limit>0),
  account_droplet_count INTEGER NOT NULL CHECK(account_droplet_count IN (10,11)),
  pool_droplet_count INTEGER NOT NULL CHECK(pool_droplet_count IN (8,9)),
  frozen_nonpool_set_sha256 TEXT NOT NULL CHECK(frozen_nonpool_set_sha256 ~ '^[0-9a-f]{64}$'),
  provider_inventory_sha256 TEXT NOT NULL CHECK(provider_inventory_sha256 ~ '^[0-9a-f]{64}$'),
  firewall_id TEXT NOT NULL,
  firewall_rules_sha256 TEXT NOT NULL CHECK(firewall_rules_sha256 ~ '^[0-9a-f]{64}$'),
  firewall_tag TEXT NOT NULL CHECK(firewall_tag='fleet:recorder-pool-v1'),
  firewall_status TEXT NOT NULL CHECK(firewall_status='succeeded'),
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE recorder_rollout_provider_objects (
  observation_id BIGINT NOT NULL REFERENCES recorder_rollout_provider_observations(id),
  provider_droplet_id BIGINT NOT NULL CHECK(provider_droplet_id>0),
  is_pool BOOLEAN NOT NULL,
  name TEXT NOT NULL CHECK(octet_length(name) BETWEEN 1 AND 128),
  region TEXT NOT NULL,
  size_slug TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('new','active','off','archive')),
  project_member BOOLEAN NOT NULL,
  pool_tag_present BOOLEAN NOT NULL,
  tags_csv TEXT NOT NULL CHECK(octet_length(tags_csv)<=2048),
  object_fingerprint_sha256 TEXT NOT NULL CHECK(object_fingerprint_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY(observation_id,provider_droplet_id),
  UNIQUE(observation_id,name),
  CHECK(NOT is_pool OR name ~ '^stoarama-rec-[A-Za-z0-9._-]+$'),
  CHECK(is_pool=pool_tag_present)
);

CREATE TABLE recorder_worker_rollout_observations (
  id BIGSERIAL PRIMARY KEY,
  droplet_id BIGINT NOT NULL REFERENCES recorder_droplets(id),
  node_id BIGINT NOT NULL REFERENCES nodes(id),
  worker_id TEXT NOT NULL CHECK(worker_id ~ '^stoarama-rec-[A-Za-z0-9._-]+$'),
  executable_build_sha TEXT NOT NULL CHECK(executable_build_sha ~ '^[0-9a-f]{40}$'),
  capacity INTEGER NOT NULL CHECK(capacity BETWEEN 1 AND 64),
  heartbeat_sec INTEGER NOT NULL CHECK(heartbeat_sec BETWEEN 1 AND 300),
  poll_sec INTEGER NOT NULL CHECK(poll_sec BETWEEN 1 AND 300),
  backend_api_origin TEXT NOT NULL CHECK(length(backend_api_origin) BETWEEN 9 AND 512),
  host_firewall_script_sha256 TEXT NOT NULL CHECK(host_firewall_script_sha256 ~ '^[0-9a-f]{64}$'),
  kernel_firewall_policy_sha256 TEXT NOT NULL CHECK(kernel_firewall_policy_sha256 ~ '^[0-9a-f]{64}$'),
  observation_sha256 TEXT NOT NULL CHECK(observation_sha256 ~ '^[0-9a-f]{64}$'),
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK(octet_length(worker_id)<=128 AND octet_length(backend_api_origin)<=512)
);
CREATE INDEX idx_recorder_worker_rollout_observations_latest
  ON recorder_worker_rollout_observations(droplet_id,observed_at DESC,id DESC);

CREATE OR REPLACE FUNCTION reject_recorder_rollout_append_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'recorder rollout observations are append-only'; END $$;
CREATE TRIGGER trg_recorder_rollout_provider_observations_no_mutation
BEFORE UPDATE OR DELETE ON recorder_rollout_provider_observations FOR EACH ROW
EXECUTE FUNCTION reject_recorder_rollout_append_mutation();
CREATE TRIGGER trg_recorder_rollout_provider_objects_no_mutation
BEFORE UPDATE OR DELETE ON recorder_rollout_provider_objects FOR EACH ROW
EXECUTE FUNCTION reject_recorder_rollout_append_mutation();
CREATE TRIGGER trg_recorder_worker_rollout_observations_no_mutation
BEFORE UPDATE OR DELETE ON recorder_worker_rollout_observations FOR EACH ROW
EXECUTE FUNCTION reject_recorder_rollout_append_mutation();

CREATE OR REPLACE FUNCTION reject_recorder_rollout_truncate() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'recorder rollout authority cannot be truncated'; END $$;
CREATE TRIGGER trg_recorder_pool_rollouts_no_truncate BEFORE TRUNCATE ON recorder_pool_rollouts
FOR EACH STATEMENT EXECUTE FUNCTION reject_recorder_rollout_truncate();
CREATE TRIGGER trg_recorder_rollout_provider_observations_no_truncate BEFORE TRUNCATE ON recorder_rollout_provider_observations
FOR EACH STATEMENT EXECUTE FUNCTION reject_recorder_rollout_truncate();
CREATE TRIGGER trg_recorder_rollout_provider_objects_no_truncate BEFORE TRUNCATE ON recorder_rollout_provider_objects
FOR EACH STATEMENT EXECUTE FUNCTION reject_recorder_rollout_truncate();
CREATE TRIGGER trg_recorder_worker_rollout_observations_no_truncate BEFORE TRUNCATE ON recorder_worker_rollout_observations
FOR EACH STATEMENT EXECUTE FUNCTION reject_recorder_rollout_truncate();

CREATE OR REPLACE FUNCTION validate_recorder_pool_rollout_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.generation<>NEW.generation OR OLD.target_sha256<>NEW.target_sha256 OR OLD.to_build_sha<>NEW.to_build_sha OR
     OLD.common_policy_sha256<>NEW.common_policy_sha256 OR OLD.baseline_worker_set_sha256<>NEW.baseline_worker_set_sha256 OR
     OLD.frozen_nonpool_set_sha256<>NEW.frozen_nonpool_set_sha256 OR OLD.firewall_id<>NEW.firewall_id OR
     OLD.firewall_rules_sha256<>NEW.firewall_rules_sha256 OR OLD.firewall_tag<>NEW.firewall_tag OR
     OLD.host_firewall_script_sha256<>NEW.host_firewall_script_sha256 OR OLD.kernel_firewall_policy_sha256<>NEW.kernel_firewall_policy_sha256 OR
     OLD.region<>NEW.region OR OLD.size_slug<>NEW.size_slug OR OLD.project_id<>NEW.project_id OR
     OLD.steady_workers<>NEW.steady_workers OR OLD.worker_capacity<>NEW.worker_capacity OR OLD.heartbeat_sec<>NEW.heartbeat_sec OR OLD.poll_sec<>NEW.poll_sec OR OLD.backend_api_origin<>NEW.backend_api_origin OR OLD.max_surge<>NEW.max_surge OR
     OLD.steady_account_droplets<>NEW.steady_account_droplets OR OLD.surge_account_droplets<>NEW.surge_account_droplets OR OLD.created_at<>NEW.created_at THEN
    RAISE EXCEPTION 'recorder rollout immutable authority changed';
  END IF;
  IF OLD.stop_reason IS NOT NULL AND NEW.stop_reason IS DISTINCT FROM OLD.stop_reason THEN
    RAISE EXCEPTION 'recorder rollout stop reason is immutable once set';
  END IF;
  IF NOT ((OLD.status='running' AND NEW.status IN ('running','stopping','complete','failed')) OR
          (OLD.status='stopping' AND NEW.status IN ('stopping','stopped','failed')) OR
          (OLD.status=NEW.status AND OLD.status IN ('stopped','complete','failed'))) THEN
    RAISE EXCEPTION 'invalid recorder rollout transition % -> %',OLD.status,NEW.status;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER trg_recorder_pool_rollouts_transition BEFORE UPDATE ON recorder_pool_rollouts
FOR EACH ROW EXECUTE FUNCTION validate_recorder_pool_rollout_transition();

CREATE OR REPLACE FUNCTION recorder_rollout_typed_field(v TEXT) RETURNS BYTEA
LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT int8send(octet_length(convert_to(v,'UTF8'))::bigint) || convert_to(v,'UTF8')
$$;

CREATE OR REPLACE FUNCTION validate_recorder_rollout_provider_object_digest() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE expected TEXT;
BEGIN
  expected:=encode(digest(
    recorder_rollout_typed_field('stoarama-recorder-provider-object-v1')||
    recorder_rollout_typed_field(NEW.provider_droplet_id::text)||recorder_rollout_typed_field(NEW.name)||
    recorder_rollout_typed_field(NEW.region)||recorder_rollout_typed_field(NEW.size_slug)||
    recorder_rollout_typed_field(NEW.status)||recorder_rollout_typed_field(NEW.project_member::text)||
    recorder_rollout_typed_field(NEW.tags_csv),'sha256'),'hex');
  IF NEW.object_fingerprint_sha256<>expected THEN
    RAISE EXCEPTION 'recorder provider object fingerprint mismatch';
  END IF;
  IF NEW.tags_csv<>array_to_string(ARRAY(SELECT value FROM unnest(string_to_array(NEW.tags_csv,',')) value ORDER BY value),',') OR
     EXISTS(SELECT 1 FROM unnest(string_to_array(NEW.tags_csv,',')) value GROUP BY value HAVING count(*)>1) OR
     NEW.pool_tag_present<>( 'fleet:recorder-pool-v1'=ANY(string_to_array(NEW.tags_csv,',')) ) THEN
    RAISE EXCEPTION 'recorder provider object tags are not canonical';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER trg_recorder_rollout_provider_objects_digest BEFORE INSERT ON recorder_rollout_provider_objects
FOR EACH ROW EXECUTE FUNCTION validate_recorder_rollout_provider_object_digest();

CREATE OR REPLACE FUNCTION validate_recorder_rollout_provider_observation() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  observation BIGINT;
  p recorder_rollout_provider_observations%ROWTYPE;
  r recorder_pool_rollouts%ROWTYPE;
  object recorder_rollout_provider_objects%ROWTYPE;
  body BYTEA;
  nonpool_body BYTEA;
  pool_count INTEGER;
  nonpool_count INTEGER;
  expected_inventory TEXT;
  expected_nonpool TEXT;
BEGIN
  observation:=CASE WHEN TG_TABLE_NAME='recorder_rollout_provider_observations' THEN NEW.id ELSE NEW.observation_id END;
  SELECT * INTO p FROM recorder_rollout_provider_observations WHERE id=observation;
  IF NOT FOUND THEN RAISE EXCEPTION 'recorder provider observation parent missing'; END IF;
  SELECT * INTO r FROM recorder_pool_rollouts WHERE generation=p.rollout_generation;
  IF NOT FOUND THEN RAISE EXCEPTION 'recorder provider observation rollout missing'; END IF;
  SELECT count(*) FILTER(WHERE is_pool),count(*) FILTER(WHERE NOT is_pool)
    INTO pool_count,nonpool_count FROM recorder_rollout_provider_objects WHERE observation_id=observation;
  nonpool_body:=recorder_rollout_typed_field('stoarama-recorder-pool-nonpool-set-v1')||recorder_rollout_typed_field(nonpool_count::text);
  FOR object IN SELECT * FROM recorder_rollout_provider_objects WHERE observation_id=observation AND NOT is_pool ORDER BY provider_droplet_id LOOP
    nonpool_body:=nonpool_body||recorder_rollout_typed_field(object.provider_droplet_id::text);
  END LOOP;
  expected_nonpool:=encode(digest(nonpool_body,'sha256'),'hex');
  body:=recorder_rollout_typed_field('stoarama-recorder-pool-provider-facts-v1')||
    recorder_rollout_typed_field(p.account_droplet_limit::text)||recorder_rollout_typed_field(p.account_droplet_count::text)||
    recorder_rollout_typed_field(p.firewall_id)||recorder_rollout_typed_field(p.firewall_status)||
    recorder_rollout_typed_field(p.firewall_rules_sha256)||recorder_rollout_typed_field(pool_count::text);
  FOR object IN SELECT * FROM recorder_rollout_provider_objects WHERE observation_id=observation AND is_pool ORDER BY provider_droplet_id LOOP
    body:=body||recorder_rollout_typed_field(object.provider_droplet_id::text);
  END LOOP;
  body:=body||recorder_rollout_typed_field(nonpool_count::text);
  FOR object IN SELECT * FROM recorder_rollout_provider_objects WHERE observation_id=observation AND NOT is_pool ORDER BY provider_droplet_id LOOP
    body:=body||recorder_rollout_typed_field(object.provider_droplet_id::text);
  END LOOP;
  body:=body||recorder_rollout_typed_field(pool_count::text);
  FOR object IN SELECT * FROM recorder_rollout_provider_objects WHERE observation_id=observation AND is_pool ORDER BY provider_droplet_id LOOP
    body:=body||recorder_rollout_typed_field(object.provider_droplet_id::text)||recorder_rollout_typed_field(object.name)||
      recorder_rollout_typed_field(object.region)||recorder_rollout_typed_field(object.size_slug)||recorder_rollout_typed_field(object.status)||
      recorder_rollout_typed_field(object.project_member::text)||recorder_rollout_typed_field(object.tags_csv);
  END LOOP;
  body:=body||recorder_rollout_typed_field(nonpool_count::text);
  FOR object IN SELECT * FROM recorder_rollout_provider_objects WHERE observation_id=observation AND NOT is_pool ORDER BY provider_droplet_id LOOP
    body:=body||recorder_rollout_typed_field(object.provider_droplet_id::text)||recorder_rollout_typed_field(object.name)||
      recorder_rollout_typed_field(object.region)||recorder_rollout_typed_field(object.size_slug)||recorder_rollout_typed_field(object.status)||
      recorder_rollout_typed_field(object.project_member::text)||recorder_rollout_typed_field(object.tags_csv);
  END LOOP;
  body:=body||recorder_rollout_typed_field('1')||recorder_rollout_typed_field(p.firewall_tag)||recorder_rollout_typed_field('0');
  expected_inventory:=encode(digest(body,'sha256'),'hex');
  IF pool_count<>p.pool_droplet_count OR pool_count+nonpool_count<>p.account_droplet_count OR nonpool_count<>2 OR
     p.account_droplet_limit<r.surge_account_droplets OR expected_nonpool<>p.frozen_nonpool_set_sha256 OR
     p.frozen_nonpool_set_sha256<>r.frozen_nonpool_set_sha256 OR expected_inventory<>p.provider_inventory_sha256 OR
     p.firewall_id<>r.firewall_id OR p.firewall_rules_sha256<>r.firewall_rules_sha256 OR p.firewall_tag<>r.firewall_tag OR
     EXISTS(SELECT 1 FROM recorder_rollout_provider_objects po WHERE po.observation_id=observation AND po.is_pool AND
       (po.region<>r.region OR po.size_slug<>r.size_slug OR po.status<>'active' OR NOT po.project_member OR NOT po.pool_tag_present)) THEN
    RAISE EXCEPTION 'recorder provider observation canonical inventory mismatch';
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER trg_recorder_rollout_provider_observations_coherence
AFTER INSERT ON recorder_rollout_provider_observations DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_recorder_rollout_provider_observation();
CREATE CONSTRAINT TRIGGER trg_recorder_rollout_provider_objects_coherence
AFTER INSERT ON recorder_rollout_provider_objects DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_recorder_rollout_provider_observation();

CREATE OR REPLACE FUNCTION validate_recorder_rollout_digests() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE expected_common TEXT; expected_target TEXT;
BEGIN
  expected_common:=encode(digest(
    recorder_rollout_typed_field('stoarama-cloud-recorder-common-policy-v2')||
    recorder_rollout_typed_field(NEW.to_build_sha)||recorder_rollout_typed_field(NEW.worker_capacity::text)||
    recorder_rollout_typed_field(NEW.heartbeat_sec::text)||recorder_rollout_typed_field(NEW.poll_sec::text)||
    recorder_rollout_typed_field(NEW.backend_api_origin)||recorder_rollout_typed_field(NEW.firewall_id)||
    recorder_rollout_typed_field(NEW.firewall_rules_sha256)||recorder_rollout_typed_field(NEW.firewall_tag)||
    recorder_rollout_typed_field(NEW.host_firewall_script_sha256)||recorder_rollout_typed_field(NEW.kernel_firewall_policy_sha256),'sha256'),'hex');
  expected_target:=encode(digest(
    recorder_rollout_typed_field('stoarama-recorder-pool-rollout-target-v1')||recorder_rollout_typed_field(NEW.to_build_sha)||
    recorder_rollout_typed_field(NEW.common_policy_sha256)||recorder_rollout_typed_field(NEW.region)||recorder_rollout_typed_field(NEW.size_slug)||
    recorder_rollout_typed_field(NEW.project_id)||recorder_rollout_typed_field(NEW.firewall_id)||recorder_rollout_typed_field(NEW.firewall_rules_sha256)||
    recorder_rollout_typed_field(NEW.firewall_tag)||recorder_rollout_typed_field(NEW.host_firewall_script_sha256)||
    recorder_rollout_typed_field(NEW.kernel_firewall_policy_sha256)||recorder_rollout_typed_field(NEW.steady_workers::text)||
    recorder_rollout_typed_field(NEW.worker_capacity::text)||recorder_rollout_typed_field(NEW.max_surge::text)||
    recorder_rollout_typed_field(NEW.frozen_nonpool_set_sha256),'sha256'),'hex');
  IF NEW.common_policy_sha256<>expected_common OR NEW.target_sha256<>expected_target THEN
    RAISE EXCEPTION 'recorder rollout canonical digest mismatch';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER trg_recorder_pool_rollouts_digests BEFORE INSERT ON recorder_pool_rollouts
FOR EACH ROW EXECUTE FUNCTION validate_recorder_rollout_digests();

CREATE OR REPLACE FUNCTION validate_recorder_worker_observation_digest() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE expected TEXT;
BEGIN
 expected:=encode(digest(recorder_rollout_typed_field('stoarama-cloud-recorder-worker-observation-v1')||
 recorder_rollout_typed_field(NEW.executable_build_sha)||recorder_rollout_typed_field(NEW.capacity::text)||
 recorder_rollout_typed_field(NEW.heartbeat_sec::text)||recorder_rollout_typed_field(NEW.poll_sec::text)||
 recorder_rollout_typed_field(NEW.backend_api_origin)||recorder_rollout_typed_field(NEW.host_firewall_script_sha256)||
 recorder_rollout_typed_field(NEW.kernel_firewall_policy_sha256),'sha256'),'hex');
 IF NEW.observation_sha256<>expected THEN RAISE EXCEPTION 'recorder worker observation digest mismatch'; END IF;
 IF NOT EXISTS(SELECT 1 FROM recorder_droplets d WHERE d.id=NEW.droplet_id AND d.node_id=NEW.node_id AND d.name=NEW.worker_id) THEN
   RAISE EXCEPTION 'recorder worker observation binding mismatch';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER trg_recorder_worker_rollout_observations_digest BEFORE INSERT ON recorder_worker_rollout_observations
FOR EACH ROW EXECUTE FUNCTION validate_recorder_worker_observation_digest();

CREATE OR REPLACE FUNCTION validate_recorder_rollout_capacity() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE r recorder_pool_rollouts%ROWTYPE; claimable_count INTEGER; claimable_capacity INTEGER; transitional INTEGER;
BEGIN
  SELECT * INTO r FROM recorder_pool_rollouts WHERE status IN ('running','stopping');
  IF NOT FOUND THEN RETURN NULL; END IF;
  SELECT count(*),COALESCE(sum(capacity),0) INTO claimable_count,claimable_capacity
    FROM recorder_droplets WHERE state='active' AND lease_admission_enabled;
  SELECT count(*) INTO transitional FROM recorder_droplets WHERE rollout_generation=r.generation AND state IN ('provisioning','standby_ready','rollout_draining','destroying');
  IF claimable_count<>r.steady_workers OR claimable_capacity<>r.steady_workers*r.worker_capacity THEN
    RAISE EXCEPTION 'recorder rollout claimable capacity must remain exact';
  END IF;
  IF transitional>r.max_surge THEN RAISE EXCEPTION 'recorder rollout exceeds bounded surge'; END IF;
  IF EXISTS(SELECT 1 FROM recorder_droplets WHERE state<>'active' AND lease_admission_enabled) THEN
    RAISE EXCEPTION 'non-active recorder cannot admit leases';
  END IF;
  IF EXISTS(SELECT 1 FROM recorder_droplets d JOIN recorder_pool_rollouts rr ON rr.generation=d.rollout_generation
    WHERE rr.status IN ('running','stopping') AND d.state='active' AND d.lease_admission_enabled AND
      (d.observed_build_sha IS DISTINCT FROM rr.to_build_sha OR d.observed_common_policy_sha256 IS DISTINCT FROM rr.common_policy_sha256 OR
       NOT EXISTS(SELECT 1 FROM recorder_worker_rollout_observations o WHERE o.droplet_id=d.id AND o.observed_at>now()-interval '90 seconds' AND
         o.executable_build_sha=rr.to_build_sha AND o.capacity=rr.worker_capacity AND o.heartbeat_sec=rr.heartbeat_sec AND o.poll_sec=rr.poll_sec AND
         o.backend_api_origin=rr.backend_api_origin AND o.host_firewall_script_sha256=rr.host_firewall_script_sha256 AND
         o.kernel_firewall_policy_sha256=rr.kernel_firewall_policy_sha256))) THEN
    RAISE EXCEPTION 'claimable rollout recorder lacks fresh exact host observation';
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER trg_recorder_droplets_rollout_capacity
AFTER INSERT OR UPDATE OR DELETE ON recorder_droplets DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_recorder_rollout_capacity();
CREATE CONSTRAINT TRIGGER trg_recorder_pool_rollouts_capacity
AFTER INSERT OR UPDATE OR DELETE ON recorder_pool_rollouts DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_recorder_rollout_capacity();

COMMIT;
