BEGIN;

-- Every function below captures this exact installation schema.  PostgreSQL
-- otherwise prepends a hostile pg_temp schema at execution time and an
-- attacker can shadow unqualified authority relations/functions.
SELECT pg_catalog.set_config(
  'search_path',
  pg_catalog.quote_ident(pg_catalog.current_schema()) || ',pg_catalog',
  true
);

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
CREATE UNIQUE INDEX uq_recorder_droplets_rollout_pending_replacement
  ON recorder_droplets(rollout_generation,replacement_for_id)
  WHERE rollout_generation IS NOT NULL AND replacement_for_id IS NOT NULL AND state IN ('provisioning','standby_ready');
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
ALTER TABLE recorder_pool_rollouts ADD CONSTRAINT recorder_pool_rollouts_generation_target_uq
  UNIQUE(generation,to_build_sha,common_policy_sha256);

ALTER TABLE recorder_droplets
  ADD CONSTRAINT recorder_droplets_rollout_authority_fk
  FOREIGN KEY (rollout_generation) REFERENCES recorder_pool_rollouts(generation),
  ADD CONSTRAINT recorder_droplets_rollout_target_fk
  FOREIGN KEY (rollout_generation,desired_build_sha,desired_common_policy_sha256)
  REFERENCES recorder_pool_rollouts(generation,to_build_sha,common_policy_sha256);

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
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$ BEGIN RAISE EXCEPTION 'recorder rollout observations are append-only'; END $$;
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
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$ BEGIN RAISE EXCEPTION 'recorder rollout authority cannot be truncated'; END $$;
CREATE TRIGGER trg_recorder_pool_rollouts_no_truncate BEFORE TRUNCATE ON recorder_pool_rollouts
FOR EACH STATEMENT EXECUTE FUNCTION reject_recorder_rollout_truncate();
CREATE TRIGGER trg_recorder_rollout_provider_observations_no_truncate BEFORE TRUNCATE ON recorder_rollout_provider_observations
FOR EACH STATEMENT EXECUTE FUNCTION reject_recorder_rollout_truncate();
CREATE TRIGGER trg_recorder_rollout_provider_objects_no_truncate BEFORE TRUNCATE ON recorder_rollout_provider_objects
FOR EACH STATEMENT EXECUTE FUNCTION reject_recorder_rollout_truncate();
CREATE TRIGGER trg_recorder_worker_rollout_observations_no_truncate BEFORE TRUNCATE ON recorder_worker_rollout_observations
FOR EACH STATEMENT EXECUTE FUNCTION reject_recorder_rollout_truncate();
CREATE TRIGGER trg_recorder_pool_rollouts_no_delete BEFORE DELETE ON recorder_pool_rollouts
FOR EACH ROW EXECUTE FUNCTION reject_recorder_rollout_append_mutation();

CREATE OR REPLACE FUNCTION validate_recorder_pool_rollout_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
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
  IF NOT ((OLD.status='running' AND NEW.status IN ('running','stopping','complete')) OR
          (OLD.status='stopping' AND NEW.status IN ('stopping','stopped')) OR
          (OLD.status=NEW.status AND OLD.status IN ('stopped','complete','failed'))) THEN
    RAISE EXCEPTION 'invalid recorder rollout transition % -> %',OLD.status,NEW.status;
  END IF;
  IF NEW.status='complete' AND OLD.status<>'complete' AND (
       NEW.completed_at IS DISTINCT FROM pg_catalog.transaction_timestamp() OR
       (SELECT count(*) FROM recorder_droplets WHERE state='active' AND lease_admission_enabled)<>NEW.steady_workers OR
       EXISTS(SELECT 1 FROM recorder_droplets WHERE state IN ('provisioning','standby_ready','rollout_draining','draining','destroying') OR
         (state='active' AND (NOT lease_admission_enabled OR rollout_generation IS DISTINCT FROM NEW.generation OR
          observed_build_sha IS DISTINCT FROM NEW.to_build_sha OR observed_common_policy_sha256 IS DISTINCT FROM NEW.common_policy_sha256))) OR
       EXISTS(SELECT 1 FROM recorder_droplets d WHERE d.state='active' AND d.lease_admission_enabled AND NOT EXISTS(
         SELECT 1 FROM LATERAL (SELECT o.* FROM recorder_worker_rollout_observations o WHERE o.droplet_id=d.id ORDER BY o.observed_at DESC,o.id DESC LIMIT 1) latest
         WHERE latest.observed_at>=pg_catalog.transaction_timestamp()-interval '90 seconds' AND latest.observed_at<=pg_catalog.transaction_timestamp() AND
           latest.executable_build_sha=NEW.to_build_sha AND latest.capacity=NEW.worker_capacity AND latest.heartbeat_sec=NEW.heartbeat_sec AND
           latest.poll_sec=NEW.poll_sec AND latest.backend_api_origin=NEW.backend_api_origin AND
           latest.host_firewall_script_sha256=NEW.host_firewall_script_sha256 AND latest.kernel_firewall_policy_sha256=NEW.kernel_firewall_policy_sha256)) OR
       NOT EXISTS(SELECT 1 FROM recorder_rollout_provider_observations p WHERE p.rollout_generation=NEW.generation AND p.phase='post_delete' AND
         p.observed_at>=pg_catalog.transaction_timestamp()-interval '60 seconds' AND p.observed_at<=pg_catalog.transaction_timestamp() AND
         p.pool_droplet_count=NEW.steady_workers AND p.account_droplet_count=NEW.steady_account_droplets)
     ) THEN
    RAISE EXCEPTION 'recorder rollout completion is not DB-sealed';
  END IF;
  IF NEW.status='stopped' AND OLD.status<>'stopped' AND (
       NEW.completed_at IS DISTINCT FROM pg_catalog.transaction_timestamp() OR
       (SELECT count(*) FROM recorder_droplets WHERE state='active' AND lease_admission_enabled)<>NEW.steady_workers OR
       EXISTS(SELECT 1 FROM recorder_droplets WHERE state IN ('provisioning','standby_ready','rollout_draining','draining','destroying')) OR
       NOT EXISTS(SELECT 1 FROM recorder_rollout_provider_observations p WHERE p.rollout_generation=NEW.generation AND p.phase='post_delete' AND
         p.observed_at>=pg_catalog.transaction_timestamp()-interval '60 seconds' AND p.observed_at<=pg_catalog.transaction_timestamp() AND
         p.pool_droplet_count=NEW.steady_workers AND p.account_droplet_count=NEW.steady_account_droplets)
     ) THEN
    RAISE EXCEPTION 'recorder rollout stop is not DB-sealed';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER trg_recorder_pool_rollouts_transition BEFORE UPDATE ON recorder_pool_rollouts
FOR EACH ROW EXECUTE FUNCTION validate_recorder_pool_rollout_transition();

CREATE OR REPLACE FUNCTION recorder_rollout_typed_field(v TEXT) RETURNS BYTEA
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
  SELECT pg_catalog.int8send(pg_catalog.octet_length(pg_catalog.convert_to(v,'UTF8'))::bigint) || pg_catalog.convert_to(v,'UTF8')
$$;

CREATE OR REPLACE FUNCTION validate_recorder_rollout_provider_object_digest() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE expected TEXT;
BEGIN
  expected:=pg_catalog.encode(pg_catalog.sha256(
    recorder_rollout_typed_field('stoarama-recorder-provider-object-v1')||
    recorder_rollout_typed_field(NEW.provider_droplet_id::text)||recorder_rollout_typed_field(NEW.name)||
    recorder_rollout_typed_field(NEW.region)||recorder_rollout_typed_field(NEW.size_slug)||
    recorder_rollout_typed_field(NEW.status)||recorder_rollout_typed_field(NEW.project_member::text)||
    recorder_rollout_typed_field(NEW.tags_csv)),'hex');
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
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
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
  IF r.status NOT IN ('running','stopping') THEN RAISE EXCEPTION 'terminal rollout cannot append provider observations'; END IF;
  SELECT count(*) FILTER(WHERE is_pool),count(*) FILTER(WHERE NOT is_pool)
    INTO pool_count,nonpool_count FROM recorder_rollout_provider_objects WHERE observation_id=observation;
  nonpool_body:=recorder_rollout_typed_field('stoarama-recorder-pool-nonpool-objects-v2')||recorder_rollout_typed_field(nonpool_count::text);
  FOR object IN SELECT * FROM recorder_rollout_provider_objects WHERE observation_id=observation AND NOT is_pool ORDER BY provider_droplet_id LOOP
    nonpool_body:=nonpool_body||recorder_rollout_typed_field(object.provider_droplet_id::text)||recorder_rollout_typed_field(object.name)||
      recorder_rollout_typed_field(object.region)||recorder_rollout_typed_field(object.size_slug)||recorder_rollout_typed_field(object.status)||
      recorder_rollout_typed_field(object.project_member::text)||recorder_rollout_typed_field(object.tags_csv);
  END LOOP;
  expected_nonpool:=pg_catalog.encode(pg_catalog.sha256(nonpool_body),'hex');
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
  expected_inventory:=pg_catalog.encode(pg_catalog.sha256(body),'hex');
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
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE expected_common TEXT; expected_target TEXT;
BEGIN
  expected_common:=pg_catalog.encode(pg_catalog.sha256(
    recorder_rollout_typed_field('stoarama-cloud-recorder-common-policy-v2')||
    recorder_rollout_typed_field(NEW.to_build_sha)||recorder_rollout_typed_field(NEW.worker_capacity::text)||
    recorder_rollout_typed_field(NEW.heartbeat_sec::text)||recorder_rollout_typed_field(NEW.poll_sec::text)||
    recorder_rollout_typed_field(NEW.backend_api_origin)||recorder_rollout_typed_field(NEW.firewall_id)||
    recorder_rollout_typed_field(NEW.firewall_rules_sha256)||recorder_rollout_typed_field(NEW.firewall_tag)||
    recorder_rollout_typed_field(NEW.host_firewall_script_sha256)||recorder_rollout_typed_field(NEW.kernel_firewall_policy_sha256)),'hex');
  expected_target:=pg_catalog.encode(pg_catalog.sha256(
    recorder_rollout_typed_field('stoarama-recorder-pool-rollout-target-v1')||recorder_rollout_typed_field(NEW.to_build_sha)||
    recorder_rollout_typed_field(NEW.common_policy_sha256)||recorder_rollout_typed_field(NEW.region)||recorder_rollout_typed_field(NEW.size_slug)||
    recorder_rollout_typed_field(NEW.project_id)||recorder_rollout_typed_field(NEW.firewall_id)||recorder_rollout_typed_field(NEW.firewall_rules_sha256)||
    recorder_rollout_typed_field(NEW.firewall_tag)||recorder_rollout_typed_field(NEW.host_firewall_script_sha256)||
    recorder_rollout_typed_field(NEW.kernel_firewall_policy_sha256)||recorder_rollout_typed_field(NEW.steady_workers::text)||
    recorder_rollout_typed_field(NEW.worker_capacity::text)||recorder_rollout_typed_field(NEW.max_surge::text)||
    recorder_rollout_typed_field(NEW.frozen_nonpool_set_sha256)),'hex');
  IF NEW.common_policy_sha256<>expected_common OR NEW.target_sha256<>expected_target THEN
    RAISE EXCEPTION 'recorder rollout canonical digest mismatch';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER trg_recorder_pool_rollouts_digests BEFORE INSERT ON recorder_pool_rollouts
FOR EACH ROW EXECUTE FUNCTION validate_recorder_rollout_digests();

CREATE OR REPLACE FUNCTION validate_recorder_droplet_rollout_transition() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE r recorder_pool_rollouts%ROWTYPE; latest recorder_worker_rollout_observations%ROWTYPE;
BEGIN
  IF TG_OP='DELETE' THEN
    IF OLD.rollout_generation IS NOT NULL THEN RAISE EXCEPTION 'rollout droplet provenance is immutable'; END IF;
    RETURN OLD;
  END IF;
  IF TG_OP='UPDATE' AND OLD.rollout_generation IS NOT NULL AND (
       OLD.id<>NEW.id OR OLD.name<>NEW.name OR OLD.node_id IS DISTINCT FROM NEW.node_id OR
       ((OLD.do_droplet_id IS DISTINCT FROM NEW.do_droplet_id OR OLD.ip_address IS DISTINCT FROM NEW.ip_address) AND
         NOT (OLD.state='provisioning' AND NEW.state='provisioning' AND OLD.do_droplet_id IS NULL AND NEW.do_droplet_id IS NOT NULL)) OR
       OLD.region<>NEW.region OR OLD.size<>NEW.size OR
       OLD.capacity<>NEW.capacity OR OLD.created_at<>NEW.created_at) THEN
    RAISE EXCEPTION 'rollout droplet identity/provider binding is immutable';
  END IF;
  IF NEW.rollout_generation IS NULL THEN
    IF TG_OP='UPDATE' AND OLD.rollout_generation IS NOT NULL THEN RAISE EXCEPTION 'rollout droplet provenance cannot be removed'; END IF;
    RETURN NEW;
  END IF;
  SELECT * INTO r FROM recorder_pool_rollouts WHERE generation=NEW.rollout_generation;
  IF NOT FOUND OR NEW.desired_build_sha IS DISTINCT FROM r.to_build_sha OR NEW.desired_common_policy_sha256 IS DISTINCT FROM r.common_policy_sha256 OR NEW.capacity<>r.worker_capacity THEN
    RAISE EXCEPTION 'rollout droplet target provenance mismatch';
  END IF;
  IF TG_OP='INSERT' THEN
    IF r.status<>'running' OR NEW.state<>'provisioning' OR NEW.rollout_ordinal<=0 OR NEW.replacement_for_id IS NULL OR NEW.lease_admission_enabled OR
       NEW.name<>('stoarama-rec-rollout-'||NEW.rollout_generation::text||'-'||NEW.rollout_ordinal::text) OR
       NOT EXISTS(SELECT 1 FROM recorder_droplets replaced WHERE replaced.id=NEW.replacement_for_id AND replaced.state='active' AND replaced.lease_admission_enabled) THEN
      RAISE EXCEPTION 'invalid new rollout droplet shape';
    END IF;
  ELSIF OLD.rollout_generation IS NULL THEN
    IF OLD.id<>NEW.id OR OLD.name<>NEW.name OR OLD.node_id IS DISTINCT FROM NEW.node_id OR OLD.do_droplet_id IS DISTINCT FROM NEW.do_droplet_id OR
       OLD.region<>NEW.region OR OLD.size<>NEW.size OR OLD.capacity<>NEW.capacity OR OLD.build_sha<>NEW.build_sha OR OLD.observed_build_sha IS DISTINCT FROM NEW.observed_build_sha OR
       OLD.observed_common_policy_sha256 IS DISTINCT FROM NEW.observed_common_policy_sha256 OR OLD.created_at<>NEW.created_at OR
       r.status NOT IN ('running','stopping') OR OLD.state<>'active' OR NOT OLD.lease_admission_enabled OR NEW.state<>'rollout_draining' OR NEW.rollout_ordinal<>0 OR NEW.replacement_for_id IS NOT NULL OR NEW.lease_admission_enabled THEN
      RAISE EXCEPTION 'invalid baseline rollout attachment';
    END IF;
  ELSE
    IF OLD.rollout_generation<>NEW.rollout_generation OR OLD.rollout_ordinal<>NEW.rollout_ordinal OR
       OLD.replacement_for_id IS DISTINCT FROM NEW.replacement_for_id OR OLD.desired_build_sha<>NEW.desired_build_sha OR
       OLD.desired_common_policy_sha256<>NEW.desired_common_policy_sha256 THEN
      RAISE EXCEPTION 'rollout droplet immutable provenance changed';
    END IF;
    IF r.status NOT IN ('running','stopping') AND OLD.state<>NEW.state THEN
      RAISE EXCEPTION 'terminal rollout cannot mutate droplet lifecycle';
    END IF;
    IF NOT ((OLD.state='provisioning' AND NEW.state IN ('provisioning','standby_ready','destroying','failed','destroyed')) OR
            (OLD.state='standby_ready' AND NEW.state IN ('standby_ready','active','destroying','destroyed')) OR
            (OLD.state='active' AND NEW.state IN ('active','rollout_draining')) OR
            (OLD.state='rollout_draining' AND NEW.state IN ('rollout_draining','destroying','destroyed')) OR
            (OLD.state='destroying' AND NEW.state IN ('destroying','destroyed')) OR
            (OLD.state=NEW.state AND OLD.state IN ('destroyed','failed'))) THEN
      RAISE EXCEPTION 'invalid rollout droplet state transition % -> %',OLD.state,NEW.state;
    END IF;
  END IF;
  IF TG_OP='UPDATE' AND NEW.state='failed' AND OLD.state<>'failed' AND NOT EXISTS(
       SELECT 1 FROM (
         SELECT p.observed_at FROM recorder_rollout_provider_observations p
         WHERE p.rollout_generation=NEW.rollout_generation AND p.phase='reconcile'
           AND NOT EXISTS(SELECT 1 FROM recorder_rollout_provider_objects po WHERE po.observation_id=p.id AND po.name=NEW.name)
         ORDER BY p.observed_at DESC,p.id DESC LIMIT 2
       ) absence HAVING count(*)=2 AND max(observed_at)>pg_catalog.transaction_timestamp()-interval '60 seconds' AND max(observed_at)>min(observed_at)
     ) THEN
    RAISE EXCEPTION 'rollout reservation failure lacks two complete absence observations';
  END IF;
  IF TG_OP='UPDATE' AND NEW.state='destroying' AND OLD.state<>'destroying' AND NOT EXISTS(
       SELECT 1 FROM (
         SELECT p.observed_at FROM recorder_rollout_provider_observations p
         JOIN recorder_rollout_provider_objects po ON po.observation_id=p.id AND po.is_pool AND po.name=NEW.name
         WHERE p.rollout_generation=NEW.rollout_generation AND p.phase='pre_delete'
         ORDER BY p.observed_at DESC,p.id DESC LIMIT 2
       ) presence HAVING count(*)=2 AND max(observed_at)>min(observed_at)
     ) THEN
    RAISE EXCEPTION 'rollout provider destruction lacks two complete presence observations';
  END IF;
  IF TG_OP='UPDATE' AND NEW.state='destroyed' AND OLD.state<>'destroying' AND NOT EXISTS(
       SELECT 1 FROM (
         SELECT p.observed_at FROM recorder_rollout_provider_observations p
         WHERE p.rollout_generation=NEW.rollout_generation AND p.phase IN ('reconcile','pre_delete')
           AND NOT EXISTS(SELECT 1 FROM recorder_rollout_provider_objects po WHERE po.observation_id=p.id AND (po.name=NEW.name OR po.provider_droplet_id=NEW.do_droplet_id))
         ORDER BY p.observed_at DESC,p.id DESC LIMIT 2
       ) absence HAVING count(*)=2 AND max(observed_at)>min(observed_at)
     ) THEN
    RAISE EXCEPTION 'rollout provider absence lacks two complete observations';
  END IF;
  IF TG_OP='UPDATE' AND OLD.rollout_generation IS NOT NULL AND OLD.build_sha IS DISTINCT FROM NEW.build_sha THEN
    SELECT * INTO latest FROM recorder_worker_rollout_observations WHERE droplet_id=NEW.id ORDER BY observed_at DESC,id DESC LIMIT 1;
    IF NOT FOUND OR NEW.build_sha IS DISTINCT FROM latest.executable_build_sha OR latest.observed_at<pg_catalog.transaction_timestamp()-interval '90 seconds' OR latest.observed_at>pg_catalog.transaction_timestamp() THEN
      RAISE EXCEPTION 'rollout build alias is not latest actual worker state';
    END IF;
  END IF;
  IF TG_OP='UPDATE' AND (OLD.observed_build_sha IS DISTINCT FROM NEW.observed_build_sha OR OLD.observed_common_policy_sha256 IS DISTINCT FROM NEW.observed_common_policy_sha256) THEN
    SELECT * INTO latest FROM recorder_worker_rollout_observations WHERE droplet_id=NEW.id ORDER BY observed_at DESC,id DESC LIMIT 1;
    IF NOT FOUND OR latest.observed_at<pg_catalog.transaction_timestamp()-interval '90 seconds' OR latest.observed_at>pg_catalog.transaction_timestamp() OR
       NEW.observed_build_sha IS DISTINCT FROM latest.executable_build_sha OR NEW.observed_common_policy_sha256 IS DISTINCT FROM r.common_policy_sha256 OR
       latest.capacity<>r.worker_capacity OR latest.heartbeat_sec<>r.heartbeat_sec OR latest.poll_sec<>r.poll_sec OR latest.backend_api_origin<>r.backend_api_origin OR
       latest.host_firewall_script_sha256<>r.host_firewall_script_sha256 OR latest.kernel_firewall_policy_sha256<>r.kernel_firewall_policy_sha256 THEN
      RAISE EXCEPTION 'rollout droplet observation is not latest actual worker state';
    END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER trg_recorder_droplets_rollout_transition
BEFORE INSERT OR UPDATE OR DELETE ON recorder_droplets FOR EACH ROW
EXECUTE FUNCTION validate_recorder_droplet_rollout_transition();

CREATE OR REPLACE FUNCTION validate_recorder_worker_observation_digest() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE expected TEXT;
BEGIN
 expected:=pg_catalog.encode(pg_catalog.sha256(recorder_rollout_typed_field('stoarama-cloud-recorder-worker-observation-v1')||
 recorder_rollout_typed_field(NEW.executable_build_sha)||recorder_rollout_typed_field(NEW.capacity::text)||
 recorder_rollout_typed_field(NEW.heartbeat_sec::text)||recorder_rollout_typed_field(NEW.poll_sec::text)||
 recorder_rollout_typed_field(NEW.backend_api_origin)||recorder_rollout_typed_field(NEW.host_firewall_script_sha256)||
 recorder_rollout_typed_field(NEW.kernel_firewall_policy_sha256)),'hex');
 IF NEW.observation_sha256<>expected THEN RAISE EXCEPTION 'recorder worker observation digest mismatch'; END IF;
 IF NOT EXISTS(SELECT 1 FROM recorder_droplets d JOIN recorder_pool_rollouts r ON r.generation=d.rollout_generation AND r.status IN ('running','stopping') WHERE d.id=NEW.droplet_id AND d.node_id=NEW.node_id AND d.name=NEW.worker_id) THEN
   RAISE EXCEPTION 'recorder worker observation binding mismatch';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER trg_recorder_worker_rollout_observations_digest BEFORE INSERT ON recorder_worker_rollout_observations
FOR EACH ROW EXECUTE FUNCTION validate_recorder_worker_observation_digest();

CREATE OR REPLACE FUNCTION validate_recorder_rollout_capacity() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE r recorder_pool_rollouts%ROWTYPE; claimable_count INTEGER; claimable_capacity INTEGER; transitional INTEGER;
BEGIN
  SELECT * INTO r FROM recorder_pool_rollouts WHERE status IN ('running','stopping') OR (status IN ('complete','stopped') AND completed_at=pg_catalog.transaction_timestamp()) ORDER BY generation DESC LIMIT 1;
  IF NOT FOUND THEN RETURN NULL; END IF;
  SELECT count(*),COALESCE(sum(capacity),0) INTO claimable_count,claimable_capacity
    FROM recorder_droplets WHERE state='active' AND lease_admission_enabled;
  SELECT count(*) INTO transitional FROM recorder_droplets WHERE state IN ('provisioning','standby_ready','rollout_draining','draining','destroying');
  IF claimable_count<>r.steady_workers OR claimable_capacity<>r.steady_workers*r.worker_capacity THEN
    RAISE EXCEPTION 'recorder rollout claimable capacity must remain exact';
  END IF;
  IF transitional>r.max_surge THEN RAISE EXCEPTION 'recorder rollout exceeds bounded surge'; END IF;
  IF EXISTS(SELECT 1 FROM recorder_droplets WHERE state<>'active' AND lease_admission_enabled) THEN
    RAISE EXCEPTION 'non-active recorder cannot admit leases';
  END IF;
  IF (r.status='complete' AND EXISTS(SELECT 1 FROM recorder_droplets d WHERE d.state='active' AND d.lease_admission_enabled AND
      (d.rollout_generation IS DISTINCT FROM r.generation OR d.observed_build_sha IS DISTINCT FROM r.to_build_sha OR d.observed_common_policy_sha256 IS DISTINCT FROM r.common_policy_sha256 OR
       NOT EXISTS(SELECT 1 FROM LATERAL (SELECT o.* FROM recorder_worker_rollout_observations o WHERE o.droplet_id=d.id ORDER BY o.observed_at DESC,o.id DESC LIMIT 1) o
         WHERE o.observed_at>=pg_catalog.transaction_timestamp()-interval '90 seconds' AND o.observed_at<=pg_catalog.transaction_timestamp() AND
         o.executable_build_sha=r.to_build_sha AND o.capacity=r.worker_capacity AND o.heartbeat_sec=r.heartbeat_sec AND o.poll_sec=r.poll_sec AND
         o.backend_api_origin=r.backend_api_origin AND o.host_firewall_script_sha256=r.host_firewall_script_sha256 AND
         o.kernel_firewall_policy_sha256=r.kernel_firewall_policy_sha256)))) OR
     (r.status IN ('running','stopping') AND EXISTS(SELECT 1 FROM recorder_droplets d WHERE d.rollout_generation=r.generation AND d.state='active' AND d.lease_admission_enabled AND
      (d.observed_build_sha IS DISTINCT FROM r.to_build_sha OR d.observed_common_policy_sha256 IS DISTINCT FROM r.common_policy_sha256 OR
       NOT EXISTS(SELECT 1 FROM LATERAL (SELECT o.* FROM recorder_worker_rollout_observations o WHERE o.droplet_id=d.id ORDER BY o.observed_at DESC,o.id DESC LIMIT 1) o
         WHERE o.observed_at>=pg_catalog.transaction_timestamp()-interval '90 seconds' AND o.observed_at<=pg_catalog.transaction_timestamp() AND o.executable_build_sha=r.to_build_sha AND
         o.capacity=r.worker_capacity AND o.heartbeat_sec=r.heartbeat_sec AND o.poll_sec=r.poll_sec AND o.backend_api_origin=r.backend_api_origin AND
         o.host_firewall_script_sha256=r.host_firewall_script_sha256 AND o.kernel_firewall_policy_sha256=r.kernel_firewall_policy_sha256)))) THEN
    RAISE EXCEPTION 'claimable rollout recorder lacks fresh exact host observation';
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER trg_recorder_droplets_rollout_capacity
AFTER INSERT OR UPDATE OR DELETE ON recorder_droplets DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_recorder_rollout_capacity();

CREATE OR REPLACE FUNCTION author_recorder_rollout_observed_at() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN NEW.observed_at:=pg_catalog.transaction_timestamp(); RETURN NEW; END $$;
CREATE TRIGGER trg_recorder_provider_observed_at BEFORE INSERT ON recorder_rollout_provider_observations
FOR EACH ROW EXECUTE FUNCTION author_recorder_rollout_observed_at();
CREATE TRIGGER trg_recorder_worker_observed_at BEFORE INSERT ON recorder_worker_rollout_observations
FOR EACH ROW EXECUTE FUNCTION author_recorder_rollout_observed_at();
CREATE CONSTRAINT TRIGGER trg_recorder_pool_rollouts_capacity
AFTER INSERT OR UPDATE ON recorder_pool_rollouts DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_recorder_rollout_capacity();

COMMIT;
