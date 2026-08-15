BEGIN;

CREATE TABLE relay_update_intents (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  request_id UUID NOT NULL,
  node_version_at_request TEXT NOT NULL CHECK (node_version_at_request <> ''),
  target_version TEXT NOT NULL CHECK (target_version ~ '^[A-Za-z0-9][A-Za-z0-9._-]*$' AND right(target_version,1) ~ '^[A-Za-z0-9]$'),
  manifest_name TEXT NOT NULL CHECK (manifest_name = 'latest-'||target_version||'.json'),
  manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','completed','superseded')),
  completed_at TIMESTAMPTZ,
  completed_version TEXT,
  superseded_at TIMESTAMPTZ,
  superseded_by_version TEXT,
  requested_by_user_id BIGINT REFERENCES users(id) ON DELETE RESTRICT,
  requested_by_api_key_id BIGINT REFERENCES account_api_keys(id) ON DELETE RESTRICT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  UNIQUE (account_id, request_id),
  CHECK (num_nonnulls(requested_by_user_id, requested_by_api_key_id) = 1),
  CHECK (
    (status='pending' AND completed_at IS NULL AND completed_version IS NULL AND superseded_at IS NULL AND superseded_by_version IS NULL) OR
    (status='completed' AND completed_at IS NOT NULL AND completed_version=target_version AND superseded_at IS NULL AND superseded_by_version IS NULL) OR
    (status='superseded' AND completed_at IS NULL AND completed_version IS NULL AND superseded_at IS NOT NULL AND superseded_by_version IS NOT NULL)
  )
);

CREATE UNIQUE INDEX relay_update_intents_one_pending_per_node
ON relay_update_intents(node_id) WHERE status='pending';

CREATE INDEX relay_update_intents_account_created
ON relay_update_intents(account_id, created_at DESC, id DESC);

CREATE FUNCTION guard_relay_update_intent() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  IF OLD.status <> 'pending' OR NEW.status NOT IN ('completed','superseded') OR
     NEW.id IS DISTINCT FROM OLD.id OR NEW.account_id IS DISTINCT FROM OLD.account_id OR
     NEW.node_id IS DISTINCT FROM OLD.node_id OR NEW.request_id IS DISTINCT FROM OLD.request_id OR
     NEW.node_version_at_request IS DISTINCT FROM OLD.node_version_at_request OR
     NEW.target_version IS DISTINCT FROM OLD.target_version OR
     NEW.manifest_name IS DISTINCT FROM OLD.manifest_name OR
     NEW.manifest_sha256 IS DISTINCT FROM OLD.manifest_sha256 OR
     NEW.requested_by_user_id IS DISTINCT FROM OLD.requested_by_user_id OR
     NEW.requested_by_api_key_id IS DISTINCT FROM OLD.requested_by_api_key_id OR
     NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'relay update intent is immutable';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER relay_update_intent_immutable
BEFORE UPDATE ON relay_update_intents
FOR EACH ROW EXECUTE FUNCTION guard_relay_update_intent();

REVOKE ALL ON TABLE relay_update_intents FROM PUBLIC;
REVOKE ALL ON SEQUENCE relay_update_intents_id_seq FROM PUBLIC;
REVOKE ALL ON FUNCTION guard_relay_update_intent() FROM PUBLIC;

COMMIT;
