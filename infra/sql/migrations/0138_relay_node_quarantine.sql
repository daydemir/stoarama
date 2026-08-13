CREATE TABLE relay_node_quarantine_events (
  id UUID PRIMARY KEY,
  idempotency_key UUID NOT NULL UNIQUE,
  request_digest TEXT NOT NULL CHECK(request_digest~'^[0-9a-f]{64}$'),
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  node_id BIGINT NOT NULL UNIQUE REFERENCES nodes(id) ON DELETE RESTRICT,
  relay_group_id BIGINT NOT NULL REFERENCES relay_groups(id) ON DELETE RESTRICT,
  relay_max_streams INTEGER NOT NULL CHECK(relay_max_streams BETWEEN 1 AND 20),
  display_name TEXT NOT NULL CHECK(octet_length(display_name) BETWEEN 1 AND 256),
  relay_version TEXT NOT NULL CHECK(octet_length(relay_version) BETWEEN 1 AND 128),
  relay_source_revision TEXT NOT NULL CHECK(relay_source_revision~'^[0-9a-f]{40}$'),
  relay_started_at TIMESTAMPTZ NOT NULL,
  heartbeat_at TIMESTAMPTZ NOT NULL,
  reason_code TEXT NOT NULL CHECK(reason_code IN ('tls_input_incompatibility','runtime_attestation_failure','repeated_no_progress')),
  evidence_sha256 TEXT NOT NULL CHECK(evidence_sha256~'^[0-9a-f]{64}$'),
  operator_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  operator_email TEXT NOT NULL CHECK(octet_length(operator_email) BETWEEN 3 AND 320),
  response_bytes BYTEA NOT NULL CHECK(octet_length(response_bytes) BETWEEN 2 AND 8192),
  created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE FUNCTION validate_relay_node_quarantine_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE n_account BIGINT; n_group BIGINT; n_cap INTEGER; n_name TEXT; n_status TEXT;
        n_version TEXT; n_revision TEXT; n_started TIMESTAMPTZ; n_heartbeat TIMESTAMPTZ;
        operator_ok BOOLEAN; operator_current_email TEXT;
BEGIN
  IF NEW.created_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'relay quarantine event time must be transaction time';
  END IF;
  SELECT account_id,relay_group_id,relay_max_streams,display_name,status,
         capabilities_jsonb->>'relay_version',capabilities_jsonb->>'relay_source_revision',
         (capabilities_jsonb->>'relay_started_at')::timestamptz,last_heartbeat_at
  INTO n_account,n_group,n_cap,n_name,n_status,n_version,n_revision,n_started,n_heartbeat
  FROM nodes WHERE id=NEW.node_id AND node_type='relay' FOR UPDATE;
  SELECT is_operator,email INTO operator_ok,operator_current_email FROM users WHERE id=NEW.operator_user_id FOR SHARE;
  IF n_account IS DISTINCT FROM NEW.account_id OR n_group IS DISTINCT FROM NEW.relay_group_id
     OR n_cap IS DISTINCT FROM NEW.relay_max_streams OR n_name IS DISTINCT FROM NEW.display_name
     OR n_status IS DISTINCT FROM 'disabled' OR n_version IS DISTINCT FROM NEW.relay_version
     OR n_revision IS DISTINCT FROM NEW.relay_source_revision
     OR n_started IS DISTINCT FROM NEW.relay_started_at OR n_heartbeat IS DISTINCT FROM NEW.heartbeat_at
     OR operator_ok IS DISTINCT FROM true
     OR lower(btrim(operator_current_email)) IS DISTINCT FROM lower(btrim(NEW.operator_email)) THEN
    RAISE EXCEPTION 'relay quarantine event does not match disabled node or operator';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER relay_node_quarantine_events_validate
BEFORE INSERT ON relay_node_quarantine_events FOR EACH ROW
EXECUTE FUNCTION validate_relay_node_quarantine_event();

CREATE FUNCTION reject_relay_node_quarantine_event_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'relay node quarantine events are append-only';
END $$;
CREATE TRIGGER relay_node_quarantine_events_immutable
BEFORE UPDATE OR DELETE ON relay_node_quarantine_events FOR EACH ROW
EXECUTE FUNCTION reject_relay_node_quarantine_event_mutation();
CREATE TRIGGER relay_node_quarantine_events_no_truncate
BEFORE TRUNCATE ON relay_node_quarantine_events FOR EACH STATEMENT
EXECUTE FUNCTION reject_relay_node_quarantine_event_mutation();

CREATE FUNCTION fence_quarantined_relay_node() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS(SELECT 1 FROM relay_node_quarantine_events q WHERE q.node_id=OLD.id) THEN
    IF TG_OP='DELETE' THEN
      RAISE EXCEPTION 'quarantined relay node is permanent until typed release';
    END IF;
    IF (to_jsonb(NEW)-ARRAY['last_heartbeat_at','capabilities_jsonb','metadata_jsonb','updated_at'])
       IS DISTINCT FROM
       (to_jsonb(OLD)-ARRAY['last_heartbeat_at','capabilities_jsonb','metadata_jsonb','updated_at']) THEN
      RAISE EXCEPTION 'quarantined relay node permits heartbeat telemetry updates only';
    END IF;
  END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER nodes_quarantine_fence
BEFORE UPDATE OR DELETE ON nodes FOR EACH ROW
EXECUTE FUNCTION fence_quarantined_relay_node();
CREATE FUNCTION reject_nodes_truncate_when_quarantined() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS(SELECT 1 FROM relay_node_quarantine_events) THEN
    RAISE EXCEPTION 'cannot truncate nodes while relay quarantine evidence exists';
  END IF;
  RETURN NULL;
END $$;
CREATE TRIGGER nodes_quarantine_no_truncate
BEFORE TRUNCATE ON nodes FOR EACH STATEMENT
EXECUTE FUNCTION reject_nodes_truncate_when_quarantined();
