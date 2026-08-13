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
  actor_kind TEXT NOT NULL CHECK(actor_kind IN ('operator_session','operator_api_key')),
  operator_user_id BIGINT REFERENCES users(id) ON DELETE RESTRICT,
  operator_api_key_id BIGINT REFERENCES account_api_keys(id) ON DELETE RESTRICT,
  actor_email TEXT NOT NULL CHECK(octet_length(actor_email) BETWEEN 3 AND 320),
  actor_key_prefix TEXT CHECK(actor_key_prefix IS NULL OR octet_length(actor_key_prefix) BETWEEN 1 AND 128),
  response_bytes BYTEA NOT NULL CHECK(octet_length(response_bytes) BETWEEN 2 AND 8192),
  created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE FUNCTION validate_relay_node_quarantine_event() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE n_account BIGINT; n_group BIGINT; n_cap INTEGER; n_name TEXT; n_status TEXT;
        n_version TEXT; n_revision TEXT; n_started TIMESTAMPTZ; n_heartbeat TIMESTAMPTZ;
        operator_ok BOOLEAN; actor_current_email TEXT; actor_current_prefix TEXT;
BEGIN
  IF NEW.created_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'relay quarantine event time must be transaction time';
  END IF;
  EXECUTE format('SELECT account_id,relay_group_id,relay_max_streams,display_name,status,
         capabilities_jsonb->>''relay_version'',capabilities_jsonb->>''relay_source_revision'',
         (capabilities_jsonb->>''relay_started_at'')::timestamptz,last_heartbeat_at
         FROM %I.nodes WHERE id=$1 AND node_type=''relay'' FOR UPDATE',TG_TABLE_SCHEMA)
  INTO n_account,n_group,n_cap,n_name,n_status,n_version,n_revision,n_started,n_heartbeat
  USING NEW.node_id;
  IF (NEW.operator_user_id IS NULL) = (NEW.operator_api_key_id IS NULL) THEN
    RAISE EXCEPTION 'relay quarantine actor identity must be exclusive';
  END IF;
  IF NEW.actor_kind='operator_session' AND NEW.operator_user_id IS NOT NULL AND NEW.actor_key_prefix IS NULL THEN
    EXECUTE format('SELECT is_operator,email FROM %I.users WHERE id=$1 FOR SHARE',TG_TABLE_SCHEMA)
      INTO operator_ok,actor_current_email USING NEW.operator_user_id;
  ELSIF NEW.actor_kind='operator_api_key' AND NEW.operator_api_key_id IS NOT NULL AND NEW.actor_key_prefix IS NOT NULL THEN
    EXECUTE format('SELECT a.role=''admin'',a.email,k.key_prefix
    FROM %I.account_api_keys k JOIN %I.accounts a ON a.id=k.account_id
    WHERE k.id=$1 AND k.revoked_at IS NULL
      AND (k.expires_at IS NULL OR k.expires_at>transaction_timestamp())
      AND a.status=''active'' FOR SHARE OF k,a',TG_TABLE_SCHEMA,TG_TABLE_SCHEMA)
      INTO operator_ok,actor_current_email,actor_current_prefix USING NEW.operator_api_key_id;
  ELSE
    RAISE EXCEPTION 'relay quarantine actor kind does not match identity';
  END IF;
  IF n_account IS DISTINCT FROM NEW.account_id OR n_group IS DISTINCT FROM NEW.relay_group_id
     OR n_cap IS DISTINCT FROM NEW.relay_max_streams OR n_name IS DISTINCT FROM NEW.display_name
     OR n_status IS DISTINCT FROM 'disabled' OR n_version IS DISTINCT FROM NEW.relay_version
     OR n_revision IS DISTINCT FROM NEW.relay_source_revision
     OR n_started IS DISTINCT FROM NEW.relay_started_at OR n_heartbeat IS DISTINCT FROM NEW.heartbeat_at
     OR operator_ok IS DISTINCT FROM true
     OR lower(btrim(actor_current_email)) IS DISTINCT FROM lower(btrim(NEW.actor_email))
     OR actor_current_prefix IS DISTINCT FROM NEW.actor_key_prefix THEN
    RAISE EXCEPTION 'relay quarantine event does not match disabled node or operator';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER relay_node_quarantine_events_validate
BEFORE INSERT ON relay_node_quarantine_events FOR EACH ROW
EXECUTE FUNCTION validate_relay_node_quarantine_event();

CREATE FUNCTION reject_relay_node_quarantine_event_mutation() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'relay node quarantine events are append-only';
END $$;
CREATE TRIGGER relay_node_quarantine_events_immutable
BEFORE UPDATE OR DELETE ON relay_node_quarantine_events FOR EACH ROW
EXECUTE FUNCTION reject_relay_node_quarantine_event_mutation();
CREATE TRIGGER relay_node_quarantine_events_no_truncate
BEFORE TRUNCATE ON relay_node_quarantine_events FOR EACH STATEMENT
EXECUTE FUNCTION reject_relay_node_quarantine_event_mutation();

CREATE FUNCTION fence_quarantined_relay_node() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE quarantined BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.relay_node_quarantine_events q WHERE q.node_id=$1)',TG_TABLE_SCHEMA)
    INTO quarantined USING OLD.id;
  IF quarantined THEN
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
CREATE FUNCTION reject_nodes_truncate_when_quarantined() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE quarantined BOOLEAN;
BEGIN
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.relay_node_quarantine_events)',TG_TABLE_SCHEMA)
    INTO quarantined;
  IF quarantined THEN
    RAISE EXCEPTION 'cannot truncate nodes while relay quarantine evidence exists';
  END IF;
  RETURN NULL;
END $$;
CREATE TRIGGER nodes_quarantine_no_truncate
BEFORE TRUNCATE ON nodes FOR EACH STATEMENT
EXECUTE FUNCTION reject_nodes_truncate_when_quarantined();
