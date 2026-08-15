BEGIN;

CREATE TABLE relay_update_intents (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE RESTRICT,
  request_id UUID NOT NULL,
  node_version_at_request TEXT NOT NULL CHECK(node_version_at_request<>'') ,
  runtime_target TEXT NOT NULL CHECK(runtime_target~'^[a-z0-9]+-[a-z0-9]+$'),
  target_version TEXT NOT NULL CHECK(target_version~'^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$'),
  manifest_name TEXT NOT NULL CHECK(manifest_name='latest-'||target_version||'.json'),
  manifest_sha256 TEXT NOT NULL CHECK(manifest_sha256~'^[0-9a-f]{64}$'),
  relay_artifact_sha256 TEXT NOT NULL CHECK(relay_artifact_sha256~'^[0-9a-f]{64}$'),
  expected_installed_binary_sha256 TEXT NOT NULL CHECK(expected_installed_binary_sha256~'^[0-9a-f]{64}$'),
  status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN('pending','completed')),
  requested_by_user_id BIGINT REFERENCES users(id) ON DELETE RESTRICT,
  requested_by_api_key_id BIGINT REFERENCES account_api_keys(id) ON DELETE RESTRICT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
  completed_at TIMESTAMPTZ,
  completed_node_token_id BIGINT REFERENCES node_tokens(id) ON DELETE RESTRICT,
  completed_claim_generation BIGINT,
  applied_manifest_sha256 TEXT,
  applied_artifact_sha256 TEXT,
  installed_binary_sha256 TEXT,
  applied_at TIMESTAMPTZ,
  UNIQUE(account_id,request_id),
  CHECK(num_nonnulls(requested_by_user_id,requested_by_api_key_id)=1),
  CHECK(
    (status='pending' AND completed_at IS NULL AND completed_node_token_id IS NULL AND completed_claim_generation IS NULL AND
      applied_manifest_sha256 IS NULL AND applied_artifact_sha256 IS NULL AND installed_binary_sha256 IS NULL AND applied_at IS NULL) OR
    (status='completed' AND completed_at IS NOT NULL AND completed_node_token_id IS NOT NULL AND completed_claim_generation IS NOT NULL AND
      applied_manifest_sha256=manifest_sha256 AND applied_artifact_sha256=relay_artifact_sha256 AND
      installed_binary_sha256~'^[0-9a-f]{64}$' AND applied_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX relay_update_intents_one_pending_per_node ON relay_update_intents(node_id) WHERE status='pending';
CREATE INDEX relay_update_intents_account_created ON relay_update_intents(account_id,created_at DESC,id DESC);

CREATE FUNCTION guard_relay_update_intent() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE owner_name TEXT;
BEGIN
  SELECT pg_catalog.pg_get_userbyid(c.relowner) INTO owner_name
  FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname=TG_TABLE_SCHEMA AND c.relname=TG_TABLE_NAME;
  IF TG_OP IN('DELETE','TRUNCATE') THEN
    RAISE EXCEPTION 'relay update intent audit is immutable';
  END IF;
  IF current_user<>owner_name THEN
    RAISE EXCEPTION 'relay update intent requires typed authority';
  END IF;
  IF TG_OP='INSERT' THEN
    IF NEW.status<>'pending' OR NEW.created_at IS DISTINCT FROM transaction_timestamp() THEN
      RAISE EXCEPTION 'relay update intent creation is not canonical';
    END IF;
    RETURN NEW;
  END IF;
  IF OLD.status<>'pending' OR NEW.status<>'completed' OR
     (NEW.id,NEW.account_id,NEW.node_id,NEW.request_id,NEW.node_version_at_request,NEW.runtime_target,
      NEW.target_version,NEW.manifest_name,NEW.manifest_sha256,NEW.relay_artifact_sha256,
      NEW.expected_installed_binary_sha256,
      NEW.requested_by_user_id,NEW.requested_by_api_key_id,NEW.created_at) IS DISTINCT FROM
     (OLD.id,OLD.account_id,OLD.node_id,OLD.request_id,OLD.node_version_at_request,OLD.runtime_target,
      OLD.target_version,OLD.manifest_name,OLD.manifest_sha256,OLD.relay_artifact_sha256,
      OLD.expected_installed_binary_sha256,
      OLD.requested_by_user_id,OLD.requested_by_api_key_id,OLD.created_at) OR
     NEW.completed_at IS DISTINCT FROM transaction_timestamp() THEN
    RAISE EXCEPTION 'relay update intent transition is not canonical';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER relay_update_intent_guard
BEFORE INSERT OR UPDATE OR DELETE ON relay_update_intents
FOR EACH ROW EXECUTE FUNCTION guard_relay_update_intent();
CREATE TRIGGER relay_update_intent_truncate_guard
BEFORE TRUNCATE ON relay_update_intents
FOR EACH STATEMENT EXECUTE FUNCTION guard_relay_update_intent();

CREATE FUNCTION guard_relay_update_node_state() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE pending_exists BOOLEAN; typed_executor BOOLEAN;
BEGIN
  IF NEW.status IS NOT DISTINCT FROM OLD.status THEN RETURN NEW; END IF;
  EXECUTE pg_catalog.format('SELECT EXISTS(SELECT 1 FROM %I.relay_update_intents WHERE node_id=$1 AND status=''pending'')',TG_TABLE_SCHEMA)
    INTO pending_exists USING OLD.id;
  SELECT pg_catalog.has_function_privilege(session_user,
    pg_catalog.to_regprocedure(pg_catalog.format('%I.relay_update_intent_create(uuid,bigint,bigint,text,text,text,text,text,text,text,bigint,bigint,bigint,text)',TG_TABLE_SCHEMA)),
    'EXECUTE') INTO typed_executor;
  IF pending_exists AND typed_executor IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'relay update drain state requires typed authority';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER relay_update_node_state_guard
BEFORE UPDATE OF status ON nodes
FOR EACH ROW EXECUTE FUNCTION guard_relay_update_node_state();

CREATE FUNCTION relay_update_authorize_account(
  p_account_id BIGINT,p_user_id BIGINT,p_session_id BIGINT,p_api_key_id BIGINT,p_credential_sha256 TEXT
) RETURNS BOOLEAN LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE authorized BOOLEAN;
BEGIN
  IF p_credential_sha256!~'^[0-9a-f]{64}$' OR num_nonnulls(p_session_id,p_api_key_id)<>1 THEN RETURN false; END IF;
  IF p_session_id IS NOT NULL THEN
    SELECT EXISTS(
      SELECT 1 FROM account_sessions session
      JOIN accounts account ON account.id=session.current_org_id
      JOIN users actor ON actor.id=session.user_id
      JOIN memberships membership ON membership.user_id=actor.id AND membership.org_id=account.id
      WHERE session.id=p_session_id AND session.user_id=p_user_id AND session.current_org_id=p_account_id
        AND session.session_hash=p_credential_sha256 AND session.revoked_at IS NULL
        AND session.expires_at>transaction_timestamp() AND account.status='active' AND membership.role='owner'
      FOR SHARE OF session,account,actor,membership
    ) INTO authorized;
  ELSE
    SELECT EXISTS(
      SELECT 1 FROM account_api_keys key JOIN accounts account ON account.id=key.account_id
      WHERE key.id=p_api_key_id AND key.account_id=p_account_id AND key.secret_hash=p_credential_sha256
        AND key.revoked_at IS NULL AND (key.expires_at IS NULL OR key.expires_at>transaction_timestamp())
        AND account.status='active' AND NOT('stoarama.pull'=ANY(key.scopes))
      FOR SHARE OF key,account
    ) INTO authorized;
  END IF;
  RETURN authorized;
END $$;

CREATE FUNCTION relay_update_intent_replay(
  p_account_id BIGINT,p_node_id BIGINT,p_request_id UUID,p_expected_version TEXT,
  p_user_id BIGINT,p_session_id BIGINT,p_api_key_id BIGINT,p_credential_sha256 TEXT
) RETURNS JSONB LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE intent relay_update_intents%ROWTYPE;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('recording-worker-claim-v1',0));
  IF NOT relay_update_authorize_account(p_account_id,p_user_id,p_session_id,p_api_key_id,p_credential_sha256) THEN
    RAISE EXCEPTION 'relay update operator authorization changed';
  END IF;
  SELECT * INTO intent FROM relay_update_intents WHERE account_id=p_account_id AND request_id=p_request_id FOR SHARE;
  IF intent.id IS NULL THEN RETURN NULL; END IF;
  IF intent.node_id<>p_node_id OR intent.target_version<>p_expected_version THEN
    RAISE EXCEPTION 'relay update request replay differs';
  END IF;
  RETURN jsonb_build_object('request_id',intent.request_id,'node_id',intent.node_id,'target_version',intent.target_version,
    'manifest_name',intent.manifest_name,'manifest_sha256',intent.manifest_sha256,
    'relay_artifact_sha256',intent.relay_artifact_sha256,
    'installed_binary_sha256',intent.expected_installed_binary_sha256,'status',intent.status);
END $$;

CREATE FUNCTION relay_update_intent_create(
  p_request_id UUID,p_account_id BIGINT,p_node_id BIGINT,p_node_version TEXT,p_runtime_target TEXT,
  p_target_version TEXT,p_manifest_name TEXT,p_manifest_sha256 TEXT,p_relay_artifact_sha256 TEXT,
  p_expected_installed_binary_sha256 TEXT,
  p_user_id BIGINT,p_session_id BIGINT,p_api_key_id BIGINT,p_credential_sha256 TEXT
) RETURNS JSONB LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE existing relay_update_intents%ROWTYPE; node_row RECORD; live_count BIGINT; inserted relay_update_intents%ROWTYPE;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('recording-worker-claim-v1',0));
  IF NOT relay_update_authorize_account(p_account_id,p_user_id,p_session_id,p_api_key_id,p_credential_sha256) THEN
    RAISE EXCEPTION 'relay update operator authorization changed';
  END IF;
  SELECT * INTO existing FROM relay_update_intents WHERE account_id=p_account_id AND request_id=p_request_id FOR SHARE;
  IF existing.id IS NOT NULL THEN
    IF (existing.node_id,existing.node_version_at_request,existing.runtime_target,existing.target_version,
        existing.manifest_name,existing.manifest_sha256,existing.relay_artifact_sha256) IS DISTINCT FROM
       (p_node_id,p_node_version,p_runtime_target,p_target_version,p_manifest_name,p_manifest_sha256,p_relay_artifact_sha256) OR
       existing.expected_installed_binary_sha256<>p_expected_installed_binary_sha256 THEN
      RAISE EXCEPTION 'relay update request replay differs';
    END IF;
    RETURN jsonb_build_object('created',false,'intent',jsonb_build_object(
      'request_id',existing.request_id,'node_id',existing.node_id,'target_version',existing.target_version,
      'manifest_name',existing.manifest_name,'manifest_sha256',existing.manifest_sha256,
      'relay_artifact_sha256',existing.relay_artifact_sha256,
      'installed_binary_sha256',existing.expected_installed_binary_sha256,'status',existing.status));
  END IF;
  SELECT id,node_type,status,last_heartbeat_at,COALESCE(capabilities_jsonb->>'relay_version','') version,
    COALESCE(capabilities_jsonb->>'relay_update_runtime_target','') runtime_target,
    COALESCE(capabilities_jsonb->>'relay_update_intents_v1'='true',false) supports
    INTO node_row FROM nodes WHERE id=p_node_id AND account_id=p_account_id FOR UPDATE;
  IF node_row.id IS NULL OR node_row.node_type<>'relay' OR node_row.status<>'active' OR
     node_row.last_heartbeat_at<transaction_timestamp()-interval '120 seconds' OR node_row.supports IS DISTINCT FROM true OR
     node_row.version<>p_node_version OR node_row.runtime_target<>p_runtime_target OR p_target_version=p_node_version THEN
    RAISE EXCEPTION 'relay node is not fresh, idle, and updater-capable';
  END IF;
  SELECT count(*) INTO live_count FROM recording_jobs job
  WHERE (job.status='leased' AND job.lease_owner='node:'||p_node_id::text AND job.lease_expires_at>transaction_timestamp()) OR
        (job.status='pending' AND job.handoff_owner='node:'||p_node_id::text AND job.handoff_until>transaction_timestamp());
  IF live_count<>0 OR EXISTS(SELECT 1 FROM relay_update_intents WHERE node_id=p_node_id AND status='pending') THEN
    RAISE EXCEPTION 'relay node has live capture authority';
  END IF;
  UPDATE nodes SET status='disabled' WHERE id=p_node_id;
  INSERT INTO relay_update_intents(account_id,node_id,request_id,node_version_at_request,runtime_target,target_version,
    manifest_name,manifest_sha256,relay_artifact_sha256,expected_installed_binary_sha256,requested_by_user_id,requested_by_api_key_id)
  VALUES(p_account_id,p_node_id,p_request_id,p_node_version,p_runtime_target,p_target_version,p_manifest_name,
    p_manifest_sha256,p_relay_artifact_sha256,p_expected_installed_binary_sha256,p_user_id,p_api_key_id) RETURNING * INTO inserted;
  RETURN jsonb_build_object('created',true,'intent',jsonb_build_object(
    'request_id',inserted.request_id,'node_id',inserted.node_id,'target_version',inserted.target_version,
    'manifest_name',inserted.manifest_name,'manifest_sha256',inserted.manifest_sha256,
    'relay_artifact_sha256',inserted.relay_artifact_sha256,
    'installed_binary_sha256',inserted.expected_installed_binary_sha256,'status',inserted.status));
END $$;

CREATE FUNCTION relay_update_intent_heartbeat(
  p_node_id BIGINT,p_node_token_id BIGINT,p_claim_generation BIGINT,p_credential_sha256 TEXT,
  p_reported_version TEXT,p_supports_intents BOOLEAN,p_result JSONB
) RETURNS JSONB LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE intent relay_update_intents%ROWTYPE; result_request UUID; installed_sha TEXT; applied_time TIMESTAMPTZ;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('recording-worker-claim-v1',0));
  IF p_credential_sha256!~'^[0-9a-f]{64}$' OR NOT EXISTS(
    SELECT 1 FROM node_tokens token JOIN nodes node ON node.id=token.node_id
    JOIN recording_worker_claim_heads head ON head.node_id=node.id
    WHERE token.id=p_node_token_id AND token.node_id=p_node_id AND token.secret_hash=p_credential_sha256
      AND token.revoked_at IS NULL AND token.recording_claim_generation=p_claim_generation
      AND token.recording_claim_purpose='claim_current' AND head.generation=p_claim_generation
      AND head.claim_token_id=token.id AND head.state='enabled' AND node.node_type='relay'
    FOR SHARE OF token,node,head
  ) THEN RAISE EXCEPTION 'relay update node authorization changed'; END IF;
  SELECT * INTO intent FROM relay_update_intents WHERE node_id=p_node_id AND status='pending' FOR UPDATE;
  IF intent.id IS NULL THEN
    IF p_result IS NULL THEN RETURN '{}'::jsonb; END IF;
    result_request:=NULLIF(p_result->>'request_id','')::uuid;
    SELECT * INTO intent FROM relay_update_intents WHERE node_id=p_node_id AND request_id=result_request AND status='completed' FOR SHARE;
    IF intent.id IS NULL OR p_reported_version<>intent.target_version OR
       p_result->>'target_version'<>intent.target_version OR p_result->>'manifest_name'<>intent.manifest_name OR
       p_result->>'manifest_sha256'<>intent.applied_manifest_sha256 OR
       p_result->>'relay_artifact_sha256'<>intent.applied_artifact_sha256 OR
       p_result->>'installed_binary_sha256'<>intent.installed_binary_sha256 OR
       NULLIF(p_result->>'applied_at','')::timestamptz IS DISTINCT FROM intent.applied_at THEN RETURN '{}'::jsonb; END IF;
    RETURN jsonb_build_object('ack_request_id',intent.request_id);
  END IF;
  IF p_result IS NULL THEN
    IF p_supports_intents IS DISTINCT FROM true OR p_reported_version<>intent.node_version_at_request THEN RETURN '{}'::jsonb; END IF;
    RETURN jsonb_build_object('intent',jsonb_build_object('request_id',intent.request_id,'node_id',intent.node_id,
      'target_version',intent.target_version,'manifest_name',intent.manifest_name,'manifest_sha256',intent.manifest_sha256,
      'relay_artifact_sha256',intent.relay_artifact_sha256,
      'installed_binary_sha256',intent.expected_installed_binary_sha256,'status',intent.status));
  END IF;
  result_request:=NULLIF(p_result->>'request_id','')::uuid;
  installed_sha:=p_result->>'installed_binary_sha256';
  applied_time:=NULLIF(p_result->>'applied_at','')::timestamptz;
  IF p_supports_intents IS DISTINCT FROM true OR result_request<>intent.request_id OR p_reported_version<>intent.target_version OR
     p_result->>'target_version'<>intent.target_version OR p_result->>'manifest_name'<>intent.manifest_name OR
     p_result->>'manifest_sha256'<>intent.manifest_sha256 OR
     p_result->>'relay_artifact_sha256'<>intent.relay_artifact_sha256 OR installed_sha!~'^[0-9a-f]{64}$' OR
     installed_sha<>intent.expected_installed_binary_sha256 OR
     applied_time IS NULL OR applied_time<intent.created_at OR applied_time>transaction_timestamp()+interval '30 seconds' THEN
    RAISE EXCEPTION 'relay update applied proof differs';
  END IF;
  UPDATE relay_update_intents SET status='completed',completed_at=transaction_timestamp(),
    completed_node_token_id=p_node_token_id,completed_claim_generation=p_claim_generation,
    applied_manifest_sha256=manifest_sha256,applied_artifact_sha256=relay_artifact_sha256,
    installed_binary_sha256=installed_sha,applied_at=applied_time WHERE id=intent.id;
  UPDATE nodes SET status='active' WHERE id=p_node_id AND status='disabled';
  RETURN jsonb_build_object('ack_request_id',intent.request_id);
END $$;

DO $relay_update_roles$
DECLARE s TEXT:=current_schema(); runtime_role TEXT:=NULLIF(current_setting('stoarama.campaign_runtime_role',true),'');
  executor_role TEXT:=NULLIF(current_setting('stoarama.campaign_executor_role',true),'');
  authority_role TEXT:=NULLIF(current_setting('stoarama.campaign_authority_role',true),''); signature TEXT;
  authority_functions TEXT[]:=ARRAY[
    'guard_relay_update_intent()','guard_relay_update_node_state()',
    'relay_update_authorize_account(bigint,bigint,bigint,bigint,text)',
    'relay_update_intent_replay(bigint,bigint,uuid,text,bigint,bigint,bigint,text)',
    'relay_update_intent_create(uuid,bigint,bigint,text,text,text,text,text,text,text,bigint,bigint,bigint,text)',
    'relay_update_intent_heartbeat(bigint,bigint,bigint,text,text,boolean,jsonb)'];
BEGIN
  IF runtime_role IS NULL OR executor_role IS NULL OR authority_role IS NULL THEN
    RAISE EXCEPTION '0142 requires the 0140 split database roles';
  END IF;
  EXECUTE format('ALTER TABLE %I.relay_update_intents OWNER TO %I',s,authority_role);
  EXECUTE format('ALTER SEQUENCE %I.relay_update_intents_id_seq OWNER TO %I',s,authority_role);
  EXECUTE format('REVOKE ALL ON TABLE %I.relay_update_intents FROM PUBLIC,%I,%I',s,runtime_role,executor_role);
  EXECUTE format('REVOKE ALL ON SEQUENCE %I.relay_update_intents_id_seq FROM PUBLIC,%I,%I',s,runtime_role,executor_role);
  FOREACH signature IN ARRAY authority_functions LOOP
    EXECUTE format('ALTER FUNCTION %I.%s OWNER TO %I',s,signature,authority_role);
    EXECUTE format('ALTER FUNCTION %I.%s SECURITY DEFINER',s,signature);
    EXECUTE format('ALTER FUNCTION %I.%s SET search_path=%I,pg_catalog,pg_temp',s,signature,s);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.%s FROM PUBLIC,%I,%I',s,signature,runtime_role,executor_role);
  END LOOP;
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.relay_update_intent_replay(bigint,bigint,uuid,text,bigint,bigint,bigint,text) TO %I',s,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.relay_update_intent_create(uuid,bigint,bigint,text,text,text,text,text,text,text,bigint,bigint,bigint,text) TO %I',s,executor_role);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %I.relay_update_intent_heartbeat(bigint,bigint,bigint,text,text,boolean,jsonb) TO %I',s,executor_role);
  EXECUTE format('GRANT SELECT ON TABLE %I.account_api_keys TO %I',s,authority_role);
  EXECUTE format('GRANT UPDATE(id) ON TABLE %I.account_api_keys TO %I',s,authority_role);
  EXECUTE format('GRANT UPDATE(status) ON TABLE %I.nodes TO %I',s,authority_role);
END;
$relay_update_roles$;

COMMIT;
