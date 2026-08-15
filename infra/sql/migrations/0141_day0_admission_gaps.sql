-- Two day-zero operator gaps only: authorize a current candidate frame refresh
-- before approval, and recover the immutable state of one known probe order.

CREATE FUNCTION recording_campaign_prepare_authoritative_frame(
  p_account_id BIGINT,p_authority_code TEXT,p_stream_id BIGINT
) RETURNS TABLE(stream_id BIGINT,source_url TEXT,source_page_url TEXT,source_revision_id BIGINT,source_snapshot_sha256 TEXT)
LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE decision_ok BOOLEAN; revision BIGINT; source_row RECORD; digest TEXT;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0));
  PERFORM 1 FROM accounts WHERE id=p_account_id AND status='active' FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'campaign account is not active'; END IF;
  SELECT EXISTS(SELECT 1 FROM recording_campaign_authority_decisions
    WHERE code=p_authority_code AND campaign_key='delivery30-2026q3'
      AND expires_at>recording_campaign_now() AND p_stream_id=ANY(permitted_stream_ids)) INTO decision_ok;
  PERFORM 1 FROM stream_source_revisions WHERE stream_id=p_stream_id ORDER BY id FOR SHARE;
  SELECT s.source_url,COALESCE(s.source_page_url,'') source_page_url,s.updated_at,
    (SELECT max(r.id) FROM stream_source_revisions r WHERE r.stream_id=s.id) source_revision_id
    INTO source_row FROM streams s WHERE s.id=p_stream_id AND s.deleted_at IS NULL FOR SHARE;
  IF decision_ok IS DISTINCT FROM true OR source_row.source_url IS NULL THEN
    RAISE EXCEPTION 'authoritative frame target is not decision-authorized';
  END IF;
  revision:=source_row.source_revision_id;
  digest:=encode(sha256(convert_to(jsonb_build_object(
    'account_id',p_account_id,'authority_code',p_authority_code,'stream_id',p_stream_id,
    'source_revision_id',revision,'source_url_sha256',encode(sha256(convert_to(source_row.source_url,'UTF8')),'hex'),
    'source_page_url_sha256',encode(sha256(convert_to(source_row.source_page_url,'UTF8')),'hex'),
    'source_updated_at_epoch_us',floor(extract(epoch from source_row.updated_at)*1000000)::bigint
  )::text,'UTF8')),'hex');
  RETURN QUERY SELECT p_stream_id,source_row.source_url,source_row.source_page_url,revision,digest;
END $$;

CREATE FUNCTION recording_campaign_authorize_authoritative_frame(
  p_account_id BIGINT,p_authority_code TEXT,p_stream_id BIGINT,p_source_revision_id BIGINT,p_source_snapshot_sha256 TEXT
) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE prepared RECORD;
BEGIN
  SELECT * INTO prepared FROM recording_campaign_prepare_authoritative_frame(p_account_id,p_authority_code,p_stream_id);
  IF prepared.stream_id IS NULL OR prepared.source_revision_id IS DISTINCT FROM NULLIF(p_source_revision_id,0) OR
     prepared.source_snapshot_sha256<>p_source_snapshot_sha256 THEN
    RAISE EXCEPTION 'authoritative frame source snapshot changed';
  END IF;
  RETURN true;
END $$;

CREATE FUNCTION recording_campaign_read_probe_order_status(
  p_account_id BIGINT,p_user_id BIGINT,p_session_id BIGINT,p_credential_sha256 TEXT,p_order_id UUID
) RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE valid BOOLEAN; result JSONB;
BEGIN
  SELECT EXISTS(SELECT 1 FROM account_sessions session
    JOIN users operator ON operator.id=session.user_id
    JOIN memberships membership ON membership.user_id=operator.id AND membership.org_id=session.current_org_id
    JOIN accounts account ON account.id=session.current_org_id
    WHERE session.id=p_session_id AND session.user_id=p_user_id AND session.current_org_id=p_account_id
      AND session.session_hash=p_credential_sha256 AND session.revoked_at IS NULL
      AND session.expires_at>recording_campaign_now() AND operator.is_operator
      AND membership.accepted_at IS NOT NULL AND membership.role IN('owner','admin') AND account.status='active'
    FOR SHARE OF session,operator,membership,account) INTO valid;
  IF valid IS DISTINCT FROM true THEN RAISE EXCEPTION 'campaign account credential is not current'; END IF;
  SELECT jsonb_build_object(
    'order_id',orders.id,'approval_id',orders.approval_id,'stream_id',orders.stream_id,
    'desired_attempts',orders.desired_attempts,'requested_at',orders.requested_at,
    'second_attempt_eligible_at',(SELECT min(e.observed_at)+interval '60 seconds'
      FROM recording_targeted_probe_attempts a JOIN recording_targeted_probe_evidence e ON e.attempt_id=a.id
      WHERE a.order_id=orders.id AND e.result='ok'),
    'attempts',COALESCE((SELECT jsonb_agg(jsonb_build_object(
      'attempt_id',a.id,'attempt_no',a.attempt_no,'started_at',a.started_at,'expires_at',a.expires_at,
      'terminal_result',terminal.result,'terminal_observed_at',terminal.observed_at,
      'evidence_id',e.id,'evidence_result',e.result,'evidence_sha256',e.evidence_sha256,'evidence_observed_at',e.observed_at,
      'presentation_id',presentation.id,'presentation_sha256',presentation.presentation_sha256,'presented_at',presentation.presented_at,
      'review_id',review.id,'review_sha256',review.review_sha256,'reviewed_at',review.reviewed_at
    ) ORDER BY a.attempt_no) FROM recording_targeted_probe_attempts a
      LEFT JOIN recording_targeted_probe_attempt_terminal_events terminal ON terminal.attempt_id=a.id
      LEFT JOIN recording_targeted_probe_evidence e ON e.attempt_id=a.id
      LEFT JOIN recording_targeted_probe_scene_presentations presentation ON presentation.probe_evidence_id=e.id
      LEFT JOIN recording_targeted_probe_scene_reviews review ON review.probe_evidence_id=e.id
      WHERE a.order_id=orders.id),'[]'::jsonb)
  ) INTO result
  FROM recording_targeted_probe_orders orders
  JOIN recording_campaign_admission_approvals approval ON approval.id=orders.approval_id
  WHERE orders.id=p_order_id AND orders.account_id=p_account_id AND orders.requested_by_user_id=p_user_id
    AND approval.deadline_at>recording_campaign_now()
    AND NOT EXISTS(SELECT 1 FROM recording_campaign_admission_reservation_terminal_events terminal
      WHERE terminal.approval_id=approval.id)
  FOR SHARE OF orders,approval;
  IF result IS NULL THEN RAISE EXCEPTION 'targeted probe order is not current'; END IF;
  RETURN result;
END $$;

DO $day0_admission_role_split$
DECLARE install_schema TEXT:=current_schema(); executor_role TEXT:=current_setting('stoarama.campaign_executor_role',true);
  authority_role TEXT:=current_setting('stoarama.campaign_authority_role',true); signature TEXT;
BEGIN
  IF install_schema IS NULL OR install_schema LIKE 'pg_temp%' OR executor_role IS NULL OR authority_role IS NULL OR
     executor_role!~'^[a-z][a-z0-9_]{0,62}$' OR authority_role!~'^[a-z][a-z0-9_]{0,62}$' THEN
    RAISE EXCEPTION '0141 requires the exact 0140 role split';
  END IF;
  FOREACH signature IN ARRAY ARRAY[
    'recording_campaign_prepare_authoritative_frame(bigint,text,bigint)',
    'recording_campaign_authorize_authoritative_frame(bigint,text,bigint,bigint,text)',
    'recording_campaign_read_probe_order_status(bigint,bigint,bigint,text,uuid)'
  ] LOOP
    EXECUTE format('ALTER FUNCTION %I.%s OWNER TO %I',install_schema,signature,authority_role);
    EXECUTE format('ALTER FUNCTION %I.%s SECURITY DEFINER',install_schema,signature);
    EXECUTE format('ALTER FUNCTION %I.%s SET search_path=%I,pg_catalog,pg_temp',install_schema,signature,install_schema);
    EXECUTE format('REVOKE ALL ON FUNCTION %I.%s FROM PUBLIC',install_schema,signature);
    EXECUTE format('GRANT EXECUTE ON FUNCTION %I.%s TO %I',install_schema,signature,executor_role);
  END LOOP;
END
$day0_admission_role_split$;
