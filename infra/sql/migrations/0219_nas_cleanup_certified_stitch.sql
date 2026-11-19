-- Keep the producer proof immutable. The connection-13 certificate and the
-- released holds are the authoritative retirement disposition.
CREATE VIEW nas_stitch_r2_certified_raw AS
 SELECT h.id AS hour_id, hold.catalog_clip_id AS clip_id, h.recording_id, r.stream_id,
  hold.bucket, hold.object_key, hold.etag, hold.size_bytes, hold.sha256,
  raw.display_path AS relative_path, raw.clip_start_at AS start_at, raw.clip_end_at AS end_at,
  raw.purged_at, cert.certified_at,
  encode(sha256(convert_to(rtrim(raw.endpoint,'/'),'UTF8')),'hex') AS endpoint_sha256,
  jsonb_build_object('hour_id',h.id,'attempt',cert.attempt,'plan_sha256',cert.plan_sha256,
   'manifest_sha256',cert.manifest_sha256,'output',(certified->'readback')-'fsynced') AS certificate
 FROM recording_clips raw
 JOIN recordings r ON r.id=raw.recording_id AND r.account_id=47
 JOIN nas_stitch_r2_hours h ON h.recording_id=raw.recording_id
  AND h.hour_at=date_trunc('hour',raw.clip_start_at,'UTC') AND h.state='certified'
 JOIN nas_stitch_r2_certificates cert ON cert.hour_id=h.id AND cert.connection_id=13
  AND cert.attempt=h.attempt AND cert.plan_sha256=h.plan_sha256 AND cert.manifest_sha256=h.staged_manifest_sha256
 JOIN nas_stitch_r2_input_holds hold ON hold.hour_id=h.id AND hold.catalog_clip_id=raw.id
  AND hold.released_at=cert.certified_at AND hold.bucket=raw.bucket AND hold.object_key=raw.object_key
  AND hold.etag=raw.etag AND hold.size_bytes=raw.size_bytes AND hold.sha256=lower(raw.sha256)
 CROSS JOIN LATERAL jsonb_array_elements(h.staged_manifest->'outputs') AS output
 JOIN nas_stitch_r2_stage_objects stage ON stage.hour_id=h.id AND stage.attempt=h.attempt AND stage.state='staged'
  AND output @> jsonb_build_object('relative_path',stage.relative_path,'sha256',stage.sha256,'size_bytes',stage.size_bytes)
 CROSS JOIN LATERAL jsonb_array_elements(cert.receipt->'objects') AS certified
 WHERE h.plan->'clips' @> jsonb_build_array(jsonb_build_object('source_id',raw.id,'relative_path',raw.display_path,
  'size_bytes',hold.size_bytes,'sha256',hold.sha256))
  AND EXISTS(SELECT 1 FROM jsonb_array_elements(h.plan->'clips') AS clip
   WHERE clip->'source_id'=to_jsonb(raw.id) AND (clip->>'start_at')::timestamptz=raw.clip_start_at
    AND (clip->>'end_at')::timestamptz=raw.clip_end_at)
  AND output->'inputs' @> jsonb_build_array(jsonb_build_object('clip_id',raw.id,'size_bytes',hold.size_bytes,
   'sha256',hold.sha256,'repaired',false,'retirement_held',true))
  AND certified->>'object_key'=stage.object_key AND certified->>'etag'=stage.etag
  AND coalesce(certified->>'version_id','')=coalesce(stage.version_id,'')
  AND certified->'readback' @> jsonb_build_object('relative_path',stage.relative_path,'sha256',stage.sha256,
   'size_bytes',stage.size_bytes,'fsynced',true)
  AND stage.relative_path NOT IN (raw.display_path,raw.display_path||'.stoarama.json')
  AND NOT EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds live WHERE live.bucket=hold.bucket
   AND live.object_key=hold.object_key AND live.released_at IS NULL);

-- Never create a cleanup reservation before B2 finishes. B2 needs its exact
-- physical raw custody on the NAS through the R2 deletion and absence check.
CREATE VIEW nas_cleanup_certified_stitch_raw AS
 SELECT c.*,(intent.custody->>'SidecarSize')::bigint AS sidecar_size,intent.custody->>'SidecarSHA' AS sidecar_sha
 FROM nas_stitch_r2_certified_raw c
 JOIN r2_keep28_intents intent ON intent.bucket=c.bucket AND intent.object_key=c.object_key
  AND intent.source_id=c.clip_id AND intent.stream_id=c.stream_id AND intent.size_bytes=c.size_bytes
  AND btrim(intent.etag,'"')=btrim(c.etag,'"') AND intent.endpoint_sha256=c.endpoint_sha256
  AND intent.created_at>=c.certified_at
  AND intent.custody @> jsonb_build_object('ConnectionID',13,'SourceID',c.clip_id,
   'Path',c.relative_path,'SizeBytes',c.size_bytes,'SHA',c.sha256)
 JOIN r2_retirement_reservations reserved ON reserved.kind='raw' AND reserved.source_id=c.clip_id
  AND reserved.bucket=c.bucket AND reserved.object_key=c.object_key AND reserved.size_bytes=c.size_bytes
  AND reserved.sha256=c.sha256 AND reserved.manifest_sha256=intent.manifest_sha256 AND reserved.retirement_class=''
 JOIN r2_keep28_attempts attempt ON attempt.bucket=intent.bucket AND attempt.object_key=intent.object_key
  AND attempt.issued_at>=intent.created_at
 JOIN r2_object_retirements retired ON retired.kind='raw' AND retired.source_id=c.clip_id
  AND retired.stream_id=c.stream_id AND retired.bucket=c.bucket AND retired.object_key=c.object_key
  AND retired.reason='keep28_exact_nas' AND retired.manifest_sha256=intent.manifest_sha256
  AND retired.policy_sha256=intent.policy_sha256 AND retired.choice=intent.choice
  AND retired.size_bytes=c.size_bytes AND retired.sha256=c.sha256 AND NOT retired.already_absent
 JOIN r2_keep28_events absent ON absent.bucket=intent.bucket AND absent.object_key=intent.object_key
  AND absent.event='absent' AND absent.observed_at>=attempt.issued_at
 WHERE c.purged_at IS NOT NULL
  AND intent.custody->>'SidecarPath'=c.relative_path||'.stoarama.json'
  AND (intent.custody->>'SidecarSize')::bigint BETWEEN 1 AND 1048576
  AND intent.custody->>'SidecarSHA' ~ '^[a-f0-9]{64}$'
  AND NOT EXISTS(SELECT 1 FROM nas_cleanup_incident_sources hold WHERE hold.clip_id=c.clip_id)
  AND NOT EXISTS(SELECT 1 FROM nas_cleanup_incident_files hold WHERE hold.kind='raw' AND hold.source_id=c.clip_id)
  AND NOT EXISTS(SELECT 1 FROM nas_cleanup_incident_roster hold WHERE hold.bucket=c.bucket AND hold.object_key=c.object_key);

CREATE FUNCTION nas_cleanup_exact_certified_stitch(item jsonb) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT item->>'permit_reason'='durham_stitch_certified' AND EXISTS(
  SELECT 1 FROM nas_cleanup_certified_stitch_raw c
  WHERE c.clip_id=(item->>'source_id')::bigint
   AND item @> jsonb_build_object('kind','raw','source_ids',to_jsonb(ARRAY[c.clip_id]),
    'recording_id',c.recording_id,'stream_id',c.stream_id,'bucket',c.bucket,'object_key',c.object_key,
    'relative_path',c.relative_path,'size_bytes',c.size_bytes,'sha256',c.sha256,'sidecar_required',true)
   AND item->'source_ids'=to_jsonb(ARRAY[c.clip_id])
   AND (item->>'start_at')::timestamptz=c.start_at AND (item->>'end_at')::timestamptz=c.end_at
   AND item->'stitch_certificate'=c.certificate
   AND item->'stitch_raw_sidecar'=jsonb_build_object('size_bytes',c.sidecar_size,'sha256',c.sidecar_sha))
$$;

CREATE FUNCTION nas_cleanup_certified_stitch_staging_absent(certificate jsonb) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_stitch_r2_certificates c
  WHERE c.hour_id=(certificate->>'hour_id')::bigint AND c.attempt=(certificate->>'attempt')::int
   AND c.plan_sha256=certificate->>'plan_sha256' AND c.manifest_sha256=certificate->>'manifest_sha256'
   AND EXISTS(SELECT 1 FROM nas_stitch_r2_stage_objects o WHERE o.hour_id=c.hour_id AND o.attempt=c.attempt AND o.state='staged')
   AND NOT EXISTS(SELECT 1 FROM nas_stitch_r2_stage_objects o
    LEFT JOIN nas_stitch_r2_stage_retirements retired ON retired.object_id=o.id
    WHERE o.hour_id=c.hour_id AND o.attempt=c.attempt AND retired.absence_observed_at IS NULL))
$$;

-- Preserve 0201's locks and ordinary veto. Only an exact, finished B2 item
-- can make an already-reserved original available to this cleanup plan.
CREATE OR REPLACE FUNCTION guard_nas_cleanup_r2_retirement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'R2 cleanup admission requires READ COMMITTED' USING ERRCODE='55000'; END IF;
 IF TG_TABLE_NAME='nas_cleanup_plans' THEN
  PERFORM c.id FROM recording_clips c WHERE c.id IN
   (SELECT (i->>'source_id')::bigint FROM jsonb_array_elements(NEW.manifest) i WHERE i->>'kind'='raw')
   ORDER BY c.id FOR KEY SHARE;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(NEW.manifest) i
   WHERE (i->>'permit_reason'='durham_stitch_certified' OR EXISTS(
    SELECT 1 FROM r2_retirement_reservations r WHERE r.kind=i->>'kind' AND r.source_id=(i->>'source_id')::bigint))
   AND NOT (NEW.account_id=47 AND NEW.connection_id=13 AND coalesce(nas_cleanup_exact_certified_stitch(i),false))) THEN
   RAISE EXCEPTION 'retention protected: R2 retirement pending or completed without exact stitch custody'; END IF;
 ELSE
  PERFORM id FROM recording_clips WHERE id=NEW.clip_id FOR KEY SHARE;
  IF EXISTS(SELECT 1 FROM r2_retirement_reservations WHERE kind='raw' AND source_id=NEW.clip_id)
   AND NOT EXISTS(SELECT 1 FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) i
    WHERE p.id=NEW.plan_id AND p.account_id=47 AND p.connection_id=13 AND (i->>'source_id')::bigint=NEW.clip_id
     AND nas_cleanup_exact_certified_stitch(i)) THEN
   RAISE EXCEPTION 'retention protected: R2 recovery copy retirement pending or completed without exact stitch custody'; END IF;
 END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION guard_nas_cleanup_certified_stitch_permit() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE item jsonb; plan nas_cleanup_plans;
BEGIN
 IF NEW.state='leased' AND NEW.item->>'permit_reason'='durham_stitch_certified'
  AND (NEW.lease_connection_id IS DISTINCT FROM 13 OR NEW.lease_client_version IS DISTINCT FROM 'r2-copy-v7-certified-stitch') THEN
  RAISE EXCEPTION 'certified stitch cleanup lease requires the reviewed connection-13 client';
 END IF;
 IF NEW.state<>'delete_issued' AND (NOT NEW.delete_ever_issued OR (TG_OP='UPDATE' AND OLD.delete_ever_issued)) THEN
  RETURN NEW;
 END IF;
 SELECT * INTO plan FROM nas_cleanup_plans WHERE id=NEW.plan_id FOR SHARE NOWAIT;
 item:=plan.manifest->(NEW.ordinal-1);
 IF item->>'permit_reason' IS DISTINCT FROM 'durham_stitch_certified' THEN
  IF NEW.recovery='nas_stitch_certified' THEN RAISE EXCEPTION 'certified stitch recovery requires a typed plan'; END IF;
  RETURN NEW;
 END IF;
 IF current_setting('transaction_isolation')<>'read committed' OR TG_OP<>'UPDATE'
  OR OLD.delete_ever_issued OR OLD.state<>'leased' OR NOT NEW.delete_ever_issued OR NEW.state<>'delete_issued'
  OR OLD.lease_expires_at IS NULL OR OLD.lease_expires_at<=clock_timestamp()
  OR plan.account_id<>47 OR plan.connection_id<>13 OR plan.mode<>'enabled'
  OR plan.approved_until IS NULL OR plan.approved_until<=clock_timestamp()
  OR OLD.lease_client_version IS DISTINCT FROM 'r2-copy-v7-certified-stitch'
  OR NEW.lease_client_version IS DISTINCT FROM 'r2-copy-v7-certified-stitch'
  OR NEW.item IS DISTINCT FROM item OR NEW.recovery<>'nas_stitch_certified'
  OR NEW.permit_expires_at IS NULL OR NEW.permit_expires_at<=clock_timestamp()
  OR (OLD.proof-'parent_members_sha256') IS DISTINCT FROM (NEW.proof-'parent_members_sha256')
  OR NEW.proof->'stitch_replacement' IS DISTINCT FROM item->'stitch_certificate'->'output'
  OR NEW.proof#>'{sidecar,sha256}' IS DISTINCT FROM item#>'{stitch_raw_sidecar,sha256}'
  OR NEW.proof#>'{sidecar,identity,0}' IS DISTINCT FROM item#>'{stitch_raw_sidecar,size_bytes}'
  OR NOT nas_cleanup_certified_stitch_staging_absent(item->'stitch_certificate')
  OR NOT coalesce(nas_cleanup_exact_certified_stitch(item),false) THEN
   RAISE EXCEPTION 'NAS raw permit requires exact confirmed stitch custody and completed keep-28 retirement';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_certified_stitch_permit BEFORE INSERT OR UPDATE OF state,delete_ever_issued ON nas_cleanup_items
 FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_certified_stitch_permit();

-- The new immutable client retains the existing nonce-bound storage budget.
ALTER TABLE connections DROP CONSTRAINT connections_nas_storage_client_version_check,
 ADD CONSTRAINT connections_nas_storage_client_version_check
 CHECK(nas_storage_client_version IN ('','r2-copy-v5','r2-copy-v6','r2-copy-v7-certified-stitch'));
