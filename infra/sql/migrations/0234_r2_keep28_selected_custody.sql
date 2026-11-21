-- A selected verifier hashes an exact NAS file in place. Bind its authenticated
-- result to the catalog when the B2 executor consumes it. Historical COPY block
-- rows remain immutable.
CREATE VIEW r2_keep28_selected_custody AS
 SELECT i.id AS verify_item_id,p.id AS verify_plan_id,p.plan_sha256,
        c.id AS source_id,c.bucket,c.object_key,c.display_path AS relative_path,
        i.size_bytes,i.sha256,i.completed_at
 FROM nas_selected_verify_items i
 JOIN nas_selected_verify_plans p ON p.id=i.plan_id
 JOIN recording_clips c ON c.id=i.source_id
 JOIN recordings r ON r.id=c.recording_id
 CROSS JOIN LATERAL (SELECT p.manifest->(i.ordinal-1) entry) m
 WHERE p.connection_id=13 AND p.account_id=47 AND r.account_id=47
 AND i.kind='raw' AND i.state='match' AND i.attempts>0 AND i.outcome_error=''
 AND i.leased_at>=p.created_at AND i.completed_at>=i.leased_at
 AND i.completed_at<=clock_timestamp() AND c.purged_at IS NULL
 AND c.display_path=i.relative_path AND c.size_bytes=i.size_bytes AND lower(c.sha256)=i.sha256
 AND m.entry->>'kind'=i.kind AND m.entry->>'source_id'=i.source_id::text
 AND m.entry->>'relative_path'=i.relative_path AND m.entry->>'size_bytes'=i.size_bytes::text
 AND m.entry->>'sha256'=i.sha256 AND m.entry->>'r2_source'=c.object_key
 AND NOT EXISTS (
  SELECT 1 FROM nas_inventory_files n WHERE n.connection_id=13 AND n.clip_id=i.source_id
   AND n.server_received_at>i.completed_at
   AND (n.state IS DISTINCT FROM 'present' OR n.relative_path IS DISTINCT FROM i.relative_path
    OR n.size_bytes IS DISTINCT FROM i.size_bytes OR n.sha256 IS DISTINCT FROM i.sha256)
 )
 AND NOT EXISTS (
  SELECT 1 FROM nas_selected_verify_items newer
  JOIN nas_selected_verify_plans np ON np.id=newer.plan_id
  WHERE np.connection_id=13 AND np.account_id=47 AND newer.kind='raw'
    AND newer.source_id=i.source_id AND newer.completed_at>i.completed_at
    AND newer.state IN ('missing','conflict','unverified')
 );

ALTER TABLE r2_retirement_reservations ADD COLUMN selected_verify_item_id bigint
 REFERENCES nas_selected_verify_items(id);

CREATE FUNCTION r2_keep28_copy_blocks(p_bucket text,p_key text,p_receipt bigint)
RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_r2_copy_items q
  WHERE (q.state<>'verified' OR q.blocked_reason<>'')
    AND q.item->>'bucket'=p_bucket AND q.item->>'object_key'=p_key
    AND NOT (q.state='blocked' AND q.blocked_reason='sidecar_conflict'
     AND q.connection_id=13 AND q.kind='raw' AND EXISTS (
       SELECT 1 FROM r2_keep28_selected_custody v WHERE v.verify_item_id=p_receipt
        AND v.bucket=p_bucket AND v.object_key=p_key AND v.source_id=q.source_id
        AND v.relative_path=q.item->>'relative_path' AND v.sha256=q.item->>'sha256'
        AND v.size_bytes::text=q.item->>'size_bytes' AND v.completed_at>q.updated_at
        AND v.completed_at>clock_timestamp()-interval '24 hours'
     )))
$$;

CREATE OR REPLACE FUNCTION guard_r2_copy_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM lock_nas_r2_copy_key(NEW.bucket,NEW.object_key);
 IF NEW.selected_verify_item_id IS NOT NULL THEN
  PERFORM 1 FROM r2_keep28_selected_custody v
   WHERE v.verify_item_id=NEW.selected_verify_item_id
    AND v.source_id=NEW.source_id AND v.bucket=NEW.bucket AND v.object_key=NEW.object_key
    AND v.size_bytes=NEW.size_bytes AND v.sha256=NEW.sha256
    AND v.completed_at>clock_timestamp()-interval '24 hours'
    AND EXISTS(SELECT 1 FROM r2_keep28_intents i WHERE i.bucket=NEW.bucket AND i.object_key=NEW.object_key
      AND i.source_id=NEW.source_id AND i.size_bytes=NEW.size_bytes AND i.manifest_sha256=NEW.manifest_sha256
      AND i.custody->>'VerifyItemID'=v.verify_item_id::text AND i.custody->>'VerifyPlanSHA'=v.plan_sha256);
  IF NOT FOUND OR NEW.kind<>'raw' OR NEW.retirement_class<>'' THEN
   RAISE EXCEPTION 'selected NAS custody receipt is absent, stale or outside retirement scope';
  END IF;
 END IF;
 IF r2_keep28_copy_blocks(NEW.bucket,NEW.object_key,NEW.selected_verify_item_id) THEN
  RAISE EXCEPTION 'COPY custody pending or blocked: R2 key protected';
 END IF;
 RETURN NEW;
END $$;
