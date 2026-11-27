-- A verified raw R2 survivor must not be retired or have its identity changed
-- once relied on by a NAS day plan. This is a durable reservation primitive,
-- not execution authority. The r2prune wire lane must participate before NAS
-- day activation can resume: its read-only preflight alone cannot close a race.
CREATE TABLE nas_cleanup_day_survivor_holds (
 bucket text NOT NULL CHECK(bucket<>''),
 object_key text NOT NULL CHECK(object_key<>''),
 clip_id bigint NOT NULL UNIQUE REFERENCES recording_clips(id),
 unit_id bigint NOT NULL REFERENCES nas_cleanup_day_units(id),
 etag text NOT NULL CHECK(etag<>'' AND etag=btrim(etag,'"')),
 version_id text NOT NULL, -- empty only for a store without version IDs; ETag still required
 size_bytes bigint NOT NULL CHECK(size_bytes>0),
 sha256 text NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
 verified_at timestamptz NOT NULL,
 verify_until timestamptz NOT NULL CHECK(verify_until=verified_at+interval '15 minutes'),
 PRIMARY KEY(bucket,object_key)
);

-- Retirement's existing per-key lock orders a committed survivor against every
-- durable retirement intent/attempt. No automatic expiry of the hold itself:
-- a lost NAS receipt may mean the survivor is the only remaining copy.
CREATE FUNCTION guard_nas_cleanup_day_survivor() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'NAS day survivor hold is durable'; END IF;
 IF current_setting('transaction_isolation')<>'read committed' OR
    NOT pg_try_advisory_xact_lock(nas_stitch_r2_hold_lock(NEW.bucket,NEW.object_key)) THEN
  RAISE EXCEPTION 'NAS day survivor admission is busy' USING ERRCODE='55P03';
 END IF;
 IF NEW.verified_at>clock_timestamp() OR NEW.verify_until<=clock_timestamp() OR NOT EXISTS(
  SELECT 1 FROM nas_cleanup_day_units u JOIN nas_cleanup_day_plans p ON p.id=u.plan_id
  JOIN recording_clips c ON c.id=NEW.clip_id JOIN recordings r ON r.id=c.recording_id
  JOIN nas_media_keep20_windows w ON w.stream_id=r.stream_id AND w.keep_everything
  JOIN nas_inventory_files f ON f.connection_id=p.connection_id AND f.clip_id=c.id AND f.state='present'
  WHERE u.id=NEW.unit_id AND p.mode='dry_run' AND u.state='pending' AND u.attempt=0
    AND p.created_at<=NEW.verified_at AND r.account_id=p.account_id AND r.stream_id=u.stream_id
    AND c.purged_at IS NULL AND c.bucket=NEW.bucket AND c.object_key=NEW.object_key
    AND btrim(c.etag,'"')=NEW.etag AND c.size_bytes=NEW.size_bytes AND lower(c.sha256)=NEW.sha256
    AND f.relative_path=c.display_path AND f.size_bytes=NEW.size_bytes AND lower(f.sha256)=NEW.sha256
    AND EXISTS(SELECT 1 FROM nas_media_keep20_epoch WHERE connection_id=p.connection_id)
    AND c.clip_end_at>c.clip_start_at AND NOT (c.clip_start_at<w.end_at AND c.clip_end_at>w.start_at)
    AND (u.local_date <= (w.start_at AT TIME ZONE w.timezone)::date-2
      OR u.local_date >= ((w.end_at-interval '1 microsecond') AT TIME ZONE w.timezone)::date+2)
    AND NOT nas_cleanup_day_clip_held(c.id,true) AND NOT nas_cleanup_day_dir_held(u.relative_dir,true)
    AND EXISTS(SELECT 1 FROM jsonb_array_elements(u.names) n
       WHERE c.display_path=u.relative_dir||'/'||(n->>'name') AND (n->>'size_bytes')::bigint=NEW.size_bytes)
  FOR SHARE OF p,u,c,f NOWAIT
 ) THEN
  RAISE EXCEPTION 'NAS day survivor lacks fresh exact plan and inventory identity';
 END IF;
 IF EXISTS(SELECT 1 FROM r2_retirement_reservations x WHERE (x.bucket=NEW.bucket AND x.object_key=NEW.object_key) OR (x.kind='raw' AND x.source_id=NEW.clip_id))
  OR EXISTS(SELECT 1 FROM r2_object_retirements x WHERE (x.bucket=NEW.bucket AND x.object_key=NEW.object_key) OR (x.kind='raw' AND x.source_id=NEW.clip_id))
  OR EXISTS(SELECT 1 FROM r2_inverse_intents x WHERE (x.bucket=NEW.bucket AND x.object_key=NEW.object_key) OR x.source_id=NEW.clip_id)
  OR EXISTS(SELECT 1 FROM r2_keep28_intents x WHERE (x.bucket=NEW.bucket AND x.object_key=NEW.object_key) OR x.source_id=NEW.clip_id) THEN
  RAISE EXCEPTION 'NAS day survivor has R2 retirement history';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_day_survivor_guard BEFORE INSERT OR UPDATE OR DELETE ON nas_cleanup_day_survivor_holds
 FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_day_survivor();
CREATE TRIGGER nas_cleanup_day_survivor_no_truncate BEFORE TRUNCATE ON nas_cleanup_day_survivor_holds
 FOR EACH STATEMENT EXECUTE FUNCTION guard_nas_cleanup_day_survivor();

-- A ledger writer uses the same lock (0223), so it either precedes the hold
-- and makes admission fail, or sees the committed hold and refuses the key.
CREATE FUNCTION guard_r2_retirement_day_survivor() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' OR
    NOT pg_try_advisory_xact_lock_shared(nas_stitch_r2_hold_lock(NEW.bucket,NEW.object_key)) THEN
  RAISE EXCEPTION 'NAS day survivor admission is busy' USING ERRCODE='55P03';
 END IF;
 IF EXISTS(SELECT 1 FROM nas_cleanup_day_survivor_holds h WHERE h.bucket=NEW.bucket AND h.object_key=NEW.object_key) THEN
  RAISE EXCEPTION 'active NAS day survivor hold' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['r2_retirement_reservations','r2_object_retirements',
  'r2_inverse_intents','r2_inverse_attempts','r2_keep28_intents','r2_keep28_attempts'] LOOP
  EXECUTE format('CREATE TRIGGER r2_day_survivor_guard BEFORE INSERT ON %I'
    ' FOR EACH ROW EXECUTE FUNCTION guard_r2_retirement_day_survivor()',t);
 END LOOP;
END $$;

CREATE FUNCTION guard_nas_cleanup_day_survivor_clip() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' AND ROW(NEW.purged_at,NEW.recording_id,NEW.storage_destination_id,NEW.endpoint,NEW.bucket,NEW.object_key,
      NEW.etag,NEW.sha256,NEW.size_bytes,NEW.display_path,NEW.clip_start_at,NEW.clip_end_at)
    IS NOT DISTINCT FROM ROW(OLD.purged_at,OLD.recording_id,OLD.storage_destination_id,OLD.endpoint,OLD.bucket,OLD.object_key,
      OLD.etag,OLD.sha256,OLD.size_bytes,OLD.display_path,OLD.clip_start_at,OLD.clip_end_at) THEN
  RETURN NEW;
 END IF;
 IF current_setting('transaction_isolation')<>'read committed' OR
    NOT pg_try_advisory_xact_lock_shared(nas_stitch_r2_hold_lock(OLD.bucket,OLD.object_key)) THEN
  RAISE EXCEPTION 'NAS day survivor admission is busy' USING ERRCODE='55P03';
 END IF;
 IF EXISTS(SELECT 1 FROM nas_cleanup_day_survivor_holds h WHERE h.clip_id=OLD.id) THEN
  RAISE EXCEPTION 'active NAS day survivor identity hold';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_cleanup_day_survivor_clip_guard BEFORE UPDATE OF purged_at,recording_id,storage_destination_id,endpoint,bucket,object_key,etag,sha256,size_bytes,display_path,clip_start_at,clip_end_at OR DELETE ON recording_clips
 FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_day_survivor_clip();

-- The current R2 preflight can see a committed hold. This is necessary but
-- not sufficient: a preflight that ran before admission may still be on wire.
CREATE OR REPLACE FUNCTION nas_cleanup_r2_file_reserved(reserved_kind text,reserved_source bigint)
RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT (reserved_kind='raw' AND EXISTS(SELECT 1 FROM nas_cleanup_day_survivor_holds h WHERE h.clip_id=reserved_source))
 OR EXISTS(SELECT 1 FROM nas_cleanup_plans p
 CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n)
 LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n
 WHERE nas_cleanup_manifest_keys(p.manifest) @> ARRAY[reserved_kind||':'||reserved_source::text]
   AND e.value->>'kind'=reserved_kind AND (e.value->>'source_id')::bigint=reserved_source
   AND (p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL) AND NOT EXISTS(SELECT 1 FROM nas_cleanup_r2_effective_releases r WHERE r.item_id=i.id))
$$;
