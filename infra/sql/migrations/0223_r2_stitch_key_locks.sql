-- Stitch steps and R2 retirement conflict only on the same R2 key or staged
-- object. Retirement executors keep their own outer lock 7343226011173; no
-- stitch step takes it any more.

-- A hold written under 0217's trigger owns 1173/1189 but no per-key lock, and
-- a ledger insert under the new trigger cannot see it before it commits. Drain
-- every in-flight hold write before either trigger changes.
LOCK TABLE nas_stitch_r2_input_holds IN SHARE MODE;

-- Hold admission and every retirement ledger insert meet on this per-key lock.
-- It is not lock_nas_r2_copy_key: inverse wire commits its attempt on a second
-- connection while its first still owns that key, so a ledger trigger must not
-- wait for, or try, the copy key.
CREATE FUNCTION nas_stitch_r2_hold_lock(b text,k text) RETURNS bigint LANGUAGE sql IMMUTABLE AS $$
 SELECT hashtextextended(jsonb_build_array(b,k)::text,7343226011221);
$$;

-- Every multi-key copy-key locker takes distinct hashes in ascending order, so
-- certification and receipt never deadlock with each other, whatever the
-- collation. Without wait it stops at the first busy key and returns false;
-- the caller then rolls back everything it holds.
CREATE FUNCTION lock_nas_r2_copy_keys(b text[],k text[],wait boolean) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE h bigint;
BEGIN
 FOR h IN SELECT DISTINCT hashtextextended(jsonb_build_array(x.b,x.k)::text,7343226011200)
  FROM unnest(b,k) x(b,k) ORDER BY 1 LOOP
  IF wait THEN PERFORM pg_advisory_xact_lock(h);
  ELSIF NOT pg_try_advisory_xact_lock(h) THEN RETURN false;
  END IF;
 END LOOP;
 RETURN true;
END $$;

-- Admission waits only for the same key. 1189 still serializes whole imports
-- with hand-written hold SQL, so at most one transaction takes hold locks
-- exclusively; ledger inserts only try them shared and never wait. An intent
-- precedes every attempt by foreign key, so an attempt is vetoed through it.
CREATE OR REPLACE FUNCTION guard_nas_stitch_r2_hold() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'stitch input hold is durable'; END IF;
 IF TG_OP='UPDATE' THEN
  IF ROW(NEW.hour_id,NEW.bucket,NEW.object_key,NEW.catalog_clip_id,NEW.sha256,NEW.size_bytes,NEW.etag)
     IS DISTINCT FROM ROW(OLD.hour_id,OLD.bucket,OLD.object_key,OLD.catalog_clip_id,OLD.sha256,OLD.size_bytes,OLD.etag)
   OR OLD.released_at IS NOT NULL OR NEW.released_at IS NULL
   OR NOT EXISTS(SELECT 1 FROM nas_stitch_r2_certificates c
    WHERE c.hour_id=OLD.hour_id AND c.certified_at=NEW.released_at) THEN
   RAISE EXCEPTION 'stitch input release requires the exact durable certificate';
  END IF;
  RETURN NEW;
 END IF;
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'stitch hold admission requires read committed' USING ERRCODE='55000';
 END IF;
 PERFORM pg_advisory_xact_lock(7343226011189);
 PERFORM pg_advisory_xact_lock(nas_stitch_r2_hold_lock(NEW.bucket,NEW.object_key));
 IF NEW.released_at IS NOT NULL OR NOT EXISTS(
  SELECT 1 FROM nas_stitch_r2_hours h
  JOIN nas_stitch_scopes s ON s.id=h.scope_id
  JOIN recordings r ON r.id=h.recording_id AND r.account_id=47 AND r.stream_id=s.stream_id
  JOIN recording_clips c ON c.id=NEW.catalog_clip_id AND c.recording_id=r.id
  JOIN nas_inventory_files f ON f.connection_id=13 AND f.clip_id=c.id AND f.state='present'
  WHERE h.id=NEW.hour_id AND h.state='pending' AND h.attempt=0 AND c.purged_at IS NULL
   AND c.bucket=NEW.bucket AND c.object_key=NEW.object_key AND c.etag=NEW.etag
   AND lower(c.sha256)=NEW.sha256 AND c.size_bytes=NEW.size_bytes
   AND f.sha256=NEW.sha256 AND f.size_bytes=NEW.size_bytes AND f.relative_path=c.display_path
   AND date_trunc('hour',c.clip_start_at,'UTC')=h.hour_at
   AND c.clip_start_at>=s.start_at AND c.clip_end_at<=s.end_at
 ) THEN
  RAISE EXCEPTION 'stitch input hold lacks exact R2 and NAS catalog identity';
 END IF;
 IF EXISTS(SELECT 1 FROM r2_retirement_reservations r WHERE r.bucket=NEW.bucket AND r.object_key=NEW.object_key)
  OR EXISTS(SELECT 1 FROM r2_inverse_intents r WHERE r.bucket=NEW.bucket AND r.object_key=NEW.object_key)
  OR EXISTS(SELECT 1 FROM r2_keep28_intents r WHERE r.bucket=NEW.bucket AND r.object_key=NEW.object_key)
  OR EXISTS(SELECT 1 FROM r2_object_retirements r WHERE r.bucket=NEW.bucket AND r.object_key=NEW.object_key) THEN
  RAISE EXCEPTION 'stitch input already has R2 retirement history';
 END IF;
 RETURN NEW;
END $$;

-- Executors skip a key on exactly these two outcomes: 55P03 'stitch input
-- hold admission is busy' and 55000 'active R2 stitch input hold'.
CREATE OR REPLACE FUNCTION guard_r2_retirement_stitch_input() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'R2 retirement ledger requires READ COMMITTED' USING ERRCODE='55000';
 END IF;
 IF NOT pg_try_advisory_xact_lock_shared(nas_stitch_r2_hold_lock(NEW.bucket,NEW.object_key)) THEN
  RAISE EXCEPTION 'stitch input hold admission is busy' USING ERRCODE='55P03';
 END IF;
 IF EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds h
  WHERE h.bucket=NEW.bucket AND h.object_key=NEW.object_key AND h.released_at IS NULL) THEN
  RAISE EXCEPTION 'active R2 stitch input hold' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;

-- Locks the hour's live input clip/inventory pairs in clip order. Without
-- wait it skips busy rows and reports false; the caller then rolls back.
CREATE FUNCTION lock_nas_stitch_r2_inputs(hour bigint,wait boolean) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE locked bigint;
BEGIN
 EXECUTE 'SELECT c.id FROM nas_stitch_r2_input_holds i
  JOIN recording_clips c ON c.id=i.catalog_clip_id
  JOIN nas_inventory_files f ON f.connection_id=13 AND f.clip_id=c.id
  WHERE i.hour_id=$1 AND i.released_at IS NULL ORDER BY c.id FOR SHARE OF c,f'
  ||CASE WHEN wait THEN '' ELSE ' SKIP LOCKED' END USING hour;
 GET DIAGNOSTICS locked=ROW_COUNT;
 RETURN locked=(SELECT count(*) FROM nas_stitch_r2_input_holds i WHERE i.hour_id=hour AND i.released_at IS NULL);
END $$;

-- Same order as inverse reserve and wire: epoch, then copy keys, then rows. A
-- same-key reservation finds the key busy, or sees every input released.
CREATE OR REPLACE FUNCTION add_nas_stitch_r2_certificate() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected jsonb;
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'NAS stitch certification requires READ COMMITTED' USING ERRCODE='55000';
 END IF;
 PERFORM 1 FROM r2_inverse_ke_epoch FOR SHARE;
 PERFORM lock_nas_r2_copy_keys(array_agg(bucket),array_agg(object_key),true)
  FROM nas_stitch_r2_input_holds WHERE hour_id=NEW.hour_id AND released_at IS NULL;
 IF NOT lock_nas_stitch_r2_inputs(NEW.hour_id,true) THEN
  RAISE EXCEPTION 'NAS stitch input custody pairs could not be locked';
 END IF;
 IF NOT nas_stitch_r2_has_exact_input_set(NEW.hour_id) THEN
  RAISE EXCEPTION 'NAS stitch input coverage changed';
 END IF;
 expected:=nas_stitch_r2_certificate_receipt(NEW.hour_id,NEW.attempt,NEW.plan_sha256,NEW.manifest_sha256);
 IF expected IS NULL OR NEW.receipt IS DISTINCT FROM expected THEN
  RAISE EXCEPTION 'NAS stitch certificate lacks exact pulled output coverage';
 END IF;
 UPDATE nas_stitch_r2_hours SET state='certified'
 WHERE id=NEW.hour_id AND state='staged' AND attempt=NEW.attempt
  AND plan_sha256=NEW.plan_sha256 AND staged_manifest_sha256=NEW.manifest_sha256;
 IF NOT FOUND THEN RAISE EXCEPTION 'NAS stitch certificate does not match staged result'; END IF;
 RETURN NEW;
END $$;
