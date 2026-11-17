-- B1 raw-only inverse retirement. No activation or deletion by migration.
-- 0200 supplies the exact COPY-key reservation/import lock and veto.
SELECT 'lock_nas_r2_copy_key(text,text)'::regprocedure;
CREATE TABLE r2_inverse_intents (
 bucket text NOT NULL, object_key text NOT NULL,
 endpoint_sha256 text NOT NULL CHECK(endpoint_sha256 ~ '^[a-f0-9]{64}$'),
 stream_id bigint NOT NULL CHECK(stream_id>0),
 source_id bigint NOT NULL CHECK(source_id>0),
 size_bytes bigint NOT NULL CHECK(size_bytes>0),
 etag text NOT NULL CHECK(etag<>''), last_modified timestamptz NOT NULL,
 manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[a-f0-9]{64}$'),
 policy_sha256 text NOT NULL CHECK(policy_sha256='060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811'),
 picks_sha256 text NOT NULL CHECK(picks_sha256 ~ '^[a-f0-9]{64}$'),
 drain_evidence_sha256 text NOT NULL CHECK(drain_evidence_sha256 ~ '^[a-f0-9]{64}$'),
	choice text NOT NULL CHECK(choice IN ('none','st','keep')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bucket,object_key), UNIQUE(source_id)
);
CREATE INDEX r2_inverse_intents_created ON r2_inverse_intents(created_at);
CREATE TABLE r2_inverse_events (
 bucket text NOT NULL,object_key text NOT NULL,event text NOT NULL CHECK(event='absent'),
 observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bucket,object_key,event),
 FOREIGN KEY(bucket,object_key) REFERENCES r2_inverse_intents(bucket,object_key)
);
CREATE TRIGGER r2_inverse_intent_immutable BEFORE UPDATE OR DELETE ON r2_inverse_intents
 FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();
CREATE TRIGGER r2_inverse_event_immutable BEFORE UPDATE OR DELETE ON r2_inverse_events
 FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();
-- A verifier may issue wire authority for an intent only once, across restarts.
CREATE TABLE r2_inverse_attempts (
 bucket text NOT NULL,object_key text NOT NULL,issued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bucket,object_key),FOREIGN KEY(bucket,object_key) REFERENCES r2_inverse_intents(bucket,object_key)
);
CREATE TRIGGER r2_inverse_attempt_immutable BEFORE UPDATE OR DELETE ON r2_inverse_attempts
 FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();
CREATE TRIGGER r2_inverse_attempt_no_truncate BEFORE TRUNCATE ON r2_inverse_attempts
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_object_retirement();

-- Derive live delivery closure, without a separately refreshed source cache.
CREATE INDEX r2_inverse_delivery_key ON delivery_keep_holds(object_key);
CREATE INDEX r2_inverse_output_sources ON recording_collation_outputs USING gin(source_clip_ids);
CREATE FUNCTION r2_inverse_delivery_held(i bigint,b text,k text,p text,sha text,n bigint) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT delivery_nas_held('raw',i,p,sha,n)
 OR EXISTS(SELECT 1 FROM delivery_keep_holds WHERE bucket=b AND object_key=k)
 OR EXISTS(SELECT 1 FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id
  WHERE o.source_clip_ids @> ARRAY[i] AND (
   EXISTS(SELECT 1 FROM delivery_nas_source_holds d WHERE d.clip_id=ANY(o.source_clip_ids))
   OR EXISTS(SELECT 1 FROM delivery_keep_holds d WHERE d.object_key=o.object_key)
   OR EXISTS(SELECT 1 FROM delivery_keep_holds d WHERE d.relative_path<>'' AND d.relative_path IN(o.nas_relative_path,regexp_replace(o.nas_relative_path,'^joined/','')))
   OR EXISTS(SELECT 1 FROM delivery_keep_holds d WHERE d.sha256<>'' AND d.sha256=o.sha256 AND d.size_bytes=o.size_bytes)
   OR EXISTS(SELECT 1 FROM delivery_keep_output_dependencies d WHERE d.output_id=o.id AND d.recording_id=h.recording_id)))
$$;

-- Minute buckets conservatively charge up to one extra minute, never less than
-- the previous 24h. Intent bytes remain charged even if the wire result is lost.
CREATE TABLE r2_inverse_budget_minutes (
 bucket text NOT NULL,minute timestamptz NOT NULL,charged_bytes bigint NOT NULL CHECK(charged_bytes>0),
 PRIMARY KEY(bucket,minute)
);

-- Existing ordinary reservations protect cleanup and COPY admissions after
-- process loss. Also prevent new joined references/scopes from adopting them.
CREATE FUNCTION guard_r2_inverse_joined_reference() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_TABLE_NAME='recording_joined_source_snapshots' THEN
  IF EXISTS(SELECT 1 FROM r2_retirement_reservations WHERE kind='raw' AND source_id=NEW.clip_id) THEN
   RAISE EXCEPTION 'R2 retirement intent protects joined source'; END IF;
 ELSE
  IF EXISTS(SELECT 1 FROM recording_clips c JOIN r2_retirement_reservations r ON r.kind='raw' AND r.source_id=c.id
   WHERE c.recording_id=NEW.recording_id AND c.recording_job_id=NEW.recording_job_id AND c.id<=NEW.high_water_clip_id) THEN
   RAISE EXCEPTION 'R2 retirement intent protects joined scope'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_inverse_joined_source BEFORE INSERT OR UPDATE ON recording_joined_source_snapshots
 FOR EACH ROW EXECUTE FUNCTION guard_r2_inverse_joined_reference();
CREATE TRIGGER r2_inverse_joined_snapshot_scope BEFORE INSERT OR UPDATE ON recording_joined_snapshot_scopes
 FOR EACH ROW EXECUTE FUNCTION guard_r2_inverse_joined_reference();
CREATE TRIGGER r2_inverse_joined_dry_scope BEFORE INSERT OR UPDATE ON recording_joined_dry_run_scopes
 FOR EACH ROW EXECUTE FUNCTION guard_r2_inverse_joined_reference();

-- Metadata uses a fail-fast epoch-row barrier, without upgrading any legacy
-- NAS advisory lock. B1 takes its shared barrier FIRST, before other locks.
-- purged_at-only publication is excluded: it executes existing purge vetoes.
CREATE FUNCTION fence_r2_inverse_protection() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'R2 protection admission requires READ COMMITTED' USING ERRCODE='55000'; END IF;
 PERFORM 1 FROM r2_inverse_ke_epoch FOR UPDATE NOWAIT;
 RETURN NULL;
END $$;
CREATE TRIGGER r2_inverse_source_identity_fence BEFORE UPDATE OF id,recording_id,recording_job_id,bucket,object_key,etag,size_bytes,sha256,display_path,clip_start_at,clip_end_at OR DELETE ON recording_clips
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_recording_identity_fence BEFORE UPDATE OF stream_id,cron_timezone,account_id ON recordings
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_stream_identity_fence BEFORE UPDATE OF local_timezone ON streams
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
-- COPY latches serialize only with their exact R2 key, preserving A1's
-- independent-connection reservation contract and unrelated copy throughput.
-- Both INSERT directions must see the commit that preceded their key lock.
CREATE FUNCTION require_r2_inverse_copy_read_committed() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'COPY/retirement admission requires READ COMMITTED' USING ERRCODE='55000'; END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER r2_inverse_copy_insert_isolation BEFORE INSERT ON nas_r2_copy_items
 FOR EACH STATEMENT EXECUTE FUNCTION require_r2_inverse_copy_read_committed();
CREATE TRIGGER r2_inverse_reservation_insert_isolation BEFORE INSERT ON r2_retirement_reservations
 FOR EACH STATEMENT EXECUTE FUNCTION require_r2_inverse_copy_read_committed();
CREATE FUNCTION fence_r2_inverse_copy_key() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'R2 COPY protection admission requires READ COMMITTED' USING ERRCODE='55000'; END IF;
 IF NEW.item->>'bucket' IS DISTINCT FROM OLD.item->>'bucket'
 OR NEW.item->>'object_key' IS DISTINCT FROM OLD.item->>'object_key' THEN
  RAISE EXCEPTION 'COPY R2 identity is immutable'; END IF;
 PERFORM lock_nas_r2_copy_key(NEW.item->>'bucket',NEW.item->>'object_key');
 RETURN NEW;
END $$;
CREATE TRIGGER r2_inverse_copy_state_fence BEFORE UPDATE OF state,item ON nas_r2_copy_items
 FOR EACH ROW EXECUTE FUNCTION fence_r2_inverse_copy_key();
CREATE TRIGGER r2_inverse_joined_source_fence BEFORE INSERT OR UPDATE ON recording_joined_source_snapshots
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_joined_snapshot_fence BEFORE INSERT OR UPDATE ON recording_joined_snapshot_scopes
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_joined_dry_fence BEFORE INSERT OR UPDATE ON recording_joined_dry_run_scopes
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();

-- Delivery admission/enrichment is serialized with the entire wire phase.
-- Reject adoption of any reserved identity, not just the direct hold key.
CREATE TRIGGER r2_inverse_delivery_key_fence BEFORE INSERT OR UPDATE ON delivery_keep_holds
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_delivery_source_fence BEFORE INSERT OR UPDATE ON delivery_nas_source_holds
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_delivery_output_fence BEFORE INSERT OR UPDATE ON delivery_keep_output_dependencies
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE FUNCTION guard_r2_inverse_delivery_admission() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM r2_retirement_reservations r JOIN recording_clips c ON r.kind='raw' AND r.source_id=c.id
  WHERE r2_inverse_delivery_held(c.id,c.bucket,c.object_key,c.display_path,lower(c.sha256),c.size_bytes))
 OR EXISTS(SELECT 1 FROM r2_retirement_reservations r JOIN delivery_keep_holds d USING(bucket,object_key))
 OR EXISTS(SELECT 1 FROM r2_inverse_intents i JOIN delivery_keep_holds d USING(bucket,object_key)) THEN
  RAISE EXCEPTION 'R2 retirement reservation forbids delivery adoption'; END IF;
 RETURN NULL;
END $$;
-- One complete closure check per statement, after new rows are visible, while
-- the BEFORE statement fence still excludes every network worker.
CREATE TRIGGER r2_inverse_delivery_key_admission AFTER INSERT OR UPDATE ON delivery_keep_holds
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_inverse_delivery_admission();
CREATE TRIGGER r2_inverse_delivery_source_admission AFTER INSERT OR UPDATE ON delivery_nas_source_holds
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_inverse_delivery_admission();
CREATE TRIGGER r2_inverse_delivery_output_admission AFTER INSERT OR UPDATE ON delivery_keep_output_dependencies
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_inverse_delivery_admission();

CREATE FUNCTION guard_r2_inverse_output_key() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM r2_retirement_reservations WHERE object_key=NEW.object_key)
 OR EXISTS(SELECT 1 FROM r2_inverse_intents WHERE object_key=NEW.object_key) THEN
  RAISE EXCEPTION 'R2 retirement reservation forbids collated key adoption'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_inverse_output_key_admission BEFORE INSERT OR UPDATE OF object_key ON recording_collation_outputs
 FOR EACH ROW EXECUTE FUNCTION guard_r2_inverse_output_key();

-- Output dependency membership/content can also create a live delivery alias.
CREATE TRIGGER r2_inverse_output_delivery_admission AFTER INSERT OR UPDATE OF object_key,nas_relative_path,collation_hour_id,source_clip_ids,sha256,size_bytes ON recording_collation_outputs
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_inverse_delivery_admission();
CREATE TRIGGER r2_inverse_hour_delivery_admission AFTER UPDATE OF recording_id ON recording_collation_hours
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_inverse_delivery_admission();

-- Catalog output IDs are identities, not a means to adopt/shed a held output.
CREATE FUNCTION guard_r2_inverse_output_id() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.id IS DISTINCT FROM OLD.id THEN RAISE EXCEPTION 'collated output ID is immutable'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_inverse_output_id_immutable BEFORE UPDATE OF id ON recording_collation_outputs
 FOR EACH ROW EXECUTE FUNCTION guard_r2_inverse_output_id();

-- Cleanup admission uses ordered clip locks, not the metadata barrier or a
-- later global-lock upgrade in an existing receipt/observe transaction.
CREATE OR REPLACE FUNCTION guard_nas_cleanup_r2_retirement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'R2 cleanup admission requires READ COMMITTED' USING ERRCODE='55000'; END IF;
 IF TG_TABLE_NAME='nas_cleanup_plans' THEN
  PERFORM c.id FROM recording_clips c WHERE c.id IN
   (SELECT (i->>'source_id')::bigint FROM jsonb_array_elements(NEW.manifest) i WHERE i->>'kind'='raw')
   ORDER BY c.id FOR KEY SHARE;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(NEW.manifest) i JOIN r2_retirement_reservations r
   ON r.kind=i->>'kind' AND r.source_id=(i->>'source_id')::bigint) THEN
   RAISE EXCEPTION 'retention protected: R2 retirement pending or completed'; END IF;
 ELSE
  PERFORM id FROM recording_clips WHERE id=NEW.clip_id FOR KEY SHARE;
  IF EXISTS(SELECT 1 FROM r2_retirement_reservations WHERE kind='raw' AND source_id=NEW.clip_id) THEN
   RAISE EXCEPTION 'retention protected: R2 recovery copy retirement pending or completed'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE FUNCTION guard_r2_inverse_hold_identity() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF ROW(NEW.plan_id,NEW.clip_id) IS DISTINCT FROM ROW(OLD.plan_id,OLD.clip_id) THEN
  RAISE EXCEPTION 'cleanup source hold identity is immutable'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_inverse_hold_identity BEFORE UPDATE ON nas_cleanup_source_holds
 FOR EACH ROW EXECUTE FUNCTION guard_r2_inverse_hold_identity();

-- Retired is terminal. A released hold must never become active again. NOWAIT
-- avoids reversing legacy plan-before-item row order for a direct item UPDATE.
CREATE FUNCTION guard_r2_inverse_retired_item() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE mode text; BEGIN
 IF NOT (NEW.delete_ever_issued OR NEW.state IN ('delete_issued','unverified','media_deleted_sidecar_failed')) THEN RETURN NEW; END IF;
 IF TG_OP='UPDATE' AND (OLD.delete_ever_issued OR OLD.state IN ('delete_issued','unverified','media_deleted_sidecar_failed')) THEN RETURN NEW; END IF;
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'cleanup hold activation requires READ COMMITTED' USING ERRCODE='55000'; END IF;
 SELECT p.mode INTO mode FROM nas_cleanup_plans p WHERE p.id=NEW.plan_id FOR SHARE NOWAIT;
 IF mode='retired' THEN RAISE EXCEPTION 'retired cleanup hold cannot reactivate'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_inverse_retired_item BEFORE INSERT OR UPDATE OF delete_ever_issued,state ON nas_cleanup_items
 FOR EACH ROW EXECUTE FUNCTION guard_r2_inverse_retired_item();

-- Freeze pending source identity after admission commits. Once confirmed
-- absent, immutable receipts retain history without permanently locking the
-- catalog or adding a foreign-key veto to later authorized metadata cleanup.
CREATE FUNCTION guard_r2_inverse_source_identity() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM r2_inverse_intents i WHERE i.source_id=OLD.id AND NOT EXISTS(SELECT 1 FROM r2_inverse_events e WHERE e.bucket=i.bucket AND e.object_key=i.object_key AND e.event='absent'))
 OR (TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM r2_inverse_intents i WHERE i.bucket=NEW.bucket AND i.object_key=NEW.object_key AND NOT EXISTS(SELECT 1 FROM r2_inverse_events e WHERE e.bucket=i.bucket AND e.object_key=i.object_key AND e.event='absent'))) THEN
  RAISE EXCEPTION 'R2 inverse intent freezes source identity'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
CREATE TRIGGER r2_inverse_source_identity BEFORE UPDATE OF id,recording_id,recording_job_id,bucket,object_key,etag,size_bytes,sha256,display_path,clip_start_at,clip_end_at OR DELETE ON recording_clips
 FOR EACH ROW EXECUTE FUNCTION guard_r2_inverse_source_identity();

-- The KE folder cache is small, but catalog identity may change between batches.
-- Version only those identities; COPY state changes remain per-key vetoes and
-- cannot globally freeze unrelated inverse deletion.
CREATE TABLE r2_inverse_ke_epoch(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),epoch bigint NOT NULL DEFAULT 0);
INSERT INTO r2_inverse_ke_epoch DEFAULT VALUES;
CREATE FUNCTION advance_r2_inverse_ke_epoch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'R2 catalog admission requires READ COMMITTED' USING ERRCODE='55000'; END IF;
 PERFORM 1 FROM r2_inverse_ke_epoch FOR UPDATE NOWAIT;
 UPDATE r2_inverse_ke_epoch SET epoch=epoch+1;
 RETURN NULL;
END $$;
CREATE TRIGGER r2_inverse_ke_raw_epoch BEFORE INSERT OR UPDATE OF bucket,display_path,recording_id OR DELETE ON recording_clips
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();
CREATE TRIGGER r2_inverse_ke_output_epoch BEFORE INSERT OR UPDATE OF object_key,nas_relative_path,collation_hour_id,source_clip_ids,sha256,size_bytes OR DELETE ON recording_collation_outputs
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();
CREATE TRIGGER r2_inverse_ke_hour_epoch BEFORE UPDATE OF recording_id ON recording_collation_hours
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();
CREATE TRIGGER r2_inverse_ke_recording_epoch BEFORE UPDATE OF stream_id ON recordings
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();

-- Intent/event history cannot be erased even by statement-level TRUNCATE.
CREATE TRIGGER r2_inverse_intents_no_truncate BEFORE TRUNCATE ON r2_inverse_intents
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_object_retirement();
CREATE TRIGGER r2_inverse_events_no_truncate BEFORE TRUNCATE ON r2_inverse_events
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_object_retirement();
CREATE FUNCTION guard_r2_inverse_budget() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP<>'UPDATE' THEN RAISE EXCEPTION 'R2 inverse budget history is permanent'; END IF;
 IF NEW.bucket<>OLD.bucket OR NEW.minute<>OLD.minute OR NEW.charged_bytes<=OLD.charged_bytes THEN
  RAISE EXCEPTION 'R2 inverse byte charges may only increase'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_inverse_budget_monotone BEFORE UPDATE OR DELETE ON r2_inverse_budget_minutes
 FOR EACH ROW EXECUTE FUNCTION guard_r2_inverse_budget();
CREATE TRIGGER r2_inverse_budget_no_truncate BEFORE TRUNCATE ON r2_inverse_budget_minutes
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_inverse_budget();
CREATE FUNCTION guard_r2_inverse_epoch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP<>'UPDATE' THEN RAISE EXCEPTION 'R2 inverse protection version is permanent'; END IF;
 IF NEW.singleton<>OLD.singleton OR NEW.epoch<=OLD.epoch THEN
  RAISE EXCEPTION 'R2 inverse protection version may only increase'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_inverse_epoch_monotone BEFORE UPDATE OR DELETE ON r2_inverse_ke_epoch
 FOR EACH ROW EXECUTE FUNCTION guard_r2_inverse_epoch();
CREATE TRIGGER r2_inverse_epoch_no_truncate BEFORE TRUNCATE ON r2_inverse_ke_epoch
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_inverse_epoch();
