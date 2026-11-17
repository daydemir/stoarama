-- B1 raw-only inverse retirement. No activation or deletion by migration.
-- 0200 supplies the exact COPY-key reservation/import lock and veto.
SELECT 'lock_nas_r2_copy_key(text,text)'::regprocedure;
CREATE TABLE r2_inverse_intents (
 bucket text NOT NULL, object_key text NOT NULL,
 source_id bigint NOT NULL REFERENCES recording_clips(id),
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

-- Acquire admission BEFORE row locks on relevant mutations. Network workers
-- take shared locks in the same order, allowing concurrent independent DELETEs.
-- purged_at-only publication is excluded: it executes existing purge vetoes.
CREATE FUNCTION fence_r2_inverse_protection() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM pg_advisory_xact_lock(7343226011103);
 PERFORM pg_advisory_xact_lock(7343226011101);
 RETURN NULL;
END $$;
CREATE TRIGGER r2_inverse_source_identity_fence BEFORE UPDATE OF recording_id,recording_job_id,bucket,object_key,etag,size_bytes,sha256,display_path,clip_start_at,clip_end_at OR DELETE ON recording_clips
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_recording_identity_fence BEFORE UPDATE OF stream_id,cron_timezone,account_id ON recordings
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_stream_identity_fence BEFORE UPDATE OF local_timezone ON streams
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_copy_state_fence BEFORE INSERT OR UPDATE OF state,item ON nas_r2_copy_items
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_plan_fence BEFORE INSERT OR UPDATE ON nas_cleanup_plans
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_source_hold_fence BEFORE INSERT OR UPDATE ON nas_cleanup_source_holds
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_joined_source_fence BEFORE INSERT OR UPDATE ON recording_joined_source_snapshots
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_joined_snapshot_fence BEFORE INSERT OR UPDATE ON recording_joined_snapshot_scopes
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();
CREATE TRIGGER r2_inverse_joined_dry_fence BEFORE INSERT OR UPDATE ON recording_joined_dry_run_scopes
 FOR EACH STATEMENT EXECUTE FUNCTION fence_r2_inverse_protection();

-- Freeze reserved source identity after the admission transaction commits.
CREATE FUNCTION guard_r2_inverse_source_identity() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF EXISTS(SELECT 1 FROM r2_inverse_intents WHERE source_id=OLD.id)
 OR (TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM r2_inverse_intents WHERE bucket=NEW.bucket AND object_key=NEW.object_key)) THEN
  RAISE EXCEPTION 'R2 inverse intent freezes source identity'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
CREATE TRIGGER r2_inverse_source_identity BEFORE UPDATE OF recording_id,recording_job_id,bucket,object_key,etag,size_bytes,sha256,display_path,clip_start_at,clip_end_at OR DELETE ON recording_clips
 FOR EACH ROW EXECUTE FUNCTION guard_r2_inverse_source_identity();

-- The KE folder cache is small, but catalog identity may change between batches.
-- Version only those identities; COPY state changes remain per-key vetoes and
-- cannot globally freeze unrelated inverse deletion.
CREATE TABLE r2_inverse_ke_epoch(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),epoch bigint NOT NULL DEFAULT 0);
INSERT INTO r2_inverse_ke_epoch DEFAULT VALUES;
CREATE FUNCTION advance_r2_inverse_ke_epoch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM pg_advisory_xact_lock(7343226011103);
 PERFORM pg_advisory_xact_lock(7343226011101);
 UPDATE r2_inverse_ke_epoch SET epoch=epoch+1;
 RETURN NULL;
END $$;
CREATE TRIGGER r2_inverse_ke_raw_epoch BEFORE INSERT OR UPDATE OF bucket,display_path,recording_id OR DELETE ON recording_clips
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();
CREATE TRIGGER r2_inverse_ke_output_epoch BEFORE INSERT OR UPDATE OF nas_relative_path,collation_hour_id OR DELETE ON recording_collation_outputs
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();
CREATE TRIGGER r2_inverse_ke_hour_epoch BEFORE UPDATE OF recording_id ON recording_collation_hours
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();
CREATE TRIGGER r2_inverse_ke_recording_epoch BEFORE UPDATE OF stream_id ON recordings
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();
