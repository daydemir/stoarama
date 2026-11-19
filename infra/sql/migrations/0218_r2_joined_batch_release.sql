-- Owner release of a frozen joined batch's sources to the R2 RAW lane.
--
-- Deniz, 2026-11-19, answering "August frozen joined batch": "Release 1.15 TB
-- (Recommended)". Only the batch's raw clips outside every pinned keep-28
-- window go to the R2 RAW lane. Its keep-everything and keep-28 parts stay
-- held: the executor never selects them for that lane, and the keep-28 lane
-- keeps treating every snapshot of the batch as a hold.

CREATE TABLE r2_joined_batch_releases (
 batch_record_id bigint PRIMARY KEY REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
 -- Pinned to the one batch the owner released; another needs a new migration.
 batch_id text NOT NULL CHECK(batch_id='goodplus-20260821-generation-1'),
 generation integer NOT NULL CHECK(generation=1),
 decision text NOT NULL CHECK(decision<>''),
 released_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION guard_r2_joined_batch_release_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'R2 joined batch releases are append-only';
END $$;
CREATE TRIGGER r2_joined_batch_release_immutable BEFORE UPDATE OR DELETE ON r2_joined_batch_releases
 FOR EACH ROW EXECUTE FUNCTION guard_r2_joined_batch_release_immutable();
CREATE TRIGGER r2_joined_batch_release_no_truncate BEFORE TRUNCATE ON r2_joined_batch_releases
 FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_joined_batch_release_immutable();
-- A release row must name the frozen batch it pins, never a decoy record.
CREATE FUNCTION guard_r2_joined_batch_release_target() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM recording_joined_batches b WHERE b.id=NEW.batch_record_id
   AND b.batch_id=NEW.batch_id AND b.generation=NEW.generation AND b.state='frozen') THEN
  RAISE EXCEPTION 'R2 joined batch release must name its own frozen batch record';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_joined_batch_release_target BEFORE INSERT ON r2_joined_batch_releases
 FOR EACH ROW EXECUTE FUNCTION guard_r2_joined_batch_release_target();
ALTER TABLE r2_joined_batch_releases ENABLE ALWAYS TRIGGER r2_joined_batch_release_target;
ALTER TABLE r2_joined_batch_releases ENABLE ALWAYS TRIGGER r2_joined_batch_release_immutable;
ALTER TABLE r2_joined_batch_releases ENABLE ALWAYS TRIGGER r2_joined_batch_release_no_truncate;

-- The owner released a frozen batch. If it exists but has moved on, the
-- decision no longer describes it: fail rather than release or skip silently.
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM recording_joined_batches WHERE batch_id='goodplus-20260821-generation-1' AND (generation<>1 OR state<>'frozen')) THEN
  RAISE EXCEPTION 'goodplus-20260821-generation-1 is not the frozen generation-1 batch the owner released';
 END IF;
END $$;
INSERT INTO r2_joined_batch_releases(batch_record_id,batch_id,generation,decision)
SELECT id,batch_id,generation,'Deniz 2026-11-19: "Release 1.15 TB (Recommended)"; outside-window raw only, keep-everything and keep-28 stay held'
FROM recording_joined_batches WHERE batch_id='goodplus-20260821-generation-1' AND generation=1 AND state='frozen';

-- r2_joined_source_released is true only when every joined claim on the clip is
-- released: each snapshot belongs to a released batch that is still frozen and
-- still matches the live clip exactly; no operator retention hold covers an
-- hour a snapshot is allocated to; no snapshotting scope covers it; and no
-- dry-run scope covers it unless that dry run's batch is frozen (0155's rule).
CREATE FUNCTION r2_joined_source_released(p_clip_id bigint) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=p_clip_id)
  AND NOT EXISTS(SELECT 1 FROM recording_joined_source_snapshots s JOIN recording_clips c ON c.id=s.clip_id
   WHERE s.clip_id=p_clip_id AND NOT coalesce(
    EXISTS(SELECT 1 FROM r2_joined_batch_releases r JOIN recording_joined_batches b ON b.id=r.batch_record_id
     WHERE r.batch_record_id=s.batch_record_id AND b.batch_id=r.batch_id AND b.generation=r.generation AND b.state='frozen')
    AND s.recording_id=c.recording_id AND s.bucket=c.bucket AND s.object_key=c.object_key
    AND s.size_bytes=c.size_bytes AND lower(s.sha256)=lower(c.sha256) AND btrim(s.ingest_etag,'"')=btrim(c.etag,'"')
    AND s.start_at=c.clip_start_at AND s.end_at=c.clip_end_at, false))
  AND NOT EXISTS(SELECT 1 FROM recording_joined_source_snapshots s
   JOIN recording_joined_sources src ON src.source_snapshot_id=s.id AND src.clip_id=s.clip_id
   JOIN recording_joined_source_retention_holds h ON h.hour_record_id=src.hour_record_id
   WHERE s.clip_id=p_clip_id)
  AND NOT EXISTS(SELECT 1 FROM recording_clips c JOIN recording_joined_snapshot_scopes scope
   ON scope.recording_id=c.recording_id AND scope.recording_job_id=c.recording_job_id AND c.id<=scope.high_water_clip_id
   JOIN recording_joined_batches batch ON batch.id=scope.batch_record_id AND batch.state='snapshotting'
   WHERE c.id=p_clip_id)
  AND NOT EXISTS(SELECT 1 FROM recording_clips c JOIN recording_joined_dry_run_scopes scope
   ON scope.recording_id=c.recording_id AND scope.recording_job_id=c.recording_job_id AND c.id<=scope.high_water_clip_id
   JOIN recording_joined_dry_runs run ON run.id=scope.dry_run_id
   WHERE c.id=p_clip_id AND NOT EXISTS(SELECT 1 FROM recording_joined_batches batch
    WHERE batch.batch_id=run.batch_id AND batch.connection_id=run.connection_id AND batch.generation=run.generation
     AND batch.state IN ('frozen','index_sealed','published')))
$$;

-- The purge exemption belongs to the R2 RAW lane only: the clip must be
-- released and carry that lane's exact durable intent for this object.
CREATE FUNCTION r2_joined_raw_retirement_released(p_clip_id bigint, p_bucket text, p_key text) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT r2_joined_source_released(p_clip_id)
  AND EXISTS(SELECT 1 FROM r2_inverse_intents i WHERE i.source_id=p_clip_id AND i.bucket=p_bucket AND i.object_key=p_key)
$$;

-- A new operator retention hold serializes with R2 retirement: it takes the
-- protection locks that reservations hold exclusively and the RAW wire holds
-- shared, then refuses an hour whose source already has an unresolved RAW
-- intent, since that DELETE may already be on the wire. Moving a hold to
-- another hour is admitted the same way for its destination hour.
CREATE FUNCTION guard_r2_joined_retention_hold_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 -- The check below must see what committed before the locks were granted.
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'joined retention hold admission requires READ COMMITTED';
 END IF;
 PERFORM pg_advisory_xact_lock(7343226011103);
 PERFORM pg_advisory_xact_lock(7343226011101);
 IF EXISTS(SELECT 1 FROM recording_joined_sources src JOIN r2_inverse_intents i ON i.source_id=src.clip_id
   WHERE src.hour_record_id=NEW.hour_record_id
   AND NOT EXISTS(SELECT 1 FROM r2_inverse_events v WHERE v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent')) THEN
  RAISE EXCEPTION 'R2 retirement already issued for a source of joined hour %; the hold cannot protect it', NEW.hour_record_id;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER r2_joined_retention_hold_admission BEFORE INSERT OR UPDATE OF hour_record_id ON recording_joined_source_retention_holds
 FOR EACH ROW EXECUTE FUNCTION guard_r2_joined_retention_hold_admission();
ALTER TABLE recording_joined_source_retention_holds ENABLE ALWAYS TRIGGER r2_joined_retention_hold_admission;

-- Same body as 0155 except the purge transition. A released source with the
-- R2 RAW lane's exact intent may gain purged_at. The verified path must now be
-- explicitly true: 0155 let an unset setting through as NULL. Deletion, id
-- changes and scoped identity stay refused.
CREATE OR REPLACE FUNCTION guard_recording_joined_clip_retention() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target_clip_id BIGINT; clip_recording BIGINT; clip_job BIGINT; old_scoped BOOLEAN; new_scoped BOOLEAN := FALSE;
  protected BOOLEAN; purge_transition BOOLEAN := FALSE;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed'
  THEN RAISE EXCEPTION 'joined source purge requires read committed'; END IF;
  PERFORM pg_advisory_xact_lock_shared(137,1);
  target_clip_id:=OLD.id; clip_recording:=OLD.recording_id; clip_job:=OLD.recording_job_id;
  SELECT EXISTS(
    SELECT 1 FROM recording_joined_snapshot_scopes scope
      JOIN recording_joined_batches batch ON batch.id=scope.batch_record_id AND batch.state='snapshotting'
      WHERE scope.recording_id=clip_recording AND scope.recording_job_id=clip_job AND target_clip_id<=scope.high_water_clip_id
    UNION ALL
    SELECT 1 FROM recording_joined_dry_run_scopes scope JOIN recording_joined_dry_runs run ON run.id=scope.dry_run_id
      WHERE scope.recording_id=clip_recording AND scope.recording_job_id=clip_job AND target_clip_id<=scope.high_water_clip_id
  ) INTO old_scoped;
  IF TG_OP='UPDATE' THEN
    SELECT EXISTS(
      SELECT 1 FROM recording_joined_snapshot_scopes scope
        JOIN recording_joined_batches batch ON batch.id=scope.batch_record_id AND batch.state='snapshotting'
        WHERE scope.recording_id=NEW.recording_id AND scope.recording_job_id=NEW.recording_job_id AND NEW.id<=scope.high_water_clip_id
      UNION ALL
      SELECT 1 FROM recording_joined_dry_run_scopes scope JOIN recording_joined_dry_runs run ON run.id=scope.dry_run_id
        WHERE scope.recording_id=NEW.recording_id AND scope.recording_job_id=NEW.recording_job_id AND NEW.id<=scope.high_water_clip_id
    ) INTO new_scoped;
    purge_transition := OLD.purged_at IS NULL AND NEW.purged_at IS NOT NULL;
  END IF;
  protected := EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=target_clip_id) OR old_scoped;
  IF (TG_OP='DELETE' OR NEW.id IS DISTINCT FROM OLD.id) AND protected
  THEN RAISE EXCEPTION 'joined frozen source is retention protected'; END IF;
  IF purge_transition AND protected AND NOT coalesce(
      (current_setting('stoarama.joined_source_purge', true) = 'verified'
       AND recording_joined_source_purge_eligible(OLD.id, OLD.recording_id, OLD.recording_job_id))
      OR r2_joined_raw_retirement_released(OLD.id, OLD.bucket, OLD.object_key), false)
  THEN RAISE EXCEPTION 'joined frozen source is retention protected'; END IF;
  IF TG_OP='UPDATE' AND (old_scoped OR new_scoped) AND ROW(NEW.recording_id,NEW.recording_job_id,NEW.storage_destination_id,
      NEW.endpoint,NEW.bucket,NEW.object_key,NEW.etag,NEW.size_bytes,NEW.sha256,NEW.clip_start_at,NEW.clip_end_at,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.recording_id,OLD.recording_job_id,OLD.storage_destination_id,
      OLD.endpoint,OLD.bucket,OLD.object_key,OLD.etag,OLD.size_bytes,OLD.sha256,OLD.clip_start_at,OLD.clip_end_at,OLD.created_at)
  THEN RAISE EXCEPTION 'joined scoped source identity is immutable'; END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$;
