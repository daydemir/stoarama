-- Owner release of a frozen joined batch's sources to the R2 RAW lane.
--
-- Deniz, 2026-11-19, answering "August frozen joined batch": "Release 1.15 TB
-- (Recommended)". Only the batch's raw clips outside every pinned keep-28
-- window go to the R2 RAW lane. Its keep-everything and keep-28 parts stay
-- held: the executor never selects them for that lane, and the keep-28 lane
-- keeps treating every snapshot of the batch as a hold.

CREATE TABLE r2_joined_batch_releases (
 batch_record_id bigint PRIMARY KEY REFERENCES recording_joined_batches(id) ON DELETE RESTRICT,
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

INSERT INTO r2_joined_batch_releases(batch_record_id,decision)
SELECT id,'Deniz 2026-11-19: "Release 1.15 TB (Recommended)"; outside-window raw only, keep-everything and keep-28 stay held'
FROM recording_joined_batches WHERE batch_id='goodplus-20260821-generation-1' AND generation=1 AND state='frozen';

-- r2_joined_source_released is true when every snapshot of a clip belongs to a
-- released batch and no snapshotting or dry-run scope covers it.
CREATE FUNCTION r2_joined_source_released(p_clip_id bigint) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=p_clip_id)
  AND NOT EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=p_clip_id
   AND NOT EXISTS(SELECT 1 FROM r2_joined_batch_releases r WHERE r.batch_record_id=s.batch_record_id))
$$;

-- Same body as 0155 except the purge transition: a clip whose every snapshot
-- belongs to a released batch, and that no live scope covers, may gain
-- purged_at. Deletion, id changes and scoped identity stay refused.
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
  IF purge_transition AND protected AND NOT (
      (current_setting('stoarama.joined_source_purge', true) = 'verified'
       AND recording_joined_source_purge_eligible(OLD.id, OLD.recording_id, OLD.recording_job_id))
      OR (NOT old_scoped AND r2_joined_source_released(OLD.id)))
  THEN RAISE EXCEPTION 'joined frozen source is retention protected'; END IF;
  IF TG_OP='UPDATE' AND (old_scoped OR new_scoped) AND ROW(NEW.recording_id,NEW.recording_job_id,NEW.storage_destination_id,
      NEW.endpoint,NEW.bucket,NEW.object_key,NEW.etag,NEW.size_bytes,NEW.sha256,NEW.clip_start_at,NEW.clip_end_at,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.recording_id,OLD.recording_job_id,OLD.storage_destination_id,
      OLD.endpoint,OLD.bucket,OLD.object_key,OLD.etag,OLD.size_bytes,OLD.sha256,OLD.clip_start_at,OLD.clip_end_at,OLD.created_at)
  THEN RAISE EXCEPTION 'joined scoped source identity is immutable'; END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$;
