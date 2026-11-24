-- 0243 refused a joined source's purge only while a batch was snapshotting it.
-- A batch that is still building also reads its frozen sources, so refuse the
-- purge until every batch holding the source is frozen or later. Same body as
-- 0243 otherwise.
CREATE OR REPLACE FUNCTION guard_recording_joined_clip_retention() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target_clip_id BIGINT; clip_recording BIGINT; clip_job BIGINT; old_scoped BOOLEAN; new_scoped BOOLEAN := FALSE;
  protected BOOLEAN;
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
  END IF;
  protected := EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=target_clip_id) OR old_scoped;
  IF (TG_OP='DELETE' OR NEW.id IS DISTINCT FROM OLD.id) AND protected
  THEN RAISE EXCEPTION 'joined frozen source is retention protected'; END IF;
  IF TG_OP='UPDATE' AND OLD.purged_at IS NULL AND NEW.purged_at IS NOT NULL AND (EXISTS(
      SELECT 1 FROM recording_joined_snapshot_scopes scope
        JOIN recording_joined_batches batch ON batch.id=scope.batch_record_id AND batch.state IN ('snapshotting','building')
        WHERE scope.recording_id=clip_recording AND scope.recording_job_id=clip_job AND target_clip_id<=scope.high_water_clip_id)
    OR EXISTS(SELECT 1 FROM recording_joined_source_snapshots s
        JOIN recording_joined_batches batch ON batch.id=s.batch_record_id AND batch.state IN ('snapshotting','building')
        WHERE s.clip_id=target_clip_id))
  THEN RAISE EXCEPTION 'joined source is still being snapshotted or built'; END IF;
  IF TG_OP='UPDATE' AND (old_scoped OR new_scoped) AND ROW(NEW.recording_id,NEW.recording_job_id,NEW.storage_destination_id,
      NEW.endpoint,NEW.bucket,NEW.object_key,NEW.etag,NEW.size_bytes,NEW.sha256,NEW.clip_start_at,NEW.clip_end_at,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.recording_id,OLD.recording_job_id,OLD.storage_destination_id,
      OLD.endpoint,OLD.bucket,OLD.object_key,OLD.etag,OLD.size_bytes,OLD.sha256,OLD.clip_start_at,OLD.clip_end_at,OLD.created_at)
  THEN RAISE EXCEPTION 'joined scoped source identity is immutable'; END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$;
