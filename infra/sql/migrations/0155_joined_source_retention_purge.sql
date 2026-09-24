-- Joined-source retention purge: the one narrow exception to "recorded content is
-- never hard-deleted" (approved by Deniz, 2026-09-24).
--
-- Once a joined hour is sealed, its hour manifest is published, every media part
-- of the hour is published, and no operator hold covers it, the 1-minute source
-- clips that hour INCLUDED are pure duplicates of the joined media. This
-- migration lets exactly those clips receive purged_at (the R2 object is gone,
-- the recording_clips row, thumbnail and every joined foreign key are kept).
--
-- The trigger stays fail closed for everything else:
--   * DELETE of a frozen source row and id changes remain forbidden.
--   * Every snapshot of the clip, in every batch, must be allocated to an hour,
--     INCLUDED (never quarantined) in it, carried by a published media part,
--     and that hour must be retention-final. A clip that a later or still
--     running generation snapshotted stays protected.
--   * Clips inside an active snapshotting scope stay protected.
--   * Dry-run scopes keep protecting clips until their batch is frozen (the
--     dry run's only consumer is the freeze; after that it is evidence only).
--   * The caller must declare, per transaction, that it verified the joined
--     objects and the external copy (SET LOCAL stoarama.joined_source_purge =
--     'verified'). Only `stoaramactl joined-source-purge --apply` does that.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '15s';

-- Operator exceptions: an hour with a hold is never retention-final, whatever
-- its publication state. Holds are insert-only from stoaramactl.
CREATE TABLE recording_joined_source_retention_holds (
  hour_record_id BIGINT PRIMARY KEY REFERENCES recording_joined_hours(id) ON DELETE RESTRICT,
  reason_code TEXT NOT NULL CHECK (reason_code ~ '^[a-z][a-z0-9_]{0,79}$'),
  note TEXT NOT NULL DEFAULT '' CHECK (octet_length(note) <= 1024),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Known exceptions in goodplus-20260821-generation-1 (no-ops elsewhere):
-- dual-recorder overlap hours on 2026-08-01, publication terminal failures, and
-- hours stuck in sealed publication. Most are already non-final; holding them
-- keeps them out even if their publication later completes.
INSERT INTO recording_joined_source_retention_holds(hour_record_id, reason_code, note)
SELECT h.id, x.reason_code, x.note
FROM (VALUES
  ('goodplus-20260821-generation-1__recording-337__date-2026-08-01__hour-07__generation-1','dual_recorder_overlap','recording 337/382 overlap 2026-08-01'),
  ('goodplus-20260821-generation-1__recording-337__date-2026-08-01__hour-08__generation-1','dual_recorder_overlap','recording 337/382 overlap 2026-08-01'),
  ('goodplus-20260821-generation-1__recording-382__date-2026-08-01__hour-07__generation-1','dual_recorder_overlap','recording 337/382 overlap 2026-08-01'),
  ('goodplus-20260821-generation-1__recording-382__date-2026-08-01__hour-08__generation-1','dual_recorder_overlap','recording 337/382 overlap 2026-08-01'),
  ('goodplus-20260821-generation-1__recording-382__date-2026-08-01__hour-09__generation-1','dual_recorder_overlap','recording 337/382 overlap 2026-08-01'),
  ('goodplus-20260821-generation-1__recording-382__date-2026-08-01__hour-10__generation-1','dual_recorder_overlap','recording 337/382 overlap 2026-08-01'),
  ('goodplus-20260821-generation-1__recording-382__date-2026-08-01__hour-11__generation-1','dual_recorder_overlap','recording 337/382 overlap 2026-08-01'),
  ('goodplus-20260821-generation-1__recording-382__date-2026-08-01__hour-12__generation-1','dual_recorder_overlap','recording 337/382 overlap 2026-08-01'),
  ('goodplus-20260821-generation-1__recording-377__date-2026-07-29__hour-07__generation-1','publication_terminal_failed',''),
  ('goodplus-20260821-generation-1__recording-382__date-2026-08-02__hour-08__generation-1','publication_terminal_failed',''),
  ('goodplus-20260821-generation-1__recording-379__date-2026-08-12__hour-02__generation-1','publication_terminal_failed',''),
  ('goodplus-20260821-generation-1__recording-382__date-2026-07-27__hour-02__generation-1','publication_stuck_sealed',''),
  ('goodplus-20260821-generation-1__recording-382__date-2026-07-27__hour-03__generation-1','publication_stuck_sealed',''),
  ('goodplus-20260821-generation-1__recording-382__date-2026-07-27__hour-04__generation-1','publication_stuck_sealed','')
) AS x(hour_id, reason_code, note)
JOIN recording_joined_hours h ON h.hour_id = x.hour_id;

-- An hour is retention-final when it has been sealed for at least an hour, its
-- manifest is published, all of its media parts are published, and no hold
-- covers it. Every lookup rides
-- an existing unique index (hours pkey, one_manifest_root, the artifact scope
-- key, holds pkey).
CREATE FUNCTION recording_joined_hour_retention_final(p_hour_record_id BIGINT) RETURNS boolean
LANGUAGE sql STABLE AS $$
  SELECT EXISTS(
    SELECT 1 FROM recording_joined_hours h
    WHERE h.id = p_hour_record_id AND h.state = 'sealed' AND h.sealed_at IS NOT NULL
      -- Settle time: any presigned source PUT (R2SignPutTTL, 15 min) issued
      -- before ingest has long expired before a source can be purged.
      AND h.sealed_at <= now() - interval '1 hour'
      AND EXISTS(SELECT 1 FROM recording_joined_artifacts manifest
        WHERE manifest.hour_record_id = h.id AND manifest.artifact_kind = 'hour_manifest'
          AND manifest.publication_state = 'published' AND manifest.published_at IS NOT NULL)
      AND NOT EXISTS(SELECT 1 FROM recording_joined_artifacts media
        WHERE media.batch_record_id = h.batch_record_id AND media.scope_kind = 'hour' AND media.scope_id = h.hour_id
          AND media.artifact_kind = 'media' AND media.published_at IS NULL)
      AND NOT EXISTS(SELECT 1 FROM recording_joined_source_retention_holds hold WHERE hold.hour_record_id = h.id))
$$;

-- The single eligibility definition shared by the trigger and the operator
-- command. Traverses from the clip's snapshots (clip_idx) so no new index on a
-- hot joined table is needed.
CREATE FUNCTION recording_joined_source_purge_eligible(p_clip_id BIGINT, p_recording_id BIGINT, p_recording_job_id BIGINT)
RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS(SELECT 1 FROM recording_joined_source_snapshots snap WHERE snap.clip_id = p_clip_id)
    AND NOT EXISTS(
      SELECT 1 FROM recording_joined_source_snapshots snap
      WHERE snap.clip_id = p_clip_id
        AND NOT EXISTS(
          SELECT 1 FROM recording_joined_sources src
          JOIN recording_joined_hour_dispositions d
            ON d.hour_record_id = src.hour_record_id AND d.source_id = src.id AND d.disposition = 'included'
          JOIN recording_joined_artifacts media
            ON media.id = d.media_artifact_id AND media.artifact_kind = 'media'
           AND media.hour_record_id = src.hour_record_id AND media.published_at IS NOT NULL
          JOIN recording_joined_media_sources ms ON ms.source_id = src.id AND ms.artifact_id = media.id
          WHERE src.source_snapshot_id = snap.id AND src.clip_id = p_clip_id
            AND recording_joined_hour_retention_final(src.hour_record_id)))
    AND NOT EXISTS(
      SELECT 1 FROM recording_joined_snapshot_scopes scope
      JOIN recording_joined_batches batch ON batch.id = scope.batch_record_id AND batch.state = 'snapshotting'
      WHERE scope.recording_id = p_recording_id AND scope.recording_job_id = p_recording_job_id
        AND p_clip_id <= scope.high_water_clip_id)
    AND NOT EXISTS(
      SELECT 1 FROM recording_joined_dry_run_scopes scope
      JOIN recording_joined_dry_runs run ON run.id = scope.dry_run_id
      WHERE scope.recording_id = p_recording_id AND scope.recording_job_id = p_recording_job_id
        AND p_clip_id <= scope.high_water_clip_id
        AND NOT EXISTS(SELECT 1 FROM recording_joined_batches batch
          WHERE batch.batch_id = run.batch_id AND batch.connection_id = run.connection_id
            AND batch.generation = run.generation AND batch.state IN ('frozen', 'index_sealed', 'published')))
$$;

-- Same body as 0141 except the purge transition: a retention-protected clip may
-- gain purged_at only through the verified, eligible path above.
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
      current_setting('stoarama.joined_source_purge', true) = 'verified'
      AND recording_joined_source_purge_eligible(OLD.id, OLD.recording_id, OLD.recording_job_id))
  THEN RAISE EXCEPTION 'joined frozen source is retention protected'; END IF;
  IF TG_OP='UPDATE' AND (old_scoped OR new_scoped) AND ROW(NEW.recording_id,NEW.recording_job_id,NEW.storage_destination_id,
      NEW.endpoint,NEW.bucket,NEW.object_key,NEW.etag,NEW.size_bytes,NEW.sha256,NEW.clip_start_at,NEW.clip_end_at,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.recording_id,OLD.recording_job_id,OLD.storage_destination_id,
      OLD.endpoint,OLD.bucket,OLD.object_key,OLD.etag,OLD.size_bytes,OLD.sha256,OLD.clip_start_at,OLD.clip_end_at,OLD.created_at)
  THEN RAISE EXCEPTION 'joined scoped source identity is immutable'; END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$;
