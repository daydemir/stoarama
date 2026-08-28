BEGIN;

-- The recordings list resolves joined coverage from one account/recording to its
-- exact source rows. Keep that lookup bounded as the historical corpus grows.
DO $$ BEGIN
  IF to_regclass('recording_joined_sources') IS NOT NULL THEN
    CREATE INDEX recording_joined_sources_account_recording_idx
      ON recording_joined_sources(account_id,recording_id,clip_id,hour_record_id,batch_record_id);
  END IF;
END $$;

-- Browser and coverage reads begin with sealed hours owned by one recording.
DO $$ BEGIN
  IF to_regclass('recording_joined_hours') IS NOT NULL THEN
    CREATE INDEX recording_joined_hours_recording_sealed_idx
      ON recording_joined_hours(account_id,recording_id,id,batch_record_id)
      WHERE state='sealed';
  END IF;
END $$;

-- A sealed hour is publishable only through its exact media and manifest rows.
-- This partial index also excludes unfinished artifact identities.
DO $$ BEGIN
  IF to_regclass('recording_joined_artifacts') IS NOT NULL THEN
    CREATE INDEX recording_joined_artifacts_published_hour_idx
      ON recording_joined_artifacts(account_id,hour_record_id,batch_record_id,artifact_kind,ordinal,id)
      WHERE published_at IS NOT NULL AND etag IS NOT NULL AND etag<>'' AND version_id IS NOT NULL;
  END IF;
END $$;

COMMIT;
