-- These indexes serve joined browsing and coverage reads while joined workers
-- may still be writing. Fresh/small databases can create them transactionally.
-- Refuse a large unindexed table rather than taking a write-blocking SHARE lock
-- during auto-deploy. On a populated database, precreate the exact missing
-- index with CREATE INDEX CONCURRENTLY, then rerun the migration.

DO $$
DECLARE
  table_oid REGCLASS := to_regclass('recording_joined_sources');
  index_oid REGCLASS := to_regclass('recording_joined_sources_account_recording_idx');
  index_ok BOOLEAN;
BEGIN
  IF table_oid IS NOT NULL THEN
    IF index_oid IS NULL THEN
      IF pg_relation_size(table_oid) > 134217728 THEN
        RAISE EXCEPTION 'precreate recording_joined_sources_account_recording_idx with CREATE INDEX CONCURRENTLY ON recording_joined_sources(account_id,recording_id,clip_id,hour_record_id,batch_record_id) before migration';
      END IF;
      CREATE INDEX recording_joined_sources_account_recording_idx
        ON recording_joined_sources(account_id,recording_id,clip_id,hour_record_id,batch_record_id);
    ELSE
      SELECT i.indisvalid AND i.indisready
             AND i.indrelid=table_oid AND i.indnkeyatts=5 AND i.indnatts=5
             AND i.indpred IS NULL
             AND pg_get_indexdef(i.indexrelid,1,false)='account_id'
             AND pg_get_indexdef(i.indexrelid,2,false)='recording_id'
             AND pg_get_indexdef(i.indexrelid,3,false)='clip_id'
             AND pg_get_indexdef(i.indexrelid,4,false)='hour_record_id'
             AND pg_get_indexdef(i.indexrelid,5,false)='batch_record_id'
        INTO index_ok
        FROM pg_index i
       WHERE i.indexrelid=index_oid;
      IF index_ok IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'recording_joined_sources_account_recording_idx exists but is not the valid exact joined-browser source index';
      END IF;
    END IF;
  END IF;
END $$;

DO $$
DECLARE
  table_oid REGCLASS := to_regclass('recording_joined_media_sources');
  index_oid REGCLASS := to_regclass('recording_joined_media_sources_source_artifact_idx');
  index_ok BOOLEAN;
BEGIN
  IF table_oid IS NOT NULL THEN
    IF index_oid IS NULL THEN
      IF pg_relation_size(table_oid) > 134217728 THEN
        RAISE EXCEPTION 'precreate recording_joined_media_sources_source_artifact_idx with CREATE INDEX CONCURRENTLY ON recording_joined_media_sources(source_id,artifact_id) before migration';
      END IF;
      CREATE INDEX recording_joined_media_sources_source_artifact_idx
        ON recording_joined_media_sources(source_id,artifact_id);
    ELSE
      SELECT i.indisvalid AND i.indisready
             AND i.indrelid=table_oid AND i.indnkeyatts=2 AND i.indnatts=2
             AND i.indpred IS NULL
             AND pg_get_indexdef(i.indexrelid,1,false)='source_id'
             AND pg_get_indexdef(i.indexrelid,2,false)='artifact_id'
        INTO index_ok
        FROM pg_index i
       WHERE i.indexrelid=index_oid;
      IF index_ok IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'recording_joined_media_sources_source_artifact_idx exists but is not the valid exact joined-browser mapping index';
      END IF;
    END IF;
  END IF;
END $$;

DO $$
DECLARE
  table_oid REGCLASS := to_regclass('recording_joined_hours');
  index_oid REGCLASS := to_regclass('recording_joined_hours_recording_sealed_idx');
  index_ok BOOLEAN;
BEGIN
  IF table_oid IS NOT NULL THEN
    IF index_oid IS NULL THEN
      IF pg_relation_size(table_oid) > 134217728 THEN
        RAISE EXCEPTION 'precreate recording_joined_hours_recording_sealed_idx with CREATE INDEX CONCURRENTLY ON recording_joined_hours(account_id,recording_id,id,batch_record_id) WHERE state=''sealed'' before migration';
      END IF;
      CREATE INDEX recording_joined_hours_recording_sealed_idx
        ON recording_joined_hours(account_id,recording_id,id,batch_record_id)
        WHERE state='sealed';
    ELSE
      SELECT i.indisvalid AND i.indisready
             AND i.indrelid=table_oid AND i.indnkeyatts=4 AND i.indnatts=4
             AND pg_get_indexdef(i.indexrelid,1,false)='account_id'
             AND pg_get_indexdef(i.indexrelid,2,false)='recording_id'
             AND pg_get_indexdef(i.indexrelid,3,false)='id'
             AND pg_get_indexdef(i.indexrelid,4,false)='batch_record_id'
             AND pg_get_expr(i.indpred,i.indrelid)='(state = ''sealed''::text)'
        INTO index_ok
        FROM pg_index i
       WHERE i.indexrelid=index_oid;
      IF index_ok IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'recording_joined_hours_recording_sealed_idx exists but is not the valid exact sealed-hour index';
      END IF;
    END IF;
  END IF;
END $$;

DO $$
DECLARE
  table_oid REGCLASS := to_regclass('recording_joined_artifacts');
  index_oid REGCLASS := to_regclass('recording_joined_artifacts_published_hour_idx');
  index_ok BOOLEAN;
BEGIN
  IF table_oid IS NOT NULL THEN
    IF index_oid IS NULL THEN
      IF pg_relation_size(table_oid) > 134217728 THEN
        RAISE EXCEPTION 'precreate recording_joined_artifacts_published_hour_idx with CREATE INDEX CONCURRENTLY ON recording_joined_artifacts(account_id,hour_record_id,batch_record_id,artifact_kind,ordinal,id) WHERE published_at IS NOT NULL AND etag IS NOT NULL AND etag<>'''' AND version_id IS NOT NULL before migration';
      END IF;
      CREATE INDEX recording_joined_artifacts_published_hour_idx
        ON recording_joined_artifacts(account_id,hour_record_id,batch_record_id,artifact_kind,ordinal,id)
        WHERE published_at IS NOT NULL AND etag IS NOT NULL AND etag<>'' AND version_id IS NOT NULL;
    ELSE
      SELECT i.indisvalid AND i.indisready
             AND i.indrelid=table_oid AND i.indnkeyatts=6 AND i.indnatts=6
             AND pg_get_indexdef(i.indexrelid,1,false)='account_id'
             AND pg_get_indexdef(i.indexrelid,2,false)='hour_record_id'
             AND pg_get_indexdef(i.indexrelid,3,false)='batch_record_id'
             AND pg_get_indexdef(i.indexrelid,4,false)='artifact_kind'
             AND pg_get_indexdef(i.indexrelid,5,false)='ordinal'
             AND pg_get_indexdef(i.indexrelid,6,false)='id'
             AND pg_get_expr(i.indpred,i.indrelid)='((published_at IS NOT NULL) AND (etag IS NOT NULL) AND (etag <> ''''::text) AND (version_id IS NOT NULL))'
        INTO index_ok
        FROM pg_index i
       WHERE i.indexrelid=index_oid;
      IF index_ok IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'recording_joined_artifacts_published_hour_idx exists but is not the valid exact published-artifact index';
      END IF;
    END IF;
  END IF;
END $$;
