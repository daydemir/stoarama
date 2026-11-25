-- Day cleanup reads every catalogued clip directly in one NAS directory with a
-- left-anchored display_path LIKE. Without a pattern index that is a full scan
-- of recording_clips (17.6 s per directory in production).
-- Large tables require exact concurrent precreation outside auto-migration:
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS recording_clips_display_path_pattern ON recording_clips (display_path text_pattern_ops);
-- Never drop/rebuild an incompatible or invalid index automatically.
DO $$
DECLARE
  table_oid REGCLASS := to_regclass('recording_clips');
  index_oid REGCLASS := to_regclass('recording_clips_display_path_pattern');
  index_ok BOOLEAN;
BEGIN
  IF index_oid IS NULL THEN
    IF pg_relation_size(table_oid) > 134217728 THEN
      RAISE EXCEPTION 'precreate recording_clips_display_path_pattern with CREATE INDEX CONCURRENTLY ON recording_clips (display_path text_pattern_ops) before migration';
    END IF;
    CREATE INDEX recording_clips_display_path_pattern ON recording_clips (display_path text_pattern_ops);
  ELSE
    SELECT i.indisvalid AND i.indisready AND NOT i.indisunique
           AND i.indrelid=table_oid AND i.indnkeyatts=1 AND i.indnatts=1 AND i.indpred IS NULL
           AND am.amname='btree'
           AND pg_get_indexdef(i.indexrelid,1,false)='display_path'
           AND oc.opcname='text_pattern_ops'
      INTO index_ok
      FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
      JOIN pg_am am ON am.oid=c.relam
      JOIN pg_opclass oc ON oc.oid=i.indclass[0]
     WHERE i.indexrelid=index_oid;
    IF index_ok IS DISTINCT FROM true THEN
      RAISE EXCEPTION 'recording_clips_display_path_pattern exists but is not the valid exact display_path text_pattern_ops index';
    END IF;
  END IF;
END $$;
