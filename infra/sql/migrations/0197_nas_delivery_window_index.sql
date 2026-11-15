-- A populated database must precreate this index CONCURRENTLY in its separately
-- reviewed operations window. New empty databases can initialize it normally.
DO $$
BEGIN
 IF to_regclass('idx_recording_clips_active_window') IS NULL THEN
  LOCK TABLE recording_clips IN SHARE MODE;
  IF EXISTS(SELECT 1 FROM recording_clips LIMIT 1) THEN
   RAISE EXCEPTION 'precreate idx_recording_clips_active_window concurrently before this migration';
  END IF;
  CREATE INDEX idx_recording_clips_active_window ON recording_clips
   (recording_id,id,clip_start_at,clip_end_at,size_bytes,created_at)
   WHERE purged_at IS NULL AND released_at IS NULL;
 END IF;
 IF NOT EXISTS(
  SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
  JOIN pg_am a ON a.oid=c.relam
  WHERE i.indexrelid=to_regclass('idx_recording_clips_active_window')
   AND i.indrelid='recording_clips'::regclass
   AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
   AND a.amname='btree' AND i.indnkeyatts=6 AND i.indnatts=6
   AND i.indexprs IS NULL AND i.indoption::text='0 0 0 0 0 0'
   AND ARRAY(SELECT pg_get_indexdef(i.indexrelid,n,false) FROM generate_series(1,6)n)
       =ARRAY['recording_id','id','clip_start_at','clip_end_at','size_bytes','created_at']
   AND pg_get_expr(i.indpred,i.indrelid)='((purged_at IS NULL) AND (released_at IS NULL))'
 ) THEN
  RAISE EXCEPTION 'idx_recording_clips_active_window is absent, invalid or has another definition';
 END IF;
END $$;
