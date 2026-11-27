-- The r2prune lane's RECEIPTS read (ops/jelly/r2prune/cycle.py) finds each window's
-- live raw clips through idx_recording_clips_recording_started, whose only range is
-- clip_start_at < end_at. purged_at, bucket and clip_end_at were heap filters, so
-- every cycle fetched each recording's purged history too: on production
-- (2026-11-27) 1,002,151 heap rows for the 350,031 it keeps, about 911k buffers and
-- 75 s of a 115 s statement timeout. This partial btree holds only live clips and
-- checks both window bounds in the index, so the heap is read only for kept rows.
-- It changes plans, not results.
--
-- Follow 0149/0185/0267: never build a write-blocking index on a large live table.
-- A recording_clips over 128 MiB needs the exact index precreated outside this
-- transaction:
-- CREATE INDEX CONCURRENTLY r2prune_receipts_live_clip_window ON recording_clips (recording_id, clip_start_at, clip_end_at) WHERE bucket = 'stoarama' AND purged_at IS NULL;
DO $$
BEGIN
 IF to_regclass('r2prune_receipts_live_clip_window') IS NULL THEN
  IF pg_relation_size('recording_clips'::regclass)>134217728 THEN
   RAISE EXCEPTION 'precreate r2prune_receipts_live_clip_window with CREATE INDEX CONCURRENTLY r2prune_receipts_live_clip_window ON recording_clips (recording_id, clip_start_at, clip_end_at) WHERE bucket = ''stoarama'' AND purged_at IS NULL before migration';
  END IF;
  CREATE INDEX r2prune_receipts_live_clip_window ON recording_clips (recording_id, clip_start_at, clip_end_at)
   WHERE bucket = 'stoarama' AND purged_at IS NULL;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_index i WHERE i.indexrelid=to_regclass('r2prune_receipts_live_clip_window')
   AND i.indrelid='recording_clips'::regclass AND i.indisvalid AND i.indisready AND NOT i.indisunique
   AND split_part(pg_get_indexdef(i.indexrelid),' USING ',2)
     ='btree (recording_id, clip_start_at, clip_end_at) WHERE ((bucket = ''stoarama''::text) AND (purged_at IS NULL))') THEN
  RAISE EXCEPTION 'r2prune_receipts_live_clip_window exists but is not the valid exact index: DROP INDEX CONCURRENTLY r2prune_receipts_live_clip_window; then precreate it CONCURRENTLY before migration';
 END IF;
END $$;
