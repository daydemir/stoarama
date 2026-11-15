-- Controller-only, after a fresh reviewed production window. One autocommit
-- CREATE, no IF NOT EXISTS/drop/retry. Inspect any failure or invalid remainder.
\set ON_ERROR_STOP on
SET application_name='stoarama-feed-index-197';
SET maintenance_work_mem='64MB';
SET max_parallel_maintenance_workers=0;
SET work_mem='16MB';
-- CIC waits on old snapshots using the lock timeout too. A 5s timeout would
-- abort on the known 55-60s readers even though they do not block ordinary DML.
SET lock_timeout='90s';
SET statement_timeout='30min';
CREATE INDEX CONCURRENTLY idx_recording_clips_active_window
 ON public.recording_clips(recording_id,id,clip_start_at,clip_end_at,size_bytes,created_at)
 WHERE purged_at IS NULL AND released_at IS NULL;
