-- r2_retirement_writes_blocked() seq-scanned recording_upload_intents (millions
-- of expired rows) and nas_restore_requests for its two expiry probes, so the R2
-- delete executor timed out calling it twice per object. These two plain btrees
-- are the only schema those probes need.
--
-- A populated database must precreate both indexes CONCURRENTLY in its separately
-- reviewed operations window, outside any transaction:
--  CREATE INDEX CONCURRENTLY IF NOT EXISTS r2wb_recording_upload_intents_expires_at ON recording_upload_intents(expires_at);
--  CREATE INDEX CONCURRENTLY IF NOT EXISTS r2wb_nas_restore_requests_put_expires_at ON nas_restore_requests(put_expires_at);
-- Empty databases create them inline below.
DO $$
BEGIN
 IF to_regclass('r2wb_recording_upload_intents_expires_at') IS NULL THEN
  LOCK TABLE recording_upload_intents IN SHARE MODE;
  IF EXISTS(SELECT 1 FROM recording_upload_intents LIMIT 1) THEN
   RAISE EXCEPTION 'precreate r2wb_recording_upload_intents_expires_at concurrently before this migration';
  END IF;
  CREATE INDEX r2wb_recording_upload_intents_expires_at ON recording_upload_intents(expires_at);
 END IF;
 IF NOT EXISTS(
  SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
  JOIN pg_am a ON a.oid=c.relam
  WHERE i.indexrelid=to_regclass('r2wb_recording_upload_intents_expires_at')
   AND i.indrelid='recording_upload_intents'::regclass
   AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
   AND a.amname='btree' AND i.indnkeyatts=1 AND i.indnatts=1
   AND i.indexprs IS NULL AND i.indpred IS NULL AND i.indoption::text='0'
   AND pg_get_indexdef(i.indexrelid,1,false)='expires_at'
 ) THEN
  RAISE EXCEPTION 'r2wb_recording_upload_intents_expires_at is absent, invalid or has another definition';
 END IF;
 IF to_regclass('r2wb_nas_restore_requests_put_expires_at') IS NULL THEN
  LOCK TABLE nas_restore_requests IN SHARE MODE;
  IF EXISTS(SELECT 1 FROM nas_restore_requests LIMIT 1) THEN
   RAISE EXCEPTION 'precreate r2wb_nas_restore_requests_put_expires_at concurrently before this migration';
  END IF;
  CREATE INDEX r2wb_nas_restore_requests_put_expires_at ON nas_restore_requests(put_expires_at);
 END IF;
 IF NOT EXISTS(
  SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
  JOIN pg_am a ON a.oid=c.relam
  WHERE i.indexrelid=to_regclass('r2wb_nas_restore_requests_put_expires_at')
   AND i.indrelid='nas_restore_requests'::regclass
   AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
   AND a.amname='btree' AND i.indnkeyatts=1 AND i.indnatts=1
   AND i.indexprs IS NULL AND i.indpred IS NULL AND i.indoption::text='0'
   AND pg_get_indexdef(i.indexrelid,1,false)='put_expires_at'
 ) THEN
  RAISE EXCEPTION 'r2wb_nas_restore_requests_put_expires_at is absent, invalid or has another definition';
 END IF;
END $$;

-- The 0198 function without a VOLATILE per-row clock in any expiry probe. The
-- R2 delete executor timed out at about 8.9s because both a bare and an
-- InitPlan-wrapped clock_timestamp() left these probes seq-scanning: the planner
-- cannot estimate or index on a VOLATILE value. Two shapes reach the indexes
-- above instead:
--  * max(col) > now() for the two indexed tables. max(x) > T is equivalent to
--    EXISTS(x > T) and an index-only or backward index scan finds it in one row;
--    NULL from an empty table coalesces to false, exactly like EXISTS.
--  * EXISTS(col > now()) for the other three clocked tables.
-- This is equally or more conservative than the per-row clock. now() is the
-- transaction start, which is never later than clock_timestamp(). With that
-- earlier time more rows count as live writers, so the function returns TRUE
-- (writes blocked, safe to delete) only in a subset of the cases where it
-- returned TRUE before. It never returns TRUE when the old one returned FALSE.
-- writer_operations has no clock and the fence-row drain_not_before check keeps
-- clock_timestamp() unchanged. Signature, language, default VOLATILE volatility,
-- owner and grants are unchanged.
CREATE OR REPLACE FUNCTION r2_retirement_writes_blocked(b text, endpoint text)
RETURNS boolean LANGUAGE sql AS $$
 SELECT EXISTS(SELECT 1 FROM r2_retirement_writer_fences
   WHERE bucket=b AND endpoint_sha256=endpoint
     AND drain_not_before<clock_timestamp())
 AND NOT (
   EXISTS(SELECT 1 FROM r2_retirement_writer_operations WHERE completed_at IS NULL)
   OR EXISTS(SELECT 1 FROM r2_retirement_writer_capabilities WHERE expires_at>now())
   OR COALESCE((SELECT max(expires_at) FROM recording_upload_intents)>now(),false)
   OR EXISTS(SELECT 1 FROM upload_intents WHERE expires_at>now())
   OR EXISTS(SELECT 1 FROM capture_session_leases WHERE lease_expires_at>now())
   OR COALESCE((SELECT max(put_expires_at) FROM nas_restore_requests)>now(),false)
 );
$$;
