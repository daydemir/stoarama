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

-- Compare the latest expiry to the cutoff outside the two large-table
-- subqueries. PostgreSQL can retrieve each max() with a backward index scan.
-- An empty table or all-NULL expiry yields false, as with the original EXISTS.
-- Sample one cutoff for all five expiry probes. Using the earlier clock means
-- a long transaction keeps its veto, and a backward wall-clock step before
-- the check cannot let transaction time prematurely clear a writer.
-- The exact fence still uses the strict wall-clock deadline. The function's
-- signature, volatility, ownership and grants are unchanged.
CREATE OR REPLACE FUNCTION r2_retirement_writes_blocked(b text, endpoint text)
RETURNS boolean LANGUAGE sql AS $$
 WITH expiry_cutoff AS MATERIALIZED (
   SELECT LEAST(now(), clock_timestamp()) AS cutoff
 )
 SELECT EXISTS(SELECT 1 FROM r2_retirement_writer_fences
   WHERE bucket=b AND endpoint_sha256=endpoint
     AND drain_not_before<clock_timestamp())
 AND NOT (
   EXISTS(SELECT 1 FROM r2_retirement_writer_operations WHERE completed_at IS NULL)
   OR EXISTS(SELECT 1 FROM r2_retirement_writer_capabilities WHERE expires_at>cutoff)
   OR COALESCE((SELECT max(expires_at) FROM recording_upload_intents)>cutoff,false)
   OR EXISTS(SELECT 1 FROM upload_intents WHERE expires_at>cutoff)
   OR EXISTS(SELECT 1 FROM capture_session_leases WHERE lease_expires_at>cutoff)
   OR COALESCE((SELECT max(put_expires_at) FROM nas_restore_requests)>cutoff,false)
 )
 FROM expiry_cutoff;
$$;
