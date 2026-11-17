-- A historical runnable job need not be dead when durable admission prevents
-- it from writing. Preserve r2_retirement_writers_drained as a diagnostic.
-- This proves database-admitted writes are blocked, not external process death:
-- execution still requires verified deployed writer coverage and drain of any
-- external PUT already accepted before its capability expired. The immutable
-- seven-day deadline is a minimum, not a bound on those requests' completion.
CREATE FUNCTION r2_retirement_writes_blocked(b text, endpoint text)
RETURNS boolean LANGUAGE sql AS $$
 SELECT EXISTS(SELECT 1 FROM r2_retirement_writer_fences
   WHERE bucket=b AND endpoint_sha256=endpoint
     AND drain_not_before<clock_timestamp())
 AND NOT (
   EXISTS(SELECT 1 FROM r2_retirement_writer_operations WHERE completed_at IS NULL)
   OR EXISTS(SELECT 1 FROM r2_retirement_writer_capabilities WHERE expires_at>clock_timestamp())
   OR EXISTS(SELECT 1 FROM recording_upload_intents WHERE expires_at>clock_timestamp())
   OR EXISTS(SELECT 1 FROM upload_intents WHERE expires_at>clock_timestamp())
   OR EXISTS(SELECT 1 FROM capture_session_leases WHERE lease_expires_at>clock_timestamp())
   OR EXISTS(SELECT 1 FROM nas_restore_requests WHERE put_expires_at>clock_timestamp())
 );
$$;
