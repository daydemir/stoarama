-- A parent/runtime observation cannot stand in for runnable process children.
-- Keep ordinary terminal updates legal; no owner death is inferred here.
CREATE TRIGGER r2_writer_capture_process BEFORE INSERT OR UPDATE ON recording_process_runs
 FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('status','starting,running');

-- Preserve all existing blockers and add process status independently of its
-- parent, owner, timestamps or whether the active-stream index includes it.
CREATE OR REPLACE FUNCTION r2_retirement_writers_drained(b text) RETURNS boolean LANGUAGE sql AS $$
 SELECT NOT (
 EXISTS(SELECT 1 FROM recordings WHERE status='active')
 OR EXISTS(SELECT 1 FROM recording_jobs WHERE status IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM streams WHERE recording_state='on')
 OR EXISTS(SELECT 1 FROM stream_capture_runtime WHERE status IN ('resolving','running'))
 OR EXISTS(SELECT 1 FROM capture_session_leases WHERE lease_expires_at>clock_timestamp())
 OR EXISTS(SELECT 1 FROM recording_upload_intents WHERE bucket=b AND expires_at>clock_timestamp())
 OR EXISTS(SELECT 1 FROM upload_intents WHERE bucket=b AND expires_at>clock_timestamp())
 OR EXISTS(SELECT 1 FROM r2_retirement_writer_operations WHERE completed_at IS NULL)
 OR EXISTS(SELECT 1 FROM r2_retirement_writer_capabilities WHERE bucket=b AND expires_at>clock_timestamp())
 OR EXISTS(SELECT 1 FROM clip_transfer_jobs WHERE status IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM inference_box_jobs WHERE status IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM nas_restore_requests WHERE state IN ('pending','leased') OR put_expires_at>clock_timestamp())
 OR EXISTS(SELECT 1 FROM nas_stitch_tasks WHERE state IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM recording_native_stitch_tasks WHERE state IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM recording_presentation_v2_probe_tasks WHERE state IN ('awaiting_retention','pending','leased'))
 OR EXISTS(SELECT 1 FROM recording_joined_batches WHERE state IN ('snapshotting','building','frozen','index_sealed'))
 OR EXISTS(SELECT 1 FROM recording_joined_hours WHERE state IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM recording_joined_stream_days WHERE state='pending')
 OR EXISTS(SELECT 1 FROM recording_joined_artifacts WHERE publication_state IN ('sealed','publishing'))
 OR EXISTS(SELECT 1 FROM recording_process_runs WHERE status IN ('starting','running'))
 );
$$;
