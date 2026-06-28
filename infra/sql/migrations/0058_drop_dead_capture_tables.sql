-- 0058_drop_dead_capture_tables.sql
--
-- Forward-only. Drops the fully-dead tables left behind after removing the OLD
-- do-capture live-capture pipeline (capturescheduled worker, capture-job queue,
-- youtube-relay control plane, legacy recording dashboard). Every table dropped
-- here has ZERO remaining references in the Go code after that cut (verified by
-- repo-wide grep over internal/ + cmd/).
--
-- KEEPS all recorded data (streams, frames, media_objects, capture_segments) and
-- the entire new recorder schema. Intentionally does NOT drop recording_settings,
-- recording_assignments, stream_capture_runtime, server_execution_capacity, or
-- recording_process_runs: those are still read by surviving dashboard/health code
-- and the kept frame-ingest path, so they stay until a later cut removes those
-- readers. streams.recording_state* columns also stay (back v_stream_overview +
-- the recording_state trigger).
--
-- capture_jobs: frames.capture_job_id and capture_segments.capture_job_id both
-- REFERENCE capture_jobs(id) ON DELETE SET NULL (frames_capture_job_id_fkey,
-- capture_segments_capture_job_id_fkey). No view or rule depends on capture_jobs
-- (verified against the live schema). DROP ... CASCADE therefore removes only
-- those two FK constraints; the capture_job_id COLUMNS and all rows on
-- frames/capture_segments survive (browse reads the value, never joins back).

BEGIN;

DROP TABLE IF EXISTS youtube_relay_events;
DROP TABLE IF EXISTS youtube_relay_routes;
DROP TABLE IF EXISTS youtube_relay_sources;
DROP TABLE IF EXISTS recording_mode_capacity;
DROP TABLE IF EXISTS capture_jobs CASCADE;

COMMIT;
