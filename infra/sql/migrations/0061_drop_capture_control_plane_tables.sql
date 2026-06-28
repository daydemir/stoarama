-- 0061_drop_capture_control_plane_tables.sql
--
-- Cleanup after the do-capture pipeline was cut. Of the five inert control-plane
-- tables left from that pipeline, only one had ZERO remaining non-comment Go
-- references once the dead readers/writers were removed, so only that one is
-- dropped here.
--
-- DROPPED (0 references after code cleanup):
--   recording_settings
--     Its sole readers were two settings.GetRecordingSettings() calls in
--     handleDashboardOverview and handleDashboardStreams whose results were
--     immediately discarded (`_ = recordingSettings`). Those dead reads and the
--     now-unused settings.GetRecordingSettings/RecordingSettings reader were
--     removed; no live response field was computed from this table, so dropping
--     it changes no browse/detail/frames/ingest behavior. No FK points at it and
--     it has no dependent trigger/view, so a plain DROP (no CASCADE) is complete.
--
-- KEPT (still read by live features the user is about to test; would risk a 500
-- on endpoints streams.html fetches, or the detail path, if forced to zero refs):
--   stream_capture_runtime
--     Read by loadRuntimeIntoStream() -> getStreamByID() which backs the detail
--     handler (handleDashboardStreamDetail) plus account export / day-zip / import
--     paths, and LEFT JOINed by handleDashboardStreams / handleDashboardOverview /
--     handleDashboardQueueHealth. Feeds capture_runtime_status + freshness that
--     streams.html renders.
--   recording_assignments
--     Read by handleDashboardServers (streams.html fetches /dashboard/servers) and
--     by the assignment machinery reachable from handleStreamsPatch (admin edit).
--   recording_process_runs
--     Read by handleDashboardOverview + handleDashboardStreams (browse health
--     totals streams.html renders) and handleDashboardServers.
--   server_execution_capacity
--     Read by loadRecordingCapacitySnapshot() -> handleDashboardServers.
--
-- Also KEPT (unchanged, not in scope): streams.recording_state column,
-- v_stream_overview, the recording_state trigger, and recording_assignment_events.

BEGIN;

DROP TABLE IF EXISTS recording_settings;

COMMIT;
