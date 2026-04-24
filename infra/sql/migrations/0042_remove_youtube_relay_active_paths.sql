BEGIN;

UPDATE streams
SET execution_class = 'youtube_direct',
    updated_at = now()
WHERE execution_class = 'youtube_relay';

UPDATE stream_capture_runtime
SET execution_class = 'youtube_direct',
    updated_at = now()
WHERE execution_class = 'youtube_relay';

UPDATE recording_assignments
SET execution_class = 'youtube_direct',
    updated_at = now()
WHERE execution_class = 'youtube_relay';

DELETE FROM server_execution_capacity
WHERE execution_class = 'youtube_relay';

DELETE FROM capture_worker_heartbeats
WHERE execution_class = 'youtube_relay';

DELETE FROM processing_worker_heartbeats
WHERE worker_kind = 'capture'
  AND execution_class = 'youtube_relay';

DELETE FROM recording_mode_capacity
WHERE execution_class = 'youtube_relay';

DELETE FROM youtube_relay_routes;

DELETE FROM youtube_relay_sources;

ALTER TABLE streams
  DROP CONSTRAINT IF EXISTS streams_execution_class_check;
ALTER TABLE streams
  ADD CONSTRAINT streams_execution_class_check
  CHECK (execution_class IN ('youtube_direct', 'video_live', 'image_poll'));

ALTER TABLE stream_capture_runtime
  DROP CONSTRAINT IF EXISTS stream_capture_runtime_execution_class_check;
ALTER TABLE stream_capture_runtime
  ADD CONSTRAINT stream_capture_runtime_execution_class_check
  CHECK (execution_class IS NULL OR execution_class IN ('youtube_direct', 'video_live', 'image_poll'));

ALTER TABLE recording_assignments
  DROP CONSTRAINT IF EXISTS chk_recording_assignments_execution_class;
ALTER TABLE recording_assignments
  ADD CONSTRAINT chk_recording_assignments_execution_class
  CHECK (execution_class IN ('youtube_direct', 'video_live', 'image_poll'));

ALTER TABLE server_execution_capacity
  DROP CONSTRAINT IF EXISTS chk_server_execution_capacity_class;
ALTER TABLE server_execution_capacity
  ADD CONSTRAINT chk_server_execution_capacity_class
  CHECK (execution_class IN ('youtube_direct', 'video_live', 'image_poll'));

ALTER TABLE capture_worker_heartbeats
  DROP CONSTRAINT IF EXISTS chk_capture_worker_heartbeats_class;
ALTER TABLE capture_worker_heartbeats
  ADD CONSTRAINT chk_capture_worker_heartbeats_class
  CHECK (execution_class IN ('youtube_direct', 'video_live', 'image_poll'));

ALTER TABLE recording_mode_capacity
  DROP CONSTRAINT IF EXISTS chk_recording_mode_capacity_class;
ALTER TABLE recording_mode_capacity
  ADD CONSTRAINT chk_recording_mode_capacity_class
  CHECK (execution_class IN ('youtube_direct', 'video_live', 'image_poll'));

COMMIT;
