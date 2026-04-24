BEGIN;

DELETE FROM server_execution_capacity
WHERE execution_class = 'image_poll';

DELETE FROM capture_worker_heartbeats
WHERE execution_class = 'image_poll';

DELETE FROM processing_worker_heartbeats
WHERE worker_kind = 'capture'
  AND execution_class = 'image_poll';

DELETE FROM recording_mode_capacity
WHERE execution_class = 'image_poll';

ALTER TABLE server_execution_capacity
  DROP CONSTRAINT IF EXISTS chk_server_execution_capacity_class;
ALTER TABLE server_execution_capacity
  ADD CONSTRAINT chk_server_execution_capacity_class
  CHECK (execution_class IN ('youtube_direct', 'video_live'));

ALTER TABLE capture_worker_heartbeats
  DROP CONSTRAINT IF EXISTS chk_capture_worker_heartbeats_class;
ALTER TABLE capture_worker_heartbeats
  ADD CONSTRAINT chk_capture_worker_heartbeats_class
  CHECK (execution_class IN ('youtube_direct', 'video_live'));

ALTER TABLE recording_mode_capacity
  DROP CONSTRAINT IF EXISTS chk_recording_mode_capacity_class;
ALTER TABLE recording_mode_capacity
  ADD CONSTRAINT chk_recording_mode_capacity_class
  CHECK (execution_class IN ('youtube_direct', 'video_live'));

COMMIT;
