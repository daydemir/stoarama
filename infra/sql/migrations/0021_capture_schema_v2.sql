BEGIN;

ALTER TABLE streams RENAME COLUMN stream_url TO source_url;
ALTER TABLE streams RENAME COLUMN capture_mode TO capture_type;
ALTER TABLE streams RENAME COLUMN capture_config_jsonb TO execution_config_jsonb;

ALTER TABLE streams
  ADD COLUMN IF NOT EXISTS source_family TEXT NOT NULL DEFAULT 'video_stream',
  ADD COLUMN IF NOT EXISTS execution_class TEXT NOT NULL DEFAULT 'video_live';

UPDATE streams
SET
  capture_type = CASE capture_type
    WHEN 'youtube_live' THEN 'youtube_watch'
    WHEN 'youtube_relay' THEN 'youtube_watch'
    WHEN 'hls_live' THEN 'hls'
    WHEN 'image_poll' THEN 'still_image'
    WHEN 'ffmpeg_direct' THEN 'http_video'
    WHEN 'auto' THEN 'unknown'
    WHEN 'unsupported' THEN 'unknown'
    ELSE capture_type
  END,
  execution_class = CASE capture_type
    WHEN 'youtube_live' THEN 'youtube_direct'
    WHEN 'youtube_relay' THEN 'youtube_relay'
    WHEN 'hls_live' THEN 'video_live'
    WHEN 'image_poll' THEN 'image_poll'
    WHEN 'ffmpeg_direct' THEN 'video_live'
    ELSE execution_class
  END,
  source_family = CASE capture_type
    WHEN 'youtube_live' THEN 'watch_page'
    WHEN 'youtube_relay' THEN 'watch_page'
    WHEN 'hls_live' THEN 'video_manifest'
    WHEN 'image_poll' THEN 'still_image'
    WHEN 'ffmpeg_direct' THEN 'video_stream'
    ELSE source_family
  END;

ALTER TABLE streams
  DROP CONSTRAINT IF EXISTS streams_source_family_check;
ALTER TABLE streams
  ADD CONSTRAINT streams_source_family_check
  CHECK (source_family IN ('watch_page', 'video_manifest', 'video_stream', 'still_image', 'provider_api', 'embed_page'));

ALTER TABLE streams
  DROP CONSTRAINT IF EXISTS streams_capture_type_check;
ALTER TABLE streams
  ADD CONSTRAINT streams_capture_type_check
  CHECK (capture_type IN ('youtube_watch', 'hls', 'dash', 'rtsp', 'rtmp', 'http_video', 'still_image', 'webrtc', 'unknown'));

ALTER TABLE streams
  DROP CONSTRAINT IF EXISTS streams_execution_class_check;
ALTER TABLE streams
  ADD CONSTRAINT streams_execution_class_check
  CHECK (execution_class IN ('youtube_direct', 'youtube_relay', 'video_live', 'image_poll'));

ALTER TABLE stream_capture_runtime RENAME COLUMN effective_mode TO execution_class;
ALTER TABLE stream_capture_runtime
  ADD COLUMN IF NOT EXISTS resolved_capture_type TEXT;

UPDATE stream_capture_runtime
SET
  execution_class = CASE execution_class
    WHEN 'youtube_live' THEN 'youtube_direct'
    WHEN 'youtube_relay' THEN 'youtube_relay'
    WHEN 'hls_live' THEN 'video_live'
    WHEN 'image_poll' THEN 'image_poll'
    WHEN 'ffmpeg_direct' THEN 'video_live'
    ELSE execution_class
  END,
  resolved_capture_type = CASE
    WHEN resolved_url IS NULL OR btrim(resolved_url) = '' THEN NULL
    WHEN position('.m3u8' in lower(resolved_url)) > 0 THEN 'hls'
    WHEN lower(resolved_url) LIKE 'rtsp://%' THEN 'rtsp'
    WHEN lower(resolved_url) LIKE 'rtmp://%' THEN 'rtmp'
    WHEN lower(resolved_url) ~ '\\.(jpg|jpeg|png|webp)(\\?|$)' THEN 'still_image'
    WHEN lower(resolved_url) LIKE 'http://%' OR lower(resolved_url) LIKE 'https://%' THEN 'http_video'
    ELSE 'unknown'
  END;

ALTER TABLE stream_capture_runtime
  DROP CONSTRAINT IF EXISTS stream_capture_runtime_execution_class_check;
ALTER TABLE stream_capture_runtime
  ADD CONSTRAINT stream_capture_runtime_execution_class_check
  CHECK (execution_class IS NULL OR execution_class IN ('youtube_direct', 'youtube_relay', 'video_live', 'image_poll'));

ALTER TABLE stream_capture_runtime
  DROP CONSTRAINT IF EXISTS stream_capture_runtime_resolved_capture_type_check;
ALTER TABLE stream_capture_runtime
  ADD CONSTRAINT stream_capture_runtime_resolved_capture_type_check
  CHECK (resolved_capture_type IS NULL OR resolved_capture_type IN ('youtube_watch', 'hls', 'dash', 'rtsp', 'rtmp', 'http_video', 'still_image', 'webrtc', 'unknown'));

ALTER TABLE recording_assignments RENAME COLUMN mode TO execution_class;
ALTER TABLE recording_assignments
  DROP CONSTRAINT IF EXISTS chk_recording_assignments_mode;
UPDATE recording_assignments
SET execution_class = CASE execution_class
  WHEN 'youtube_live' THEN 'youtube_direct'
  WHEN 'youtube_relay' THEN 'youtube_relay'
  WHEN 'hls_live' THEN 'video_live'
  WHEN 'image_poll' THEN 'image_poll'
  WHEN 'ffmpeg_direct' THEN 'video_live'
  ELSE execution_class
END;

ALTER TABLE recording_assignments
  ADD CONSTRAINT chk_recording_assignments_execution_class
  CHECK (execution_class IN ('youtube_direct', 'youtube_relay', 'video_live', 'image_poll'));

ALTER INDEX IF EXISTS idx_recording_assignments_server_mode RENAME TO idx_recording_assignments_server_execution_class;

ALTER TABLE server_mode_capacity RENAME TO server_execution_capacity;
ALTER TABLE server_execution_capacity RENAME COLUMN mode TO execution_class;
ALTER TABLE server_execution_capacity
  DROP CONSTRAINT IF EXISTS chk_server_mode_capacity_mode;

CREATE TEMP TABLE tmp_server_execution_capacity AS
SELECT
  server_id,
  CASE execution_class
    WHEN 'youtube_live' THEN 'youtube_direct'
    WHEN 'youtube_relay' THEN 'youtube_relay'
    WHEN 'hls_live' THEN 'video_live'
    WHEN 'image_poll' THEN 'image_poll'
    WHEN 'ffmpeg_direct' THEN 'video_live'
    ELSE execution_class
  END AS execution_class,
  MAX(max_active) AS max_active,
  BOOL_OR(draining) AS draining,
  MAX(heartbeat_at) AS heartbeat_at,
  MAX(lease_expires_at) AS lease_expires_at,
  COALESCE((ARRAY_AGG(metadata_jsonb ORDER BY updated_at DESC NULLS LAST))[1], '{}'::jsonb) AS metadata_jsonb,
  MAX(updated_at) AS updated_at
FROM server_execution_capacity
GROUP BY 1, 2;

DELETE FROM server_execution_capacity;

INSERT INTO server_execution_capacity (
  server_id,
  execution_class,
  max_active,
  draining,
  heartbeat_at,
  lease_expires_at,
  metadata_jsonb,
  updated_at
)
SELECT
  server_id,
  execution_class,
  max_active,
  draining,
  heartbeat_at,
  lease_expires_at,
  metadata_jsonb,
  updated_at
FROM tmp_server_execution_capacity;

DROP TABLE tmp_server_execution_capacity;

ALTER TABLE server_execution_capacity
  ADD CONSTRAINT chk_server_execution_capacity_class
  CHECK (execution_class IN ('youtube_direct', 'youtube_relay', 'video_live', 'image_poll'));

ALTER INDEX IF EXISTS idx_server_mode_capacity_expiry RENAME TO idx_server_execution_capacity_expiry;
ALTER INDEX IF EXISTS idx_server_mode_capacity_mode_expiry RENAME TO idx_server_execution_capacity_class_expiry;

ALTER TABLE capture_worker_heartbeats RENAME COLUMN mode TO execution_class;
ALTER TABLE capture_worker_heartbeats
  DROP CONSTRAINT IF EXISTS chk_capture_worker_heartbeats_mode;
UPDATE capture_worker_heartbeats
SET execution_class = CASE execution_class
  WHEN 'youtube_live' THEN 'youtube_direct'
  WHEN 'youtube_relay' THEN 'youtube_relay'
  WHEN 'hls_live' THEN 'video_live'
  WHEN 'image_poll' THEN 'image_poll'
  WHEN 'ffmpeg_direct' THEN 'video_live'
  ELSE execution_class
END;

ALTER TABLE capture_worker_heartbeats
  ADD CONSTRAINT chk_capture_worker_heartbeats_class
  CHECK (execution_class IN ('youtube_direct', 'youtube_relay', 'video_live', 'image_poll'));

ALTER INDEX IF EXISTS idx_capture_worker_heartbeats_mode_expiry RENAME TO idx_capture_worker_heartbeats_class_expiry;

ALTER TABLE processing_worker_heartbeats RENAME COLUMN mode TO execution_class;

UPDATE processing_worker_heartbeats
SET execution_class = CASE execution_class
  WHEN 'youtube_live' THEN 'youtube_direct'
  WHEN 'youtube_relay' THEN 'youtube_relay'
  WHEN 'hls_live' THEN 'video_live'
  WHEN 'image_poll' THEN 'image_poll'
  WHEN 'ffmpeg_direct' THEN 'video_live'
  ELSE execution_class
END;

ALTER TABLE recording_process_runs RENAME COLUMN mode TO execution_class;
ALTER TABLE recording_process_runs
  DROP CONSTRAINT IF EXISTS chk_recording_process_runs_mode;
UPDATE recording_process_runs
SET execution_class = CASE execution_class
  WHEN 'youtube_live' THEN 'youtube_direct'
  WHEN 'youtube_relay' THEN 'youtube_relay'
  WHEN 'hls_live' THEN 'video_live'
  WHEN 'image_poll' THEN 'image_poll'
  WHEN 'ffmpeg_direct' THEN 'video_live'
  ELSE execution_class
END;

ALTER TABLE recording_process_runs
  ADD CONSTRAINT chk_recording_process_runs_class
  CHECK (execution_class IN ('youtube_direct', 'youtube_relay', 'video_live', 'image_poll'));

ALTER TABLE recording_assignment_events RENAME COLUMN mode TO execution_class;

ALTER TABLE recording_mode_capacity RENAME COLUMN mode TO execution_class;
ALTER TABLE recording_mode_capacity
  DROP CONSTRAINT IF EXISTS chk_recording_mode_capacity_mode;

CREATE TEMP TABLE tmp_recording_mode_capacity AS
SELECT
  CASE execution_class
    WHEN 'youtube_live' THEN 'youtube_direct'
    WHEN 'youtube_relay' THEN 'youtube_relay'
    WHEN 'hls_live' THEN 'video_live'
    WHEN 'image_poll' THEN 'image_poll'
    WHEN 'ffmpeg_direct' THEN 'video_live'
    ELSE execution_class
  END AS execution_class,
  MAX(max_active) AS max_active,
  MAX(updated_at) AS updated_at
FROM recording_mode_capacity
GROUP BY 1;

DELETE FROM recording_mode_capacity;

INSERT INTO recording_mode_capacity (execution_class, max_active, updated_at)
SELECT execution_class, max_active, updated_at
FROM tmp_recording_mode_capacity;

DROP TABLE tmp_recording_mode_capacity;

ALTER TABLE recording_mode_capacity
  ADD CONSTRAINT chk_recording_mode_capacity_class
  CHECK (execution_class IN ('youtube_direct', 'youtube_relay', 'video_live', 'image_poll'));

COMMIT;
