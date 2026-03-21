BEGIN;

ALTER TABLE stream_capture_runtime
  DROP CONSTRAINT IF EXISTS stream_capture_runtime_resolved_capture_type_check;

ALTER TABLE stream_capture_runtime
  ADD CONSTRAINT stream_capture_runtime_resolved_capture_type_check
  CHECK (
    resolved_capture_type IS NULL
    OR resolved_capture_type IN (
      'youtube_watch',
      'hls',
      'dash',
      'rtsp',
      'rtmp',
      'http_video',
      'still_image',
      'webrtc',
      'unknown'
    )
  );

COMMIT;
