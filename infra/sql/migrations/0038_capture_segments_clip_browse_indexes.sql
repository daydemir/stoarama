CREATE INDEX IF NOT EXISTS idx_capture_segments_stream_status_start_id_downloadable
ON capture_segments (stream_id, capture_status, segment_start_at DESC, id DESC)
WHERE media_object_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_capture_segments_stream_status_end_start_downloadable
ON capture_segments (stream_id, capture_status, segment_end_at, segment_start_at)
WHERE media_object_id IS NOT NULL;
