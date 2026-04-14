CREATE INDEX IF NOT EXISTS idx_capture_segments_stream_status_end_id
ON capture_segments (stream_id, capture_status, segment_end_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_capture_segments_stream_thumb_end_id
ON capture_segments (stream_id, capture_status, segment_end_at DESC, id DESC)
WHERE thumbnail_media_object_id IS NOT NULL;
