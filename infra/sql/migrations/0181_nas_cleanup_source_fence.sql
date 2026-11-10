-- Existing streams may predate the invalidation triggers in 0180. Every
-- authorization needs a real row to lock; an absent row cannot fence writers.
-- New streams already get a row from nas_cleanup_rankings_streams. Preserve
-- all existing versions (and cache validity) when backfilling untouched streams.
INSERT INTO nas_cleanup_rankings_versions(stream_id,version)
 SELECT id,0 FROM streams ORDER BY id
 ON CONFLICT(stream_id) DO NOTHING;
