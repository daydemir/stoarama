BEGIN;

ALTER TABLE youtube_relay_sources
  ADD COLUMN IF NOT EXISTS node_id BIGINT REFERENCES nodes(id) ON DELETE SET NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_youtube_relay_sources_node_id
ON youtube_relay_sources (node_id)
WHERE node_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_youtube_relay_sources_node_lease
ON youtube_relay_sources (node_id, lease_expires_at DESC)
WHERE node_id IS NOT NULL;

COMMIT;
