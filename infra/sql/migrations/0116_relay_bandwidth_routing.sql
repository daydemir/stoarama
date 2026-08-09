ALTER TABLE relay_groups
  ADD COLUMN IF NOT EXISTS bandwidth_capacity_bps BIGINT;

ALTER TABLE relay_groups
  DROP CONSTRAINT IF EXISTS relay_groups_bandwidth_capacity_bps_check;
ALTER TABLE relay_groups
  ADD CONSTRAINT relay_groups_bandwidth_capacity_bps_check
  CHECK (bandwidth_capacity_bps IS NULL OR bandwidth_capacity_bps BETWEEN 1000000 AND 10000000000);

CREATE TABLE IF NOT EXISTS recording_bandwidth_observations (
  recording_id BIGINT PRIMARY KEY REFERENCES recordings(id) ON DELETE CASCADE,
  observed_bandwidth_bps BIGINT NOT NULL CHECK (observed_bandwidth_bps > 0),
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON COLUMN relay_groups.bandwidth_capacity_bps IS
  'Conservative aggregate native-media bandwidth budget for this shared internet connection. NULL derives a budget from max_streams.';
COMMENT ON COLUMN recording_bandwidth_observations.observed_bandwidth_bps IS
  'High-water native clip bitrate with slow decay, learned from successful ingests; scheduling signal only and never a transcode target.';
