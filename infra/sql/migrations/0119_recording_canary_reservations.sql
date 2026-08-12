CREATE TABLE IF NOT EXISTS recording_canary_reservations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS recording_canary_reservations_active_idx
  ON recording_canary_reservations (recording_id, expires_at);

CREATE INDEX IF NOT EXISTS recording_canary_reservations_node_active_idx
  ON recording_canary_reservations (node_id, expires_at);
