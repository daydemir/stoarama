-- Additive only: two new tables, no change to existing tables or rows.

-- Per-node no-progress surrender memory for one continuous window. The relay
-- lease path makes a node that already failed a window yield it to nodes that
-- have not, so a source a node cannot capture (for example a TLS chain its
-- FFmpeg rejects) is not handed back to that node after the 5-minute handoff.
CREATE TABLE recording_job_node_failures (
  recording_job_id BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE CASCADE,
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  failure_count INTEGER NOT NULL CHECK (failure_count > 0),
  last_failed_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (recording_job_id, node_id)
);

-- Episode dedupe for operator alerts that are not scoped to one recording
-- (recording-scoped signals keep using recorder_health_alerts). alert_key is
-- the stable identity of the subject, e.g. 'recorder_droplet_stale:1132'.
CREATE TABLE ops_alert_episodes (
  alert_key TEXT PRIMARY KEY CHECK (btrim(alert_key) <> ''),
  signal TEXT NOT NULL CHECK (btrim(signal) <> ''),
  first_detected_at TIMESTAMPTZ NOT NULL,
  last_detected_at TIMESTAMPTZ NOT NULL,
  last_alerted_at TIMESTAMPTZ,
  resolved_at TIMESTAMPTZ
);
CREATE INDEX ops_alert_episodes_open_idx ON ops_alert_episodes (signal) WHERE resolved_at IS NULL;
