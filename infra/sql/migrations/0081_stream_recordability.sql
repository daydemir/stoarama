BEGIN;

-- Cloud-recordability probe state (ship-dark). Two additive tables, both start
-- EMPTY, so nothing changes behavior until the probe (gated behind
-- STREAM_RECORDABILITY_PROBE_ENABLED, default off) writes rows.
--
-- The migration runner applies each file in ONE transaction (see 0080), so this
-- is plain in-txn DDL: no CREATE INDEX CONCURRENTLY. Additive + backfill-safe.

-- Per-stream probe result (test-once memory). result='ok' means the stream
-- recorded fine once from a datacenter IP and is never re-probed. A stream with
-- NO row here is untested. 'source_unstable'/'inconclusive' rows are transient
-- and re-probed on a later sweep.
CREATE TABLE IF NOT EXISTS stream_recordability (
  stream_id      BIGINT PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
  last_probed_at TIMESTAMPTZ NOT NULL,
  result         TEXT NOT NULL CHECK (result IN ('ok','blocked','source_unstable','inconclusive')),
  detail         TEXT NOT NULL DEFAULT '',   -- ffmpeg/ffprobe signature, valid_ratio, error class (audit)
  probe_host     TEXT NOT NULL DEFAULT ''    -- which DO host ran it (audit)
);

-- Provider-level heuristic flag. A confirmed block on any stream flags the whole
-- provider (exact streams.provider string) so other UNTESTED streams from it are
-- prioritized for probing and default to relay. Sticky-true: never auto-cleared.
CREATE TABLE IF NOT EXISTS provider_recordability (
  provider         TEXT PRIMARY KEY,          -- exact streams.provider string
  needs_relay      BOOLEAN NOT NULL DEFAULT false,
  set_by_stream_id BIGINT REFERENCES streams(id) ON DELETE SET NULL,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
