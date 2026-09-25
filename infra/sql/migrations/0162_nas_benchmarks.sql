BEGIN;

-- Additive: NAS host facts and one-shot media benchmarks.
--
-- The NAS pull client reports what it runs on (CPU architecture and count,
-- memory, container cgroup limits, whether its state volume may execute
-- binaries) on every heartbeat. A benchmark measures, on one real hour of clips
-- already on the NAS, what NAS-side collation would cost: remux, packet hash
-- chain, seam-only decode and full strict decode. The client never modifies
-- /clips; its only writes are a temp output under /state that it deletes.
ALTER TABLE connections ADD COLUMN IF NOT EXISTS nas_host JSONB;
ALTER TABLE connections ADD COLUMN IF NOT EXISTS nas_host_reported_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS nas_benchmarks (
  id BIGSERIAL PRIMARY KEY,
  connection_id BIGINT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
  state TEXT NOT NULL DEFAULT 'requested'
    CHECK (state IN ('requested', 'claimed', 'reported', 'abandoned')),
  requested_by TEXT NOT NULL CHECK (requested_by IN ('auto', 'operator')),
  requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Optional operator pin; otherwise the server picks a recent complete hour.
  recording_id BIGINT,
  window_start_at TIMESTAMPTZ,
  -- Frozen at first claim: the exact clips (path, size, sha256) to read.
  plan JSONB,
  claim_count INTEGER NOT NULL DEFAULT 0 CHECK (claim_count >= 0),
  claimed_at TIMESTAMPTZ,
  lease_expires_at TIMESTAMPTZ,
  reported_at TIMESTAMPTZ,
  client_version TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  error TEXT NOT NULL DEFAULT '',
  host JSONB,
  result JSONB,
  CHECK ((recording_id IS NULL) = (window_start_at IS NULL)),
  CHECK ((state = 'reported') = (reported_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS nas_benchmarks_connection_idx
  ON nas_benchmarks (connection_id, id DESC);

COMMIT;
