-- A dedicated canary worker is outside the shared autoscaler pool.  It is
-- deliberately a separate role rather than an overloaded name convention so
-- capacity accounting and lease admission can fail closed.
ALTER TABLE recorder_droplets
  ADD COLUMN IF NOT EXISTS pool_role TEXT NOT NULL DEFAULT 'shared';

ALTER TABLE recorder_droplets
  DROP CONSTRAINT IF EXISTS recorder_droplets_pool_role_check,
  ADD CONSTRAINT recorder_droplets_pool_role_check
  CHECK (pool_role IN ('shared', 'dedicated_canary'));

CREATE INDEX IF NOT EXISTS idx_recorder_droplets_pool_role_state
  ON recorder_droplets (pool_role, state);

-- The reservation is the sole authority for a dedicated canary lease.  It is
-- append-auditable by state, owner/fence bound, and intentionally remains an
-- active blocker after expiry until its owner explicitly releases it.  That
-- prevents an expired canary from silently falling back onto protected shared
-- capacity.
CREATE TABLE IF NOT EXISTS recording_dedicated_canary_reservations (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  recording_id  BIGINT NOT NULL REFERENCES recordings(id) ON DELETE RESTRICT,
  worker_name   TEXT NOT NULL,
  owner         TEXT NOT NULL CHECK (length(btrim(owner)) BETWEEN 1 AND 128),
  expires_at    TIMESTAMPTZ NOT NULL,
  state         TEXT NOT NULL DEFAULT 'active'
                CHECK (state IN ('active', 'released', 'failed')),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  released_at   TIMESTAMPTZ,
  CHECK (length(btrim(worker_name)) BETWEEN 1 AND 128),
  CHECK ((state='active' AND released_at IS NULL) OR (state<>'active' AND released_at IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dedicated_canary_recording_active
  ON recording_dedicated_canary_reservations (recording_id)
  WHERE state='active';
CREATE UNIQUE INDEX IF NOT EXISTS idx_dedicated_canary_worker_active
  ON recording_dedicated_canary_reservations (worker_name)
  WHERE state='active';
CREATE INDEX IF NOT EXISTS idx_dedicated_canary_expiry
  ON recording_dedicated_canary_reservations (expires_at)
  WHERE state='active';
