BEGIN;

-- Droplet pool bookkeeping for the recorder workers. The autoscaler that fills
-- this table is a later phase; the recorder-worker lease/heartbeat path reads it
-- now for the draining-exclusion check and the per-droplet liveness heartbeat.
-- A manually-provisioned worker simply has no row here and is never excluded.
CREATE TABLE IF NOT EXISTS recorder_droplets (
  id               BIGSERIAL PRIMARY KEY,
  name             TEXT NOT NULL,                               -- droplet name; also worker_id / lease_owner
  node_id          BIGINT REFERENCES nodes(id) ON DELETE SET NULL, -- the local_recorder node minted for this droplet
  do_droplet_id    BIGINT,                                      -- DO Droplet.ID; NULL until Create returns
  region           TEXT NOT NULL,
  size             TEXT NOT NULL,
  capacity         INTEGER NOT NULL CHECK (capacity > 0),       -- = worker concurrency
  state            TEXT NOT NULL DEFAULT 'provisioning'
                     CHECK (state IN ('provisioning','active','draining','destroying','destroyed','failed')),
  ip_address       TEXT NOT NULL DEFAULT '',
  last_seen_at     TIMESTAMPTZ,                                 -- worker droplet-heartbeat (independent of job heartbeat)
  provision_error  TEXT NOT NULL DEFAULT '',
  idle_since       TIMESTAMPTZ,
  drain_started_at TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  destroyed_at     TIMESTAMPTZ
);

DROP TRIGGER IF EXISTS trg_recorder_droplets_updated_at ON recorder_droplets;
CREATE TRIGGER trg_recorder_droplets_updated_at BEFORE UPDATE ON recorder_droplets
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE UNIQUE INDEX IF NOT EXISTS idx_recorder_droplets_do_id
  ON recorder_droplets (do_droplet_id) WHERE do_droplet_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_recorder_droplets_name ON recorder_droplets (name);
CREATE INDEX IF NOT EXISTS idx_recorder_droplets_state
  ON recorder_droplets (state) WHERE state IN ('provisioning','active','draining');

-- Cooldown ledger; survives restart. Leadership is process-level via the dedicated control service.
CREATE TABLE IF NOT EXISTS recorder_pool_state (
  id                 INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  last_scale_up_at   TIMESTAMPTZ,
  last_scale_down_at TIMESTAMPTZ,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO recorder_pool_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

COMMIT;
