BEGIN;

-- connections: an account-owned, self-serve NAS pull connection. Each row pairs
-- a label + poll interval with exactly ONE account_api_keys row whose scopes are
-- limited to 'stoarama.pull' (see confineAccountScope in the API). The pull
-- client posts a heartbeat each tick to advance last_cursor_id/clips_pulled and
-- stamp last_seen_at, which drives the derived health shown in the account UI.
-- The 'stoarama.pull' scope value is added at mint time (account_api_keys.scopes
-- is already a TEXT[] defaulting to {stoarama.read}); no column change is needed
-- and existing read/full keys keep working unchanged.
CREATE TABLE IF NOT EXISTS connections (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('nas_pull')),
  label TEXT NOT NULL DEFAULT 'NAS',
  api_key_id BIGINT NOT NULL REFERENCES account_api_keys(id) ON DELETE CASCADE,
  last_cursor_id BIGINT NOT NULL DEFAULT 0,
  last_seen_at TIMESTAMPTZ,
  clips_pulled BIGINT NOT NULL DEFAULT 0,
  poll_interval_sec INT NOT NULL DEFAULT 90 CHECK (poll_interval_sec BETWEEN 10 AND 3600),
  created_by BIGINT REFERENCES accounts(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_connections_updated_at ON connections;
CREATE TRIGGER trg_connections_updated_at
BEFORE UPDATE ON connections
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_connections_account_created
ON connections (account_id, created_at DESC);

-- One connection per key: the heartbeat resolves a connection by its calling
-- api_key_id, so the api_key_id -> connection mapping must be unique.
CREATE UNIQUE INDEX IF NOT EXISTS idx_connections_api_key_id
ON connections (api_key_id);

COMMIT;
