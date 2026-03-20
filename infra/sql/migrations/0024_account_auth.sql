BEGIN;

CREATE TABLE IF NOT EXISTS accounts (
  id BIGSERIAL PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
  role TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('member', 'admin')),
  email_verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_accounts_updated_at ON accounts;
CREATE TRIGGER trg_accounts_updated_at
BEFORE UPDATE ON accounts
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS account_magic_links (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  token_hash TEXT NOT NULL UNIQUE,
  purpose TEXT NOT NULL DEFAULT 'login' CHECK (purpose IN ('login')),
  redirect_path TEXT NOT NULL DEFAULT '/account',
  requester_ip TEXT NOT NULL DEFAULT '',
  user_agent TEXT NOT NULL DEFAULT '',
  expires_at TIMESTAMPTZ NOT NULL,
  consumed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_account_magic_links_account_created
ON account_magic_links (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_account_magic_links_active
ON account_magic_links (expires_at)
WHERE consumed_at IS NULL;

CREATE TABLE IF NOT EXISTS account_sessions (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  session_hash TEXT NOT NULL UNIQUE,
  expires_at TIMESTAMPTZ NOT NULL,
  last_used_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_account_sessions_updated_at ON account_sessions;
CREATE TRIGGER trg_account_sessions_updated_at
BEFORE UPDATE ON account_sessions
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_account_sessions_account_created
ON account_sessions (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_account_sessions_active
ON account_sessions (expires_at)
WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS account_api_keys (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  key_prefix TEXT NOT NULL,
  secret_hash TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL DEFAULT 'default',
  scopes TEXT[] NOT NULL DEFAULT ARRAY['stoarama.read']::TEXT[],
  expires_at TIMESTAMPTZ,
  last_used_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_account_api_keys_updated_at ON account_api_keys;
CREATE TRIGGER trg_account_api_keys_updated_at
BEFORE UPDATE ON account_api_keys
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_account_api_keys_account_created
ON account_api_keys (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_account_api_keys_active
ON account_api_keys (created_at DESC)
WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS account_auth_events (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
  api_key_id BIGINT REFERENCES account_api_keys(id) ON DELETE SET NULL,
  event_type TEXT NOT NULL,
  actor_type TEXT NOT NULL DEFAULT 'system',
  actor_ref TEXT NOT NULL DEFAULT '',
  detail_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_account_auth_events_account_created
ON account_auth_events (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_accounts_role_created
ON accounts (role, created_at DESC);

COMMIT;
