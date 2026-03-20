BEGIN;

CREATE TABLE IF NOT EXISTS research_accounts (
  id BIGSERIAL PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
  email_verified_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_research_accounts_updated_at ON research_accounts;
CREATE TRIGGER trg_research_accounts_updated_at
BEFORE UPDATE ON research_accounts
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS research_magic_links (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES research_accounts(id) ON DELETE CASCADE,
  token_hash TEXT NOT NULL UNIQUE,
  purpose TEXT NOT NULL DEFAULT 'login' CHECK (purpose IN ('login')),
  redirect_path TEXT NOT NULL DEFAULT '/account',
  requester_ip TEXT NOT NULL DEFAULT '',
  user_agent TEXT NOT NULL DEFAULT '',
  expires_at TIMESTAMPTZ NOT NULL,
  consumed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_research_magic_links_account_created
ON research_magic_links (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_magic_links_active
ON research_magic_links (expires_at)
WHERE consumed_at IS NULL;

CREATE TABLE IF NOT EXISTS research_sessions (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES research_accounts(id) ON DELETE CASCADE,
  session_hash TEXT NOT NULL UNIQUE,
  expires_at TIMESTAMPTZ NOT NULL,
  last_used_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_research_sessions_updated_at ON research_sessions;
CREATE TRIGGER trg_research_sessions_updated_at
BEFORE UPDATE ON research_sessions
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_research_sessions_account_created
ON research_sessions (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_sessions_active
ON research_sessions (expires_at)
WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS research_api_keys (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES research_accounts(id) ON DELETE CASCADE,
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

DROP TRIGGER IF EXISTS trg_research_api_keys_updated_at ON research_api_keys;
CREATE TRIGGER trg_research_api_keys_updated_at
BEFORE UPDATE ON research_api_keys
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_research_api_keys_account_created
ON research_api_keys (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_api_keys_active
ON research_api_keys (created_at DESC)
WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS research_auth_events (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT REFERENCES research_accounts(id) ON DELETE SET NULL,
  api_key_id BIGINT REFERENCES research_api_keys(id) ON DELETE SET NULL,
  event_type TEXT NOT NULL,
  actor_type TEXT NOT NULL DEFAULT 'system',
  actor_ref TEXT NOT NULL DEFAULT '',
  detail_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_research_auth_events_account_created
ON research_auth_events (account_id, created_at DESC);

COMMIT;
