BEGIN;

CREATE TABLE IF NOT EXISTS research_nodes (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES research_accounts(id) ON DELETE CASCADE,
  node_type TEXT NOT NULL CHECK (node_type IN ('yt_relay_source', 'inference_node')),
  display_name TEXT NOT NULL,
  hostname TEXT NOT NULL DEFAULT '',
  platform TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
  enrolled_at TIMESTAMPTZ,
  last_heartbeat_at TIMESTAMPTZ,
  capabilities_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_research_nodes_updated_at ON research_nodes;
CREATE TRIGGER trg_research_nodes_updated_at
BEFORE UPDATE ON research_nodes
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_research_nodes_account_created
ON research_nodes (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_nodes_last_heartbeat
ON research_nodes (last_heartbeat_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_research_nodes_account_display_name
ON research_nodes (account_id, lower(display_name));

CREATE TABLE IF NOT EXISTS research_node_enrollment_tokens (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES research_accounts(id) ON DELETE CASCADE,
  token_prefix TEXT NOT NULL,
  token_hash TEXT NOT NULL UNIQUE,
  node_type TEXT NOT NULL CHECK (node_type IN ('yt_relay_source', 'inference_node')),
  label TEXT NOT NULL DEFAULT '',
  expires_at TIMESTAMPTZ NOT NULL,
  consumed_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_research_node_enrollment_tokens_updated_at ON research_node_enrollment_tokens;
CREATE TRIGGER trg_research_node_enrollment_tokens_updated_at
BEFORE UPDATE ON research_node_enrollment_tokens
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_research_node_enrollment_tokens_account_created
ON research_node_enrollment_tokens (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_node_enrollment_tokens_active
ON research_node_enrollment_tokens (expires_at)
WHERE consumed_at IS NULL AND revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS research_node_tokens (
  id BIGSERIAL PRIMARY KEY,
  node_id BIGINT NOT NULL REFERENCES research_nodes(id) ON DELETE CASCADE,
  key_prefix TEXT NOT NULL,
  secret_hash TEXT NOT NULL UNIQUE,
  revoked_at TIMESTAMPTZ,
  last_used_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_research_node_tokens_updated_at ON research_node_tokens;
CREATE TRIGGER trg_research_node_tokens_updated_at
BEFORE UPDATE ON research_node_tokens
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_research_node_tokens_node_created
ON research_node_tokens (node_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_node_tokens_active
ON research_node_tokens (created_at DESC)
WHERE revoked_at IS NULL;

COMMIT;
