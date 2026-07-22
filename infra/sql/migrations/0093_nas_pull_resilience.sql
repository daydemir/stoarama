ALTER TABLE connections
  ADD COLUMN IF NOT EXISTS bytes_pulled BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS client_version TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS client_started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS client_boot_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS client_phase TEXT NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS client_previous_exit TEXT NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS client_last_success_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS client_last_error TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS client_last_error_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_outage_class TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS last_outage_started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_outage_recovered_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_outage_failure_count INT NOT NULL DEFAULT 0;

ALTER TABLE connections
  DROP CONSTRAINT IF EXISTS chk_connections_client_phase,
  ADD CONSTRAINT chk_connections_client_phase CHECK (
    client_phase IN ('unknown', 'starting', 'idle', 'draining', 'updating', 'blocked', 'degraded')
  ),
  DROP CONSTRAINT IF EXISTS chk_connections_client_previous_exit,
  ADD CONSTRAINT chk_connections_client_previous_exit CHECK (
    client_previous_exit IN ('unknown', 'clean', 'self_update', 'unclean_process', 'unclean_reboot')
  ),
  DROP CONSTRAINT IF EXISTS chk_connections_last_outage_class,
  ADD CONSTRAINT chk_connections_last_outage_class CHECK (
    last_outage_class IN ('', 'dns_failed', 'timeout', 'connection', 'http', 'other')
  ),
  DROP CONSTRAINT IF EXISTS chk_connections_nonnegative_transfer_totals,
  ADD CONSTRAINT chk_connections_nonnegative_transfer_totals CHECK (
    bytes_pulled >= 0 AND last_outage_failure_count >= 0
  );

-- The pull feed is account-wide, so two clients for one account would race to
-- release the same clips. Keep the real product invariant explicit.
CREATE UNIQUE INDEX IF NOT EXISTS idx_connections_one_nas_pull_per_account
  ON connections (account_id)
  WHERE kind = 'nas_pull';
