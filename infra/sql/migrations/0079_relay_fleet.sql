BEGIN;

-- Relay fleet (P1, deployable dark). Adds the per-recording capture routing flag,
-- the per-relay-node stream cap, the 'relay' node_type, and the three lease/readiness
-- indexes. With no 'relay' node enrolled and no capture_via='relay' recording, every
-- query plan and response is unchanged: capture_via defaults to 'cloud' on every
-- existing row and the new node_type is simply an additional allowed value.

-- capture_via: which infrastructure runs the worker loop for this recording.
-- 'cloud' = operator droplet pool (default; all pre-existing rows).
-- 'relay' = an account-owned relay node on a user machine.
-- Orthogonal to delivery (0076): a relay recording still uses managed/nas_pull.
ALTER TABLE recordings
  ADD COLUMN capture_via TEXT NOT NULL DEFAULT 'cloud'
  CHECK (capture_via IN ('cloud', 'relay'));

-- relay_max_streams: max concurrent recording_jobs a relay node may hold leased.
-- Participates in the lease-gate capacity arithmetic, hence a typed column (cookie
-- state and other display-only relay signals live in capabilities_jsonb, not here).
ALTER TABLE nodes
  ADD COLUMN relay_max_streams INTEGER NOT NULL DEFAULT 5
  CHECK (relay_max_streams >= 1);

-- Relays are a distinct node_type from cloud droplets (which are also enrolled as
-- 'local_recorder'). Extend the CHECK constraint on nodes.node_type and on the
-- matching enrollment-token constraint to include 'relay'. This mirrors migration
-- 0039, which added 'yt_relay_source'. Droplet rows keep node_type='local_recorder'
-- unchanged, so the branch discriminator is a real typed field, not a heuristic.
ALTER TABLE nodes
  DROP CONSTRAINT IF EXISTS nodes_node_type_check;
ALTER TABLE nodes
  ADD CONSTRAINT nodes_node_type_check
  CHECK (node_type IN ('yt_relay_source', 'inference_node', 'local_recorder', 'relay'));

ALTER TABLE node_enrollment_tokens
  DROP CONSTRAINT IF EXISTS node_enrollment_tokens_node_type_check;
ALTER TABLE node_enrollment_tokens
  ADD CONSTRAINT node_enrollment_tokens_node_type_check
  CHECK (node_type IN ('yt_relay_source', 'inference_node', 'local_recorder', 'relay'));

-- Indexes for the relay lease gate + readiness queries.
--
-- H2 (index build vs. the live lease path): the migration runner (backend/internal/
-- db/migrate.go MigrateUp) executes EVERY migration file inside one transaction
-- (pool.Begin -> tx.Exec(whole file) -> Commit), so CREATE INDEX CONCURRENTLY is not
-- usable here (it cannot run in a transaction block). Plain CREATE INDEX takes a
-- SHARE lock that blocks writes on the table for the build duration. Measured prod
-- row counts (read-only, 2026-07-04): recording_jobs=50, recordings=14, nodes=41 --
-- all far below any size where the build is not effectively instantaneous, so the
-- brief lock on recording_jobs is a non-event for the live lease path. Plain
-- CREATE INDEX is the correct choice at this scale.
CREATE INDEX IF NOT EXISTS idx_recordings_capture_via_account
  ON recordings (account_id, capture_via)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_nodes_account_relay_heartbeat
  ON nodes (account_id, last_heartbeat_at DESC)
  WHERE node_type = 'relay' AND status = 'active';

CREATE INDEX IF NOT EXISTS idx_recording_jobs_leased_by_owner
  ON recording_jobs (lease_owner, lease_expires_at)
  WHERE status = 'leased';

COMMIT;
