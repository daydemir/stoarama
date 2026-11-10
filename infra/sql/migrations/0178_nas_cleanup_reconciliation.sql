-- Metadata-only reconciliation audit. Never clears issued authority or
-- recovery holds. No existing evidence is rewritten or backfilled.
DO $migration$ BEGIN
 IF to_regclass('nas_cleanup_items') IS NULL THEN RETURN; END IF;
CREATE TABLE nas_cleanup_reconciliations (
 item_id bigint NOT NULL REFERENCES nas_cleanup_items(id),
 attempt integer NOT NULL,
 observation jsonb NOT NULL,
 action text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(item_id,attempt)
);
CREATE OR REPLACE FUNCTION guard_nas_cleanup_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'cleanup audit is immutable';
END $$;
CREATE TRIGGER nas_cleanup_reconciliation_immutable BEFORE UPDATE OR DELETE ON nas_cleanup_reconciliations FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_audit();

-- Nullable lease identity: existing leases deliberately cannot grant authority.
ALTER TABLE nas_cleanup_items
 ADD COLUMN lease_connection_id bigint,
 ADD COLUMN lease_client_boot_id text,
 ADD COLUMN lease_client_version text;
-- Committed independently before observation work, so a rollback never makes
-- a submitted request new again. Global request IDs fence cross-boot replay.
CREATE TABLE nas_cleanup_observation_requests (
 request_id text PRIMARY KEY,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER nas_cleanup_observation_request_immutable BEFORE UPDATE OR DELETE ON nas_cleanup_observation_requests FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_audit();
END $migration$;
