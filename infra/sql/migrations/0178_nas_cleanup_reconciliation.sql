-- Metadata-only reconciliation audit. Never clears issued authority or
-- recovery holds. No existing evidence is rewritten or backfilled.
CREATE TABLE nas_cleanup_reconciliations (
 item_id bigint NOT NULL REFERENCES nas_cleanup_items(id),
 attempt integer NOT NULL,
 observation jsonb NOT NULL,
 action text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(item_id,attempt)
);
CREATE FUNCTION guard_nas_cleanup_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'cleanup audit is immutable';
END $$;
CREATE TRIGGER nas_cleanup_reconciliation_immutable BEFORE UPDATE OR DELETE ON nas_cleanup_reconciliations FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_audit();
