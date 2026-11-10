-- Metadata-only audit tables. Neither operation clears issued authority or
-- recovery holds. No existing evidence is rewritten or backfilled.
CREATE TABLE nas_cleanup_reconciliations (
 item_id bigint NOT NULL REFERENCES nas_cleanup_items(id),
 attempt integer NOT NULL,
 observation jsonb NOT NULL,
 action text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(item_id,attempt)
);
CREATE TABLE nas_cleanup_unexecuted_releases (
 item_id bigint PRIMARY KEY REFERENCES nas_cleanup_items(id),
 plan_sha256 text NOT NULL CHECK(plan_sha256 ~ '^[a-f0-9]{64}$'),
 attempt integer NOT NULL,
 receipt jsonb NOT NULL,
 reason text NOT NULL CHECK(length(trim(reason)) BETWEEN 1 AND 500),
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE FUNCTION guard_nas_cleanup_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'cleanup audit is immutable';
END $$;
CREATE TRIGGER nas_cleanup_reconciliation_immutable BEFORE UPDATE OR DELETE ON nas_cleanup_reconciliations FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_audit();
CREATE TRIGGER nas_cleanup_release_immutable BEFORE UPDATE OR DELETE ON nas_cleanup_unexecuted_releases FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_audit();

-- A release only affects reservations of a terminal retired plan. Issued flags,
-- receipts, permits, and the plan's permanent R2/source recovery holds survive.
CREATE OR REPLACE VIEW nas_cleanup_reserved_files AS
 SELECT p.account_id,p.connection_id,p.label,
        e.value->>'kind' AS kind,(e.value->>'source_id')::bigint AS source_id
 FROM nas_cleanup_plans p
 CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n)
 LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n
 LEFT JOIN nas_cleanup_unexecuted_releases u ON u.item_id=i.id
 WHERE p.mode<>'retired' OR (i.delete_ever_issued AND u.item_id IS NULL) OR i.id IS NULL;
