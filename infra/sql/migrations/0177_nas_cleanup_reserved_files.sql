-- Shared read-only reservation rule for plan admission and keep28 preparation.
-- No table rewrite, backlog UPDATE, index build or constraint validation. This
-- takes only read locks on the existing tables under the runner's 5s lock_timeout.
-- Retired plans release only never-issued identities, across ALL policies.
-- A missing item row is incomplete evidence, so fail closed even for retirement.
-- Recovery/source holds remain governed by 0170, unchanged: any issued item
-- retains the entire plan's R2 recovery holds permanently.
CREATE VIEW nas_cleanup_reserved_files AS
 SELECT p.account_id,p.connection_id,p.label,
        e.value->>'kind' AS kind,(e.value->>'source_id')::bigint AS source_id
 FROM nas_cleanup_plans p
 CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n)
 LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n
 WHERE p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL;
