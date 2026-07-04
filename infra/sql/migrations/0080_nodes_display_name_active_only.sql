BEGIN;

-- idx_nodes_account_display_name (0025) enforced UNIQUE (account_id, lower(display_name))
-- across ALL statuses, so a disabled node kept reserving its name. Removing a computer
-- disables its node row (DELETE /account/nodes/{id} -> status='disabled'), so re-enrolling
-- a computer with the same name hit "duplicate key ... idx_nodes_account_display_name
-- (SQLSTATE 23505)". Recreate the index PARTIAL so only non-disabled nodes reserve a name.
--
-- Safe as a plain in-txn drop+create (the migration runner applies each file in ONE
-- transaction, so CREATE INDEX CONCURRENTLY is not available). Measured on prod
-- 2026-07-04: nodes has 50 rows (4 active, 46 disabled) and zero active-name collisions
-- (SELECT ... WHERE status <> 'disabled' GROUP BY account_id, lower(display_name)
-- HAVING count(*) > 1 returned no rows), so the partial index builds without violation.

DROP INDEX IF EXISTS idx_nodes_account_display_name;

CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_account_display_name
ON nodes (account_id, lower(display_name))
WHERE status <> 'disabled';

COMMIT;
