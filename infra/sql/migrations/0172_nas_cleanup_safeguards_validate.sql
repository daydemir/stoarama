-- Validate the 0171 constraints in their own transaction. VALIDATE CONSTRAINT
-- takes only SHARE UPDATE EXCLUSIVE, so cleanup traffic keeps reading and
-- writing while the rows are checked. Validating nas_cleanup_plans_no_nas_only
-- proves no legacy plan row has allow_nas_only=true.
ALTER TABLE nas_cleanup_plans VALIDATE CONSTRAINT nas_cleanup_plans_no_nas_only;
ALTER TABLE nas_cleanup_items VALIDATE CONSTRAINT nas_cleanup_items_state_check;
