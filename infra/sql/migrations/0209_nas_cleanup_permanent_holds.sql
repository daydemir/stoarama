-- Source holds have no deletion lifecycle; effective releases are append-only
-- receipts, never removal of the original hold. Incident roster/sources are
-- immutable VALUES views (0202), already refusing DELETE and TRUNCATE.
CREATE FUNCTION guard_nas_cleanup_hold_removal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'retention protected: NAS cleanup source holds are permanent' USING ERRCODE='55000';
END $$;
CREATE TRIGGER nas_cleanup_hold_delete BEFORE DELETE ON nas_cleanup_source_holds
 FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_hold_removal();
CREATE TRIGGER nas_cleanup_hold_truncate BEFORE TRUNCATE ON nas_cleanup_source_holds
 FOR EACH STATEMENT EXECUTE FUNCTION guard_nas_cleanup_hold_removal();
