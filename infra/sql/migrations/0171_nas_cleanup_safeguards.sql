-- NAS cleanup belt-and-braces safeguards (safety council, 2026-11).
-- Grants no deletion authority; it only narrows what can be recorded.
--
-- 1. NAS-only deletion (deleting a NAS file with no verified R2 recovery copy)
--    was removed. The API rejects allow_nas_only; this constraint also refuses
--    it at the database. NOT VALID only so the migration can never fail on a
--    historical row (production had none); every new or updated row is
--    checked. Re-introducing NAS-only deletion needs its own reviewed change.
ALTER TABLE nas_cleanup_plans ADD CONSTRAINT nas_cleanup_plans_no_nas_only CHECK (NOT allow_nas_only) NOT VALID;

-- 2. An honest receipt state: the media file was unlinked but the sidecar
--    step failed afterwards (previously recorded as plain 'unverified').
ALTER TABLE nas_cleanup_items DROP CONSTRAINT nas_cleanup_items_state_check;
ALTER TABLE nas_cleanup_items ADD CONSTRAINT nas_cleanup_items_state_check
 CHECK (state IN ('pending','leased','would_delete','keep','missing','conflict','delete_issued','deleted','unverified','media_deleted_sidecar_failed'));
