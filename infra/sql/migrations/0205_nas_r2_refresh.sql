-- Bounded verify-existing work only. Default off; no activation or timestamp refresh.
CREATE TABLE r2_keep28_refresh_control (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 enabled boolean NOT NULL DEFAULT false,
 last_mode text NOT NULL DEFAULT 'copy' CHECK(last_mode IN ('copy','verify_existing'))
);
INSERT INTO r2_keep28_refresh_control DEFAULT VALUES;
CREATE TRIGGER r2_refresh_control_no_delete BEFORE DELETE ON r2_keep28_refresh_control FOR EACH ROW EXECUTE FUNCTION guard_r2_object_retirement();
CREATE TRIGGER r2_refresh_control_no_truncate BEFORE TRUNCATE ON r2_keep28_refresh_control FOR EACH STATEMENT EXECUTE FUNCTION guard_r2_object_retirement();
ALTER TABLE nas_r2_copy_items
 ADD COLUMN work_mode text NOT NULL DEFAULT 'copy' CHECK(work_mode IN ('copy','verify_existing')),
 ADD COLUMN custody_refreshed_at timestamptz,
 ADD COLUMN refresh_count bigint NOT NULL DEFAULT 0 CHECK(refresh_count>=0),
 DROP CONSTRAINT nas_r2_copy_items_state_check,
 ADD CONSTRAINT nas_r2_copy_items_state_check CHECK(state IN ('pending','leased','verified','blocked','refresh_pending','refresh_leased')),
 DROP CONSTRAINT nas_r2_copy_items_blocked_reason_check,
 ADD CONSTRAINT nas_r2_copy_items_blocked_reason_check CHECK(blocked_reason IN ('','source_changed','r2_identity','final_conflict','sidecar_conflict','partial_conflict','path_conflict','checksum_mismatch','verify_missing')),
 ADD CONSTRAINT nas_r2_refresh_never_download CHECK (
  (work_mode='copy' AND state NOT IN ('refresh_pending','refresh_leased')) OR
  (work_mode='verify_existing' AND state NOT IN ('pending','leased') AND verified_at IS NOT NULL
   AND range_until IS NULL AND range_id IS NULL AND range_length=0)
 );
CREATE INDEX nas_r2_copy_active ON nas_r2_copy_items(connection_id,state,lease_expires_at) WHERE state IN ('pending','leased','refresh_pending','refresh_leased');
CREATE FUNCTION guard_nas_r2_refresh_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF OLD.work_mode='verify_existing' THEN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'refresh history is retained'; END IF;
  IF NEW.work_mode<>OLD.work_mode OR NEW.item<>OLD.item OR NEW.verified_at IS DISTINCT FROM OLD.verified_at
   OR NEW.completion<>OLD.completion OR NEW.downloaded_bytes<>OLD.downloaded_bytes
   OR NEW.committed_bytes<>OLD.committed_bytes OR NEW.attempts<OLD.attempts
   OR NEW.refresh_count<OLD.refresh_count THEN RAISE EXCEPTION 'refresh preserves original completion and byte history'; END IF;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
CREATE TRIGGER nas_r2_refresh_history BEFORE UPDATE OR DELETE ON nas_r2_copy_items FOR EACH ROW EXECUTE FUNCTION guard_nas_r2_refresh_history();

-- Both current and legacy lease admissions take the exclusive COPY lock.
-- Only a new/reassigned token enters here. Ordinary shared-lock renewals do not
-- acquire or upgrade a lock. Direct admissions fail fast if the lock is busy.
CREATE FUNCTION guard_nas_r2_combined_lease_cap() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.state IN ('leased','refresh_leased') AND NEW.lease_expires_at>clock_timestamp()
 AND (TG_OP='INSERT' OR OLD.state NOT IN ('leased','refresh_leased') OR NEW.lease_token IS DISTINCT FROM OLD.lease_token) THEN
  IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'COPY admission requires READ COMMITTED'; END IF;
  IF NOT pg_try_advisory_xact_lock(7343226011192) THEN RAISE EXCEPTION 'COPY admission busy' USING ERRCODE='55P03'; END IF;
  IF (SELECT count(*) FROM nas_r2_copy_items WHERE connection_id=NEW.connection_id AND id<>NEW.id AND state IN ('leased','refresh_leased') AND lease_expires_at>clock_timestamp())>=4 THEN
   RAISE EXCEPTION 'COPY four-lease capacity exhausted'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_r2_combined_lease_cap BEFORE INSERT OR UPDATE OF state,lease_token,lease_expires_at ON nas_r2_copy_items FOR EACH ROW EXECUTE FUNCTION guard_nas_r2_combined_lease_cap();
