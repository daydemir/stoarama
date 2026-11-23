-- The API applies the configured batch cap under the same exclusive COPY lock.
-- The database independently enforces the hard maximum for the new release.
CREATE OR REPLACE FUNCTION guard_nas_r2_combined_lease_cap() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE capacity integer := 4;
BEGIN
 IF NEW.state IN ('leased','refresh_leased') AND NEW.lease_expires_at>clock_timestamp()
 AND (TG_OP='INSERT' OR OLD.state NOT IN ('leased','refresh_leased') OR NEW.lease_token IS DISTINCT FROM OLD.lease_token) THEN
  IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'COPY admission requires READ COMMITTED'; END IF;
  IF NOT pg_try_advisory_xact_lock(7343226011192) THEN RAISE EXCEPTION 'COPY admission busy' USING ERRCODE='55P03'; END IF;
  IF EXISTS (SELECT 1 FROM connections WHERE id=NEW.connection_id AND client_version='r2-copy-v8-fast-copy') THEN
   capacity := 64;
  END IF;
  IF (SELECT count(*) FROM nas_r2_copy_items WHERE connection_id=NEW.connection_id AND id<>NEW.id AND state IN ('leased','refresh_leased') AND lease_expires_at>clock_timestamp())>=capacity THEN
   IF capacity=4 THEN RAISE EXCEPTION 'COPY four-lease capacity exhausted'; END IF;
   RAISE EXCEPTION 'COPY fast lease capacity exhausted';
  END IF;
 END IF;
 RETURN NEW;
END $$;
