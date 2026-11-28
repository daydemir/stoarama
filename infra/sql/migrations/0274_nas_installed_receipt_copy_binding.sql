-- Keep existing raw-copy and nonce-bound storage telemetry contracts for the
-- exact installed-receipt successor; no cleanup or stitch admission here.
ALTER TABLE connections DROP CONSTRAINT connections_nas_storage_client_version_check,
 ADD CONSTRAINT connections_nas_storage_client_version_check
 CHECK(nas_storage_client_version IN ('','r2-copy-v5','r2-copy-v6','r2-copy-v7-certified-stitch','r2-copy-v8-low-memory','r2-copy-v8-low-memory-cleanup','r2-copy-v8-keep20-cleanup','r2-copy-v8-fast-copy','r2-copy-v8-tunable-copy','r2-copy-v8-tunable-day','r2-copy-v8-tunable-modes','r2-copy-v8-ke-day','r2-copy-v8-output-restore','r2-copy-v8-inventory','r2-copy-v8-inventory-receipt'));

CREATE OR REPLACE FUNCTION guard_nas_r2_combined_lease_cap() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE capacity integer := 4;
BEGIN
 IF NEW.state IN ('leased','refresh_leased') AND NEW.lease_expires_at>clock_timestamp()
 AND (TG_OP='INSERT' OR OLD.state NOT IN ('leased','refresh_leased') OR NEW.lease_token IS DISTINCT FROM OLD.lease_token) THEN
  IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'COPY admission requires READ COMMITTED'; END IF;
  IF NOT pg_try_advisory_xact_lock(7343226011192) THEN RAISE EXCEPTION 'COPY admission busy' USING ERRCODE='55P03'; END IF;
  IF EXISTS (SELECT 1 FROM connections WHERE id=NEW.connection_id AND client_version IN ('r2-copy-v8-fast-copy','r2-copy-v8-tunable-copy','r2-copy-v8-tunable-day','r2-copy-v8-tunable-modes','r2-copy-v8-ke-day','r2-copy-v8-output-restore','r2-copy-v8-inventory','r2-copy-v8-inventory-receipt')) THEN
   capacity := 64;
  END IF;
  IF (SELECT count(*) FROM nas_r2_copy_items WHERE connection_id=NEW.connection_id AND id<>NEW.id AND state IN ('leased','refresh_leased') AND lease_expires_at>clock_timestamp())>=capacity THEN
   IF capacity=4 THEN RAISE EXCEPTION 'COPY four-lease capacity exhausted'; END IF;
   RAISE EXCEPTION 'COPY fast lease capacity exhausted';
  END IF;
 END IF;
 RETURN NEW;
END $$;
