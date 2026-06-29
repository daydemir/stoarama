-- 0062_webdav_shared_storage.sql
--
-- An MIT lab wants recorded clips saved to their Synology NAS, reachable via a
-- Synology QuickConnect HTTPS URL relaying DSM WebDAV (no public SFTP port). This
-- migration adds:
--   1. A 'webdav' provider value (the provider column already has no CHECK
--      constraint, so 'webdav' coexists with 's3_compatible'/'r2_managed' with no
--      schema change; for a WebDAV destination endpoint=base WebDAV URL,
--      access_key_id=username, key_prefix=base path, secret_access_key_enc=the
--      encrypted password).
--   2. Reusable/shared destinations: deniz (the only admin) creates a destination
--      with shared=true and grants specific accounts access. Granted accounts may
--      SELECT it for recordings but never see its credentials.
--   3. capture-to-managed-then-transfer delivery: a WebDAV recording captures into
--      the account's MANAGED destination (unchanged presign path), then on ingest
--      auto-enqueues a clip_transfer_job to the WebDAV destination and, on
--      confirmed delivery, auto-purges the managed staging copy.

BEGIN;

-- Shared (admin-owned, reusable) destinations. Per-account BYO/managed rows stay
-- shared=false. provider gains 'webdav' as free text (no CHECK on provider today).
ALTER TABLE storage_destinations
  ADD COLUMN shared BOOLEAN NOT NULL DEFAULT false;

-- Which accounts may SELECT a shared destination for their recordings. The grant
-- never exposes credentials; it only authorizes selection. granted_by records the
-- admin who issued the grant. ON DELETE CASCADE drops grants when either the
-- destination or the account is removed.
CREATE TABLE storage_destination_grants (
  storage_destination_id BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE CASCADE,
  account_id             BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  granted_by             BIGINT NOT NULL REFERENCES accounts(id),
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (storage_destination_id, account_id)
);

CREATE INDEX idx_storage_destination_grants_account
  ON storage_destination_grants (account_id);

-- The delivery target for a WebDAV recording. storage_destination_id stays the
-- CAPTURE dest (the account's managed staging area); delivery_storage_destination_id
-- is the WebDAV destination the staged clip is transferred to. NULL for ordinary
-- S3/managed recordings (no transfer, no purge). ON DELETE RESTRICT so a WebDAV
-- destination referenced by a live recording cannot be dropped out from under it.
ALTER TABLE recordings
  ADD COLUMN delivery_storage_destination_id BIGINT REFERENCES storage_destinations(id) ON DELETE RESTRICT;

-- Distinguishes an auto delivery transfer (capture-to-managed staging that must be
-- purged after confirmed delivery) from a user-initiated clip copy (never purges
-- its source). Set true only by the ingest auto-enqueue path.
ALTER TABLE clip_transfer_jobs
  ADD COLUMN auto_purge_source BOOLEAN NOT NULL DEFAULT false;

COMMIT;
