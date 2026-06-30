-- 0063_user_created_streams.sql
--
-- An MIT lab asked to add public CCTV/livestream URLs they have found directly
-- to the shared catalog. Stream additions submitted by a signed-in account go
-- straight into the catalog (no moderation queue) but must be distinguishable
-- from admin/service imports and attributable to the account that added them.
--   1. user_created flags rows added by an account through the add-stream form.
--   2. created_by_account_id records which account added it (NULL for admin and
--      service imports). ON DELETE behavior is the default (RESTRICT) so an
--      account that has contributed catalog entries cannot be silently dropped.

BEGIN;

ALTER TABLE streams
  ADD COLUMN IF NOT EXISTS user_created BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE streams
  ADD COLUMN IF NOT EXISTS created_by_account_id BIGINT NULL REFERENCES accounts(id);

COMMIT;
