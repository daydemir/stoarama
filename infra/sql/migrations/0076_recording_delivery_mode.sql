BEGIN;

-- delivery is the per-recording storage-delivery mode: 'managed' (footage lives in
-- the account's managed storage / BYO / WebDAV delivery, and stays there) or
-- 'nas_pull' (the account's NAS pull client drains and releases each clip). It is
-- the single per-recording flag that (a) gates which recordings' clips the pull
-- feed ever hands out and (b) drives the storage label. Default 'managed' so every
-- non-NAS recording is correct by construction.
ALTER TABLE recordings
  ADD COLUMN delivery TEXT NOT NULL DEFAULT 'managed'
  CHECK (delivery IN ('managed', 'nas_pull'));

-- Backfill to preserve today's behavior EXACTLY: currently the pull feed is scoped
-- only by account, so any account with an active NAS-pull connection has ALL of its
-- clips drained account-wide. Mark every recording of such an account 'nas_pull' so
-- the newly-added feed predicate (r.delivery='nas_pull') keeps draining the same
-- clips it does today. Everything else stays 'managed'. Going forward delivery is
-- set explicitly per recording at create time, so this account-wide sweep is a
-- one-time migration of the pre-existing implicit behavior, not the ongoing rule.
UPDATE recordings
SET delivery = 'nas_pull'
WHERE account_id IN (
  SELECT account_id FROM connections WHERE kind = 'nas_pull'
);

COMMIT;
