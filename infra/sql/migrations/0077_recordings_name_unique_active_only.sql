BEGIN;

-- Recording names are unique per account (case-insensitive). But deleting a
-- recording is a SOFT delete: handleAccountRecordingDelete flips status to
-- 'canceled' and KEEPS the row (release-not-delete, task #62). The original
-- unique index idx_recordings_account_name (0051) had no status predicate, so a
-- canceled recording reserved its name forever: re-creating a recording with a
-- previously-used name (e.g. after a test) failed with 23505 ("a recording with
-- that name already exists"), even though the create handler's pre-check already
-- excludes canceled rows (server_recordings.go: "status <> 'canceled'"). That
-- made the DB disagree with the app and blocked legitimate re-creates.
--
-- Make the index PARTIAL so only live (non-canceled) recordings reserve a name.
-- Canceled rows keep all their data and clips; they simply stop holding the name.
-- Active/paused names remain unique as before. This exactly matches the app-layer
-- pre-check, so the 23505 backstop is now only hit on a genuine live-name race.
DROP INDEX IF EXISTS idx_recordings_account_name;
CREATE UNIQUE INDEX IF NOT EXISTS idx_recordings_account_name
  ON recordings (account_id, lower(name))
  WHERE status <> 'canceled';

COMMIT;
