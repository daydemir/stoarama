BEGIN;

ALTER TABLE accounts
ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'member'
CHECK (role IN ('member', 'admin'));

COMMIT;
