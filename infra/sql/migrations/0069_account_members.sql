BEGIN;

-- account_members: a LEAN team layer over the existing single-account model. A
-- person belongs to exactly ONE account/team, so member_email is GLOBALLY UNIQUE
-- and sign-in stays unambiguous (an email resolves to exactly one account_id).
-- This is intentionally separate from accounts.role (the operator/admin flag used
-- by requireAdminAuth); account_members.role is the team owner/member role only.
-- member_email is stored with the same normalization as accounts.email
-- (lowercased + trimmed on write), so the email->account_id resolution matches.
CREATE TABLE IF NOT EXISTS account_members (
  account_id   BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  member_email TEXT NOT NULL,
  role         TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('owner','member')),
  invited_by   BIGINT REFERENCES accounts(id),
  invited_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  accepted_at  TIMESTAMPTZ,
  UNIQUE (member_email)
);

CREATE INDEX IF NOT EXISTS idx_account_members_account_id ON account_members (account_id);

-- Carry the email the magic link was issued for through to the session so the
-- principal can resolve the acting member's identity + team role. Nullable so
-- legacy rows (created before this migration) stay valid; a NULL member_email is
-- treated as the account owner in lookupAccountSession.
ALTER TABLE account_sessions   ADD COLUMN IF NOT EXISTS member_email TEXT;
ALTER TABLE account_magic_links ADD COLUMN IF NOT EXISTS member_email TEXT;

-- Backfill: every existing account becomes its own owner, so nothing regresses.
-- accounts.email is already lowercased on write, so it matches normalization.
INSERT INTO account_members (account_id, member_email, role, accepted_at)
SELECT id, email, 'owner', now()
FROM accounts
ON CONFLICT (member_email) DO NOTHING;

COMMIT;
