BEGIN;

-- Phase 1 identity foundation: the standard User/Org/Membership shape, layered
-- ON TOP of the existing accounts table (which already IS the org and keys every
-- resource + billing object via account_id). This is a RENAME-IN-PLACE: accounts
-- stays as the org; users + memberships are added and every existing session is
-- backfilled so no live login breaks. account_members, accounts.role, and
-- account_sessions.member_email are intentionally KEPT this phase for rollback
-- safety; a later phase drops them.

-- users: one row per real person, keyed by normalized email (lower+trim, matching
-- the codebase's normalizeAccountEmail). is_operator is the platform admin flag
-- that requireAdminAuth keys on (replaces accounts.role='admin').
CREATE TABLE users (
  id                BIGSERIAL PRIMARY KEY,
  email             TEXT NOT NULL UNIQUE,
  name              TEXT NOT NULL DEFAULT '',
  is_operator       BOOLEAN NOT NULL DEFAULT false,
  email_verified_at TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- memberships: a user's role within an org (accounts row). NO unique-email
-- constraint, so one user can belong to several orgs (this drops the old
-- one-team-per-email limit). Three roles: owner, billing_admin, member.
CREATE TABLE memberships (
  user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  org_id      BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  role        TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('owner','billing_admin','member')),
  invited_by  BIGINT REFERENCES users(id),
  invited_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  accepted_at TIMESTAMPTZ,
  PRIMARY KEY (user_id, org_id)
);

CREATE INDEX idx_memberships_org_id ON memberships (org_id);

-- accounts (org) gains is_personal: an org with a single member is a personal org
-- (auto-created on first sign-in); a multi-member org is a real team.
ALTER TABLE accounts ADD COLUMN is_personal BOOLEAN NOT NULL DEFAULT false;

-- Session and magic link now carry the acting user + the org they are acting in.
-- account_id stays (NOT NULL) as the org for backward compatibility; current_org_id
-- is the authoritative org for session resolution going forward.
ALTER TABLE account_sessions    ADD COLUMN user_id BIGINT REFERENCES users(id);
ALTER TABLE account_sessions    ADD COLUMN current_org_id BIGINT REFERENCES accounts(id);
ALTER TABLE account_magic_links ADD COLUMN user_id BIGINT REFERENCES users(id);
ALTER TABLE account_magic_links ADD COLUMN target_org_id BIGINT REFERENCES accounts(id);

-- Cheap FK for later per-member cost attribution; legacy recordings stay NULL.
ALTER TABLE recordings ADD COLUMN created_by_user_id BIGINT REFERENCES users(id);

-- Backfill (all in this txn) -------------------------------------------------

-- 1) users: one row per DISTINCT normalized email across accounts.email UNION
-- account_members.member_email. is_operator=true only where the email owns an
-- accounts row with role='admin' (today: only deniz@aydemir.us).
INSERT INTO users (email, name, is_operator)
SELECT e, '', bool_or(is_admin)
FROM (
  SELECT lower(trim(email)) AS e, (role = 'admin') AS is_admin FROM accounts
  UNION ALL
  SELECT lower(trim(member_email)) AS e, false AS is_admin FROM account_members
) s
GROUP BY e;

-- 2) memberships: from account_members. invited_by is remapped from an
-- accounts.id to the corresponding users.id (via that account's email). The
-- LEFT JOINs tolerate any unresolvable inviter as NULL (nullable FK). accepted_at
-- is copied as-is, preserving invited-but-not-accepted rows (deniz18418 NULL).
INSERT INTO memberships (user_id, org_id, role, invited_by, invited_at, accepted_at)
SELECT u.id, am.account_id, am.role, iu.id, am.invited_at, am.accepted_at
FROM account_members am
JOIN users u ON u.email = lower(trim(am.member_email))
LEFT JOIN accounts ib ON ib.id = am.invited_by
LEFT JOIN users iu ON iu.email = lower(trim(ib.email));

-- 3) accounts.is_personal: true where the org has exactly ONE membership. Runs
-- AFTER memberships is populated. account 1 has 2 members -> stays false.
UPDATE accounts a
SET is_personal = true
WHERE (SELECT count(*) FROM memberships m WHERE m.org_id = a.id) = 1;

-- 4) account_sessions: bind every existing session to its acting user + org so no
-- live session is logged out. The acting user is COALESCE(member_email, accounts
-- .email) (the invited member's own email for team sessions, else the account
-- owner). current_org_id is the session's account_id. The correlation to the
-- session (rs) lives in WHERE, not in a FROM-list JOIN ON, because Postgres does
-- not expose the UPDATE target inside the FROM joins' ON clauses.
UPDATE account_sessions rs
SET current_org_id = rs.account_id,
    user_id = u.id
FROM accounts a, users u
WHERE a.id = rs.account_id
  AND u.email = lower(trim(COALESCE(rs.member_email, a.email)));

-- 5) account_magic_links: bind unconsumed, unexpired links so a link minted just
-- before this deploy still resolves through the new complete path. Consumed and
-- expired links are left NULL (they can never complete).
UPDATE account_magic_links ml
SET target_org_id = ml.account_id,
    user_id = u.id
FROM accounts a, users u
WHERE a.id = ml.account_id
  AND u.email = lower(trim(COALESCE(ml.member_email, a.email)))
  AND ml.consumed_at IS NULL
  AND ml.expires_at > now();

-- 6) recordings.created_by_user_id: left NULL for legacy rows (seoul crosswalk).

COMMIT;
