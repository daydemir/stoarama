-- One-shot admission is an operator-scoped claim permit. Claim transactions
-- bind it to the exact active-claim set and consume it at most once.
ALTER TABLE recording_joined_admission_controls
  ADD COLUMN one_shot_expected_active_claims_sha256 TEXT,
  ADD COLUMN one_shot_claims_remaining SMALLINT NOT NULL DEFAULT 0,
  ADD CONSTRAINT recording_joined_admission_one_shot_check CHECK (
    (one_shot_claims_remaining=0 AND one_shot_expected_active_claims_sha256 IS NULL)
    OR (claims_paused AND one_shot_claims_remaining=1
      AND one_shot_expected_active_claims_sha256 ~ '^[0-9a-f]{64}$')
  );
