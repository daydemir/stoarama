-- The capture-health window for a paused recording ends at the instant it was
-- paused. updated_at cannot serve this purpose because schedule edits also write
-- it. Existing paused rows get the best available one-time boundary; all future
-- pause/resume transitions maintain paused_at explicitly.
ALTER TABLE recordings ADD COLUMN paused_at TIMESTAMPTZ;
ALTER TABLE recordings ADD COLUMN completed_captured_clip_count BIGINT;
ALTER TABLE recordings ADD COLUMN completed_expected_clip_count BIGINT;
COMMENT ON COLUMN recordings.completed_expected_clip_count IS
  'Frozen at completion; NULL means capture health is unavailable for a legacy or oversized schedule';
UPDATE recordings SET paused_at=updated_at WHERE status='paused';
ALTER TABLE recordings ADD CONSTRAINT recordings_paused_at_chk CHECK (
  (status='paused' AND paused_at IS NOT NULL) OR
  (status<>'paused' AND paused_at IS NULL)
);
