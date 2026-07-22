BEGIN;

-- The capture-health window for a paused recording ends at the instant it was
-- paused. updated_at cannot serve this purpose because schedule edits also write
-- it. Existing paused rows get the best available one-time boundary; all future
-- pause/resume transitions maintain paused_at explicitly.
ALTER TABLE recordings ADD COLUMN paused_at TIMESTAMPTZ;
UPDATE recordings SET paused_at=updated_at WHERE status='paused';
ALTER TABLE recordings ADD CONSTRAINT recordings_paused_at_chk CHECK (
  (status='paused' AND paused_at IS NOT NULL) OR
  (status<>'paused' AND paused_at IS NULL)
);

COMMIT;
