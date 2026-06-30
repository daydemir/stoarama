BEGIN;

-- Per-recording normalize-to FPS for the recorder.
-- NULL = Source/native: the recorder stream-copies the clip (-c copy) and the
-- output keeps the stream's native frame rate, which is the existing behavior.
-- A non-null value (the UI offers 15 and 30) re-encodes each captured clip to
-- that exact frame rate (upsampling a slower source, downsampling a faster one).
ALTER TABLE recordings
  ADD COLUMN IF NOT EXISTS target_fps INTEGER
    CHECK (target_fps IS NULL OR target_fps BETWEEN 1 AND 240);

COMMENT ON COLUMN recordings.target_fps IS
  'Per-recording normalize-to FPS. NULL = Source/native (-c copy, no re-encode). Non-null re-encodes each clip to this rate.';

COMMIT;
