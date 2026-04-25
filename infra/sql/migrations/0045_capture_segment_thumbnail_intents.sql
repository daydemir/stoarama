ALTER TABLE upload_intents
  DROP CONSTRAINT IF EXISTS upload_intents_kind_check;

ALTER TABLE upload_intents
  ADD CONSTRAINT upload_intents_kind_check
  CHECK (kind IN ('boxed', 'capture_segment', 'capture_segment_thumbnail'));
