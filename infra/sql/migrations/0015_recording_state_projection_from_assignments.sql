BEGIN;

CREATE OR REPLACE FUNCTION sync_stream_recording_state_from_assignment()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    UPDATE streams
    SET
      recording_state = 'off',
      recording_failed_reason = NULL,
      recording_failed_at = NULL,
      updated_at = now()
    WHERE id = OLD.stream_id
      AND (
        recording_state <> 'off'
        OR recording_failed_reason IS NOT NULL
        OR recording_failed_at IS NOT NULL
      );
    RETURN NULL;
  END IF;

  UPDATE streams
  SET
    recording_state = 'on',
    recording_failed_reason = NULL,
    recording_failed_at = NULL,
    updated_at = now()
  WHERE id = NEW.stream_id
    AND (
      recording_state <> 'on'
      OR recording_failed_reason IS NOT NULL
      OR recording_failed_at IS NOT NULL
    );
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_recording_assignments_sync_stream_state ON recording_assignments;
CREATE TRIGGER trg_recording_assignments_sync_stream_state
AFTER INSERT OR UPDATE OR DELETE ON recording_assignments
FOR EACH ROW
EXECUTE FUNCTION sync_stream_recording_state_from_assignment();

UPDATE streams
SET
  recording_state = 'off',
  recording_failed_reason = NULL,
  recording_failed_at = NULL,
  updated_at = now()
WHERE
  recording_state <> 'off'
  OR recording_failed_reason IS NOT NULL
  OR recording_failed_at IS NOT NULL;

UPDATE streams s
SET
  recording_state = 'on',
  recording_failed_reason = NULL,
  recording_failed_at = NULL,
  updated_at = now()
FROM recording_assignments ra
WHERE s.id = ra.stream_id
  AND (
    s.recording_state <> 'on'
    OR s.recording_failed_reason IS NOT NULL
    OR s.recording_failed_at IS NOT NULL
  );

COMMIT;
