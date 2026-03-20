BEGIN;

-- recording_state is operator intent on the stream. It must not be projected
-- from live assignment rows, which are revocable placement state.
DROP TRIGGER IF EXISTS trg_recording_assignments_sync_stream_state ON recording_assignments;
DROP FUNCTION IF EXISTS sync_stream_recording_state_from_assignment();

COMMIT;
