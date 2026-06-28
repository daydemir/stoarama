BEGIN;

-- Link a recording to the streams catalog. Nullable: a pasted-URL recording
-- leaves this null. Capture still runs off recordings.stream_url (the stable
-- raw reference the worker re-resolves each fire), so this column carries zero
-- live-schedule risk. ON DELETE SET NULL: dropping a catalog stream un-links its
-- recordings without disturbing their schedule.
ALTER TABLE recordings ADD COLUMN stream_id BIGINT REFERENCES streams(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_recordings_stream_id ON recordings(stream_id);

COMMIT;
