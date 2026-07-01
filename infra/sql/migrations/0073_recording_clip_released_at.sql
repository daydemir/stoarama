BEGIN;

-- released_at: a clip has been DETACHED from the org (billing stops + org can no
-- longer see it) but its R2 object, its recording_clips row, and its
-- recording/storage_destination association are ALL retained. This is DISTINCT
-- from purged_at (legacy: the R2 object was actually deleted and is gone). No
-- backend path hard-deletes recorded clip content anymore; every former "delete"
-- of recorded content sets released_at instead and keeps the bytes, so a future
-- feature can serve our recorded clips to other users. Nullable: existing clips
-- are un-released. purged_at semantics are unchanged.
ALTER TABLE recording_clips ADD COLUMN released_at TIMESTAMPTZ;

-- The org clip surfaces and the managed-storage snapshot filter on
-- (purged_at IS NULL AND released_at IS NULL); a partial index over the still-org-
-- visible working set keeps the NAS pull cursor scan and the nightly snapshot cheap
-- as released rows accumulate.
CREATE INDEX IF NOT EXISTS idx_recording_clips_active
  ON recording_clips (recording_id, id)
  WHERE purged_at IS NULL AND released_at IS NULL;

COMMIT;
