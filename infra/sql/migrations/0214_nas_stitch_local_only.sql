-- NAS stitching is a local-only writer: it reads raw clips from the NAS and
-- writes new NAS files plus nas_stitch_* DB rows. It makes no R2 PUT. Its only
-- R2 call is the pinned tool fetch, which the API serves as a presigned GET
-- (read-only, exact object, no upload). The 0187 writer fence therefore must not
-- gate the stitch lane: without this migration every pending/leased
-- nas_stitch_tasks row and every nas_stitch_outputs row raises while the fence
-- is active. Ordinary readiness retains each input and releases the lane.
--
-- Retire the old NAS permit path before opening the stitch writer lane. The
-- previous API can still serve after the pre-deploy transaction commits.
-- Validating the constraint fences old plan activation and waits for any
-- in-flight permit transaction holding a plan lock. A previously issued
-- permit must expire before the migration can release the old writer fence.
ALTER TABLE nas_cleanup_plans ADD CONSTRAINT nas_cleanup_no_stitch_retirement
 CHECK (mode <> 'enabled' OR NOT (manifest @> '[{"permit_reason":"stitched_included"}]'::jsonb));

DO $$ BEGIN
 IF EXISTS (
  SELECT 1 FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id
  WHERE i.permit_expires_at > clock_timestamp()
    AND p.manifest @> '[{"permit_reason":"stitched_included"}]'::jsonb
 ) THEN
  RAISE EXCEPTION 'wait for existing NAS stitch delete permits to expire before deploying';
 END IF;
END $$;

-- Only the two stitch triggers are removed. Every other 0187 trigger stays.
--
-- Functions that read these tables (grep for r2_retirement_writers_drained and
-- r2_retirement_writes_blocked):
--   * r2_retirement_writers_drained(b) (0187, replaced by 0196) counts
--     nas_stitch_tasks in ('pending','leased') as a drain blocker. 0198 keeps
--     that function as an explicitly diagnostic-only observation (see the
--     0198 header and docs/r2-owner-fence-policy.md); it is not an admission
--     gate, so a pending stitch task can no longer veto an R2 retirement batch.
--   * r2_retirement_writes_blocked(b, endpoint) (0198) does not read
--     nas_stitch_tasks or nas_stitch_outputs at all; it is unchanged.
--   * stoatamactl r2-retire reconcile-writers / head-guard and
--     r2RetireCheckStoppedWriters call r2_retirement_writers_drained for a
--     read-only JSONL observation only (r2RetireCheckStoppedWriters is used
--     only by tests). No production executor calls it before a delete batch.
-- No function reads nas_stitch_outputs outside the fence triggers.
DROP TRIGGER r2_writer_stitch ON nas_stitch_tasks;
DROP TRIGGER r2_writer_insert ON nas_stitch_outputs;
