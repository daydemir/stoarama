-- Candidate identities come from the inventory evidence, after the R2 listing
-- has matched size and (for raw) the recorded R2 ETag. Return catalog vetoes.
-- Deniz (Nov 24): "just drop the recordings its fine"; delivery pages hold nothing (0244).
-- cycle.py includes this once per chunk of at most ROWS_PER_STATEMENT candidates, with
-- psql variables kinds (text[]) and ids (bigint[]) in step; every check is per row.
WITH candidates AS MATERIALIZED (
  SELECT * FROM unnest(:'kinds'::text[], :'ids'::bigint[]) AS j(kind, source_id)
)
SELECT j.kind, j.source_id
FROM candidates j
LEFT JOIN recording_clips c ON j.kind='raw' AND c.id=j.source_id AND c.bucket='stoarama'
LEFT JOIN recording_collation_outputs o ON j.kind='collated' AND o.id=j.source_id
WHERE CASE WHEN j.kind='raw' THEN
    c.id IS NULL OR c.purged_at IS NOT NULL
    OR nas_cleanup_clip_held(c.id)
    -- 0249: a batch still snapshotting or building the source refuses its purge.
    OR EXISTS(SELECT FROM recording_joined_snapshot_scopes s
       JOIN recording_joined_batches b ON b.id=s.batch_record_id AND b.state IN ('snapshotting','building')
       WHERE s.recording_id=c.recording_id AND s.recording_job_id=c.recording_job_id AND c.id<=s.high_water_clip_id)
    OR EXISTS(SELECT FROM recording_joined_source_snapshots s
       JOIN recording_joined_batches b ON b.id=s.batch_record_id AND b.state IN ('snapshotting','building')
       WHERE s.clip_id=c.id)
  ELSE
    o.id IS NULL OR o.r2_verified_at IS NULL
    OR EXISTS(SELECT FROM recording_collation_outputs other WHERE other.object_key=o.object_key AND other.id<>o.id)
  END
  OR nas_cleanup_r2_file_reserved(j.kind,j.source_id)
  OR EXISTS(SELECT FROM r2_retirement_reservations r WHERE r.bucket='stoarama' AND r.object_key=coalesce(c.object_key,o.object_key))
  OR EXISTS(SELECT FROM r2_object_retirements r WHERE r.bucket='stoarama' AND r.object_key=coalesce(c.object_key,o.object_key))
  OR EXISTS(SELECT FROM r2_object_retirements r WHERE r.kind=j.kind AND r.source_id=j.source_id);
