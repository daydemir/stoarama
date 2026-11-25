-- READ-ONLY DRY RUN of receipt-apply.sql for one receipt-backfill.tsv.
-- Run from the folder holding receipt-backfill.tsv, in a read-only session, e.g. on Jelly:
--   (set -a; . ~/Build/stoarama/local/recording-supervisor.env; set +a; ...)  # PG* from DATABASE_URL, never printed
--   PGOPTIONS='-c default_transaction_read_only=on' psql -XAt -v ON_ERROR_STOP=1 -f ../dry-run.sql
-- Add -v inserts=1 to print every row the apply would INSERT into r2_object_retirements.
-- Plain SELECTs only: no temp objects, no row locks. The TSV is passed as one literal.
-- Every row is insert | already_applied | blocked; a blocked row lists each check in
-- receipt-apply.sql (source_matches, drift, holds) and each trigger the apply fires
-- (r2_object_retirements INSERT, recording_clips UPDATE OF purged_at) that would raise.
-- One blocked row aborts its whole 500-row apply batch, so the TSV to apply must have none.
\set ON_ERROR_STOP on
\set tsv `cat receipt-backfill.tsv`
BEGIN READ ONLY;
SET LOCAL statement_timeout = '900s';
WITH b AS MATERIALIZED (
  SELECT split_part(l,E'\t',1)::bigint AS seq, split_part(l,E'\t',2) AS kind, split_part(l,E'\t',3)::bigint AS source_id,
         split_part(l,E'\t',4)::bigint AS stream_id, split_part(l,E'\t',5) AS object_key, split_part(l,E'\t',6)::bigint AS size_bytes,
         split_part(l,E'\t',7) AS sha256, split_part(l,E'\t',8) AS etag, split_part(l,E'\t',9)::timestamptz AS deleted_at,
         split_part(l,E'\t',10) AS reason, split_part(l,E'\t',11) AS choice, split_part(l,E'\t',12) AS manifest_sha256,
         split_part(l,E'\t',13) AS policy_sha256
  FROM string_to_table(:'tsv', E'\n') l WHERE l<>''
), s AS (
  -- receipt-apply.sql's ledger_backfill_status, verbatim, plus the clip's job for the joined trigger.
  SELECT b.*,
    CASE WHEN b.kind='raw' THEN
      c.id IS NOT NULL AND c.size_bytes=b.size_bytes AND lower(c.sha256)=b.sha256
      AND btrim(c.etag,'"')=b.etag AND c.created_at<=b.deleted_at AND r.stream_id=b.stream_id
      AND nullif(btrim(c.display_path),'') IS NOT NULL
    ELSE
      o.id IS NOT NULL AND o.size_bytes=b.size_bytes AND lower(o.sha256)=b.sha256
      AND o.created_at<=b.deleted_at AND o.r2_verified_at<=b.deleted_at AND ro.stream_id=b.stream_id
      AND NOT EXISTS (SELECT FROM recording_collation_outputs other WHERE other.object_key=b.object_key AND other.id<>b.source_id)
    END AS source_matches,
    c.purged_at, c.recording_id AS clip_recording, c.recording_job_id AS clip_job,
    x.id AS receipt_id,
    x.id IS NOT NULL
      AND ROW(x.kind,x.source_id,x.stream_id,x.size_bytes,x.sha256) = ROW(b.kind,b.source_id,b.stream_id,b.size_bytes,b.sha256)
      AND ROW(x.reason,x.choice,x.manifest_sha256,x.policy_sha256,x.already_absent,x.retired_at) =
          ROW(b.reason,b.choice,b.manifest_sha256,b.policy_sha256,false,b.deleted_at) AS receipt_matches
  FROM b
  LEFT JOIN recording_clips c ON b.kind='raw' AND c.id=b.source_id AND c.bucket='stoarama' AND c.object_key=b.object_key
  LEFT JOIN recordings r ON r.id=c.recording_id
  LEFT JOIN recording_collation_outputs o ON b.kind='collated' AND o.id=b.source_id AND o.object_key=b.object_key
  LEFT JOIN recording_collation_hours h ON h.id=o.collation_hour_id
  LEFT JOIN recordings ro ON ro.id=h.recording_id
  LEFT JOIN r2_object_retirements x ON x.bucket='stoarama' AND x.object_key=b.object_key
), v AS (
  SELECT s.*, (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AS writes, array_remove(ARRAY[
    -- apply_ledger_backfill: source, timestamp or receipt drift
    CASE WHEN s.source_matches IS NOT TRUE THEN 'source_drift' END,
    CASE WHEN s.kind='raw' AND s.purged_at IS NOT NULL AND s.purged_at<>s.deleted_at THEN 'purged_at_differs' END,
    CASE WHEN s.kind='raw' AND s.purged_at IS NOT NULL AND s.receipt_id IS NULL THEN 'purged_without_receipt' END,
    CASE WHEN s.receipt_id IS NOT NULL AND s.receipt_matches IS NOT TRUE THEN 'key_receipted_differently' END,
    -- UNIQUE (kind,source_id)
    CASE WHEN s.receipt_id IS NULL AND EXISTS(SELECT FROM r2_object_retirements y WHERE y.kind=s.kind AND y.source_id=s.source_id) THEN 'source_receipted_under_other_key' END,
    -- apply_ledger_backfill: reservation or hold
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND r2_owner_keep_held('stoarama',s.object_key) THEN 'owner_keep' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND EXISTS(SELECT FROM r2_retirement_reservations z WHERE z.bucket='stoarama' AND z.object_key=s.object_key) THEN 'retirement_reservation' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND s.kind='raw' AND EXISTS(SELECT FROM nas_restore_requests n WHERE n.clip_id=s.source_id
          AND n.target='original' AND (n.state IN ('pending','leased') OR n.clip_restored_at>s.deleted_at)) THEN 'nas_restore' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND nas_cleanup_r2_file_reserved(s.kind,s.source_id) THEN 'nas_cleanup_file_reserved' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND EXISTS(SELECT FROM nas_cleanup_incident_roster z WHERE z.bucket='stoarama' AND z.object_key=s.object_key) THEN 'incident_roster' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND EXISTS(SELECT FROM nas_cleanup_incident_files z WHERE z.kind=s.kind AND z.source_id=s.source_id) THEN 'incident_file' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND s.kind='collated' AND EXISTS(SELECT FROM recording_collation_outputs o
          JOIN nas_cleanup_incident_sources z ON z.clip_id=ANY(o.source_clip_ids) WHERE o.id=s.source_id) THEN 'incident_source' END,
    -- r2_object_retirements BEFORE INSERT: r2_owner_keep_hold (same as owner_keep) and r2_stitch_input_hold_guard
    CASE WHEN s.receipt_id IS NULL AND EXISTS(SELECT FROM nas_stitch_r2_input_holds z WHERE z.bucket='stoarama' AND z.object_key=s.object_key AND z.released_at IS NULL) THEN 'trigger_stitch_input_hold' END,
    -- recording_clips BEFORE UPDATE OF purged_at: nas_cleanup_source_hold, recording_joined_clip_retention_update
    -- (r2_owner_keep_purge is owner_keep). The owner_accepted_outside_window exemption needs a reservation, held above.
    CASE WHEN s.kind='raw' AND s.purged_at IS NULL AND nas_cleanup_clip_held(s.source_id) THEN 'trigger_nas_cleanup_source_hold' END,
    CASE WHEN s.kind='raw' AND s.purged_at IS NULL AND (EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes scope
          JOIN recording_joined_batches batch ON batch.id=scope.batch_record_id AND batch.state IN ('snapshotting','building')
          WHERE scope.recording_id=s.clip_recording AND scope.recording_job_id=s.clip_job AND s.source_id<=scope.high_water_clip_id)
        OR EXISTS(SELECT 1 FROM recording_joined_source_snapshots ss JOIN recording_joined_batches batch ON batch.id=ss.batch_record_id
          AND batch.state IN ('snapshotting','building') WHERE ss.clip_id=s.source_id)) THEN 'trigger_joined_source_building' END
  ], NULL) AS reasons
  FROM s
), d AS (
  SELECT v.*, CASE WHEN cardinality(v.reasons)>0 THEN 'blocked'
                   WHEN v.receipt_matches AND (v.kind='collated' OR v.purged_at=v.deleted_at) THEN 'already_applied'
                   ELSE 'insert' END AS action
  FROM v
)
SELECT action, kind, array_to_string(reasons,',') AS reasons, count(*) AS rows, sum(size_bytes) AS bytes,
       count(*) FILTER (WHERE kind='raw' AND purged_at IS NULL) AS raw_purged_at_updates
FROM d GROUP BY 1,2,3 ORDER BY 1,2,3;
\if :{?inserts}
WITH b AS MATERIALIZED (
  SELECT split_part(l,E'\t',1)::bigint AS seq, split_part(l,E'\t',2) AS kind, split_part(l,E'\t',3)::bigint AS source_id,
         split_part(l,E'\t',4)::bigint AS stream_id, split_part(l,E'\t',5) AS object_key, split_part(l,E'\t',6)::bigint AS size_bytes,
         split_part(l,E'\t',7) AS sha256, split_part(l,E'\t',8) AS etag, split_part(l,E'\t',9)::timestamptz AS deleted_at,
         split_part(l,E'\t',10) AS reason, split_part(l,E'\t',11) AS choice, split_part(l,E'\t',12) AS manifest_sha256,
         split_part(l,E'\t',13) AS policy_sha256
  FROM string_to_table(:'tsv', E'\n') l WHERE l<>''
), s AS (
  -- receipt-apply.sql's ledger_backfill_status, verbatim, plus the clip's job for the joined trigger.
  SELECT b.*,
    CASE WHEN b.kind='raw' THEN
      c.id IS NOT NULL AND c.size_bytes=b.size_bytes AND lower(c.sha256)=b.sha256
      AND btrim(c.etag,'"')=b.etag AND c.created_at<=b.deleted_at AND r.stream_id=b.stream_id
      AND nullif(btrim(c.display_path),'') IS NOT NULL
    ELSE
      o.id IS NOT NULL AND o.size_bytes=b.size_bytes AND lower(o.sha256)=b.sha256
      AND o.created_at<=b.deleted_at AND o.r2_verified_at<=b.deleted_at AND ro.stream_id=b.stream_id
      AND NOT EXISTS (SELECT FROM recording_collation_outputs other WHERE other.object_key=b.object_key AND other.id<>b.source_id)
    END AS source_matches,
    c.purged_at, c.recording_id AS clip_recording, c.recording_job_id AS clip_job,
    x.id AS receipt_id,
    x.id IS NOT NULL
      AND ROW(x.kind,x.source_id,x.stream_id,x.size_bytes,x.sha256) = ROW(b.kind,b.source_id,b.stream_id,b.size_bytes,b.sha256)
      AND ROW(x.reason,x.choice,x.manifest_sha256,x.policy_sha256,x.already_absent,x.retired_at) =
          ROW(b.reason,b.choice,b.manifest_sha256,b.policy_sha256,false,b.deleted_at) AS receipt_matches
  FROM b
  LEFT JOIN recording_clips c ON b.kind='raw' AND c.id=b.source_id AND c.bucket='stoarama' AND c.object_key=b.object_key
  LEFT JOIN recordings r ON r.id=c.recording_id
  LEFT JOIN recording_collation_outputs o ON b.kind='collated' AND o.id=b.source_id AND o.object_key=b.object_key
  LEFT JOIN recording_collation_hours h ON h.id=o.collation_hour_id
  LEFT JOIN recordings ro ON ro.id=h.recording_id
  LEFT JOIN r2_object_retirements x ON x.bucket='stoarama' AND x.object_key=b.object_key
), v AS (
  SELECT s.*, (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AS writes, array_remove(ARRAY[
    -- apply_ledger_backfill: source, timestamp or receipt drift
    CASE WHEN s.source_matches IS NOT TRUE THEN 'source_drift' END,
    CASE WHEN s.kind='raw' AND s.purged_at IS NOT NULL AND s.purged_at<>s.deleted_at THEN 'purged_at_differs' END,
    CASE WHEN s.kind='raw' AND s.purged_at IS NOT NULL AND s.receipt_id IS NULL THEN 'purged_without_receipt' END,
    CASE WHEN s.receipt_id IS NOT NULL AND s.receipt_matches IS NOT TRUE THEN 'key_receipted_differently' END,
    -- UNIQUE (kind,source_id)
    CASE WHEN s.receipt_id IS NULL AND EXISTS(SELECT FROM r2_object_retirements y WHERE y.kind=s.kind AND y.source_id=s.source_id) THEN 'source_receipted_under_other_key' END,
    -- apply_ledger_backfill: reservation or hold
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND r2_owner_keep_held('stoarama',s.object_key) THEN 'owner_keep' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND EXISTS(SELECT FROM r2_retirement_reservations z WHERE z.bucket='stoarama' AND z.object_key=s.object_key) THEN 'retirement_reservation' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND s.kind='raw' AND EXISTS(SELECT FROM nas_restore_requests n WHERE n.clip_id=s.source_id
          AND n.target='original' AND (n.state IN ('pending','leased') OR n.clip_restored_at>s.deleted_at)) THEN 'nas_restore' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND nas_cleanup_r2_file_reserved(s.kind,s.source_id) THEN 'nas_cleanup_file_reserved' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND EXISTS(SELECT FROM nas_cleanup_incident_roster z WHERE z.bucket='stoarama' AND z.object_key=s.object_key) THEN 'incident_roster' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND EXISTS(SELECT FROM nas_cleanup_incident_files z WHERE z.kind=s.kind AND z.source_id=s.source_id) THEN 'incident_file' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND s.kind='collated' AND EXISTS(SELECT FROM recording_collation_outputs o
          JOIN nas_cleanup_incident_sources z ON z.clip_id=ANY(o.source_clip_ids) WHERE o.id=s.source_id) THEN 'incident_source' END,
    -- r2_object_retirements BEFORE INSERT: r2_owner_keep_hold (same as owner_keep) and r2_stitch_input_hold_guard
    CASE WHEN s.receipt_id IS NULL AND EXISTS(SELECT FROM nas_stitch_r2_input_holds z WHERE z.bucket='stoarama' AND z.object_key=s.object_key AND z.released_at IS NULL) THEN 'trigger_stitch_input_hold' END,
    -- recording_clips BEFORE UPDATE OF purged_at: nas_cleanup_source_hold, recording_joined_clip_retention_update
    -- (r2_owner_keep_purge is owner_keep). The owner_accepted_outside_window exemption needs a reservation, held above.
    CASE WHEN s.kind='raw' AND s.purged_at IS NULL AND nas_cleanup_clip_held(s.source_id) THEN 'trigger_nas_cleanup_source_hold' END,
    CASE WHEN s.kind='raw' AND s.purged_at IS NULL AND (EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes scope
          JOIN recording_joined_batches batch ON batch.id=scope.batch_record_id AND batch.state IN ('snapshotting','building')
          WHERE scope.recording_id=s.clip_recording AND scope.recording_job_id=s.clip_job AND s.source_id<=scope.high_water_clip_id)
        OR EXISTS(SELECT 1 FROM recording_joined_source_snapshots ss JOIN recording_joined_batches batch ON batch.id=ss.batch_record_id
          AND batch.state IN ('snapshotting','building') WHERE ss.clip_id=s.source_id)) THEN 'trigger_joined_source_building' END
  ], NULL) AS reasons
  FROM s
), d AS (
  SELECT v.*, CASE WHEN cardinality(v.reasons)>0 THEN 'blocked'
                   WHEN v.receipt_matches AND (v.kind='collated' OR v.purged_at=v.deleted_at) THEN 'already_applied'
                   ELSE 'insert' END AS action
  FROM v
)
SELECT seq, kind, source_id, stream_id, 'stoarama' AS bucket, object_key, size_bytes, sha256, reason, choice,
       manifest_sha256, policy_sha256, false AS already_absent, deleted_at AS retired_at
FROM d WHERE action='insert' ORDER BY seq;
\endif
WITH b AS MATERIALIZED (
  SELECT split_part(l,E'\t',1)::bigint AS seq, split_part(l,E'\t',2) AS kind, split_part(l,E'\t',3)::bigint AS source_id,
         split_part(l,E'\t',4)::bigint AS stream_id, split_part(l,E'\t',5) AS object_key, split_part(l,E'\t',6)::bigint AS size_bytes,
         split_part(l,E'\t',7) AS sha256, split_part(l,E'\t',8) AS etag, split_part(l,E'\t',9)::timestamptz AS deleted_at,
         split_part(l,E'\t',10) AS reason, split_part(l,E'\t',11) AS choice, split_part(l,E'\t',12) AS manifest_sha256,
         split_part(l,E'\t',13) AS policy_sha256
  FROM string_to_table(:'tsv', E'\n') l WHERE l<>''
), s AS (
  -- receipt-apply.sql's ledger_backfill_status, verbatim, plus the clip's job for the joined trigger.
  SELECT b.*,
    CASE WHEN b.kind='raw' THEN
      c.id IS NOT NULL AND c.size_bytes=b.size_bytes AND lower(c.sha256)=b.sha256
      AND btrim(c.etag,'"')=b.etag AND c.created_at<=b.deleted_at AND r.stream_id=b.stream_id
      AND nullif(btrim(c.display_path),'') IS NOT NULL
    ELSE
      o.id IS NOT NULL AND o.size_bytes=b.size_bytes AND lower(o.sha256)=b.sha256
      AND o.created_at<=b.deleted_at AND o.r2_verified_at<=b.deleted_at AND ro.stream_id=b.stream_id
      AND NOT EXISTS (SELECT FROM recording_collation_outputs other WHERE other.object_key=b.object_key AND other.id<>b.source_id)
    END AS source_matches,
    c.purged_at, c.recording_id AS clip_recording, c.recording_job_id AS clip_job,
    x.id AS receipt_id,
    x.id IS NOT NULL
      AND ROW(x.kind,x.source_id,x.stream_id,x.size_bytes,x.sha256) = ROW(b.kind,b.source_id,b.stream_id,b.size_bytes,b.sha256)
      AND ROW(x.reason,x.choice,x.manifest_sha256,x.policy_sha256,x.already_absent,x.retired_at) =
          ROW(b.reason,b.choice,b.manifest_sha256,b.policy_sha256,false,b.deleted_at) AS receipt_matches
  FROM b
  LEFT JOIN recording_clips c ON b.kind='raw' AND c.id=b.source_id AND c.bucket='stoarama' AND c.object_key=b.object_key
  LEFT JOIN recordings r ON r.id=c.recording_id
  LEFT JOIN recording_collation_outputs o ON b.kind='collated' AND o.id=b.source_id AND o.object_key=b.object_key
  LEFT JOIN recording_collation_hours h ON h.id=o.collation_hour_id
  LEFT JOIN recordings ro ON ro.id=h.recording_id
  LEFT JOIN r2_object_retirements x ON x.bucket='stoarama' AND x.object_key=b.object_key
), v AS (
  SELECT s.*, (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AS writes, array_remove(ARRAY[
    -- apply_ledger_backfill: source, timestamp or receipt drift
    CASE WHEN s.source_matches IS NOT TRUE THEN 'source_drift' END,
    CASE WHEN s.kind='raw' AND s.purged_at IS NOT NULL AND s.purged_at<>s.deleted_at THEN 'purged_at_differs' END,
    CASE WHEN s.kind='raw' AND s.purged_at IS NOT NULL AND s.receipt_id IS NULL THEN 'purged_without_receipt' END,
    CASE WHEN s.receipt_id IS NOT NULL AND s.receipt_matches IS NOT TRUE THEN 'key_receipted_differently' END,
    -- UNIQUE (kind,source_id)
    CASE WHEN s.receipt_id IS NULL AND EXISTS(SELECT FROM r2_object_retirements y WHERE y.kind=s.kind AND y.source_id=s.source_id) THEN 'source_receipted_under_other_key' END,
    -- apply_ledger_backfill: reservation or hold
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND r2_owner_keep_held('stoarama',s.object_key) THEN 'owner_keep' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND EXISTS(SELECT FROM r2_retirement_reservations z WHERE z.bucket='stoarama' AND z.object_key=s.object_key) THEN 'retirement_reservation' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND s.kind='raw' AND EXISTS(SELECT FROM nas_restore_requests n WHERE n.clip_id=s.source_id
          AND n.target='original' AND (n.state IN ('pending','leased') OR n.clip_restored_at>s.deleted_at)) THEN 'nas_restore' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND nas_cleanup_r2_file_reserved(s.kind,s.source_id) THEN 'nas_cleanup_file_reserved' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND EXISTS(SELECT FROM nas_cleanup_incident_roster z WHERE z.bucket='stoarama' AND z.object_key=s.object_key) THEN 'incident_roster' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND EXISTS(SELECT FROM nas_cleanup_incident_files z WHERE z.kind=s.kind AND z.source_id=s.source_id) THEN 'incident_file' END,
    CASE WHEN (s.receipt_id IS NULL OR (s.kind='raw' AND s.purged_at IS NULL)) AND s.kind='collated' AND EXISTS(SELECT FROM recording_collation_outputs o
          JOIN nas_cleanup_incident_sources z ON z.clip_id=ANY(o.source_clip_ids) WHERE o.id=s.source_id) THEN 'incident_source' END,
    -- r2_object_retirements BEFORE INSERT: r2_owner_keep_hold (same as owner_keep) and r2_stitch_input_hold_guard
    CASE WHEN s.receipt_id IS NULL AND EXISTS(SELECT FROM nas_stitch_r2_input_holds z WHERE z.bucket='stoarama' AND z.object_key=s.object_key AND z.released_at IS NULL) THEN 'trigger_stitch_input_hold' END,
    -- recording_clips BEFORE UPDATE OF purged_at: nas_cleanup_source_hold, recording_joined_clip_retention_update
    -- (r2_owner_keep_purge is owner_keep). The owner_accepted_outside_window exemption needs a reservation, held above.
    CASE WHEN s.kind='raw' AND s.purged_at IS NULL AND nas_cleanup_clip_held(s.source_id) THEN 'trigger_nas_cleanup_source_hold' END,
    CASE WHEN s.kind='raw' AND s.purged_at IS NULL AND (EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes scope
          JOIN recording_joined_batches batch ON batch.id=scope.batch_record_id AND batch.state IN ('snapshotting','building')
          WHERE scope.recording_id=s.clip_recording AND scope.recording_job_id=s.clip_job AND s.source_id<=scope.high_water_clip_id)
        OR EXISTS(SELECT 1 FROM recording_joined_source_snapshots ss JOIN recording_joined_batches batch ON batch.id=ss.batch_record_id
          AND batch.state IN ('snapshotting','building') WHERE ss.clip_id=s.source_id)) THEN 'trigger_joined_source_building' END
  ], NULL) AS reasons
  FROM s
), d AS (
  SELECT v.*, CASE WHEN cardinality(v.reasons)>0 THEN 'blocked'
                   WHEN v.receipt_matches AND (v.kind='collated' OR v.purged_at=v.deleted_at) THEN 'already_applied'
                   ELSE 'insert' END AS action
  FROM v
)
SELECT seq, action, array_to_string(reasons,','), kind, source_id, size_bytes FROM d WHERE action<>'insert' ORDER BY seq;
ROLLBACK;
