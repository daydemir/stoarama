-- Adapted from purged-backfill-20261123b/backfill.sql.
\set ON_ERROR_STOP on
\set VERBOSITY verbose
SET TIME ZONE 'UTC';
SET search_path = pg_catalog, public;
SET lock_timeout = '2s';
SET statement_timeout = '120s';
SELECT (session_user='stoarama_20260821' AND current_user='stoarama'
        AND current_database()='stoarama' AND NOT pg_is_in_recovery()
        AND current_setting('session_replication_role')='origin'
        AND current_setting('stoarama.joined_source_purge',true) IS DISTINCT FROM 'verified') AS expected_db \gset
\if :expected_db
\else
DO $$ BEGIN RAISE EXCEPTION 'unexpected production database or role'; END $$;
\endif

CREATE TEMP TABLE ledger_backfill (
    seq bigint PRIMARY KEY CHECK (seq > 0),
    kind text NOT NULL CHECK (kind IN ('raw','collated')),
    source_id bigint NOT NULL CHECK (source_id > 0),
    stream_id bigint NOT NULL CHECK (stream_id > 0),
    object_key text NOT NULL UNIQUE,
    size_bytes bigint NOT NULL CHECK (size_bytes > 0),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    etag text NOT NULL CHECK (etag <> ''),
    deleted_at timestamptz NOT NULL,
    reason text NOT NULL CHECK (reason ~ '^r2prune_ledger_[a-z0-9_]+$'),
    choice text NOT NULL CHECK (choice IN ('none','st','keep')),
    manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    policy_sha256 text NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    existing_receipt_id bigint,
    UNIQUE (kind,source_id)
);
SET statement_timeout = '900s';
\copy pg_temp.ledger_backfill FROM 'receipt-backfill.tsv' WITH (FORMAT text)
SET statement_timeout = '120s';
ANALYZE pg_temp.ledger_backfill;

-- A batch is either untouched or completely applied at its ledger timestamp.
-- The outer transaction supplies the local write fences and the batch boundary.
CREATE TEMP VIEW ledger_backfill_status AS
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
  c.purged_at,
  x.id AS receipt_id,
  x.id IS NOT NULL
    AND ROW(x.kind,x.source_id,x.stream_id,x.size_bytes,x.sha256) =
        ROW(b.kind,b.source_id,b.stream_id,b.size_bytes,b.sha256)
    AND CASE WHEN b.existing_receipt_id IS NOT NULL THEN x.id=b.existing_receipt_id ELSE
      ROW(x.reason,x.choice,x.manifest_sha256,x.policy_sha256,x.already_absent,x.retired_at) =
      ROW(b.reason,b.choice,b.manifest_sha256,b.policy_sha256,false,b.deleted_at)
    END AS receipt_matches
FROM pg_temp.ledger_backfill b
LEFT JOIN recording_clips c ON b.kind='raw' AND c.id=b.source_id AND c.bucket='stoarama' AND c.object_key=b.object_key
LEFT JOIN recordings r ON r.id=c.recording_id
LEFT JOIN recording_collation_outputs o ON b.kind='collated' AND o.id=b.source_id AND o.object_key=b.object_key
LEFT JOIN recording_collation_hours h ON h.id=o.collation_hour_id
LEFT JOIN recordings ro ON ro.id=h.recording_id
LEFT JOIN r2_object_retirements x ON x.bucket='stoarama' AND x.object_key=b.object_key;

CREATE FUNCTION pg_temp.apply_ledger_backfill(first_seq bigint,last_seq bigint)
RETURNS TABLE(updated_raw bigint, inserted_receipts bigint, already_applied bigint) LANGUAGE plpgsql AS $$
DECLARE
  bad bigint;
  expected_updates bigint;
  expected_inserts bigint;
BEGIN
  -- Same order as inverseProtectionLocks, then the owner-keep fence.
  IF NOT pg_try_advisory_xact_lock(7343226011103) THEN
    RAISE EXCEPTION 'R2 retirement admission busy' USING ERRCODE='55P03';
  END IF;
  IF NOT pg_try_advisory_xact_lock_shared(137,1) THEN
    RAISE EXCEPTION 'joined retention fence busy' USING ERRCODE='55P03';
  END IF;
  IF NOT pg_try_advisory_xact_lock(7343226011101) THEN
    RAISE EXCEPTION 'NAS cleanup fence busy' USING ERRCODE='55P03';
  END IF;
  IF NOT pg_try_advisory_xact_lock_shared(7343226011220) THEN
    RAISE EXCEPTION 'R2 owner keep fence busy' USING ERRCODE='55P03';
  END IF;

  PERFORM c.id FROM pg_temp.ledger_backfill b JOIN recording_clips c ON b.kind='raw' AND c.id=b.source_id
  WHERE b.seq BETWEEN first_seq AND last_seq ORDER BY c.id FOR UPDATE OF c NOWAIT;
  PERFORM o.id FROM pg_temp.ledger_backfill b JOIN recording_collation_outputs o ON b.kind='collated' AND o.id=b.source_id
  WHERE b.seq BETWEEN first_seq AND last_seq ORDER BY o.id FOR SHARE OF o NOWAIT;

  SELECT b.seq INTO bad
  FROM pg_temp.ledger_backfill_status b
  WHERE b.seq BETWEEN first_seq AND last_seq AND (
    b.source_matches IS NOT TRUE
    OR (b.kind='raw' AND b.purged_at IS NOT NULL AND b.purged_at<>b.deleted_at)
    OR (b.existing_receipt_id IS NOT NULL AND b.receipt_matches IS NOT TRUE)
    OR (b.receipt_id IS NOT NULL AND b.receipt_matches IS NOT TRUE)
    OR (b.kind='raw' AND b.existing_receipt_id IS NULL AND (b.purged_at IS NULL)<>(b.receipt_id IS NULL))
  ) LIMIT 1;
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'source, timestamp, or receipt drift at TSV row %',bad; END IF;

  -- Keep reservations with their original executor. Never publish an inverse
  -- absence event here. Existing catalog/receipt triggers remain in force.
  SELECT b.seq INTO bad FROM pg_temp.ledger_backfill_status b
  WHERE b.seq BETWEEN first_seq AND last_seq
    AND (b.receipt_id IS NULL OR (b.kind='raw' AND b.purged_at IS NULL)) AND (
      r2_owner_keep_held('stoarama',b.object_key)
      OR EXISTS(SELECT FROM r2_retirement_reservations x WHERE x.bucket='stoarama' AND x.object_key=b.object_key)
      OR (b.kind='raw' AND EXISTS(SELECT FROM nas_restore_requests n WHERE n.clip_id=b.source_id
            AND n.target='original' AND (n.state IN ('pending','leased') OR n.clip_restored_at>b.deleted_at)))
      OR nas_cleanup_r2_file_reserved(b.kind,b.source_id)
      OR EXISTS(SELECT FROM nas_cleanup_incident_roster x WHERE x.bucket='stoarama' AND x.object_key=b.object_key)
      OR EXISTS(SELECT FROM nas_cleanup_incident_files x WHERE x.kind=b.kind AND x.source_id=b.source_id)
      OR (b.kind='collated' AND EXISTS(SELECT FROM recording_collation_outputs o
           JOIN nas_cleanup_incident_sources x ON x.clip_id=ANY(o.source_clip_ids) WHERE o.id=b.source_id))
    ) LIMIT 1;
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'reservation or hold at TSV row %',bad; END IF;

  SELECT count(*) FILTER (WHERE b.kind='raw' AND b.purged_at IS NULL),
         count(*) FILTER (WHERE b.receipt_id IS NULL),
         count(*) FILTER (WHERE b.receipt_matches AND (b.kind='collated' OR b.purged_at=b.deleted_at))
  INTO expected_updates,expected_inserts,already_applied
  FROM pg_temp.ledger_backfill_status b WHERE b.seq BETWEEN first_seq AND last_seq;

  INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,
                                   choice,manifest_sha256,policy_sha256,already_absent,retired_at)
  SELECT b.kind,b.source_id,b.stream_id,'stoarama',b.object_key,b.size_bytes,b.sha256,b.reason,
         b.choice,b.manifest_sha256,b.policy_sha256,false,b.deleted_at
  FROM pg_temp.ledger_backfill_status b
  WHERE b.seq BETWEEN first_seq AND last_seq AND b.receipt_id IS NULL
  ORDER BY b.seq;
  GET DIAGNOSTICS inserted_receipts=ROW_COUNT;

  UPDATE recording_clips c SET purged_at=b.deleted_at
  FROM pg_temp.ledger_backfill b
  WHERE b.seq BETWEEN first_seq AND last_seq AND b.kind='raw' AND c.purged_at IS NULL
    AND c.id=b.source_id AND c.bucket='stoarama' AND c.object_key=b.object_key;
  GET DIAGNOSTICS updated_raw=ROW_COUNT;

  IF updated_raw<>expected_updates OR inserted_receipts<>expected_inserts OR EXISTS(
    SELECT FROM pg_temp.ledger_backfill_status b WHERE b.seq BETWEEN first_seq AND last_seq AND (
     b.source_matches IS NOT TRUE OR b.receipt_matches IS NOT TRUE
     OR (b.kind='raw' AND b.purged_at IS DISTINCT FROM b.deleted_at)
    )
  ) THEN RAISE EXCEPTION 'backfill count or end state mismatch in rows % through %',first_seq,last_seq; END IF;
  RETURN NEXT;
END $$;

-- Emit the cursor only after COMMIT so the next process can resume safely.
SELECT format('BEGIN ISOLATION LEVEL READ COMMITTED, READ WRITE; SELECT * FROM pg_temp.apply_ledger_backfill(%s,%s); COMMIT; SELECT %L;',
              first_seq, last_seq, 'committed:'||last_seq)
FROM generate_series(:start_seq, :total, 500) g(first_seq)
CROSS JOIN LATERAL (SELECT least(first_seq+499,:total) AS last_seq) b
ORDER BY first_seq
\gexec

SELECT (count(*)=:total AND count(*) FILTER (
    WHERE source_matches IS NOT TRUE OR receipt_matches IS NOT TRUE
       OR (kind='raw' AND purged_at IS DISTINCT FROM deleted_at)
)=0) AS verified FROM pg_temp.ledger_backfill_status \gset
\if :verified
\else
DO $$ BEGIN RAISE EXCEPTION 'receipt readback differs from ledger'; END $$;
\endif
