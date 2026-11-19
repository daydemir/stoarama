-- Measurements are append-only. The view admits exact source coverage when
-- both the reviewed media evidence and connection-13 custody are present.
-- No retirement, hold, delivery or copy guard consumes it in this migration.
CREATE TABLE delivery_source_coverage_runs (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 output_id bigint NOT NULL REFERENCES delivery_nas_handoff_outputs(output_id) ON DELETE RESTRICT,
 collation_hour_id bigint NOT NULL REFERENCES recording_collation_hours(id) ON DELETE RESTRICT,
 source_clip_ids bigint[] NOT NULL CHECK(cardinality(source_clip_ids)>0),
 manifest_object_key text NOT NULL CHECK (manifest_object_key<>''),
 manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
 hour_id text NOT NULL CHECK(hour_id<>''),
 batch_id text NOT NULL CHECK(batch_id<>''),
 part integer NOT NULL CHECK(part>0),
 parts integer NOT NULL CHECK(parts>=part),
 output_start_at timestamptz NOT NULL,
 output_end_at timestamptz NOT NULL CHECK(output_end_at>output_start_at),
 evidence_sha256 text NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
 media_evidence jsonb NOT NULL CHECK(jsonb_typeof(media_evidence)='object'),
 observed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX delivery_source_coverage_output_latest
 ON delivery_source_coverage_runs(output_id,id DESC);

CREATE TABLE delivery_source_coverage_clips (
 run_id bigint NOT NULL REFERENCES delivery_source_coverage_runs(id) ON DELETE RESTRICT,
 clip_id bigint NOT NULL REFERENCES recording_clips(id) ON DELETE RESTRICT,
 ordinal integer NOT NULL CHECK(ordinal>0),
 bucket text NOT NULL CHECK(bucket<>''),
 object_key text NOT NULL CHECK(object_key<>''),
 etag text NOT NULL CHECK(etag<>''),
 sha256 text NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
 size_bytes bigint NOT NULL CHECK(size_bytes>0),
 display_path text NOT NULL CHECK(display_path<>''),
 start_at timestamptz NOT NULL,
 end_at timestamptz NOT NULL CHECK(end_at>start_at),
 proven boolean NOT NULL,
 PRIMARY KEY(run_id,ordinal),
 UNIQUE(run_id,clip_id)
);

CREATE FUNCTION reject_delivery_source_coverage_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'delivery source coverage evidence is immutable';
END
$$;
CREATE TRIGGER delivery_source_coverage_runs_no_update_delete
 BEFORE UPDATE OR DELETE ON delivery_source_coverage_runs
 FOR EACH ROW EXECUTE FUNCTION reject_delivery_source_coverage_mutation();
CREATE TRIGGER delivery_source_coverage_runs_no_truncate
 BEFORE TRUNCATE ON delivery_source_coverage_runs
 FOR EACH STATEMENT EXECUTE FUNCTION reject_delivery_source_coverage_mutation();
CREATE TRIGGER delivery_source_coverage_clips_no_update_delete
 BEFORE UPDATE OR DELETE ON delivery_source_coverage_clips
 FOR EACH ROW EXECUTE FUNCTION reject_delivery_source_coverage_mutation();
CREATE TRIGGER delivery_source_coverage_clips_no_truncate
 BEFORE TRUNCATE ON delivery_source_coverage_clips
 FOR EACH STATEMENT EXECUTE FUNCTION reject_delivery_source_coverage_mutation();

-- Bind the latest complete observation to current raw and output identities.
-- An ambiguous duplicate collation candidate or newer failed result withholds
-- evidence. Historical delivery holds may be withdrawn after a separate
-- handoff, so the durable custody check uses the exact approval and readback.
CREATE VIEW delivery_source_coverage_certified AS
SELECT p.clip_id,o.id AS output_id,d.connection_id,p.bucket,p.object_key,p.etag,p.sha256,p.size_bytes,
       p.display_path,p.start_at,p.end_at,e.evidence_sha256,e.manifest_sha256,e.observed_at,
       d.nas_relative_path,d.mtime_ns,d.ctime_ns,d.inode,d.device,d.observed_at AS nas_verified_at
FROM delivery_source_coverage_clips p
JOIN delivery_source_coverage_runs e ON e.id=p.run_id
JOIN delivery_nas_handoff_outputs selected ON selected.output_id=e.output_id
JOIN recording_collation_outputs o ON o.id=e.output_id AND o.collation_hour_id=e.collation_hour_id
 AND o.object_key=selected.object_key AND o.nas_relative_path=selected.nas_relative_path
 AND o.sha256=selected.sha256 AND o.size_bytes=selected.size_bytes
 AND o.part=e.part AND o.parts=e.parts AND o.start_at=e.output_start_at AND o.end_at=e.output_end_at
 AND o.source_clip_ids=e.source_clip_ids AND o.source_clip_ids[p.ordinal]=p.clip_id
JOIN recording_collation_hours h ON h.id=e.collation_hour_id AND h.recording_id=selected.recording_id
 AND h.generation=2 AND h.status='collated' AND h.manifest_object_key=e.manifest_object_key
 AND h.manifest_sha256=e.manifest_sha256 AND h.hour_id=e.hour_id AND h.batch_id=e.batch_id
JOIN recording_clips c ON c.id=p.clip_id AND c.recording_id=h.recording_id AND c.bucket=p.bucket
 AND c.object_key=p.object_key AND c.etag=p.etag AND c.sha256=p.sha256 AND c.size_bytes=p.size_bytes
 AND c.display_path=p.display_path AND c.clip_start_at=p.start_at AND c.clip_end_at=p.end_at
JOIN delivery_nas_handoff_certified d ON d.output_id=o.id AND d.connection_id=13
 AND d.nas_relative_path=o.nas_relative_path AND d.sha256=o.sha256 AND d.size_bytes=o.size_bytes
WHERE p.proven
 AND NOT EXISTS (SELECT 1 FROM delivery_source_coverage_runs newer WHERE newer.output_id=e.output_id AND newer.id>e.id)
 AND NOT EXISTS (
  SELECT 1 FROM recording_collation_outputs alternative
  JOIN recording_collation_hours alt_hour ON alt_hour.id=alternative.collation_hour_id AND alt_hour.status='collated'
  WHERE alternative.id<>o.id AND alternative.source_clip_ids @> ARRAY[p.clip_id]
 )
 AND NOT EXISTS (
  SELECT 1 FROM r2_object_retirements retired
  WHERE ((retired.kind='collated' AND retired.source_id=o.id)
      OR (retired.bucket=selected.bucket AND retired.object_key=selected.object_key))
    AND ROW(retired.kind,retired.source_id,retired.bucket,retired.object_key,retired.sha256,retired.size_bytes)
     IS DISTINCT FROM ROW('collated'::text,o.id,selected.bucket,selected.object_key,selected.sha256,selected.size_bytes)
 );
