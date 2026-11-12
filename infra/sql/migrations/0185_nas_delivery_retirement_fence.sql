-- Opt-in only: activation is an operator CLI action after every API instance
-- runs the admission gate. No production fence is activated by this migration.
CREATE TABLE nas_delivery_retirement_fences (
 connection_id bigint PRIMARY KEY REFERENCES connections(id) CHECK(connection_id=13),
 policy_bytes bytea NOT NULL,
 policy_sha256 text NOT NULL CHECK(policy_sha256='060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811'),
 effective_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 legacy_expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '7 days',
 CHECK(encode(sha256(policy_bytes),'hex')=policy_sha256),
 CHECK(legacy_expires_at>=effective_at+interval '7 days')
);
CREATE TABLE nas_delivery_download_leases (
 id bigserial PRIMARY KEY, connection_id bigint NOT NULL REFERENCES connections(id),
 expires_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION guard_nas_delivery_activation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM pg_advisory_xact_lock(7343226011185);
 IF NOT EXISTS(SELECT 1 FROM connections WHERE id=NEW.connection_id AND account_id=47 AND kind='nas_pull') THEN RAISE EXCEPTION 'unreviewed NAS fence connection'; END IF;
 NEW.effective_at:=clock_timestamp();NEW.legacy_expires_at:=NEW.effective_at+interval '7 days';RETURN NEW;
 END $$;
CREATE TRIGGER nas_delivery_activation_guard BEFORE INSERT ON nas_delivery_retirement_fences FOR EACH ROW EXECUTE FUNCTION guard_nas_delivery_activation();
CREATE INDEX nas_delivery_lease_drain ON nas_delivery_download_leases(connection_id,expires_at);
CREATE TABLE nas_delivery_retirement_windows (
 connection_id bigint NOT NULL REFERENCES nas_delivery_retirement_fences(connection_id),
 stream_id bigint NOT NULL, timezone text NOT NULL, start_at timestamptz NOT NULL, end_at timestamptz NOT NULL,
 PRIMARY KEY(connection_id,stream_id), CHECK(end_at>start_at)
);
CREATE FUNCTION guard_nas_delivery_window() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM nas_delivery_retirement_fences f CROSS JOIN LATERAL jsonb_array_elements(convert_from(f.policy_bytes,'UTF8')::jsonb) w
 WHERE f.connection_id=NEW.connection_id AND (w->>'stream_id')::bigint=NEW.stream_id AND w->>'tz'=NEW.timezone
 AND (w->>'keep_start_utc')::timestamptz=NEW.start_at AND (w->>'keep_end_utc')::timestamptz=NEW.end_at) THEN
 RAISE EXCEPTION 'window differs from immutable pinned policy'; END IF; RETURN NEW; END $$;
CREATE TRIGGER nas_delivery_window_guard BEFORE INSERT ON nas_delivery_retirement_windows FOR EACH ROW EXECUTE FUNCTION guard_nas_delivery_window();
-- A separate append-only record acknowledges that the NAS has stopped/waited
-- for every active transfer. Expiry alone cannot terminate a running HTTP GET.
CREATE TABLE nas_delivery_retirement_settlements (
 connection_id bigint PRIMARY KEY REFERENCES nas_delivery_retirement_fences(connection_id),
 settled_at timestamptz NOT NULL DEFAULT clock_timestamp(), drain_acknowledgment text NOT NULL CHECK(length(btrim(drain_acknowledgment))>0)
);
CREATE FUNCTION guard_nas_delivery_settlement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM pg_advisory_xact_lock(7343226011185);
 IF EXISTS(SELECT 1 FROM nas_delivery_retirement_fences f WHERE f.connection_id=NEW.connection_id AND f.legacy_expires_at>=clock_timestamp())
 OR EXISTS(SELECT 1 FROM nas_delivery_download_leases l WHERE l.connection_id=NEW.connection_id AND l.expires_at>=clock_timestamp()) THEN
  RAISE EXCEPTION 'delivery retirement capabilities have not drained';
 END IF;
 NEW.settled_at:=clock_timestamp(); RETURN NEW;
END $$;
CREATE TRIGGER nas_delivery_settlement_guard BEFORE INSERT ON nas_delivery_retirement_settlements FOR EACH ROW EXECUTE FUNCTION guard_nas_delivery_settlement();
CREATE FUNCTION guard_nas_delivery_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'delivery fence authority is immutable'; END $$;
CREATE TRIGGER nas_delivery_fence_immutable BEFORE UPDATE OR DELETE ON nas_delivery_retirement_fences FOR EACH ROW EXECUTE FUNCTION guard_nas_delivery_immutable();
CREATE TRIGGER nas_delivery_settlement_immutable BEFORE UPDATE OR DELETE ON nas_delivery_retirement_settlements FOR EACH ROW EXECUTE FUNCTION guard_nas_delivery_immutable();
CREATE TRIGGER nas_delivery_leases_immutable BEFORE UPDATE OR DELETE ON nas_delivery_download_leases FOR EACH ROW EXECUTE FUNCTION guard_nas_delivery_immutable();
CREATE TRIGGER nas_delivery_windows_immutable BEFORE UPDATE OR DELETE ON nas_delivery_retirement_windows FOR EACH ROW EXECUTE FUNCTION guard_nas_delivery_immutable();

-- -1: demonstrably wholly outside; 1: intersects the keep window; 0: unknown.
-- Unknown identity never becomes fence-based deletion authority.
CREATE FUNCTION nas_delivery_span(stream bigint,tz text,a timestamptz,b timestamptz) RETURNS integer LANGUAGE sql STABLE AS $$
 SELECT coalesce((SELECT CASE WHEN a IS NULL OR b IS NULL OR b<=a OR a<'2020-01-01' OR b>='2028-01-01' OR tz IS NULL OR w.timezone<>tz THEN 0
 WHEN a<w.end_at AND b>w.start_at THEN 1 ELSE -1 END
 FROM nas_delivery_retirement_windows w WHERE w.connection_id=13 AND w.stream_id=stream),0)
$$;
CREATE FUNCTION nas_delivery_object_span(kind text,source bigint) RETURNS integer LANGUAGE plpgsql STABLE AS $$ BEGIN
 CASE kind
 WHEN 'raw' THEN RETURN coalesce((SELECT nas_delivery_span(r.stream_id,coalesce(nullif(s.local_timezone,''),r.cron_timezone),c.clip_start_at,c.clip_end_at)
 FROM recording_clips c JOIN recordings r ON r.id=c.recording_id LEFT JOIN streams s ON s.id=r.stream_id WHERE c.id=source AND r.account_id=47),0);
 WHEN 'collated' THEN RETURN coalesce((SELECT CASE WHEN cardinality(o.source_clip_ids)=0 OR count(c.id)<>cardinality(o.source_clip_ids) OR bool_or(nas_delivery_object_span('raw',c.id)=0) THEN 0
 WHEN nas_delivery_span(r.stream_id,coalesce(nullif(s.local_timezone,''),r.cron_timezone),o.start_at,o.end_at)=-1 AND bool_and(nas_delivery_object_span('raw',c.id)=-1) THEN -1
 WHEN nas_delivery_span(r.stream_id,coalesce(nullif(s.local_timezone,''),r.cron_timezone),o.start_at,o.end_at)=1 THEN 1 ELSE 0 END
 FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id LEFT JOIN streams s ON s.id=r.stream_id
 LEFT JOIN recording_clips c ON c.id=ANY(o.source_clip_ids) WHERE o.id=source AND r.account_id=47 GROUP BY o.id,r.id,s.local_timezone),0);
 WHEN 'joined' THEN RETURN coalesce((SELECT CASE WHEN a.artifact_kind='batch_index' THEN 1 ELSE nas_delivery_span(r.stream_id,coalesce(nullif(s.local_timezone,''),r.cron_timezone),coalesce(h.scheduled_start_at,d.scheduled_start_at),coalesce(h.scheduled_end_at,d.scheduled_end_at)) END
 FROM recording_joined_artifacts a LEFT JOIN recording_joined_hours h ON h.id=a.hour_record_id LEFT JOIN recording_joined_stream_days d ON d.id=a.stream_day_id
 LEFT JOIN recordings r ON r.id=coalesce(h.recording_id,d.recording_id) LEFT JOIN streams s ON s.id=r.stream_id WHERE a.id=source AND a.connection_id=13),0);
 ELSE RETURN 0; END CASE; END
$$;
CREATE FUNCTION nas_delivery_allowed(connection bigint,kind text,source bigint) RETURNS boolean LANGUAGE plpgsql STABLE AS $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM nas_delivery_retirement_fences WHERE connection_id=connection) THEN RETURN true; END IF;
 RETURN nas_delivery_object_span(kind,source)=1; END
$$;

CREATE FUNCTION nas_delivery_object_paths(kind text,source bigint) RETURNS SETOF text LANGUAGE plpgsql STABLE AS $$ BEGIN
 IF kind='raw' THEN RETURN QUERY SELECT display_path FROM recording_clips WHERE id=source UNION SELECT relative_path FROM nas_inventory_files WHERE connection_id=13 AND clip_id=source;
 ELSIF kind='collated' THEN RETURN QUERY SELECT nas_relative_path FROM recording_collation_outputs WHERE id=source UNION SELECT 'joined/'||nas_relative_path FROM recording_collation_outputs WHERE id=source;
 END IF; END $$;
CREATE FUNCTION nas_delivery_folder_covered(p text,folders text[]) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
 SELECT coalesce((CASE WHEN position('/' in p)>0 THEN split_part(p,'/',1) ELSE '.' END)=ANY(folders),false)
$$;
-- Shared paths are future NAS writers too. One kept/unknown alias prevents
-- absence authority for every catalog identity using that namespace path.
CREATE FUNCTION nas_delivery_fence_coverage() RETURNS TABLE(kind text,source_id bigint,covered boolean) LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN QUERY
 WITH objects AS MATERIALIZED (
 SELECT 'raw'::text kind,c.id source_id,nas_delivery_object_span('raw',c.id)=-1 outside FROM recording_clips c JOIN recordings r ON r.id=c.recording_id WHERE r.account_id=47
 UNION ALL SELECT 'collated',o.id,nas_delivery_object_span('collated',o.id)=-1 FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id WHERE r.account_id=47
 ), paths AS MATERIALIZED (SELECT o.*,p FROM objects o CROSS JOIN LATERAL nas_delivery_object_paths(o.kind,o.source_id) p),
 blocked AS (SELECT DISTINCT p FROM paths WHERE NOT outside)
 SELECT o.kind,o.source_id,o.outside AND NOT EXISTS(SELECT 1 FROM paths p JOIN blocked b USING(p) WHERE p.kind=o.kind AND p.source_id=o.source_id) covered FROM objects o; END $$;

-- Follow 0149: never build a write-blocking index on a large live table.
-- Precreate the exact index CONCURRENTLY outside the migration transaction.
DO $$
DECLARE
 table_oid REGCLASS := 'recording_clips'::regclass;
 index_oid REGCLASS := to_regclass('nas_delivery_raw_writer_path');
 index_ok BOOLEAN;
BEGIN
 IF index_oid IS NULL THEN
  IF pg_relation_size(table_oid)>134217728 THEN
   RAISE EXCEPTION 'precreate nas_delivery_raw_writer_path with CREATE INDEX CONCURRENTLY ON recording_clips(display_path) WHERE display_path<>'''' before migration';
  END IF;
  CREATE INDEX nas_delivery_raw_writer_path ON recording_clips(display_path) WHERE display_path<>'';
 ELSE
  SELECT i.indisvalid AND i.indisready AND NOT i.indisunique
   AND i.indrelid=table_oid AND i.indnkeyatts=1 AND i.indnatts=1
   AND a.amname='btree' AND i.indoption[0]=0
   AND i.indcollation[0]=c.attcollation
   AND o.opcdefault AND o.opcintype=c.atttypid
   AND pg_get_indexdef(i.indexrelid,1,false)='display_path'
   AND pg_get_expr(i.indpred,i.indrelid)='(display_path <> ''''::text)'
  INTO index_ok
  FROM pg_index i JOIN pg_class x ON x.oid=i.indexrelid
  JOIN pg_am a ON a.oid=x.relam
  JOIN pg_attribute c ON c.attrelid=table_oid AND c.attname='display_path'
  JOIN pg_opclass o ON o.oid=i.indclass[0]
  WHERE i.indexrelid=index_oid;
  IF index_ok IS DISTINCT FROM true THEN
   RAISE EXCEPTION 'nas_delivery_raw_writer_path exists but is not the valid exact delivery writer index';
  END IF;
 END IF;
END $$;
CREATE FUNCTION nas_delivery_fence_object_covered(k text,i bigint) RETURNS boolean LANGUAGE plpgsql STABLE AS $$ BEGIN
 IF nas_delivery_object_span(k,i)<>-1 THEN RETURN false; END IF;
 RETURN NOT EXISTS(SELECT 1 FROM nas_delivery_object_paths(k,i) p WHERE
 EXISTS(SELECT 1 FROM recording_clips c WHERE c.display_path=p AND nas_delivery_object_span('raw',c.id)<>-1)
 OR EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.relative_path=p AND n.connection_id=13 AND nas_delivery_object_span('raw',n.clip_id)<>-1)
 OR EXISTS(SELECT 1 FROM recording_collation_outputs o WHERE (o.nas_relative_path=p OR 'joined/'||o.nas_relative_path=p) AND nas_delivery_object_span('collated',o.id)<>-1));
 END $$;
