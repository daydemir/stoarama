"""Read-only R2 clip coverage export. Run on Jelly; never copies credentials."""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import subprocess

DEFINITION = ('Union of catalogued R2 clip seconds inside the stream’s scheduled daily '
              'windows, divided by the union of scheduled seconds for each local day. '
              'Overlapping windows and clips count once; overnight windows split at local '
              'midnight. Timezone boundaries account for DST. Gap days count as zero. '
              'Excludes purged clips and R2 retirements; catalog presence, not a fresh '
              'R2 HEAD check. Stitched outputs do not fill raw coverage gaps.')


def query(policy):
    literal = json.dumps(policy).replace("'", "''")
    return """
WITH policy AS (
 SELECT * FROM jsonb_to_recordset('%s'::jsonb)
 AS p(stream_id bigint, tz text, keep_start text, keep_end_excl text)
), clips AS MATERIALIZED (
 SELECT r.stream_id, c.clip_start_at a, c.clip_end_at b, c.size_bytes, c.object_key,
        coalesce(p.tz, nullif(s.local_timezone,'')) tz,
        p.keep_start, p.keep_end_excl
 FROM recording_clips c JOIN recordings r ON r.id=c.recording_id
 JOIN streams s ON s.id=r.stream_id LEFT JOIN policy p ON p.stream_id=s.id
 WHERE r.account_id=47 AND c.purged_at IS NULL
 AND c.endpoint LIKE '%%.r2.cloudflarestorage.com' AND c.bucket<>'' AND c.object_key<>''
 AND c.clip_end_at>c.clip_start_at
 AND NOT EXISTS (SELECT 1 FROM r2_object_retirements x
   WHERE (x.kind='raw' AND x.source_id=c.id) OR (x.bucket=c.bucket AND x.object_key=c.object_key))
), history AS (
 SELECT r.stream_id,
   min((c.clip_start_at AT TIME ZONE coalesce(p.tz,nullif(s.local_timezone,'')))::date) first_day,
   max(((c.clip_end_at-interval '1 microsecond') AT TIME ZONE coalesce(p.tz,nullif(s.local_timezone,'')))::date) last_day
 FROM recording_clips c JOIN recordings r ON r.id=c.recording_id
 JOIN streams s ON s.id=r.stream_id LEFT JOIN policy p ON p.stream_id=s.id
 WHERE r.account_id=47 AND c.clip_end_at>c.clip_start_at
 AND c.endpoint LIKE '%%.r2.cloudflarestorage.com'
 GROUP BY r.stream_id
), schedules AS (
 SELECT DISTINCT r.stream_id, r.daily_window_start ws, r.daily_window_end we,
 coalesce(p.tz,nullif(s.local_timezone,'')) tz
 FROM recordings r JOIN streams s ON s.id=r.stream_id
 LEFT JOIN policy p ON p.stream_id=s.id WHERE r.account_id=47
), window_spans AS (
 SELECT sc.stream_id, d::date local_day,
 tstzrange(d::date::timestamp AT TIME ZONE sc.tz,
 (d::date+1)::timestamp AT TIME ZONE sc.tz,'[)') *
 tstzrange((base::date+ws) AT TIME ZONE sc.tz,
 (base::date+we+
 CASE WHEN we<=ws THEN interval '1 day' ELSE interval '0 day' END)
 AT TIME ZONE sc.tz,'[)') span
 FROM schedules sc JOIN history h ON h.stream_id=sc.stream_id
 CROSS JOIN LATERAL generate_series(h.first_day::timestamp,h.last_day::timestamp,interval '1 day') d
 CROSS JOIN LATERAL generate_series(d-interval '1 day',d,interval '1 day') base
), windows AS (
 SELECT stream_id,local_day,unnest(range_agg(span)) span
 FROM window_spans WHERE NOT isempty(span) GROUP BY stream_id,local_day
), scheduled AS (
 SELECT stream_id,jsonb_object_agg(local_day,seconds) scheduled
 FROM (SELECT stream_id,local_day,sum(extract(epoch FROM upper(span)-lower(span))) seconds
 FROM windows GROUP BY stream_id,local_day) t GROUP BY stream_id
), labels AS (
 SELECT stream_id,jsonb_agg(jsonb_build_array(to_char(ws,'HH24:MI'),
 to_char(we,'HH24:MI')) ORDER BY ws,we) windows
 FROM schedules GROUP BY stream_id
), spans AS (
 SELECT stream_id, tz, unnest(range_agg(tstzrange(a,b,'[)'))) span
 FROM clips GROUP BY stream_id,tz
), days AS (
 SELECT w.stream_id,w.local_day,
 sum(extract(epoch FROM upper(w.span * s.span)-lower(w.span * s.span))) seconds
 FROM windows w JOIN spans s ON s.stream_id=w.stream_id AND w.span && s.span
 GROUP BY w.stream_id,w.local_day
), daily AS (
 SELECT stream_id,jsonb_object_agg(local_day,seconds) days FROM days GROUP BY stream_id
), sizes AS (
 SELECT stream_id, sum(size_bytes) raw_bytes,
 sum(size_bytes) FILTER (WHERE (a AT TIME ZONE tz)::date >= keep_start::date
   AND (a AT TIME ZONE tz)::date < keep_end_excl::date) raw_keep
 FROM clips GROUP BY stream_id
), stitched AS (
 SELECT r.stream_id, sum(o.size_bytes) stitched_bytes,
 sum(o.size_bytes) FILTER (WHERE (o.start_at AT TIME ZONE p.tz)::date >= p.keep_start::date
  AND (o.start_at AT TIME ZONE p.tz)::date < p.keep_end_excl::date) stitched_keep
 FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id
 JOIN recordings r ON r.id=h.recording_id LEFT JOIN policy p ON p.stream_id=r.stream_id
 WHERE r.account_id=47 AND o.object_key<>''
 AND NOT EXISTS (SELECT 1 FROM r2_object_retirements x WHERE
   (x.kind='collated' AND x.source_id=o.id) OR x.object_key=o.object_key)
 AND NOT EXISTS (SELECT 1 FROM clips c WHERE c.object_key=o.object_key)
 GROUP BY r.stream_id
), ids AS (
 SELECT stream_id FROM policy UNION SELECT stream_id FROM clips UNION SELECT stream_id FROM stitched
)
SELECT jsonb_build_object('generated_at',now(),'streams',jsonb_agg(jsonb_build_object(
 'stream_id',s.id,'name',s.name,'city',s.location_city,'country',s.location_country,
 'tz',coalesce(p.tz,nullif(s.local_timezone,'')), 'first_day',hist.first_day,'last_day',hist.last_day, 'days',coalesce(d.days,'{}'::jsonb),
 'scheduled',coalesce(sc.scheduled,'{}'::jsonb),'windows',lb.windows,
 'raw_bytes',coalesce(z.raw_bytes,0),'raw_keep',coalesce(z.raw_keep,0),
 'stitched_bytes',coalesce(st.stitched_bytes,0),'stitched_keep',coalesce(st.stitched_keep,0)) ORDER BY s.id))
FROM ids JOIN streams s ON s.id=ids.stream_id LEFT JOIN policy p ON p.stream_id=s.id
LEFT JOIN scheduled sc ON sc.stream_id=s.id LEFT JOIN labels lb ON lb.stream_id=s.id
LEFT JOIN daily d ON d.stream_id=s.id LEFT JOIN sizes z ON z.stream_id=s.id
LEFT JOIN stitched st ON st.stream_id=s.id LEFT JOIN history hist ON hist.stream_id=s.id;
""" % literal


def build(result, policy, catalog):
    pinned = {r['stream_id']: r for r in policy}
    previous = {r['stream_id']: r for r in catalog}
    rows = []
    for data in result['streams']:
        sid = data['stream_id']
        row = dict(previous.get(sid, {}))
        # The policy at origin/main is authoritative; selection.json is never read/written.
        row.update(pinned.get(sid, {}))
        row.update({k: data[k] for k in ('stream_id', 'name', 'city', 'country', 'tz')})
        if not row['tz']:
            raise ValueError('Missing stream timezone: %s' % sid)
        if not data['windows'] or any(a is None or b is None for a, b in data['windows']):
            raise ValueError('Missing daily recording window: %s' % sid)
        days = data['days']
        dates = sorted(days)
        first = data.get('first_day') or (dates[0] if dates else None)
        last = data.get('last_day') or (dates[-1] if dates else None)
        seconds = []
        scheduled = []
        if first and last:
            date = dt.date.fromisoformat(first)
            while date <= dt.date.fromisoformat(last):
                scheduled.append(float(data['scheduled'][str(date)]))
                seconds.append(round(float(days.get(str(date), 0)), 6))
                date += dt.timedelta(days=1)
        percentages = [s / w * 100 for s, w in zip(seconds, scheduled)]
        row.update(scheduled_seconds=scheduled, windows=data['windows'], first_day=first, last_day=last, seconds=seconds,
                   days_recorded=sum(s > 0 for s in seconds),
                   total_hours=round(sum(seconds)/3600, 2),
                   avg_coverage=sum(percentages)/len(percentages) if percentages else 0,
                   days90=sum(p >= 90 for p in percentages),
                   r2_raw_keep=data['raw_keep'], r2_raw_out=data['raw_bytes']-data['raw_keep'],
                   r2_st_keep=data['stitched_keep'], r2_st_out=data['stitched_bytes']-data['stitched_keep'])
        rows.append(row)
    if not set(pinned).issubset(r['stream_id'] for r in rows):
        raise ValueError('Export omitted pinned streams')
    return dict(generated_at=result['generated_at'], definition=DEFINITION, streams=rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--policy', type=Path, required=True)
    parser.add_argument('--catalog', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--query-result', type=Path)
    parser.add_argument('--sql-only', action='store_true')
    args = parser.parse_args()
    policy = json.loads(args.policy.read_text())
    sql = query(policy)
    if args.sql_only:
        print(sql)
        return
    if args.query_result:
        result = json.loads(args.query_result.read_text())
    else:
        env = dict(os.environ, PGOPTIONS='-c default_transaction_read_only=on')
        # Capture and sanitize errors so libpq cannot echo the connection string.
        proc = subprocess.run(['/opt/homebrew/opt/postgresql@17/bin/psql', env['DATABASE_URL'], '-X', '-qAt',
                               '-v', 'ON_ERROR_STOP=1'], input=sql, text=True,
                              capture_output=True, env=env)
        if proc.returncode:
            raise SystemExit('Read-only coverage query failed (psql exit %s)' % proc.returncode)
        result = json.loads(proc.stdout)
    output = build(result, policy, json.loads(args.catalog.read_text()))
    args.output.write_text(json.dumps(output, ensure_ascii=False, separators=(',', ':'))+'\n')
    print('%s streams exported' % len(output['streams']))


if __name__ == '__main__':
    main()
