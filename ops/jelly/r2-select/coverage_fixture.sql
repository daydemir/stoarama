-- CTE fixtures shadow production tables. This is SELECT-only, even on Jelly.
recordings(id,account_id,stream_id,daily_window_start,daily_window_end) AS (VALUES (1,47,1,'00:00'::time,'00:00'::time),(2,48,2,'00:00'::time,'00:00'::time),(3,47,3,'23:00'::time,'01:00'::time),(4,47,1,'08:00'::time,'20:00'::time)),
streams(id,name,location_city,location_country,local_timezone) AS (
 VALUES (1,'DST overlap','Los Angeles','United States','America/Los_Angeles'),
 (2,'Other account','','','UTC'),(3,'Extra R2 stream','','','UTC')),
recording_clips(id,recording_id,clip_start_at,clip_end_at,size_bytes,object_key,purged_at,endpoint,bucket) AS (
 VALUES
 (1,1,'2026-11-01 07:00Z'::timestamptz,'2026-11-02 08:00Z'::timestamptz,100,'a',NULL::timestamptz,'https://example.r2.cloudflarestorage.com','test'),
 (2,1,'2026-11-01 08:00Z'::timestamptz,'2026-11-01 12:00Z'::timestamptz,20,'b',NULL::timestamptz,'https://example.r2.cloudflarestorage.com','test'),
 (3,1,'2026-11-03 08:00Z'::timestamptz,'2026-11-04 08:00Z'::timestamptz,100,'purged','2026-11-05Z'::timestamptz,'https://example.r2.cloudflarestorage.com','test'),
 (4,1,'2026-11-04 08:00Z'::timestamptz,'2026-11-05 08:00Z'::timestamptz,100,'retired',NULL::timestamptz,'https://example.r2.cloudflarestorage.com','test'),
 (5,2,'2026-11-01 00:00Z'::timestamptz,'2026-11-02 00:00Z'::timestamptz,100,'foreign',NULL::timestamptz,'https://example.r2.cloudflarestorage.com','test'),
 (6,3,'2026-11-01 23:30Z'::timestamptz,'2026-11-02 00:30Z'::timestamptz,100,'extra',NULL::timestamptz,'https://example.r2.cloudflarestorage.com','test')),
r2_object_retirements(kind,source_id,bucket,object_key) AS (VALUES ('raw',4,'test','retired')),
recording_collation_outputs(collation_hour_id,size_bytes,object_key,id,start_at) AS (
 SELECT 1,1,'',1,now() WHERE false),
recording_collation_hours(id,recording_id) AS (SELECT 1,1 WHERE false),
