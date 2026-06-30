BEGIN;

-- Pricing pivot: bill recording usage per RECORD-HOUR ($0.05) instead of per
-- record-day ($0.50), and managed storage per STREAM-HOUR-MONTH ($0.10) instead of
-- per GB-month ($0.10). The day/byte views and columns are KEPT (superseded, never
-- dropped) for forward-only safety; the metering + /billing paths now read the new
-- hour-granular shapes below.

-- Billable record-hours: DISTINCT (recording, UTC hour) with at least one successful
-- clip, inside the recording's [start_at, end_at) window. The exact analog of
-- recording_billing_days but truncating the clip's day column to the UTC hour. A
-- recording_clips row exists only after a verified upload, so row-presence IS
-- "successful capture" (there is no capture_status column).
CREATE VIEW recording_billing_hours AS
SELECT DISTINCT
  c.recording_id,
  r.account_id,
  date_trunc('hour', c.clip_start_at AT TIME ZONE 'UTC') AS rec_hour
FROM recording_clips c
JOIN recordings r ON r.id = c.recording_id
WHERE c.clip_start_at >= r.start_at
  AND c.clip_start_at <  COALESCE(r.end_at, 'infinity'::timestamptz);

-- Daily stored stream-hours of managed footage, alongside the existing byte total.
-- SUM over a managed account's non-purged clips of clip duration in hours
-- (clip_end_at - clip_start_at); the period AVERAGE of this column is the billable
-- stream-hour-months. duration_ms is unreliable (0 on many rows) so the wall-clock
-- span is used. Default 0 backfills existing snapshot rows.
ALTER TABLE account_storage_snapshots
  ADD COLUMN stream_hours_stored DOUBLE PRECISION NOT NULL DEFAULT 0;

COMMIT;
