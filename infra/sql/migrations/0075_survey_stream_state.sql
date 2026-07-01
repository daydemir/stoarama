BEGIN;

-- Per-stream survey attempt state, used only by the survey sweep to prioritize
-- targets and rate-limit chronically-failing streams. It is intentionally
-- separate from stream_health (the recording-health pipeline): survey failures
-- must not pollute recording-health counters, and the survey stays decoupled.
--
-- consecutive_failures drives the two things the sweep needs:
--   1. A cross-run first-failure marker: the sweep no longer sleeps a worker for
--      5 minutes to confirm a failure. The first failure records the marker here;
--      a later run confirms (tags) if it fails again. So one failure never holds
--      a worker slot.
--   2. Bounded backoff: after K straight failures a stream is skipped until
--      last_attempt_at + a backoff window (capped), so chronically-broken streams
--      stop dominating every sweep but are still re-checked before too long.
-- A successful capture deletes the row (streams with no row have never failed
-- recently), which the survey does alongside clearing the 'error' tag.
CREATE TABLE IF NOT EXISTS survey_stream_state (
  stream_id            BIGINT PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
  consecutive_failures INT NOT NULL DEFAULT 0,
  first_failure_at     TIMESTAMPTZ,
  last_failure_at      TIMESTAMPTZ,
  last_attempt_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
