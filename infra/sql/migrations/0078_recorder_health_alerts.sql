BEGIN;

-- Dedup ledger for the hourly recording-health-check cron (stoaramactl
-- recording-health run). Each row tracks one open incident keyed by
-- (recording_id, signal): the cron UPSERTs on every detection, emails operators
-- only on first sight or a re-notify (after resolve, or once past 24h), and
-- stamps resolved_at when a prior incident is no longer detected. This is what
-- catches the silent-death failure (a continuous recording marked done/idle
-- mid-window producing zero clips) without re-emailing every hour.
CREATE TABLE IF NOT EXISTS recorder_health_alerts (
  recording_id    BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
  signal          TEXT   NOT NULL,
  first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_alerted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at     TIMESTAMPTZ,
  PRIMARY KEY (recording_id, signal)
);

COMMIT;
