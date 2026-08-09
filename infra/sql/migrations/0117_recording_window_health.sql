-- Persist exact completed-window timeline measurements so list pages never scan
-- recording_clips. The hourly health sweep refreshes recent windows to absorb
-- late deliveries; older rows are stable historical facts.
CREATE TABLE IF NOT EXISTS recording_window_health (
  recording_id          BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
  -- Logical source id only: recording_jobs may be pruned after completion, while
  -- this compact historical health fact must remain for whole-period reporting.
  job_id                BIGINT NOT NULL,
  window_start_at       TIMESTAMPTZ NOT NULL,
  window_end_at         TIMESTAMPTZ NOT NULL,
  expected_seconds      BIGINT NOT NULL CHECK (expected_seconds > 0),
  covered_seconds       DOUBLE PRECISION NOT NULL CHECK (covered_seconds >= 0),
  coverage_pct          DOUBLE PRECISION NOT NULL CHECK (coverage_pct >= 0 AND coverage_pct <= 100),
  largest_gap_seconds   DOUBLE PRECISION NOT NULL CHECK (largest_gap_seconds >= 0),
  gap_count             INTEGER NOT NULL CHECK (gap_count >= 0),
  overlap_count         INTEGER NOT NULL CHECK (overlap_count >= 0),
  overlap_seconds       DOUBLE PRECISION NOT NULL CHECK (overlap_seconds >= 0),
  longest_run_seconds   DOUBLE PRECISION NOT NULL CHECK (longest_run_seconds >= 0),
  layout_change_count   INTEGER NOT NULL CHECK (layout_change_count >= 0),
  clip_count            INTEGER NOT NULL CHECK (clip_count >= 0),
  calculated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (recording_id, job_id),
  CHECK (window_end_at > window_start_at)
);

CREATE INDEX IF NOT EXISTS recording_window_health_recording_end_idx
  ON recording_window_health (recording_id, window_end_at DESC);

-- One cheap list/read model per recording. "Recent" is the completed windows
-- ending in the last 48 wall-clock hours; "lifetime" includes every materialized
-- completed window. Percentages are duration-weighted, never clip-count based.
CREATE TABLE IF NOT EXISTS recording_health_summaries (
  recording_id                    BIGINT PRIMARY KEY REFERENCES recordings(id) ON DELETE CASCADE,
  recent_expected_seconds         BIGINT NOT NULL DEFAULT 0 CHECK (recent_expected_seconds >= 0),
  recent_covered_seconds          DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (recent_covered_seconds >= 0),
  recent_coverage_pct             DOUBLE PRECISION,
  recent_largest_gap_seconds      DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (recent_largest_gap_seconds >= 0),
  recent_gap_count                BIGINT NOT NULL DEFAULT 0 CHECK (recent_gap_count >= 0),
  recent_overlap_count            BIGINT NOT NULL DEFAULT 0 CHECK (recent_overlap_count >= 0),
  recent_overlap_seconds          DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (recent_overlap_seconds >= 0),
  recent_layout_change_count      BIGINT NOT NULL DEFAULT 0 CHECK (recent_layout_change_count >= 0),
  recent_window_count             INTEGER NOT NULL DEFAULT 0 CHECK (recent_window_count >= 0),
  lifetime_expected_seconds       BIGINT NOT NULL DEFAULT 0 CHECK (lifetime_expected_seconds >= 0),
  lifetime_covered_seconds        DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (lifetime_covered_seconds >= 0),
  lifetime_coverage_pct           DOUBLE PRECISION,
  lifetime_largest_gap_seconds    DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (lifetime_largest_gap_seconds >= 0),
  lifetime_gap_count              BIGINT NOT NULL DEFAULT 0 CHECK (lifetime_gap_count >= 0),
  lifetime_overlap_count          BIGINT NOT NULL DEFAULT 0 CHECK (lifetime_overlap_count >= 0),
  lifetime_overlap_seconds        DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (lifetime_overlap_seconds >= 0),
  lifetime_layout_change_count    BIGINT NOT NULL DEFAULT 0 CHECK (lifetime_layout_change_count >= 0),
  lifetime_window_count           INTEGER NOT NULL DEFAULT 0 CHECK (lifetime_window_count >= 0),
  -- Durable high-water mark. Each sweep raises this by counting already
  -- materialized facts plus any still-visible completed jobs. It never falls if
  -- the operational job ledger is later pruned.
  lifetime_expected_window_count  INTEGER NOT NULL DEFAULT 0 CHECK (lifetime_expected_window_count >= 0),
  lifetime_complete               BOOLEAN NOT NULL DEFAULT false,
  calculated_at                   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Establish the durable baseline in the same migration that introduces the
-- health facts. Future operational job pruning cannot erase knowledge of how
-- many already-completed windows the historical backfill must cover.
INSERT INTO recording_health_summaries (recording_id, lifetime_expected_window_count)
SELECT r.id, COUNT(j.id)::int
FROM recordings r
JOIN recording_jobs j ON j.recording_id=r.id
WHERE j.kind='continuous_window' AND j.window_end_at<=now()
GROUP BY r.id
ON CONFLICT (recording_id) DO UPDATE SET
  lifetime_expected_window_count=GREATEST(
    recording_health_summaries.lifetime_expected_window_count,
    EXCLUDED.lifetime_expected_window_count
  );
