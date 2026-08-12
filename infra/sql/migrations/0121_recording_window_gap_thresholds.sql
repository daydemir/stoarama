-- Qualification grades distinguish a few long outages from incessant short
-- fragmentation. Persist exact strict-threshold counts from merged coverage
-- intervals; the existing gap_count is an operational seam counter and cannot
-- answer these questions because it excludes leading/trailing gaps.
ALTER TABLE recording_window_health
  ADD COLUMN IF NOT EXISTS gap_over_30s_count INTEGER CHECK (gap_over_30s_count >= 0),
  ADD COLUMN IF NOT EXISTS gap_over_5m_count INTEGER CHECK (gap_over_5m_count >= 0),
  ADD COLUMN IF NOT EXISTS metric_version INTEGER CHECK (metric_version > 0);

-- NULL is intentional for historical rows until the bounded hourly
-- materializer recomputes them. Qualification must fail closed on NULL rather
-- than manufacture a passing zero.
