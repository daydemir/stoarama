-- Optional NAS cleanup status reported by the app in its heartbeat (replay
-- failures, quarantined journal entries, batch-size fallbacks, last lease).
-- Diagnostics only; no authority. Nullable without default: metadata-only.
ALTER TABLE connections
 ADD COLUMN nas_cleanup_status jsonb,
 ADD COLUMN nas_cleanup_status_at timestamptz;
