BEGIN;

-- Standalone paid stream-recorder. A logged-in user configures a recording: an
-- HLS/HTTPS stream URL + an existing storage_destination + a 5-field cron + a
-- clip-duration knob, and we capture one clip per cron fire to THEIR bucket.
-- This is fully separate from streams/capture_jobs/capture_segments/media_objects.
-- recordings.status is USER INTENT only; paid/dunning state lives in
-- account_billing and is surfaced via the recording_billing_state view (0052).
CREATE TABLE IF NOT EXISTS recordings (
  id                       BIGSERIAL PRIMARY KEY,
  account_id               BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  storage_destination_id   BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE RESTRICT,
  name                     TEXT NOT NULL,
  stream_url               TEXT NOT NULL,
  source_kind              TEXT NOT NULL DEFAULT 'auto'
                             CHECK (source_kind IN ('auto','hls_live','ffmpeg_direct')),
  cron_expr                TEXT NOT NULL,                       -- standard 5-field
  cron_timezone            TEXT NOT NULL DEFAULT 'UTC',         -- IANA tz
  clip_duration_sec        INTEGER NOT NULL DEFAULT 60 CHECK (clip_duration_sec BETWEEN 5 AND 900),
  -- forward cursor for catch-up: the last fire instant already materialized into recording_jobs.
  last_enqueued_fire_at    TIMESTAMPTZ,
  next_fire_at             TIMESTAMPTZ,                         -- denormalized cache (display + forecast hint)
  -- user intent ONLY. billing/dunning state lives in account_billing.
  status                   TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','paused','canceled')),
  last_clip_at             TIMESTAMPTZ,                         -- written by ingest
  last_error_text          TEXT NOT NULL DEFAULT '',           -- health surfacing
  last_error_at            TIMESTAMPTZ,
  consecutive_failures     INTEGER NOT NULL DEFAULT 0,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_recordings_updated_at ON recordings;
CREATE TRIGGER trg_recordings_updated_at BEFORE UPDATE ON recordings
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- name unique per account, case-insensitive (expression index; cannot be an
-- inline UNIQUE table constraint). Mirrors idx_storage_destinations_account_name.
CREATE UNIQUE INDEX IF NOT EXISTS idx_recordings_account_name ON recordings (account_id, lower(name));
CREATE INDEX IF NOT EXISTS idx_recordings_account_created ON recordings (account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_recordings_active ON recordings (id) WHERE status = 'active';

-- recording_jobs: one row per cron fire.
CREATE TABLE IF NOT EXISTS recording_jobs (
  id                BIGSERIAL PRIMARY KEY,
  recording_id      BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
  fire_at           TIMESTAMPTZ NOT NULL,                       -- the cron fire instant (UTC)
  scheduled_for     TIMESTAMPTZ NOT NULL,                       -- = fire_at; leasable when scheduled_for <= now()
  clip_duration_sec INTEGER NOT NULL CHECK (clip_duration_sec > 0), -- snapshot at enqueue
  status            TEXT NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending','leased','done','error','canceled')),
  lease_owner       TEXT,                                       -- = worker_id = recorder_droplets.name
  lease_expires_at  TIMESTAMPTZ,
  attempt_count     INTEGER NOT NULL DEFAULT 0,
  max_attempts      INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
  error_text        TEXT NOT NULL DEFAULT '',
  idempotency_key   TEXT NOT NULL,                              -- 'recjob:<recording_id>:<floor(epoch(fire_at))>'
  completed_at      TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (idempotency_key)
);

DROP TRIGGER IF EXISTS trg_recording_jobs_updated_at ON recording_jobs;
CREATE TRIGGER trg_recording_jobs_updated_at BEFORE UPDATE ON recording_jobs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_recording_jobs_pending_due
  ON recording_jobs (scheduled_for, id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_recording_jobs_leased_expiry
  ON recording_jobs (lease_expires_at) WHERE status = 'leased';
CREATE INDEX IF NOT EXISTS idx_recording_jobs_recording_fire
  ON recording_jobs (recording_id, fire_at DESC);
-- NOTE: there is intentionally NO unique "one active per recording" partial index.
-- Bulk cron enqueue + such an index aborts the whole sweep when a recording lags.

-- recording_clips: metadata per uploaded clip in the USER's bucket. Standalone (not media_objects).
CREATE TABLE IF NOT EXISTS recording_clips (
  id                       BIGSERIAL PRIMARY KEY,
  recording_id             BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
  recording_job_id         BIGINT REFERENCES recording_jobs(id) ON DELETE SET NULL,
  storage_destination_id   BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE RESTRICT,
  endpoint                 TEXT NOT NULL,                       -- destination snapshot (locatable after edit/delete)
  bucket                   TEXT NOT NULL,
  object_key               TEXT NOT NULL,
  thumbnail_object_key     TEXT NOT NULL DEFAULT '',
  mime_type                TEXT NOT NULL DEFAULT 'video/mp4',
  container                TEXT NOT NULL DEFAULT 'mp4',
  size_bytes               BIGINT NOT NULL CHECK (size_bytes >= 0),
  etag                     TEXT NOT NULL DEFAULT '',
  sha256                   TEXT NOT NULL DEFAULT '',
  duration_ms              BIGINT NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
  video_codec              TEXT NOT NULL DEFAULT '',
  audio_codec              TEXT NOT NULL DEFAULT '',
  audio_present            BOOLEAN NOT NULL DEFAULT false,
  actual_fps               DOUBLE PRECISION,
  resolved_url             TEXT NOT NULL DEFAULT '',
  fire_at                  TIMESTAMPTZ NOT NULL,                -- the cron instant this clip belongs to
  clip_start_at            TIMESTAMPTZ NOT NULL,
  clip_end_at              TIMESTAMPTZ NOT NULL,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (bucket, object_key)
);

CREATE INDEX IF NOT EXISTS idx_recording_clips_recording_started
  ON recording_clips (recording_id, clip_start_at DESC);

-- recording_upload_intents: per-clip presign ledger against the user destination.
CREATE TABLE IF NOT EXISTS recording_upload_intents (
  id                     UUID PRIMARY KEY,
  recording_id           BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
  recording_job_id       BIGINT NOT NULL REFERENCES recording_jobs(id) ON DELETE CASCADE,
  storage_destination_id BIGINT NOT NULL REFERENCES storage_destinations(id) ON DELETE RESTRICT,
  endpoint               TEXT NOT NULL,
  bucket                 TEXT NOT NULL,
  object_key             TEXT NOT NULL,
  mime_type              TEXT NOT NULL DEFAULT 'video/mp4',
  max_size_bytes         BIGINT NOT NULL,                       -- presign content-length cap
  status                 TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','consumed','expired')),
  expires_at             TIMESTAMPTZ NOT NULL,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_recording_upload_intents_job ON recording_upload_intents (recording_job_id);

COMMIT;
