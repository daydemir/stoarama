BEGIN;

ALTER TABLE pipelines
  ADD COLUMN IF NOT EXISTS pipeline_family TEXT NOT NULL DEFAULT 'inference';

ALTER TABLE pipelines
  DROP CONSTRAINT IF EXISTS pipelines_family_check;
ALTER TABLE pipelines
  ADD CONSTRAINT pipelines_family_check
  CHECK (pipeline_family IN ('discovery', 'metadata', 'inference'));

CREATE TABLE IF NOT EXISTS source_candidates (
  id BIGSERIAL PRIMARY KEY,
  provider TEXT NOT NULL DEFAULT '',
  external_id TEXT NOT NULL DEFAULT '',
  source_family TEXT NOT NULL,
  capture_type TEXT NOT NULL,
  source_url TEXT NOT NULL,
  source_page_url TEXT NOT NULL DEFAULT '',
  title TEXT NOT NULL DEFAULT '',
  slug TEXT NOT NULL DEFAULT '',
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  review_status TEXT NOT NULL DEFAULT 'pending',
  review_reason TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT source_candidates_source_family_check
    CHECK (source_family IN ('watch_page', 'video_manifest', 'video_stream', 'still_image', 'provider_api', 'embed_page')),
  CONSTRAINT source_candidates_capture_type_check
    CHECK (capture_type IN ('youtube_watch', 'hls', 'dash', 'rtsp', 'rtmp', 'http_video', 'still_image', 'webrtc', 'unknown')),
  CONSTRAINT source_candidates_review_status_check
    CHECK (review_status IN ('pending', 'accepted', 'rejected', 'invalid'))
);

DROP TRIGGER IF EXISTS trg_source_candidates_updated_at ON source_candidates;
CREATE TRIGGER trg_source_candidates_updated_at
BEFORE UPDATE ON source_candidates
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE UNIQUE INDEX IF NOT EXISTS idx_source_candidates_provider_external
ON source_candidates (provider, external_id)
WHERE btrim(provider) <> '' AND btrim(external_id) <> '';

CREATE INDEX IF NOT EXISTS idx_source_candidates_review_status
ON source_candidates (review_status, created_at DESC);

CREATE TABLE IF NOT EXISTS source_candidate_runs (
  id BIGSERIAL PRIMARY KEY,
  candidate_id BIGINT NOT NULL REFERENCES source_candidates(id) ON DELETE CASCADE,
  pipeline_id TEXT REFERENCES pipelines(id) ON DELETE SET NULL,
  worker_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  error_text TEXT NOT NULL DEFAULT '',
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT source_candidate_runs_status_check
    CHECK (status IN ('running', 'success', 'error'))
);

CREATE INDEX IF NOT EXISTS idx_source_candidate_runs_candidate_started
ON source_candidate_runs (candidate_id, started_at DESC);

CREATE TABLE IF NOT EXISTS source_candidate_reviews (
  id BIGSERIAL PRIMARY KEY,
  candidate_id BIGINT NOT NULL REFERENCES source_candidates(id) ON DELETE CASCADE,
  reviewer TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT source_candidate_reviews_status_check
    CHECK (status IN ('accepted', 'rejected', 'invalid'))
);

CREATE INDEX IF NOT EXISTS idx_source_candidate_reviews_candidate_created
ON source_candidate_reviews (candidate_id, created_at DESC);

COMMIT;
