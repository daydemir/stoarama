BEGIN;

ALTER TABLE pipelines
  DROP CONSTRAINT IF EXISTS pipelines_kind_check;

ALTER TABLE pipelines
  ADD CONSTRAINT pipelines_kind_check
  CHECK (kind IN ('detector', 'vlm'));

CREATE TABLE IF NOT EXISTS inference_signals (
  id BIGSERIAL PRIMARY KEY,
  inference_result_id BIGINT NOT NULL REFERENCES inference_results(id) ON DELETE CASCADE,
  signal_type TEXT NOT NULL,
  signal_key TEXT NOT NULL,
  confidence DOUBLE PRECISION,
  value_num DOUBLE PRECISION,
  value_text TEXT,
  extra_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT inference_signals_signal_type_chk CHECK (length(btrim(signal_type)) > 0),
  CONSTRAINT inference_signals_signal_key_chk CHECK (length(btrim(signal_key)) > 0)
);

CREATE INDEX IF NOT EXISTS idx_inference_signals_result_id
ON inference_signals(inference_result_id, id);

CREATE INDEX IF NOT EXISTS idx_inference_signals_type_key
ON inference_signals(signal_type, signal_key);

COMMIT;
