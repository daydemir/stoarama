BEGIN;

ALTER TABLE streams
  ADD COLUMN IF NOT EXISTS location_country_code TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS location_country TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS location_region TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS location_city TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS location_locality TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS location_source TEXT NOT NULL DEFAULT '';

UPDATE streams
SET
  location_country_code = UPPER(TRIM(COALESCE(NULLIF(location_country_code, ''), NULLIF(metadata_jsonb->>'country_code', ''), ''))),
  location_country = TRIM(COALESCE(NULLIF(location_country, ''), NULLIF(metadata_jsonb->>'country', ''), '')),
  location_region = TRIM(COALESCE(NULLIF(location_region, ''), NULLIF(metadata_jsonb->>'region', ''), NULLIF(metadata_jsonb->>'state', ''), NULLIF(metadata_jsonb->>'province', ''), '')),
  location_city = TRIM(COALESCE(
    NULLIF(location_city, ''),
    NULLIF(metadata_jsonb->>'city', ''),
    NULLIF(metadata_jsonb->>'locality', ''),
    NULLIF(metadata_jsonb->>'town', ''),
    NULLIF(metadata_jsonb->>'municipality', ''),
    NULLIF(split_part(COALESCE(location_text, ''), ',', 1), ''),
    ''
  )),
  location_locality = TRIM(COALESCE(NULLIF(location_locality, ''), NULLIF(metadata_jsonb->>'district', ''), NULLIF(metadata_jsonb->>'neighborhood', ''), '')),
  location_source = TRIM(COALESCE(NULLIF(location_source, ''), 'legacy_backfill'))
WHERE
  location_country_code = ''
  OR location_country = ''
  OR location_region = ''
  OR location_city = ''
  OR location_locality = ''
  OR location_source = '';

CREATE INDEX IF NOT EXISTS idx_streams_location_country_city
ON streams (LOWER(location_country), LOWER(location_city), id);

CREATE INDEX IF NOT EXISTS idx_streams_location_city
ON streams (LOWER(location_city), id);

COMMIT;
