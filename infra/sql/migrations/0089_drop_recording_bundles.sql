BEGIN;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM recording_bundles) OR
     EXISTS (SELECT 1 FROM recordings WHERE bundle_id IS NOT NULL) THEN
    RAISE EXCEPTION 'cannot remove recording bundles while bundle data exists';
  END IF;
END
$$;

ALTER TABLE recordings DROP COLUMN IF EXISTS bundle_id;
DROP TABLE IF EXISTS recording_bundles;

COMMIT;
