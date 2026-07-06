BEGIN;

ALTER TABLE nodes
  ALTER COLUMN relay_max_streams SET DEFAULT 6;

UPDATE nodes
SET relay_max_streams = 6
WHERE node_type = 'relay'
  AND relay_max_streams = 5;

COMMIT;
