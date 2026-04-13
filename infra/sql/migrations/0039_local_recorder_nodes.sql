BEGIN;

ALTER TABLE nodes
  DROP CONSTRAINT IF EXISTS nodes_node_type_check;
ALTER TABLE nodes
  ADD CONSTRAINT nodes_node_type_check
  CHECK (node_type IN ('yt_relay_source', 'inference_node', 'local_recorder'));

ALTER TABLE node_enrollment_tokens
  DROP CONSTRAINT IF EXISTS node_enrollment_tokens_node_type_check;
ALTER TABLE node_enrollment_tokens
  ADD CONSTRAINT node_enrollment_tokens_node_type_check
  CHECK (node_type IN ('yt_relay_source', 'inference_node', 'local_recorder'));

COMMIT;
