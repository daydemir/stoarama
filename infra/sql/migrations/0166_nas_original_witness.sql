-- Read-only, SHA-bound original-video presentation evidence; no deletion authority.
ALTER TABLE nas_selected_verify_plans ADD COLUMN original_witness BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE nas_selected_verify_items ADD COLUMN original_witness JSONB;
ALTER TABLE nas_selected_verify_items ADD CONSTRAINT nas_original_witness_shape CHECK (
 original_witness IS NULL OR (state='match' AND jsonb_typeof(original_witness)='object')
);
