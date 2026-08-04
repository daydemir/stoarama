ALTER TABLE recorder_droplets
    ADD COLUMN build_sha TEXT NOT NULL DEFAULT '';

ALTER TABLE recorder_droplets
    ADD CONSTRAINT recorder_droplets_build_sha_shape
    CHECK (build_sha = '' OR build_sha ~ '^[0-9a-f]{40,64}$');
