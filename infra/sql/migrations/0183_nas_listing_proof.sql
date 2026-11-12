-- Hash-free namespace evidence is separate from the content-hashing inventory.
ALTER TABLE connections ADD COLUMN nas_listing_enabled boolean NOT NULL DEFAULT false,
 ADD COLUMN nas_listing_cursor_at timestamptz;
-- Connection 13 is the only reviewed rollout. No NAS environment change.
ALTER TABLE connections ALTER COLUMN nas_listing_enabled SET DEFAULT false;
UPDATE connections SET nas_listing_enabled=true WHERE id=13 AND account_id=47;
CREATE TABLE nas_listing_generations (
 connection_id bigint NOT NULL REFERENCES connections(id), generation text NOT NULL,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(), completed_at timestamptz,
 folders text[] NOT NULL CHECK(cardinality(folders) BETWEEN 1 AND 4096),
 PRIMARY KEY(connection_id,generation)
);
CREATE TABLE nas_listing_folders (
 connection_id bigint NOT NULL, generation text NOT NULL, folder text NOT NULL,
 next_page integer NOT NULL DEFAULT 0, file_count bigint NOT NULL DEFAULT 0,
 total_bytes bigint NOT NULL DEFAULT 0, completed_at timestamptz,
 PRIMARY KEY(connection_id,generation,folder),
 FOREIGN KEY(connection_id,generation) REFERENCES nas_listing_generations
);
CREATE TABLE nas_listing_pages (
 connection_id bigint NOT NULL, generation text NOT NULL, folder text NOT NULL,
 page integer NOT NULL, digest text NOT NULL,
 PRIMARY KEY(connection_id,generation,folder,page),
 FOREIGN KEY(connection_id,generation,folder) REFERENCES nas_listing_folders
);
CREATE TABLE nas_listing_files (
 connection_id bigint NOT NULL, generation text NOT NULL, folder text NOT NULL,
 relative_path text NOT NULL, size_bytes bigint NOT NULL CHECK(size_bytes>=0),
 mtime_ns bigint NOT NULL, inode bigint NOT NULL, device bigint NOT NULL, ctime_ns bigint NOT NULL,
 PRIMARY KEY(connection_id,generation,relative_path),
 FOREIGN KEY(connection_id,generation,folder) REFERENCES nas_listing_folders
);
CREATE INDEX nas_listing_path ON nas_listing_files(connection_id,relative_path,generation);
-- Evidence and writer-watermark publication share retirement's final fence.
CREATE FUNCTION fence_nas_listing_evidence() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM pg_advisory_xact_lock_shared(7343226011101);RETURN NEW;
END $$;
CREATE TRIGGER nas_listing_evidence_fence BEFORE INSERT OR UPDATE ON nas_listing_generations
 FOR EACH ROW EXECUTE FUNCTION fence_nas_listing_evidence();
CREATE TRIGGER nas_listing_presence_fence BEFORE INSERT ON nas_listing_files
 FOR EACH ROW EXECUTE FUNCTION fence_nas_listing_evidence();
CREATE TRIGGER nas_inventory_retire_fence BEFORE INSERT OR UPDATE ON nas_inventory_files
 FOR EACH ROW EXECUTE FUNCTION fence_nas_listing_evidence();
CREATE TRIGGER nas_unmatched_retire_fence BEFORE INSERT OR UPDATE ON nas_inventory_unmatched_files
 FOR EACH ROW EXECUTE FUNCTION fence_nas_listing_evidence();
CREATE TRIGGER nas_collated_ack_retire_fence BEFORE INSERT ON recording_collation_output_acks
 FOR EACH ROW EXECUTE FUNCTION fence_nas_listing_evidence();
CREATE FUNCTION fence_nas_listing_cursor() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.nas_listing_cursor_at IS DISTINCT FROM OLD.nas_listing_cursor_at THEN
 PERFORM pg_advisory_xact_lock_shared(7343226011101);END IF;RETURN NEW;
END $$;
CREATE TRIGGER nas_listing_cursor_fence BEFORE UPDATE ON connections
 FOR EACH ROW EXECUTE FUNCTION fence_nas_listing_cursor();
-- A new/reused catalog path is a possible future write, even before ACK.
CREATE TRIGGER nas_listing_raw_writer_fence BEFORE INSERT OR UPDATE OF display_path,released_at ON recording_clips
 FOR EACH ROW EXECUTE FUNCTION fence_nas_listing_evidence();
CREATE TRIGGER nas_listing_collated_writer_fence BEFORE INSERT OR UPDATE ON recording_collation_outputs
 FOR EACH ROW EXECUTE FUNCTION fence_nas_listing_evidence();
