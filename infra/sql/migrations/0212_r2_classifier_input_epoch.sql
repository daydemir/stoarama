-- The R2 inverse classifier reads a fixed set of catalog inputs. The KE epoch is
-- how a cached batch approval notices that identity changed, so EVERY classifier
-- input must bump it. Both P1 findings came from that contract being implicit:
-- the output's own interval and the collated->RAW source closure were inputs the
-- epoch did not cover.
--
-- This migration states the classifier inputs ONCE, as the column lists below,
-- and makes the epoch triggers cover exactly them. The Go test
-- TestR2InverseEpochCoversClassifierInputs enumerates the same struct against
-- pg_trigger, so a future classifier input that is added without widening the
-- epoch fails CI instead of silently escaping the freeze.
--
-- No authority, hold, lifecycle or user goal state is changed here. This only
-- widens which catalog edits bump the existing epoch row.

-- Exactly the columns inverseCatalogSQL / inverseNonrawColumns read.
CREATE OR REPLACE FUNCTION advance_r2_inverse_ke_epoch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'R2 catalog admission requires READ COMMITTED' USING ERRCODE='55000'; END IF;
 PERFORM 1 FROM r2_inverse_ke_epoch FOR UPDATE NOWAIT;
 UPDATE r2_inverse_ke_epoch SET epoch=epoch+1;
 RETURN NULL;
END $$;

-- RAW and collated-source identity: interval, content and path version the
-- approval. purged_at is deliberately NOT here: the pre-delete purge probe sets
-- it inside the wire transaction that already holds this epoch row FOR SHARE, so
-- bumping it there would self-deadlock. Purging is instead caught by the locked
-- per-key reload, whose identity queries already require purged_at IS NULL, so a
-- purge HOLDs the key rather than versioning the whole approval.
DROP TRIGGER r2_inverse_ke_raw_epoch ON recording_clips;
CREATE TRIGGER r2_inverse_ke_raw_epoch BEFORE INSERT
 OR UPDATE OF bucket,recording_id,object_key,display_path,etag,size_bytes,sha256,clip_start_at,clip_end_at
 OR DELETE ON recording_clips
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();

-- Collated outputs contribute their own interval and verification timestamp in
-- addition to key, path, content, size and source set.
DROP TRIGGER r2_inverse_ke_output_epoch ON recording_collation_outputs;
CREATE TRIGGER r2_inverse_ke_output_epoch BEFORE INSERT
 OR UPDATE OF object_key,nas_relative_path,collation_hour_id,source_clip_ids,sha256,size_bytes,start_at,end_at,r2_verified_at
 OR DELETE ON recording_collation_outputs
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();

-- Hours, recordings and streams supply stream, timezone and account identity.
DROP TRIGGER r2_inverse_ke_hour_epoch ON recording_collation_hours;
CREATE TRIGGER r2_inverse_ke_hour_epoch BEFORE UPDATE OF recording_id ON recording_collation_hours
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();
DROP TRIGGER r2_inverse_ke_recording_epoch ON recordings;
CREATE TRIGGER r2_inverse_ke_recording_epoch BEFORE UPDATE OF stream_id,cron_timezone,account_id ON recordings
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();
-- streams had no epoch trigger at all, so a local_timezone edit could change the
-- pinned-window match without versioning the approval.
CREATE TRIGGER r2_inverse_ke_stream_epoch BEFORE UPDATE OF local_timezone OR DELETE ON streams
 FOR EACH STATEMENT EXECUTE FUNCTION advance_r2_inverse_ke_epoch();
