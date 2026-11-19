-- A Durham attempt must use the native profile. Previously staged amd64
-- results are retained as evidence and cannot release source holds.
ALTER TABLE nas_stitch_r2_hours
 ADD COLUMN lease_toolchain_sha256 text CHECK(lease_toolchain_sha256 ~ '^[a-f0-9]{64}$'),
 ADD COLUMN lease_toolchain_attempt integer CHECK(lease_toolchain_attempt>0),
 ADD CHECK((lease_toolchain_sha256 IS NULL)=(lease_toolchain_attempt IS NULL));

CREATE FUNCTION nas_stitch_r2_native_profile() RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
  SELECT '{
   "architecture": "linux-arm64",
   "policy": "v85r",
   "october_ffmpeg_recovered": false,
   "manifest_sha256": "7d6e12320e3a1ac48a2a390d4d9842b7a9d68116d235e49dae5ad9c82288ce51",
   "evaluator_sha256": "0c90ed4c9527e0f9b6eedc432b71dd581a4dac0f758850988df80d2e684b2fed",
   "ffmpeg_version": "7.1.5-0+deb13u1",
   "ffprobe_version": "7.1.5-0+deb13u1",
   "ffmpeg_sha256": "74f28ebfa9c5758614c9457e680bb13610ba6d11f22858c4e3259751b383294d",
   "ffprobe_sha256": "501fbdc51c0b8fa4ecc90d0562c678262eef62b0141806940d186f0b71f9f1c4"
  }'::jsonb;
$$;

CREATE FUNCTION nas_stitch_r2_native_attempt(h nas_stitch_r2_hours) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
 SELECT coalesce(h.lease_toolchain_attempt=h.attempt AND h.lease_toolchain_sha256=
  nas_stitch_r2_native_profile()->>'manifest_sha256',false);
$$;

CREATE FUNCTION guard_nas_stitch_r2_native_lease() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT nas_stitch_r2_native_attempt(NEW) THEN
  RAISE EXCEPTION 'reviewed Durham lease toolchain required' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nas_stitch_r2_native_lease_guard BEFORE UPDATE OF attempt,lease_token ON nas_stitch_r2_hours
 FOR EACH ROW WHEN (NEW.state='leased' AND
  ROW(NEW.attempt,NEW.lease_token) IS DISTINCT FROM ROW(OLD.attempt,OLD.lease_token))
 EXECUTE FUNCTION guard_nas_stitch_r2_native_lease();

CREATE FUNCTION guard_nas_stitch_r2_native_stage() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM 1 FROM nas_stitch_r2_hours h WHERE h.id=NEW.hour_id AND h.attempt=NEW.attempt
  AND nas_stitch_r2_native_attempt(h) FOR SHARE;
 IF NOT FOUND THEN
  RAISE EXCEPTION 'reviewed Durham staging toolchain required' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
-- Sort after the existing admission trigger, which acquires the writer fence.
CREATE TRIGGER nas_stitch_r2_stage_native_guard BEFORE INSERT ON nas_stitch_r2_stage_objects
 FOR EACH ROW EXECUTE FUNCTION guard_nas_stitch_r2_native_stage();

CREATE FUNCTION nas_stitch_r2_native_proof(h nas_stitch_r2_hours) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
 SELECT nas_stitch_r2_native_attempt(h) AND (
  SELECT count(*)>0 AND bool_and(output->'provenance' IS NOT DISTINCT FROM nas_stitch_r2_native_profile())
  FROM jsonb_array_elements(CASE WHEN jsonb_typeof(h.staged_manifest->'outputs')='array'
   THEN h.staged_manifest->'outputs' ELSE '[]'::jsonb END) output
 );
$$;

ALTER FUNCTION nas_stitch_r2_certificate_receipt(bigint, integer, text, text)
 RENAME TO nas_stitch_r2_copy_custody_receipt;

CREATE FUNCTION nas_stitch_r2_certificate_receipt(hour_id bigint, attempt_number integer, plan_digest text, manifest_digest text)
RETURNS jsonb LANGUAGE sql STABLE AS $$
 SELECT nas_stitch_r2_copy_custody_receipt($1,$2,$3,$4)
 FROM nas_stitch_r2_hours h
 WHERE h.id=$1 AND h.attempt=$2 AND nas_stitch_r2_native_proof(h);
$$;
