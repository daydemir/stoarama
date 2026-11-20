-- The AAC stream-copy package is the only toolchain a fresh Durham attempt may
-- bind. 0222's profile stays the exact expected JSON for evidence that was
-- already stamped and staged under it, so that evidence keeps its custody path.
CREATE FUNCTION nas_stitch_r2_aac_profile() RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
  SELECT nas_stitch_r2_native_profile() || jsonb_build_object(
   'manifest_sha256','ab1103b38ab74dd426ded52448bb2f301965596387dff5146dc9e88748f5229a',
   'audio_proof','aac-lc-primed-pcm-v1');
$$;

-- Select the full expected provenance from the digest an attempt actually
-- stamped. Another attempt's stamp, an unknown digest or a missing stamp has no
-- expected profile at all, so it cannot pass as native proof.
CREATE FUNCTION nas_stitch_r2_stamped_profile(h nas_stitch_r2_hours) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
 SELECT CASE h.lease_toolchain_sha256
  WHEN nas_stitch_r2_aac_profile()->>'manifest_sha256' THEN nas_stitch_r2_aac_profile()
  WHEN nas_stitch_r2_native_profile()->>'manifest_sha256' THEN nas_stitch_r2_native_profile()
 END
 WHERE h.lease_toolchain_attempt=h.attempt;
$$;

-- Historical matching spans both reviewed stamps so completed copies can still
-- be renewed and certified. Only a new bind uses the active predicate below.
CREATE OR REPLACE FUNCTION nas_stitch_r2_native_attempt(h nas_stitch_r2_hours) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
 SELECT nas_stitch_r2_stamped_profile(h) IS NOT NULL;
$$;

CREATE FUNCTION nas_stitch_r2_aac_attempt(h nas_stitch_r2_hours) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
 SELECT coalesce(h.lease_toolchain_attempt=h.attempt AND h.lease_toolchain_sha256=
  nas_stitch_r2_aac_profile()->>'manifest_sha256',false);
$$;

-- A predecessor that replays its own old digest cannot take a fresh lease.
-- 0222 watched only attempt and lease_token on UPDATE, so two writes reached a
-- leased row without ever consulting the toolchain: a direct INSERT that
-- arrived already leased, and a state-only UPDATE whose prepopulated stamp was
-- never edited. Both are new binds, so the replacement trigger fires on INSERT
-- and on any write that leaves the row leased, and every such row must carry
-- the active AAC stamp for its own attempt.
CREATE OR REPLACE FUNCTION guard_nas_stitch_r2_native_lease() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT nas_stitch_r2_aac_attempt(NEW) THEN
  RAISE EXCEPTION 'reviewed Durham lease toolchain required' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER nas_stitch_r2_native_lease_guard ON nas_stitch_r2_hours;
CREATE TRIGGER nas_stitch_r2_native_lease_guard
 BEFORE INSERT OR UPDATE OF state,attempt,lease_token,lease_toolchain_sha256,lease_toolchain_attempt ON nas_stitch_r2_hours
 FOR EACH ROW WHEN (NEW.state='leased')
 EXECUTE FUNCTION guard_nas_stitch_r2_native_lease();

-- New staging authority requires the active bind; the late provider-response
-- exception for a write issued before the upgrade is preserved verbatim.
CREATE OR REPLACE FUNCTION guard_nas_stitch_r2_native_stage() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 -- Preserve a provider response for a write whose issuance committed before
 -- the upgrade. Recording its upload ID or object identity grants no new write.
 IF TG_OP='UPDATE' AND NEW.grants_drain_at IS NOT DISTINCT FROM OLD.grants_drain_at AND
  ((OLD.state='starting' AND OLD.start_issued_at IS NOT NULL AND NEW.state='uploading') OR
   (OLD.state='completing' AND NEW.state='staged')) THEN
  RETURN NEW;
 END IF;
 PERFORM 1 FROM nas_stitch_r2_hours h WHERE h.id=NEW.hour_id AND h.attempt=NEW.attempt
  AND nas_stitch_r2_aac_attempt(h) FOR SHARE;
 IF NOT FOUND THEN
  RAISE EXCEPTION 'reviewed Durham staging toolchain required' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;

-- Every output must carry the profile its own attempt stamped, so a mixed,
-- foreign or unknown manifest cannot be read as native proof.
CREATE OR REPLACE FUNCTION nas_stitch_r2_native_proof(h nas_stitch_r2_hours) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
 SELECT nas_stitch_r2_native_attempt(h) AND (
  SELECT count(*)>0 AND bool_and(output->'provenance' IS NOT DISTINCT FROM nas_stitch_r2_stamped_profile(h))
  FROM jsonb_array_elements(CASE WHEN jsonb_typeof(h.staged_manifest->'outputs')='array'
   THEN h.staged_manifest->'outputs' ELSE '[]'::jsonb END) output
  );
$$;

-- Copy eligibility, renewal and the exact custody certificate keep routing
-- through nas_stitch_r2_native_proof, so both reviewed digests resolve to their
-- own full provenance. Only new enrollment demands the active AAC bind.
CREATE OR REPLACE FUNCTION guard_nas_stitch_r2_native_copy_enrollment() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (
  SELECT 1 FROM nas_stitch_r2_stage_objects o JOIN nas_stitch_r2_hours h ON h.id=o.hour_id AND h.attempt=o.attempt
  WHERE o.id=NEW.source_id AND o.state='staged' AND nas_stitch_r2_aac_attempt(h)
 ) THEN
  RAISE EXCEPTION 'reviewed Durham copy source required' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
