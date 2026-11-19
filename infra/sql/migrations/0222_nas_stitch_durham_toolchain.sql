-- A Durham attempt must use the native profile. Previously staged amd64
-- results are retained as evidence and cannot release source holds.
ALTER TABLE nas_stitch_r2_hours
 ADD COLUMN lease_toolchain_sha256 text CHECK(lease_toolchain_sha256 ~ '^[a-f0-9]{64}$'),
 ADD COLUMN lease_toolchain_attempt integer CHECK(lease_toolchain_attempt>0),
 ADD CHECK((lease_toolchain_sha256 IS NULL)=(lease_toolchain_attempt IS NULL));

ALTER FUNCTION nas_stitch_r2_certificate_receipt(bigint, integer, text, text)
 RENAME TO nas_stitch_r2_copy_custody_receipt;

CREATE FUNCTION nas_stitch_r2_certificate_receipt(hour_id bigint, attempt_number integer, plan_digest text, manifest_digest text)
RETURNS jsonb LANGUAGE sql STABLE AS $$
 WITH profile AS (
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
  }'::jsonb AS provenance
 )
 SELECT nas_stitch_r2_copy_custody_receipt($1,$2,$3,$4)
 FROM nas_stitch_r2_hours h CROSS JOIN profile p
 WHERE h.id=$1 AND h.attempt=$2 AND h.lease_toolchain_attempt=h.attempt
  AND h.lease_toolchain_sha256=p.provenance->>'manifest_sha256'
  AND NOT EXISTS (
   SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(h.staged_manifest->'outputs')='array'
    THEN h.staged_manifest->'outputs' ELSE '[]'::jsonb END) output
   WHERE output->'provenance' IS DISTINCT FROM p.provenance
  );
$$;
