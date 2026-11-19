-- guard_r2_retirement_reservation checked nas_cleanup_r2_reserved_files and
-- nas_cleanup_owner_reserved_files, which decode every plan manifest for each
-- inserted reservation. In production that cost about 0.5 s per reservation,
-- inside the R2 inverse executor's exclusive reserve lock. The guard now calls
-- index-narrowed functions with each view's exact rule. The views stay as they are.

-- Exactly nas_cleanup_r2_reserved_files's rule for one kind and source. The index narrows plans
-- before their manifests are decoded, as 0207's nas_cleanup_file_reserved does.
CREATE FUNCTION nas_cleanup_r2_file_reserved(reserved_kind text,reserved_source bigint)
RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_cleanup_plans p
 CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n)
 LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n
 WHERE nas_cleanup_manifest_keys(p.manifest) @> ARRAY[reserved_kind||':'||reserved_source::text]
   AND e.value->>'kind'=reserved_kind AND (e.value->>'source_id')::bigint=reserved_source
   AND (p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL) AND NOT EXISTS(SELECT 1 FROM nas_cleanup_r2_effective_releases r WHERE r.item_id=i.id))
$$;

-- Exactly nas_cleanup_owner_reserved_files's rule for one kind and source. The index narrows plans
-- before their manifests are decoded, as 0207's nas_cleanup_file_reserved does.
CREATE FUNCTION nas_cleanup_owner_file_reserved(reserved_kind text,reserved_source bigint)
RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM nas_cleanup_plans p
 CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n)
 LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n
 WHERE nas_cleanup_manifest_keys(p.manifest) @> ARRAY[reserved_kind||':'||reserved_source::text]
   AND e.value->>'kind'=reserved_kind AND (e.value->>'source_id')::bigint=reserved_source
   AND (p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL OR i.state IN ('delete_issued','unverified','media_deleted_sidecar_failed')) AND NOT EXISTS(SELECT 1 FROM nas_cleanup_owner_effective_releases r WHERE r.item_id=i.id))
$$;

CREATE OR REPLACE FUNCTION guard_r2_retirement_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM nas_cleanup_incident_roster i WHERE i.bucket=NEW.bucket AND i.object_key=NEW.object_key)
 OR EXISTS(SELECT 1 FROM nas_cleanup_incident_files i WHERE i.kind=NEW.kind AND i.source_id=NEW.source_id)
 OR (NEW.kind='raw' AND EXISTS(SELECT 1 FROM nas_cleanup_incident_sources i WHERE i.clip_id=NEW.source_id))
 OR (NEW.kind='collated' AND EXISTS(SELECT 1 FROM recording_collation_outputs o JOIN nas_cleanup_incident_sources i ON i.clip_id=ANY(o.source_clip_ids) WHERE o.id=NEW.source_id)) THEN
  RAISE EXCEPTION 'retention protected: permanent incident source or key';
 END IF;
 IF NEW.retirement_class='owner_accepted_outside_window' THEN
  IF nas_cleanup_owner_file_reserved(NEW.kind,NEW.source_id)
  OR (NEW.kind='raw' AND nas_cleanup_owner_clip_held(NEW.source_id)) THEN
   RAISE EXCEPTION 'retention protected: owner class recovery hold';
  END IF;
 ELSE
  IF nas_cleanup_r2_file_reserved(NEW.kind,NEW.source_id)
  OR (NEW.kind='raw' AND nas_cleanup_clip_held(NEW.source_id)) THEN
   RAISE EXCEPTION 'retention protected: NAS cleanup already reserves R2 recovery';
  END IF;
 END IF;
 RETURN NEW;
END $$;
