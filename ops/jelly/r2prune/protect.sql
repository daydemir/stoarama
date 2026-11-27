-- DB protect sets for r2prune plan/delete (--db-protect). Read-only:
-- psql -XqAt -v ON_ERROR_STOP=1 -f protect.sql > protect-db.tsv
-- page_link dropped (Deniz Nov 24: "just drop the recordings its fine"; 0244).
COPY (SELECT 'stitch_input_hold',object_key FROM nas_stitch_r2_input_holds WHERE bucket='stoarama' AND released_at IS NULL) TO STDOUT;
COPY (SELECT 'nas_copy_pending',item->>'object_key' FROM nas_r2_copy_items WHERE item->>'bucket'='stoarama' AND (state<>'verified' OR blocked_reason<>'')) TO STDOUT;
COPY (SELECT 'nas_restore_pending',k FROM nas_restore_requests r CROSS JOIN LATERAL (VALUES (r.object_key),(r.source_object_key)) v(k) WHERE r.state IN ('pending','leased') AND coalesce(k,'')<>'') TO STDOUT;
COPY (SELECT 'nas_restore_pending',c.object_key FROM nas_restore_requests r JOIN recording_clips c ON c.id=r.clip_id WHERE r.state IN ('pending','leased') AND c.bucket='stoarama') TO STDOUT;
COPY (SELECT 'nas_incident',object_key FROM nas_cleanup_incident_roster WHERE bucket='stoarama') TO STDOUT;
COPY (SELECT 'nas_incident',c.object_key FROM nas_cleanup_incident_sources i JOIN recording_clips c ON c.id=i.clip_id WHERE c.bucket='stoarama') TO STDOUT;
COPY (SELECT 'nas_incident',c.object_key FROM nas_cleanup_incident_files i JOIN recording_clips c ON i.kind='raw' AND c.id=i.source_id WHERE c.bucket='stoarama') TO STDOUT;
COPY (SELECT 'nas_incident',o.object_key FROM nas_cleanup_incident_files i JOIN recording_collation_outputs o ON i.kind='collated' AND o.id=i.source_id) TO STDOUT;
COPY (WITH held AS (SELECT kind,source_id FROM nas_cleanup_reserved_files
 UNION SELECT 'raw',h.clip_id FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
 WHERE p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND (i.delete_ever_issued OR i.state IN ('delete_issued','unverified','media_deleted_sidecar_failed'))))
 SELECT 'nas_cleanup_hold',c.object_key FROM held h JOIN recording_clips c ON h.kind='raw' AND c.id=h.source_id WHERE c.bucket='stoarama'
 UNION SELECT 'nas_cleanup_hold',o.object_key FROM held h JOIN recording_collation_outputs o ON h.kind='collated' AND o.id=h.source_id) TO STDOUT;
COPY (SELECT 'owner_keep_prefix',prefix FROM r2_owner_keep_prefixes WHERE bucket='stoarama') TO STDOUT;
COPY (SELECT 'page_exempt',x.object_key FROM r2_delivery_exemptions x WHERE x.bucket='stoarama' AND r2_delivery_exempt(x.bucket,x.object_key)) TO STDOUT;
