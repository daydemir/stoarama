package main

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
)

// This class waives absence proof, never presence or recovery protection.
// Policy rows are generated from the pinned policy and exact saved picks.
func r2RetireOwnerPolicy(windows map[int64]api.Keep28Window, picks map[int64]r2RetirePick) []byte {
	type row struct {
		Stream int64     `json:"stream"`
		Zone   string    `json:"zone"`
		Start  time.Time `json:"start"`
		End    time.Time `json:"end"`
	}
	rows := []row{}
	for id, w := range windows {
		p, ok := picks[id]
		choice := r2RetireEffectiveChoice(p, ok, w)
		if choice != "" && choice != "all" {
			rows = append(rows, row{id, w.Timezone, w.Start, w.End})
		}
	}
	raw, _ := json.Marshal(rows)
	return raw
}

const r2OwnerPolicySQL = `WITH policy AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS p(stream bigint,zone text,start timestamptz,"end" timestamptz)),
 safe_raw AS (
 SELECT c.id FROM recording_clips c JOIN recordings r ON r.id=c.recording_id
 LEFT JOIN streams s ON s.id=r.stream_id JOIN policy p ON p.stream=r.stream_id
 WHERE r.account_id=$2 AND coalesce(nullif(s.local_timezone,''),r.cron_timezone,'')=p.zone
 AND c.clip_start_at>='2020-01-01' AND c.clip_end_at<'2028-01-01' AND c.clip_end_at>c.clip_start_at
 AND (c.clip_end_at<=p.start OR c.clip_start_at>=p."end")),
 unsafe_outputs AS (
 SELECT o.id,o.object_key,o.source_clip_ids FROM recording_collation_outputs o
 JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id
 LEFT JOIN streams s ON s.id=r.stream_id LEFT JOIN policy p ON p.stream=r.stream_id
 WHERE NOT coalesce(r.account_id=$2 AND coalesce(nullif(s.local_timezone,''),r.cron_timezone,'')=p.zone
 AND o.start_at>='2020-01-01' AND o.end_at<'2028-01-01' AND o.end_at>o.start_at
 AND (o.end_at<=p.start OR o.start_at>=p."end") AND cardinality(o.source_clip_ids)>0
 AND NOT EXISTS(SELECT 1 FROM unnest(o.source_clip_ids) source WHERE NOT EXISTS(SELECT 1 FROM safe_raw s WHERE s.id=source)),false)) `

// Bulk planning joins source membership once. The live query keeps indexed,
// per-object lookups instead of rebuilding coverage for the entire catalog.
func r2RetireOwnerPlanSQL() string {
	sql := strings.Replace(r2OwnerPolicySQL, "safe_raw AS (", `catalog_raw AS MATERIALIZED (
        SELECT id,recording_id,clip_start_at,clip_end_at FROM recording_clips), safe_raw AS (`, 1)
	sql = strings.Replace(sql, "FROM recording_clips c JOIN recordings r", "FROM catalog_raw c JOIN recordings r", 1)
	sql = strings.Replace(sql, "unsafe_outputs AS (", `source_coverage AS MATERIALIZED (
        SELECT o.id,bool_and(s.id IS NOT NULL) safe FROM recording_collation_outputs o
        CROSS JOIN LATERAL unnest(o.source_clip_ids) source
        LEFT JOIN safe_raw s ON s.id=source GROUP BY o.id),
        unsafe_outputs AS (`, 1)
	sql = strings.Replace(sql, "LEFT JOIN policy p ON p.stream=r.stream_id\n WHERE NOT", "LEFT JOIN policy p ON p.stream=r.stream_id LEFT JOIN source_coverage coverage ON coverage.id=o.id\n WHERE NOT", 1)
	return strings.Replace(sql, `NOT EXISTS(SELECT 1 FROM unnest(o.source_clip_ids) source WHERE NOT EXISTS(SELECT 1 FROM safe_raw s WHERE s.id=source))`, "coalesce(coverage.safe,false)", 1)
}

func (s *r2RetireSets) loadOwnerProtections(ctx context.Context, q r2RetireQuerier, scope r2RetireScope, windows map[int64]api.Keep28Window, picks map[int64]r2RetirePick) error {
	// Hold-release approval does not assert absence or select deletions.
	// Actual retirement planning always materializes broad presence.
	if !scope.OwnerPlanning {
		s.PresentRaw = map[int64]bool{}
		s.PresentPaths = map[string]bool{}
		s.PresentContent = map[r2RetireNASIdentity]bool{}
		var id int64
		var path, sha string
		var size int64
		err := r2RetireInventoryEach(ctx, q, `n.clip_id,n.relative_path,coalesce(n.sha256,''),coalesce(n.size_bytes,0)`, `n.state<>'missing'`, nil,
			[]any{&id, &path, &sha, &size}, func() error {
				s.PresentRaw[id] = true
				s.PresentPaths[path] = true
				if size > 0 && sha != "" {
					s.PresentContent[r2RetireNASIdentity{"", sha, size}] = true
				}
				return nil
			})
		if err != nil {
			return err
		}

		presence, err := q.Query(ctx, `SELECT relative_path FROM nas_inventory_unmatched_files WHERE state<>'missing'`)
		if err != nil {
			return err
		}
		for presence.Next() {
			var path string
			if err := presence.Scan(&path); err != nil {
				presence.Close()
				return err
			}
			s.PresentPaths[path] = true
		}
		presence.Close()
		if presence.Err() != nil {
			return presence.Err()
		}
	}
	s.ProtectedRaw = map[int64]bool{}
	s.ProtectedCollated = map[int64]bool{}
	sql, policy := r2RetireOwnerPlanSQL(), r2RetireOwnerPolicy(windows, picks)
	if scope.Class == r2RetireDeliveryOnly {
		sql = r2RetireDeliverySourceSQL(r2OwnerPolicySQL)
		// Keep source membership as indexed, correlated ID lookups.
		sql = strings.Replace(sql, "SELECT 1 FROM safe_raw s WHERE s.id=source", "SELECT 1 FROM LATERAL (SELECT id FROM safe_raw WHERE id=source OFFSET 0) s", 1)
		policy = r2RetireDeliveryPolicy(windows, picks)
	}
	rows, err := q.Query(ctx, sql+`SELECT id,source_clip_ids FROM unsafe_outputs`, policy, scope.AccountID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		var sources []int64
		if err := rows.Scan(&id, &sources); err != nil {
			rows.Close()
			return err
		}
		s.ProtectedCollated[id] = true
		for _, id := range sources {
			s.ProtectedRaw[id] = true
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return rows.Err()
	}
	// Incident holds survive every plan/receipt transition.
	for _, id := range r2RetireIncidentSources {
		s.HeldRaw[id] = true
	}
	rows, err = q.Query(ctx, `SELECT id FROM recording_collation_outputs WHERE source_clip_ids && $1::bigint[]`, r2RetireIncidentSources)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		s.HeldCollated[id] = true
	}
	rows.Close()
	if rows.Err() != nil {
		return rows.Err()
	}
	// recording_clips has UNIQUE(bucket,object_key). Cross-catalog aliases
	// are protected by OutputKeyCount/ArtifactKeys and rechecked live.
	return nil
}

// Reselect source windows/picks, references, key aliases and incidents live,
// under the existing admission/public fences. Never clear broad NASCopy.
func r2RetireOwnerLive(ctx context.Context, q r2RetireQuerier, c *r2RetireCandidate, scope r2RetireScope) error {
	var protected, shared, incident bool
	query := r2OwnerPolicySQL + `SELECT
 EXISTS(SELECT 1 FROM unsafe_outputs o WHERE ($3='raw' AND o.source_clip_ids @> ARRAY[$4::bigint]) OR ($3='collated' AND o.id=$4)),
 (SELECT count(*) FROM recording_clips WHERE bucket=$5 AND object_key=$6)>CASE WHEN $3='raw' THEN 1 ELSE 0 END
 OR (SELECT count(*) FROM recording_collation_outputs WHERE object_key=$6)>CASE WHEN $3='collated' THEN 1 ELSE 0 END,
 ($3='raw' AND $4=ANY($7::bigint[])) OR ($3='collated' AND EXISTS(SELECT 1 FROM recording_collation_outputs WHERE id=$4 AND source_clip_ids && $7::bigint[]))`
	if scope.Class == r2RetireDeliveryOnly {
		query = r2RetireDeliverySourceSQL(query)
		query = strings.Replace(query, "SELECT 1 FROM safe_raw s WHERE s.id=source", "SELECT 1 FROM LATERAL (SELECT id FROM safe_raw WHERE id=source OFFSET 0) s", 1)
	}
	if err := q.QueryRow(ctx, query, scope.OwnerPolicy, scope.AccountID, c.Kind, c.SourceID, c.Bucket, c.ObjectKey, r2RetireIncidentSources).Scan(&protected, &shared, &incident); err != nil {
		return err
	}
	c.Facts.JoinedRef = c.Facts.JoinedRef || protected
	c.Facts.SharedKey = c.Facts.SharedKey || shared
	c.Facts.Held = c.Facts.Held || incident
	if scope.OwnerSchema {
		var held bool
		heldFunction := "nas_cleanup_clip_held"
		if scope.Class == r2RetireOwnerAccepted {
			heldFunction = "nas_cleanup_owner_clip_held"
		}
		reservedView := "nas_cleanup_r2_reserved_files"
		if scope.Class == r2RetireOwnerAccepted {
			reservedView = "nas_cleanup_owner_reserved_files"
		}
		if err := q.QueryRow(ctx, `SELECT CASE WHEN $1='raw' THEN `+heldFunction+`($2) ELSE EXISTS(SELECT 1 FROM `+reservedView+` WHERE kind=$1 AND source_id=$2) END`, c.Kind, c.SourceID).Scan(&held); err != nil {
			return err
		}
		c.Facts.Held = c.Facts.Held || held
	}
	return nil
}
