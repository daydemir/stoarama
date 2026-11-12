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
	sql := strings.Replace(r2OwnerPolicySQL, "unsafe_outputs AS (", `source_coverage AS MATERIALIZED (
        SELECT o.id,bool_and(s.id IS NOT NULL) safe FROM recording_collation_outputs o
        CROSS JOIN LATERAL unnest(o.source_clip_ids) source
        LEFT JOIN safe_raw s ON s.id=source GROUP BY o.id),
        unsafe_outputs AS (`, 1)
	sql = strings.Replace(sql, "LEFT JOIN policy p ON p.stream=r.stream_id\n WHERE NOT", "LEFT JOIN policy p ON p.stream=r.stream_id LEFT JOIN source_coverage coverage ON coverage.id=o.id\n WHERE NOT", 1)
	return strings.Replace(sql, `NOT EXISTS(SELECT 1 FROM unnest(o.source_clip_ids) source WHERE NOT EXISTS(SELECT 1 FROM safe_raw s WHERE s.id=source))`, "coalesce(coverage.safe,false)", 1)
}

func (s *r2RetireSets) loadOwnerProtections(ctx context.Context, q r2RetireQuerier, scope r2RetireScope, windows map[int64]api.Keep28Window, picks map[int64]r2RetirePick) error {
	s.PresentRaw = map[int64]bool{}
	s.PresentPaths = map[string]bool{}
	s.PresentContent = map[r2RetireNASIdentity]bool{}
	presence, err := q.Query(ctx, `SELECT clip_id,relative_path,coalesce(sha256,''),coalesce(size_bytes,0) FROM nas_inventory_files WHERE state<>'missing'`)
	if err != nil {
		return err
	}
	for presence.Next() {
		var id int64
		var path, sha string
		var size int64
		if err := presence.Scan(&id, &path, &sha, &size); err != nil {
			presence.Close()
			return err
		}
		s.PresentRaw[id] = true
		s.PresentPaths[path] = true
		if size > 0 && sha != "" {
			s.PresentContent[r2RetireNASIdentity{"", sha, size}] = true
		}
	}
	presence.Close()
	if presence.Err() != nil {
		return presence.Err()
	}
	presence, err = q.Query(ctx, `SELECT relative_path FROM nas_inventory_unmatched_files WHERE state<>'missing'`)
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
	s.ProtectedRaw = map[int64]bool{}
	s.ProtectedCollated = map[int64]bool{}
	s.SharedRawKeys = map[string]bool{}
	rows, err := q.Query(ctx, r2RetireOwnerPlanSQL()+`SELECT id,source_clip_ids FROM unsafe_outputs`, r2RetireOwnerPolicy(windows, picks), scope.AccountID)
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
	rows, err = q.Query(ctx, `SELECT object_key FROM recording_clips WHERE bucket=$1 GROUP BY object_key HAVING count(*)>1`, scope.Bucket)
	if err != nil {
		return err
	}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		s.SharedRawKeys[key] = true
	}
	rows.Close()
	return rows.Err()
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
	if err := q.QueryRow(ctx, query, scope.OwnerPolicy, scope.AccountID, c.Kind, c.SourceID, c.Bucket, c.ObjectKey, r2RetireIncidentSources).Scan(&protected, &shared, &incident); err != nil {
		return err
	}
	c.Facts.JoinedRef = c.Facts.JoinedRef || protected
	c.Facts.SharedKey = c.Facts.SharedKey || shared
	c.Facts.Held = c.Facts.Held || incident
	if scope.OwnerSchema {
		var held bool
		if err := q.QueryRow(ctx, `SELECT CASE WHEN $1='raw' THEN nas_cleanup_owner_clip_held($2) ELSE EXISTS(SELECT 1 FROM nas_cleanup_owner_reserved_files WHERE kind=$1 AND source_id=$2) END`, c.Kind, c.SourceID).Scan(&held); err != nil {
			return err
		}
		c.Facts.Held = c.Facts.Held || held
	}
	return nil
}
