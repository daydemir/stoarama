package main

import (
	"context"
	"fmt"
	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/jackc/pgx/v5"
	"time"
)

const r2RetireListingMaxAge = 24 * time.Hour

type r2RetireEvidence struct {
	ObservedRaw                  map[int64]time.Time
	ObservedPaths                map[string]time.Time
	ForeignRaw                   map[int64]bool
	ListingSchema, ReleaseSchema bool
	Start                        time.Time
	Paths                        map[string]bool
	AbsentRaw, AbsentCollated    map[int64]bool
	Receipts                     map[string]time.Time
}

func retireIdentity(kind string, id int64) string { return fmt.Sprintf("%s:%d", kind, id) }
func loadR2RetireEvidence(ctx context.Context, q r2RetireQuerier) (*r2RetireEvidence, error) {
	e := &r2RetireEvidence{ObservedRaw: map[int64]time.Time{}, ObservedPaths: map[string]time.Time{}, ForeignRaw: map[int64]bool{}, Paths: map[string]bool{}, AbsentRaw: map[int64]bool{}, AbsentCollated: map[int64]bool{}, Receipts: map[string]time.Time{}}
	if err := q.QueryRow(ctx, `SELECT to_regclass('nas_listing_generations') IS NOT NULL,to_regclass('nas_cleanup_r2_effective_releases') IS NOT NULL`).Scan(&e.ListingSchema, &e.ReleaseSchema); err != nil {
		return nil, err
	}
	if e.ReleaseSchema {
		rows, err := q.Query(ctx, `SELECT coalesce(i.item,p.manifest->(i.ordinal-1))->>'kind',(coalesce(i.item,p.manifest->(i.ordinal-1))->>'source_id')::bigint,coalesce(i.permit_expires_at-interval '30 seconds',p.created_at) FROM nas_cleanup_r2_effective_releases x JOIN nas_cleanup_items i ON i.id=x.item_id JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE p.connection_id=13 AND p.account_id=47`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var kind string
			var id int64
			var at time.Time
			if err := rows.Scan(&kind, &id, &at); err != nil {
				rows.Close()
				return nil, err
			}
			e.Receipts[retireIdentity(kind, id)] = at
		}
		rows.Close()
		if rows.Err() != nil {
			return nil, rows.Err()
		}
	}
	if !e.ListingSchema && !e.ReleaseSchema {
		return e, nil
	}
	rows, err := q.Query(ctx, `SELECT clip_id,relative_path,coalesce(server_received_at,'9999-12-31'::timestamptz),connection_id FROM nas_inventory_files WHERE state<>'missing'`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, cid int64
		var p string
		var at time.Time
		if err := rows.Scan(&id, &p, &at, &cid); err != nil {
			rows.Close()
			return nil, err
		}
		if cid != 13 {
			e.ForeignRaw[id] = true
			continue
		}
		if at.After(e.ObservedRaw[id]) {
			e.ObservedRaw[id] = at
		}
		if at.After(e.ObservedPaths[p]) {
			e.ObservedPaths[p] = at
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	rows, err = q.Query(ctx, `SELECT relative_path,coalesce(server_received_at,'9999-12-31'::timestamptz) FROM nas_inventory_unmatched_files WHERE connection_id=13 AND state<>'missing'`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var p string
		var at time.Time
		if err := rows.Scan(&p, &at); err != nil {
			rows.Close()
			return nil, err
		}
		if at.After(e.ObservedPaths[p]) {
			e.ObservedPaths[p] = at
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	rows, err = q.Query(ctx, `SELECT o.nas_relative_path,a.verified_at FROM recording_collation_outputs o JOIN recording_collation_output_acks a ON a.output_id=o.id AND a.connection_id=13`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var p string
		var at time.Time
		if err := rows.Scan(&p, &at); err != nil {
			rows.Close()
			return nil, err
		}
		if at.After(e.ObservedPaths[p]) {
			e.ObservedPaths[p] = at
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	if !e.ListingSchema {
		return e, nil
	}
	err = q.QueryRow(ctx, `SELECT started_at FROM nas_listing_generations WHERE connection_id=13 AND completed_at IS NOT NULL AND started_at>now()-interval '24 hours' ORDER BY started_at DESC LIMIT 1`).Scan(&e.Start)
	if err == pgx.ErrNoRows {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	rows, err = q.Query(ctx, `SELECT DISTINCT f.relative_path FROM nas_listing_files f JOIN nas_listing_generations g USING(connection_id,generation) WHERE f.connection_id=13 AND g.started_at>=$1`, e.Start)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return nil, err
		}
		e.Paths[p] = true
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	if e.Start.IsZero() {
		return e, nil
	}

	rows, err = q.Query(ctx, `SELECT c.id,c.display_path,greatest(c.created_at,c.released_at,n.nas_listing_cursor_at),n.nas_listing_cursor_at IS NOT NULL AND (c.released_at IS NOT NULL OR c.id<=n.last_cursor_id) FROM recording_clips c JOIN recordings r ON r.id=c.recording_id JOIN connections n ON n.id=13 AND n.account_id=r.account_id WHERE r.account_id=47 AND c.purged_at IS NULL AND c.display_path<>''`)
	if err != nil {
		return nil, err
	}
	paths := map[int64]string{}
	latest := map[string]time.Time{}
	unsettled := map[string]bool{}
	for rows.Next() {
		var id int64
		var path string
		var at time.Time
		var known bool
		if err := rows.Scan(&id, &path, &at, &known); err != nil {
			rows.Close()
			return nil, err
		}
		paths[id] = path
		if !known {
			unsettled[path] = true
		}
		if at.After(latest[path]) {
			latest[path] = at
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	for id, path := range paths {
		e.AbsentRaw[id] = !unsettled[path] && latest[path].Before(e.Start)
	}

	rows, err = q.Query(ctx, `SELECT o.id FROM recording_collation_outputs o JOIN recording_collation_output_acks a ON a.output_id=o.id AND a.connection_id=13 WHERE greatest(o.created_at,a.verified_at)<$1 AND NOT EXISTS(SELECT 1 FROM recording_collation_outputs x LEFT JOIN recording_collation_output_acks b ON b.output_id=x.id AND b.connection_id=13 WHERE x.nas_relative_path=o.nas_relative_path AND (b.output_id IS NULL OR greatest(x.created_at,b.verified_at)>=$1))`, e.Start)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		e.AbsentCollated[id] = true
	}
	rows.Close()
	return e, rows.Err()
}
func (e *r2RetireEvidence) apply(c *r2RetireCandidate) {
	if e == nil {
		return
	}
	if (e.ListingSchema || e.ReleaseSchema) && !api.NASListingPathValid(c.RelativePath) {
		c.Facts.NASAbsent = false
		c.Facts.ExactNAS = false
		return
	}
	presence := e.Paths[c.RelativePath] || (c.Kind == "collated" && e.Paths["joined/"+c.RelativePath])
	c.Facts.ListingPresence = presence
	c.Facts.NASAbsent = !presence && ((e.AbsentRaw[c.SourceID] && c.Kind == "raw") || (e.AbsentCollated[c.SourceID] && c.Kind == "collated") || !e.Receipts[retireIdentity(c.Kind, c.SourceID)].IsZero())
	cutoff := e.Start
	receipt := e.Receipts[retireIdentity(c.Kind, c.SourceID)]
	if receipt.After(cutoff) {
		cutoff = receipt
	}
	newer := !e.ObservedPaths[c.RelativePath].Before(cutoff) || (c.Kind == "raw" && (e.ForeignRaw[c.SourceID] || !e.ObservedRaw[c.SourceID].Before(cutoff))) || (c.Kind == "collated" && !e.ObservedPaths["joined/"+c.RelativePath].Before(cutoff))
	if !cutoff.IsZero() && !presence && !newer {
		c.Facts.NASCopy = false
		c.Facts.ExactNAS = false
	}

}

// Apply rechecks generation/freshness, later partial presence and writer time.
func r2RetireLiveEvidence(ctx context.Context, q r2RetireQuerier, c *r2RetireCandidate, e *r2RetireEvidence) error {
	if e == nil {
		return nil
	}
	if (e.ListingSchema || e.ReleaseSchema) && !api.NASListingPathValid(c.RelativePath) {
		c.Facts.NASAbsent = false
		c.Facts.ExactNAS = false
		return nil
	}
	var absent, present bool
	if e.ListingSchema {
		watermark := `SELECT max(greatest(x.created_at,x.released_at,n.nas_listing_cursor_at)) at FROM recording_clips x JOIN recordings r ON r.id=x.recording_id JOIN connections n ON n.id=13 AND n.account_id=r.account_id WHERE x.display_path=(SELECT display_path FROM recording_clips WHERE id=$1) HAVING bool_and(n.nas_listing_cursor_at IS NOT NULL AND (x.released_at IS NOT NULL OR x.id<=n.last_cursor_id))`
		paths := []string{c.RelativePath}
		if c.Kind == "collated" {
			paths = append(paths, "joined/"+c.RelativePath)
			watermark = `SELECT max(greatest(x.created_at,a.verified_at)) at FROM recording_collation_outputs x LEFT JOIN recording_collation_output_acks a ON a.output_id=x.id AND a.connection_id=13 WHERE x.nas_relative_path=(SELECT nas_relative_path FROM recording_collation_outputs WHERE id=$1) HAVING bool_and(a.output_id IS NOT NULL)`
		}
		err := q.QueryRow(ctx, `WITH g AS (SELECT started_at FROM nas_listing_generations WHERE connection_id=13 AND completed_at IS NOT NULL AND started_at>now()-interval '24 hours' ORDER BY started_at DESC LIMIT 1),w AS (`+watermark+`) SELECT coalesce((SELECT g.started_at>w.at FROM g,w),false),EXISTS(SELECT 1 FROM nas_listing_files f JOIN nas_listing_generations n USING(connection_id,generation) WHERE f.connection_id=13 AND f.relative_path=ANY($2) AND n.started_at>=coalesce((SELECT started_at FROM g),'-infinity'))`, c.SourceID, paths).Scan(&absent, &present)
		if err != nil {
			return err
		}
	}
	c.Facts.ListingPresence = present
	c.Facts.NASAbsent = absent && !present
	if e.ReleaseSchema && !present {
		var receipt bool
		err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_r2_effective_releases x JOIN nas_cleanup_items i ON i.id=x.item_id JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE p.connection_id=13 AND p.account_id=47 AND coalesce(i.item,p.manifest->(i.ordinal-1))->>'kind'=$1 AND (coalesce(i.item,p.manifest->(i.ordinal-1))->>'source_id')::bigint=$2)`, c.Kind, c.SourceID).Scan(&receipt)
		if err != nil {
			return err
		}
		c.Facts.NASAbsent = c.Facts.NASAbsent || receipt
	}
	if e.ListingSchema || e.ReleaseSchema {
		var cutoff *time.Time
		receiptSQL := "NULL::timestamptz"
		if e.ReleaseSchema {
			receiptSQL = `(SELECT max(coalesce(i.permit_expires_at-interval '30 seconds',p.created_at)) FROM nas_cleanup_r2_effective_releases x JOIN nas_cleanup_items i ON i.id=x.item_id JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE p.connection_id=13 AND coalesce(i.item,p.manifest->(i.ordinal-1))->>'kind'=$1 AND (coalesce(i.item,p.manifest->(i.ordinal-1))->>'source_id')::bigint=$2)`
		}
		listingSQL := "NULL::timestamptz"
		if e.ListingSchema {
			listingSQL = `(SELECT max(started_at) FROM nas_listing_generations WHERE connection_id=13 AND completed_at IS NOT NULL AND started_at>now()-interval '24 hours')`
		}
		if err := q.QueryRow(ctx, `SELECT greatest(`+receiptSQL+`,`+listingSQL+`) WHERE $1::text<>'' AND $2::bigint>0`, c.Kind, c.SourceID).Scan(&cutoff); err != nil {
			return err
		}
		if cutoff != nil && !present {
			paths := []string{c.RelativePath}
			if c.Kind == "collated" {
				paths = append(paths, "joined/"+c.RelativePath)
			}
			var newer bool
			if err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.state<>'missing' AND (($1='raw' AND n.clip_id=$2 AND (n.connection_id<>13 OR (n.server_received_at IS NULL OR n.server_received_at>=$3))) OR (n.connection_id=13 AND n.relative_path=ANY($4) AND (n.server_received_at IS NULL OR n.server_received_at>=$3)))) OR EXISTS(SELECT 1 FROM nas_inventory_unmatched_files n WHERE n.connection_id=13 AND n.state<>'missing' AND n.relative_path=ANY($4) AND (n.server_received_at IS NULL OR n.server_received_at>=$3)) OR ($1='collated' AND EXISTS(SELECT 1 FROM recording_collation_output_acks a WHERE a.connection_id=13 AND a.output_id=$2 AND a.verified_at>=$3))`, c.Kind, c.SourceID, *cutoff, paths).Scan(&newer); err != nil {
				return err
			}
			if !newer {
				c.Facts.NASCopy = false
				c.Facts.ExactNAS = false
			}
		}
	}
	return nil
}
