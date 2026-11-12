package main

import (
	"context"
	"fmt"
	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/jackc/pgx/v5"
	"time"
)

type r2RetireEvidence struct {
	RawPaths                     map[int64]string
	MismatchRaw                  map[int64]bool
	MismatchPaths                map[r2RetireNASIdentity]bool
	ObservedRaw                  map[int64]time.Time
	ObservedPaths                map[string]time.Time
	ForeignRaw                   map[int64]bool
	ListingSchema, ReleaseSchema bool
	FenceSchema                  bool
	FencedRaw, FencedCollated    map[int64]bool
	Folders                      []string
	FenceSettled                 *time.Time
	Start                        time.Time
	Paths                        map[string]bool
	Receipts                     map[string]time.Time
}

func retireIdentity(kind string, id int64) string { return fmt.Sprintf("%s:%d", kind, id) }
func loadR2RetireEvidence(ctx context.Context, q r2RetireQuerier) (*r2RetireEvidence, error) {
	e := &r2RetireEvidence{RawPaths: map[int64]string{}, MismatchRaw: map[int64]bool{}, MismatchPaths: map[r2RetireNASIdentity]bool{}, ObservedRaw: map[int64]time.Time{}, ObservedPaths: map[string]time.Time{}, ForeignRaw: map[int64]bool{}, Paths: map[string]bool{}, Receipts: map[string]time.Time{}}
	if err := q.QueryRow(ctx, `SELECT to_regclass('nas_listing_generations') IS NOT NULL,to_regclass('nas_cleanup_r2_effective_releases') IS NOT NULL,to_regclass('nas_delivery_retirement_fences') IS NOT NULL`).Scan(&e.ListingSchema, &e.ReleaseSchema, &e.FenceSchema); err != nil {
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
	rows, err := q.Query(ctx, `SELECT clip_id,relative_path,coalesce(server_received_at,'9999-12-31'::timestamptz),connection_id,state FROM nas_inventory_files`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, cid int64
		var p string
		var at time.Time
		var state string
		if err := rows.Scan(&id, &p, &at, &cid, &state); err != nil {
			rows.Close()
			return nil, err
		}
		if cid == 13 {
			e.RawPaths[id] = p
		}
		if state == "missing" {
			continue
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
	err = q.QueryRow(ctx, `SELECT started_at,folders FROM nas_listing_generations WHERE connection_id=13 AND completed_at IS NOT NULL AND started_at>clock_timestamp()-interval '24 hours' ORDER BY started_at DESC LIMIT 1`).Scan(&e.Start, &e.Folders)
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

	rows, err = q.Query(ctx, `SELECT n.clip_id,n.relative_path,n.sha256,n.size_bytes FROM nas_inventory_files n WHERE n.connection_id=13 AND n.state='present' AND n.verified_at>now()-interval '24 hours' AND n.server_received_at>now()-interval '24 hours' AND EXISTS(SELECT 1 FROM nas_listing_files f JOIN nas_listing_generations g USING(connection_id,generation) WHERE f.connection_id=13 AND f.relative_path=n.relative_path AND g.started_at>=$1 AND ROW(f.size_bytes,f.mtime_ns,f.ctime_ns,f.inode,f.device) IS DISTINCT FROM ROW(n.size_bytes,n.file_mtime_ns,n.file_ctime_ns,n.file_inode,n.file_device))`, e.Start)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int64
		var identity r2RetireNASIdentity
		if err := rows.Scan(&id, &identity.Path, &identity.SHA256, &identity.SizeBytes); err != nil {
			rows.Close()
			return nil, err
		}
		e.MismatchRaw[id] = true
		e.MismatchPaths[identity] = true
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	if e.Start.IsZero() {
		return e, nil
	}

	if err := loadR2FenceEvidence(ctx, q, e); err != nil {
		return nil, err
	}
	return e, nil
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
	presence := e.Paths[c.RelativePath] || (c.Kind == "raw" && e.Paths[e.RawPaths[c.SourceID]]) || (c.Kind == "collated" && e.Paths["joined/"+c.RelativePath])
	if c.Kind == "raw" && e.MismatchRaw[c.SourceID] || c.Kind == "collated" && (e.MismatchPaths[r2RetireNASIdentity{c.RelativePath, c.SHA256, c.SizeBytes}] || e.MismatchPaths[r2RetireNASIdentity{"joined/" + c.RelativePath, c.SHA256, c.SizeBytes}]) {
		c.Facts.ExactNAS = false
	}
	c.Facts.ListingPresence = presence
	fenced := e.FenceSettled != nil && e.Start.After(*e.FenceSettled) && e.foldersCover(c) && ((c.Kind == "raw" && e.FencedRaw[c.SourceID]) || (c.Kind == "collated" && e.FencedCollated[c.SourceID]))
	c.Facts.NASAbsent = !presence && (fenced || !e.Receipts[retireIdentity(c.Kind, c.SourceID)].IsZero())
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

// Apply rechecks settled fence coverage, generation freshness and later presence.
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
		paths := []string{c.RelativePath}
		if c.Kind == "collated" {
			paths = append(paths, "joined/"+c.RelativePath)
		}
		err := q.QueryRow(ctx, `WITH g AS (SELECT started_at FROM nas_listing_generations WHERE connection_id=13 AND completed_at IS NOT NULL AND started_at>clock_timestamp()-interval '24 hours' ORDER BY started_at DESC LIMIT 1) SELECT EXISTS(SELECT 1 FROM nas_listing_files f JOIN nas_listing_generations n USING(connection_id,generation) WHERE f.connection_id=13 AND (f.relative_path=ANY($2) OR ($3::text='raw' AND EXISTS(SELECT 1 FROM nas_inventory_files known WHERE known.connection_id=13 AND known.clip_id=$1 AND known.relative_path=f.relative_path))) AND n.started_at>=coalesce((SELECT started_at FROM g),'-infinity'))`, c.SourceID, paths, c.Kind).Scan(&present)
		if err != nil {
			return err
		}
	}
	if e.ListingSchema && present && c.Facts.ExactNAS {
		paths := []string{c.RelativePath}
		if c.Kind == "collated" {
			paths = append(paths, "joined/"+c.RelativePath)
		}
		var mismatch bool
		if err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_inventory_files n JOIN nas_listing_files f ON f.connection_id=13 AND f.relative_path=n.relative_path JOIN nas_listing_generations g ON g.connection_id=f.connection_id AND g.generation=f.generation WHERE n.connection_id=13 AND n.state='present' AND n.relative_path=ANY($3) AND n.sha256=$4 AND n.size_bytes=$5 AND ($1='collated' OR n.clip_id=$2) AND g.started_at>=coalesce((SELECT max(started_at) FROM nas_listing_generations WHERE connection_id=13 AND completed_at IS NOT NULL AND started_at>clock_timestamp()-interval '24 hours'),'-infinity') AND ROW(f.size_bytes,f.mtime_ns,f.ctime_ns,f.inode,f.device) IS DISTINCT FROM ROW(n.size_bytes,n.file_mtime_ns,n.file_ctime_ns,n.file_inode,n.file_device))`, c.Kind, c.SourceID, paths, c.SHA256, c.SizeBytes).Scan(&mismatch); err != nil {
			return err
		}
		if mismatch {
			c.Facts.ExactNAS = false
		}
	}
	if e.ListingSchema && e.FenceSchema {
		if err := q.QueryRow(ctx, r2FenceLiveAbsenceSQL, c.Kind, c.SourceID).Scan(&absent); err != nil {
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
			listingSQL = `(SELECT max(started_at) FROM nas_listing_generations WHERE connection_id=13 AND completed_at IS NOT NULL AND started_at>clock_timestamp()-interval '24 hours')`
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
