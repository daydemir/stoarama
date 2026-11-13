package main

import (
	"context"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
)

// Release and cursor advancement both mean the NAS may hold raw media, even
// without a hashing inventory row. All stitched/joined output ACKs live in the
// collation ACK table, independently of their object-key prefix.
const r2OwnerRawDeliveryTimeSQL = `greatest(c.released_at,c.created_at,
 CASE WHEN c.id<=n.last_cursor_id THEN coalesce(n.nas_listing_cursor_at,'9999-12-31'::timestamptz) END)`

func (e *r2RetireEvidence) loadOwnerDeliveries(ctx context.Context, q r2RetireQuerier) error {
	e.Delivered = map[string]time.Time{}
	var after int64
	for {
		rows, err := q.Query(ctx, `SELECT c.id,CASE WHEN r.account_id=47 AND (c.released_at IS NOT NULL OR c.id<=n.last_cursor_id) THEN `+r2OwnerRawDeliveryTimeSQL+` END
 FROM (SELECT id,recording_id,released_at,created_at FROM recording_clips WHERE id>$1 ORDER BY id LIMIT 5000) c
 LEFT JOIN recordings r ON r.id=c.recording_id LEFT JOIN connections n ON n.id=13 AND n.account_id=r.account_id ORDER BY c.id`, after)
		if err != nil {
			return err
		}
		count := 0
		for rows.Next() {
			var at *time.Time
			if err := rows.Scan(&after, &at); err != nil {
				rows.Close()
				return err
			}
			if at != nil {
				e.Delivered[retireIdentity("raw", after)] = *at
			}
			count++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if count < r2RetireEvidencePageSize {
			break
		}
	}
	rows, err := q.Query(ctx, `SELECT output_id,max(verified_at) FROM recording_collation_output_acks WHERE connection_id=13 GROUP BY output_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var at time.Time
		if err := rows.Scan(&id, &at); err != nil {
			return err
		}
		e.Delivered[retireIdentity("collated", id)] = at
	}
	return rows.Err()

}

func (e *r2RetireEvidence) applyOwner(c *r2RetireCandidate) {
	identity := retireIdentity(c.Kind, c.SourceID)
	c.Facts.ListingPresence = e.OwnerPaths[c.RelativePath] ||
		(c.Kind == "raw" && e.OwnerPaths[e.RawPaths[c.SourceID]]) ||
		(c.Kind == "collated" && e.OwnerPaths["joined/"+c.RelativePath])
	cutoff := e.Receipts[identity]
	if e.FenceSettled != nil && e.Start.After(*e.FenceSettled) && e.foldersCover(c) &&
		((c.Kind == "raw" && e.FencedRaw[c.SourceID]) || (c.Kind == "collated" && e.FencedCollated[c.SourceID])) && e.Start.After(cutoff) {
		cutoff = e.Start
	}
	if at, delivered := e.Delivered[identity]; delivered {
		// Never clear inventory presence. Only delivery evidence may be
		// superseded, and an ordinary pre-settlement listing cannot do that.
		c.Facts.NASCopy = c.Facts.NASCopy || c.Facts.ListingPresence || !api.NASListingPathValid(c.RelativePath) || cutoff.IsZero() || !at.Before(cutoff)
	}
}

func r2RetireOwnerLiveEvidence(ctx context.Context, q r2RetireQuerier, c *r2RetireCandidate, e *r2RetireEvidence) error {
	if e == nil {
		return nil
	}
	paths := []string{c.RelativePath}
	if c.Kind == "collated" {
		paths = append(paths, "joined/"+c.RelativePath)
	}
	if e.ListingSchema {
		// Every generation counts, including partial and older completed walks.
		if err := q.QueryRow(ctx, `WITH paths AS (SELECT unnest($1::text[]) path UNION ALL SELECT relative_path FROM nas_inventory_files WHERE $2='raw' AND connection_id=13 AND clip_id=$3)
 SELECT EXISTS(SELECT 1 FROM paths p CROSS JOIN LATERAL (SELECT 1 FROM nas_listing_files f WHERE f.connection_id=13 AND f.relative_path=p.path LIMIT 1) found)`, paths, c.Kind, c.SourceID).Scan(&c.Facts.ListingPresence); err != nil {
			return err
		}
	}
	var delivered *time.Time
	if c.Kind == "raw" {
		if err := q.QueryRow(ctx, `SELECT CASE WHEN c.released_at IS NOT NULL OR c.id<=n.last_cursor_id THEN `+r2OwnerRawDeliveryTimeSQL+` END
 FROM recording_clips c JOIN connections n ON n.id=13 WHERE c.id=$1`, c.SourceID).Scan(&delivered); err != nil {
			return err
		}
	} else {
		if err := q.QueryRow(ctx, `SELECT max(verified_at) FROM recording_collation_output_acks WHERE connection_id=13 AND output_id=$1`, c.SourceID).Scan(&delivered); err != nil {
			return err
		}
	}
	if delivered == nil {
		return nil
	}
	var cutoff *time.Time
	if e.ReleaseSchema {
		if err := q.QueryRow(ctx, `SELECT max(coalesce(i.permit_expires_at-interval '30 seconds',p.created_at)) FROM `+e.ReceiptView+` x
 JOIN nas_cleanup_items i ON i.id=x.item_id JOIN nas_cleanup_plans p ON p.id=i.plan_id
 WHERE p.connection_id=13 AND p.account_id=47 AND coalesce(i.item,p.manifest->(i.ordinal-1))->>'kind'=$1
 AND (coalesce(i.item,p.manifest->(i.ordinal-1))->>'source_id')::bigint=$2`, c.Kind, c.SourceID).Scan(&cutoff); err != nil {
			return err
		}
	}
	if e.ListingSchema && e.FenceSchema {
		var fenced bool
		if err := q.QueryRow(ctx, r2FenceLiveAbsenceSQL, c.Kind, c.SourceID).Scan(&fenced); err != nil {
			return err
		}
		if fenced {
			var started time.Time
			if err := q.QueryRow(ctx, `SELECT max(started_at) FROM nas_listing_generations WHERE connection_id=13 AND completed_at IS NOT NULL AND started_at>clock_timestamp()-interval '24 hours'`).Scan(&started); err != nil {
				return err
			}
			if cutoff == nil || started.After(*cutoff) {
				cutoff = &started
			}
		}
	}
	c.Facts.NASCopy = c.Facts.NASCopy || c.Facts.ListingPresence || !api.NASListingPathValid(c.RelativePath) || cutoff == nil || !delivered.Before(*cutoff)
	return nil
}
