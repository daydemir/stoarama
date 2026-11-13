package main

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (e *r2RetireEvidence) foldersCover(c *r2RetireCandidate) bool {
	paths := []string{c.RelativePath}
	if c.Kind == "raw" && e.RawPaths[c.SourceID] != "" {
		paths = append(paths, e.RawPaths[c.SourceID])
	}
	if c.Kind == "collated" {
		paths = append(paths, "joined/"+c.RelativePath)
	}
	for _, path := range paths {
		folder := "."
		if strings.Contains(path, "/") {
			folder = strings.SplitN(path, "/", 2)[0]
		}
		found := false
		for _, f := range e.Folders {
			if f == folder {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func loadR2FenceEvidence(ctx context.Context, q r2RetireQuerier, e *r2RetireEvidence, bounded bool) error {
	if !e.FenceSchema {
		return nil
	}
	err := q.QueryRow(ctx, `SELECT s.settled_at FROM nas_delivery_retirement_settlements s JOIN nas_delivery_retirement_fences f USING(connection_id) WHERE s.connection_id=13 AND f.policy_sha256='060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811'`).Scan(&e.FenceSettled)
	if err == pgx.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if e.FenceSettled == nil || !e.Start.After(*e.FenceSettled) {
		return nil
	}
	e.FencedRaw = map[int64]bool{}
	e.FencedCollated = map[int64]bool{}
	if bounded {
		for _, kind := range []string{"raw", "collated"} {
			var after int64
			for {
				first, last, found, err := r2RetireIDRange(ctx, q, kind, after, r2RetireEvidencePageSize)
				if err != nil {
					return err
				}
				if !found {
					break
				}
				table := "recording_clips"
				if kind == "collated" {
					table = "recording_collation_outputs"
				}
				rows, err := q.Query(ctx, `SELECT id,nas_delivery_fence_object_covered($1,id) FROM `+table+` WHERE id BETWEEN $2 AND $3 ORDER BY id`, kind, first, last)
				if err != nil {
					return err
				}
				for rows.Next() {
					var covered bool
					if err := rows.Scan(&after, &covered); err != nil {
						rows.Close()
						return err
					}
					if covered {
						if kind == "raw" {
							e.FencedRaw[after] = true
						} else {
							e.FencedCollated[after] = true
						}
					}
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					return err
				}
				after = last
			}
		}
		return nil
	}
	rows, err := q.Query(ctx, `SELECT kind,source_id FROM nas_delivery_fence_coverage() WHERE covered`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		var id int64
		if err := rows.Scan(&kind, &id); err != nil {
			return err
		}
		if kind == "raw" {
			e.FencedRaw[id] = true
		} else {
			e.FencedCollated[id] = true
		}
	}
	return rows.Err()
}

const r2FenceLiveAbsenceSQL = `SELECT EXISTS(
 SELECT 1 FROM nas_delivery_retirement_settlements s JOIN nas_delivery_retirement_fences f USING(connection_id)
 JOIN LATERAL (SELECT started_at,folders FROM nas_listing_generations WHERE connection_id=13 AND completed_at IS NOT NULL AND started_at>clock_timestamp()-interval '24 hours' ORDER BY started_at DESC LIMIT 1) g ON g.started_at>s.settled_at
 WHERE s.connection_id=13 AND f.policy_sha256='060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811'
 AND nas_delivery_fence_object_covered($1,$2)
 AND NOT EXISTS(SELECT 1 FROM nas_delivery_object_paths($1,$2) p WHERE NOT nas_delivery_folder_covered(p,g.folders))
 )`
