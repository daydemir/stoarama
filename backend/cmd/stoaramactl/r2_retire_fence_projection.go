package main

import (
	"context"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
)

// Project the SAME path-writer veto as the deployed fence without requiring or
// manufacturing its schema in production. Every query is read-only.
func (p *r2RetireProjection) loadFenceWriters(ctx context.Context, q r2RetireQuerier) error {
	_, windows, err := api.Keep28Policy()
	if err != nil {
		return err
	}
	outside := func(stream int64, tz string, a, b *time.Time) bool {
		w, ok := windows[stream]
		return ok && tz == w.Timezone && a != nil && b != nil && r2RetireOutside(w, *a, *b)
	}
	p.unsafePaths = map[string]bool{}
	p.rawAliases = map[int64]string{}
	rawOutside := map[int64]bool{}
	rows, err := q.Query(ctx, `SELECT c.id,c.display_path,coalesce(r.stream_id,0),coalesce(nullif(s.local_timezone,''),r.cron_timezone,''),c.clip_start_at,c.clip_end_at FROM recording_clips c JOIN recordings r ON r.id=c.recording_id LEFT JOIN streams s ON s.id=r.stream_id WHERE r.account_id=47`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, stream int64
		var path, tz string
		var a, b *time.Time
		if err := rows.Scan(&id, &path, &stream, &tz, &a, &b); err != nil {
			rows.Close()
			return err
		}
		rawOutside[id] = outside(stream, tz, a, b)
		if !rawOutside[id] {
			p.unsafePaths[path] = true
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return rows.Err()
	}
	rows, err = q.Query(ctx, `SELECT clip_id,relative_path FROM nas_inventory_files WHERE connection_id=13`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		var path string
		if err := rows.Scan(&id, &path); err != nil {
			rows.Close()
			return err
		}
		p.rawAliases[id] = path
		if !rawOutside[id] {
			p.unsafePaths[path] = true
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return rows.Err()
	}
	rows, err = q.Query(ctx, `SELECT o.nas_relative_path,coalesce(r.stream_id,0),coalesce(nullif(s.local_timezone,''),r.cron_timezone,''),o.start_at,o.end_at,o.source_clip_ids FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id LEFT JOIN streams s ON s.id=r.stream_id WHERE r.account_id=47`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var path, tz string
		var stream int64
		var a, b *time.Time
		var sources []int64
		if err := rows.Scan(&path, &stream, &tz, &a, &b, &sources); err != nil {
			return err
		}
		covered := outside(stream, tz, a, b) && len(sources) > 0
		for _, id := range sources {
			covered = covered && rawOutside[id]
		}
		if !covered {
			p.unsafePaths[path] = true
			p.unsafePaths["joined/"+path] = true
		}
	}
	return rows.Err()
}
