package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
)

const r2RetireEvidencePageSize = 5000

// Inventory is keyed by (connection_id,clip_id). Bound every planning read;
// apply uses the per-object live queries instead of enumerating inventory.
func r2RetireInventoryEach(ctx context.Context, q r2RetireQuerier, columns, predicate string, args []any, dest []any, visit func() error) error {
	var connection, clip int64
	for {
		// Find the next actual key, then bound the page by a fixed clip-ID range
		// within that connection. Even a bitmap+sort plan cannot visit more than
		// 5000 unique inventory rows, and sparse IDs do not require empty-page walks.
		var firstConnection, firstClip int64
		err := q.QueryRow(ctx, `SELECT connection_id,clip_id FROM nas_inventory_files
 WHERE (connection_id,clip_id)>($1,$2) ORDER BY connection_id,clip_id LIMIT 1`, connection, clip).Scan(&firstConnection, &firstClip)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		lastClip := r2RetirePageEnd(firstClip, r2RetireEvidencePageSize)
		query := fmt.Sprintf(`SELECT n.connection_id,n.clip_id,coalesce((%s),false),%s
 FROM nas_inventory_files n
 WHERE n.connection_id=$%d AND n.clip_id BETWEEN $%d AND $%d
 ORDER BY n.clip_id`, predicate, columns, len(args)+1, len(args)+2, len(args)+3)
		params := append(append([]any{}, args...), firstConnection, firstClip, lastClip)
		rows, err := q.Query(ctx, query, params...)
		if err != nil {
			return err
		}
		var included bool
		// Advance across every primary-key row, even a wholly excluded page.
		// Excluded projections may be NULL, so decode item fields only on inclusion.
		cursorFields := make([]any, 3+len(dest))
		cursorFields[0], cursorFields[1], cursorFields[2] = &connection, &clip, &included
		itemFields := append([]any{&connection, &clip, &included}, dest...)
		_, err = pgx.ForEachRow(rows, cursorFields, func() error {
			if !included {
				return nil
			}
			if err := rows.Scan(itemFields...); err != nil {
				return err
			}
			return visit()
		})
		if err != nil {
			return err
		}
		connection, clip = firstConnection, lastClip
	}
}

// Range endpoints are chosen before eligibility filters. The primary key makes
// each query independent of how many purged/foreign rows precede a candidate.
func r2RetireIDRange(ctx context.Context, q r2RetireQuerier, kind string, after int64, size int) (first, last int64, found bool, err error) {
	var table string
	switch kind {
	case "raw":
		table = "recording_clips"
	case "collated":
		table = "recording_collation_outputs"
	default:
		return 0, 0, false, fmt.Errorf("unknown catalog kind %q", kind)
	}
	if size < 1 {
		return 0, 0, false, errors.New("catalog page size must be positive")
	}
	size = min(size, r2RetireEvidencePageSize)
	err = q.QueryRow(ctx, `SELECT id FROM `+table+` WHERE id>$1 ORDER BY id LIMIT 1`, after).Scan(&first)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, err
	}
	return first, r2RetirePageEnd(first, size), true, nil
}
func r2RetirePageEnd(first int64, size int) int64 {
	if first > math.MaxInt64-int64(size-1) {
		return math.MaxInt64
	}
	return first + int64(size-1)
}
