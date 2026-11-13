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
		lastClip := int64(math.MaxInt64)
		if firstClip <= math.MaxInt64-(r2RetireEvidencePageSize-1) {
			lastClip = firstClip + r2RetireEvidencePageSize - 1
		}
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
