package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const r2RetireEvidencePageSize = 5000

// Inventory is keyed by (connection_id,clip_id). Bound every planning read;
// apply uses the per-object live queries instead of enumerating inventory.
func r2RetireInventoryEach(ctx context.Context, q r2RetireQuerier, columns, predicate string, args []any, dest []any, visit func() error) error {
	var connection, clip int64
	for {
		query := fmt.Sprintf(`SELECT n.connection_id,n.clip_id,%s FROM nas_inventory_files n
 WHERE (n.connection_id,n.clip_id)>($%d,$%d) AND (%s)
 ORDER BY n.connection_id,n.clip_id LIMIT %d`, columns, len(args)+1, len(args)+2, predicate, r2RetireEvidencePageSize)
		params := append(append([]any{}, args...), connection, clip)
		rows, err := q.Query(ctx, query, params...)
		if err != nil {
			return err
		}
		fields := append([]any{&connection, &clip}, dest...)
		count, err := pgx.ForEachRow(rows, fields, visit)
		if err != nil {
			return err
		}
		if count.RowsAffected() < r2RetireEvidencePageSize {
			return nil
		}
	}
}
