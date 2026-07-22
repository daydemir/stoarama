package recordingnaming

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// EnsureStreamPlazaID returns the stable organization-local plaza number for a
// catalog stream. Allocation is serialized per organization and starts above
// both mapped IDs and IDs already stored on older recordings.
func EnsureStreamPlazaID(ctx context.Context, tx pgx.Tx, accountID, streamID int64) (string, error) {
	if err := LockOrganizationPlazaIDs(ctx, tx, accountID); err != nil {
		return "", err
	}
	var plazaID int64
	err := tx.QueryRow(ctx, `
		SELECT plaza_id
		FROM account_stream_plaza_ids
		WHERE account_id=$1 AND stream_id=$2
	`, accountID, streamID).Scan(&plazaID)
	if err == nil {
		return strconv.FormatInt(plazaID, 10), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("load plaza id: %w", err)
	}
	err = tx.QueryRow(ctx, `
		SELECT (rec.naming_metadata_jsonb->>'plaza_id')::bigint
		FROM recordings rec
		WHERE rec.account_id=$1
		  AND rec.stream_id=$2
		  AND rec.naming_profile='plaza_hourly_v1'
		  AND rec.naming_metadata_jsonb->>'plaza_id' ~ '^[+]?[0-9]{1,18}$'
		  AND (rec.naming_metadata_jsonb->>'plaza_id')::bigint > 0
		  AND NOT EXISTS (
			SELECT 1
			FROM account_stream_plaza_ids used
			WHERE used.account_id=rec.account_id
			  AND used.plaza_id=(rec.naming_metadata_jsonb->>'plaza_id')::bigint
		  )
		  AND NOT EXISTS (
			SELECT 1
			FROM recordings other
			WHERE other.account_id=rec.account_id
			  AND other.stream_id IS DISTINCT FROM rec.stream_id
			  AND other.naming_profile='plaza_hourly_v1'
			  AND other.naming_metadata_jsonb->>'plaza_id' ~ '^[+]?[0-9]{1,18}$'
			  AND (other.naming_metadata_jsonb->>'plaza_id')::bigint=(rec.naming_metadata_jsonb->>'plaza_id')::bigint
		  )
		ORDER BY rec.id
		LIMIT 1
	`, accountID, streamID).Scan(&plazaID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("load existing stream plaza id: %w", err)
	}
	if err == nil {
		return saveStreamPlazaID(ctx, tx, accountID, streamID, plazaID)
	}
	if err := tx.QueryRow(ctx, `
		WITH used AS (
			SELECT plaza_id
			FROM account_stream_plaza_ids
			WHERE account_id=$1
			UNION ALL
			SELECT (naming_metadata_jsonb->>'plaza_id')::bigint
			FROM recordings
			WHERE account_id=$1
			  AND naming_profile='plaza_hourly_v1'
			  AND naming_metadata_jsonb->>'plaza_id' ~ '^[+]?[0-9]{1,18}$'
		)
		SELECT COALESCE(MAX(plaza_id), 0) + 1 FROM used
	`, accountID).Scan(&plazaID); err != nil {
		return "", fmt.Errorf("allocate plaza id: %w", err)
	}
	return saveStreamPlazaID(ctx, tx, accountID, streamID, plazaID)
}

func LockOrganizationPlazaIDs(ctx context.Context, tx pgx.Tx, accountID int64) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('account_stream_plaza_ids:' || $1::text, 0))`, accountID); err != nil {
		return fmt.Errorf("lock plaza id sequence: %w", err)
	}
	return nil
}

func ValidateManualPlazaID(ctx context.Context, tx pgx.Tx, accountID, recordingID int64, raw string) error {
	plazaID, err := parsePlazaID(raw)
	if err != nil {
		return err
	}
	if err := LockOrganizationPlazaIDs(ctx, tx, accountID); err != nil {
		return err
	}
	var used bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM account_stream_plaza_ids
			WHERE account_id=$1 AND plaza_id=$2
			UNION ALL
			SELECT 1
			FROM recordings
			WHERE account_id=$1
			  AND id <> $3
			  AND naming_profile='plaza_hourly_v1'
			  AND naming_metadata_jsonb->>'plaza_id' ~ '^[+]?[0-9]{1,18}$'
			  AND (naming_metadata_jsonb->>'plaza_id')::bigint=$2
		)
	`, accountID, plazaID, recordingID).Scan(&used); err != nil {
		return fmt.Errorf("check plaza id: %w", err)
	}
	if used {
		return fmt.Errorf("plaza id %d is already used by this organization", plazaID)
	}
	return nil
}

func parsePlazaID(raw string) (int64, error) {
	plazaID, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || plazaID <= 0 {
		return 0, fmt.Errorf("plaza id must be a positive integer")
	}
	return plazaID, nil
}

func saveStreamPlazaID(ctx context.Context, tx pgx.Tx, accountID, streamID, plazaID int64) (string, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO account_stream_plaza_ids (account_id, stream_id, plaza_id)
		VALUES ($1, $2, $3)
	`, accountID, streamID, plazaID); err != nil {
		return "", fmt.Errorf("save plaza id: %w", err)
	}
	return strconv.FormatInt(plazaID, 10), nil
}
