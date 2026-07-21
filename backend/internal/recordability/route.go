package recordability

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/model"
)

// RouteNeedsRelay decides whether a stream should DEFAULT to relay, given its own
// probe verdict and its provider's sticky flag. A proven stream verdict beats the
// provider heuristic (we trust a direct observation of THIS stream over the
// provider-wide guess). Pure so the precedence is unit-tested without a DB.
//
//   - stream proven blocked      -> relay
//   - stream proven ok           -> cloud (we have seen it work; ignore provider flag)
//   - stream untested (no row)   -> follow provider_recordability.needs_relay
//   - anything else              -> cloud
//
// With both tables empty (ship-dark default) every stream is untested and no
// provider is flagged, so this always returns false (cloud). Inert until a probe
// writes a row.
func RouteNeedsRelay(streamResult string, streamHasRow bool, providerNeedsRelay bool) bool {
	if streamHasRow {
		switch streamResult {
		case ResultBlocked:
			return true
		case ResultOK:
			return false
		}
	}
	return providerNeedsRelay
}

// NeedsRelay applies hard routing, then reads the two recordability tables and
// applies RouteNeedsRelay. A stream with no id (raw pasted URL, streamID<=0) is never
// downgraded here (we have no provider/verdict for it). Never returns an error for
// a missing stream row; only a real query failure is surfaced.
func NeedsRelay(ctx context.Context, pool *pgxpool.Pool, streamID int64, provider, sourceURL string) (bool, error) {
	if pool == nil || streamID <= 0 {
		return false, nil
	}
	if model.StreamRequiresRelay(provider, sourceURL) {
		return true, nil
	}
	var (
		result   *string
		provRely *bool
	)
	err := pool.QueryRow(ctx, `
		SELECT sr.result, pr.needs_relay
		FROM streams s
		LEFT JOIN stream_recordability sr ON sr.stream_id = s.id
		LEFT JOIN provider_recordability pr ON pr.provider = s.provider
		WHERE s.id = $1
	`, streamID).Scan(&result, &provRely)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("recordability needs-relay lookup for stream %d: %w", streamID, err)
	}
	streamResult := ""
	streamHasRow := result != nil
	if result != nil {
		streamResult = *result
	}
	providerNeedsRelay := provRely != nil && *provRely
	return RouteNeedsRelay(streamResult, streamHasRow, providerNeedsRelay), nil
}
