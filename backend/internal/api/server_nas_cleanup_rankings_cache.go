package api

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const cleanupRankingsCacheStoreSQL = `INSERT INTO nas_cleanup_rankings_cache(plan_id,stream_id,source_version,best_start,fingerprint,checked_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(plan_id,stream_id) DO UPDATE SET source_version=excluded.source_version,best_start=excluded.best_start,fingerprint=excluded.fingerprint,checked_at=excluded.checked_at`

const cleanupRankingsVersionExpr = `coalesce((SELECT version FROM nas_cleanup_rankings_versions WHERE stream_id=$1),0)`
const cleanupRankingsCacheLoadSQL = `SELECT best_start FROM nas_cleanup_rankings_cache WHERE plan_id=$2 AND stream_id=$1 AND source_version=$3 AND checked_at>clock_timestamp()-interval '60 seconds' AND checked_at<=clock_timestamp()`
const cleanupRankingsVersionedReplaySQL = cleanupRankingsInputSQL + cleanupRankingsScoresSQL + ` SELECT ` + cleanupRankingsWinnerExpr + `,` + cleanupRankingsFingerprintExpr + `,` + cleanupRankingsVersionExpr + `,statement_timestamp()`

// SHARE conflicts with the triggers' non-key version UPDATE (KEY SHARE does
// not). The row must exist: never turn a missing fence into version zero.
const cleanupRankingsVersionLockSQL = `SELECT version FROM nas_cleanup_rankings_versions WHERE stream_id=$1 FOR SHARE`

// Called after locking the catalog row, then the stream's source-version row,
// under the plan execution fence. Source writers take catalog locks before
// their version UPDATE, so taking these locks in reverse order can deadlock.
// The source SHARE lock survives through the permit's transaction commit,
// including the uncached fallback replay when execution_rankings is absent.
// Cache/replay versions must match that locked version. A cache miss runs the
// FULL replay; all cache writes roll back with a failed observation.
func (s *Server) cleanupExecutionRankingsUnchanged(ctx context.Context, tx pgx.Tx, plan, sourceVersion int64, mode string, stored []byte, stages map[string]any) cleanupRankingsUnchanged {
	if mode != "enabled" || len(stored) == 0 {
		return nil
	}
	var pinned map[string]cleanupExecutionRanking
	if json.Unmarshal(stored, &pinned) != nil {
		return nil
	}
	return func(stream int64, best string) (bool, error) {
		want, ok := pinned[strconv.FormatInt(stream, 10)]
		if !ok || want.BestStart != best || want.Fingerprint == "" {
			return false, nil
		}
		var current string
		err := tx.QueryRow(ctx, cleanupRankingsCacheLoadSQL, stream, plan, sourceVersion).Scan(&current)
		if err == nil {
			if stages != nil {
				stages["rankings_cache"] = "hit"
			}
			return current == best, nil
		}
		if err != pgx.ErrNoRows {
			return false, err
		}
		if stages != nil {
			stages["rankings_cache"] = "replay"
		}
		var fingerprint string
		var version int64
		var checked time.Time
		if err = tx.QueryRow(ctx, cleanupRankingsVersionedReplaySQL, stream).Scan(&current, &fingerprint, &version, &checked); err != nil {
			return false, err
		}
		if version != sourceVersion {
			return false, errors.New("cleanup source version differs from locked fence; KEEP")
		}
		// Statement start is conservative: time spent replaying counts toward TTL.
		_, err = tx.Exec(ctx, cleanupRankingsCacheStoreSQL, plan, stream, version, current, fingerprint, checked)
		return current == best, err
	}
}
