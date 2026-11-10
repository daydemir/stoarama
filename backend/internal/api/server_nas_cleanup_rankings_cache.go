package api

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const cleanupRankingsCacheStoreSQL = `INSERT INTO nas_cleanup_rankings_cache(plan_id,stream_id,freeze_generation,best_start,fingerprint,checked_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(plan_id,stream_id) DO UPDATE SET freeze_generation=excluded.freeze_generation,best_start=excluded.best_start,fingerprint=excluded.fingerprint,checked_at=excluded.checked_at`
const cleanupRankingsFreezeSQL = `SELECT generation FROM nas_cleanup_frozen_streams WHERE plan_id=$1 AND stream_id=$2 AND validated`
const cleanupRankingsCacheLoadSQL = `SELECT best_start,fingerprint FROM nas_cleanup_rankings_cache WHERE plan_id=$2 AND stream_id=$1 AND freeze_generation=$3 AND checked_at>clock_timestamp()-interval '60 seconds' AND checked_at<=clock_timestamp()`
const cleanupRankingsReplaySQL = cleanupRankingsWithFingerprintSQL + `,statement_timestamp()`

// The plan execution fence protects the freeze lifecycle. Source triggers reject
// writers without taking any version/catalog locks. A missing freeze fails
// closed even for legacy plans without a stored rankings verdict.
func (s *Server) cleanupExecutionRankingsUnchanged(ctx context.Context, tx pgx.Tx, plan, freezeGeneration int64, mode string, stored []byte, stages map[string]any) cleanupRankingsUnchanged {
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
		var current, fingerprint string
		err := tx.QueryRow(ctx, cleanupRankingsCacheLoadSQL, stream, plan, freezeGeneration).Scan(&current, &fingerprint)
		if err == nil {
			if stages != nil {
				stages["rankings_cache"] = "hit"
			}
			return current == best && fingerprint == want.Fingerprint, nil
		}
		if err != pgx.ErrNoRows {
			return false, err
		}
		if stages != nil {
			stages["rankings_cache"] = "replay"
		}
		var checked time.Time
		if err = tx.QueryRow(ctx, cleanupRankingsReplaySQL, stream).Scan(&current, &fingerprint, &checked); err != nil {
			return false, err
		}
		// Statement start is conservative: time spent replaying counts toward TTL.
		_, err = tx.Exec(ctx, cleanupRankingsCacheStoreSQL, plan, stream, freezeGeneration, current, fingerprint, checked)
		return current == best && fingerprint == want.Fingerprint, err
	}
}
