package api

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const cleanupRankingsCacheStoreSQL = `INSERT INTO nas_cleanup_rankings_cache(plan_id,stream_id,freeze_generation,best_start,fingerprint,checked_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(plan_id,stream_id) DO UPDATE SET freeze_generation=excluded.freeze_generation,best_start=excluded.best_start,fingerprint=excluded.fingerprint,checked_at=excluded.checked_at`
const cleanupRankingsFreezeSQL = `SELECT generation FROM nas_cleanup_frozen_streams WHERE plan_id=$1 AND stream_id=$2 AND validated`
const cleanupRankingsCacheLoadSQL = `SELECT c.best_start,c.fingerprint,c.freeze_generation FROM nas_cleanup_rankings_cache c JOIN nas_cleanup_frozen_streams f USING(plan_id,stream_id) JOIN nas_cleanup_plans p ON p.id=c.plan_id WHERE c.plan_id=$2 AND c.stream_id=$1 AND f.generation=$3 AND f.validated AND p.mode='enabled'`
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
		if freezeGeneration <= 0 || (want.Generation != 0 && want.Generation != freezeGeneration) {
			return false, errors.New("cleanup activation freeze generation changed; fresh enable required")
		}
		var current, fingerprint string
		var cachedGeneration int64
		err := tx.QueryRow(ctx, cleanupRankingsCacheLoadSQL, stream, plan, freezeGeneration).Scan(&current, &fingerprint, &cachedGeneration)
		if err == nil {
			if cachedGeneration != freezeGeneration {
				return false, errors.New("cleanup rankings freeze generation changed; fresh enable required")
			}
			if stages != nil {
				stages["rankings_cache"] = "hit"
			}
			return current == best && fingerprint == want.Fingerprint, nil
		}
		if err != pgx.ErrNoRows {
			return false, err
		}
		if want.Generation == 0 {
			// Older activations bind through their persisted cache row only.
			return false, errors.New("cleanup activation freeze generation unavailable; fresh enable required")
		}
		// A cold cache may be rebuilt only under the exact live, validated freeze.
		// Never turn a lost or changed freeze into a fresh execution verdict.
		var currentGeneration int64
		if err = tx.QueryRow(ctx, cleanupRankingsFreezeSQL, plan, stream).Scan(&currentGeneration); err != nil || currentGeneration != freezeGeneration {
			return false, errors.New("cleanup rankings source freeze unavailable or changed")
		}
		if stages != nil {
			stages["rankings_cache"] = "replay"
		}
		var checked time.Time
		if err = tx.QueryRow(ctx, cleanupRankingsReplaySQL, stream).Scan(&current, &fingerprint, &checked); err != nil {
			return false, err
		}
		// Source triggers preserve this verdict until this freeze is released.
		_, err = tx.Exec(ctx, cleanupRankingsCacheStoreSQL, plan, stream, freezeGeneration, current, fingerprint, checked)
		return current == best && fingerprint == want.Fingerprint, err
	}
}
