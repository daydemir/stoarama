package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const finishRefreshTarget = finishTarget / 2

// Fixed half share is allocated BEFORE ordinary fill. Odd spare slots stay
// with ordinary copy; unused refresh slots may also be used by ordinary work.
func finishRefreshShare(depth, refresh int) int {
	return max(0, min((finishTarget-depth)/2, finishRefreshTarget-refresh))
}

type finishRefreshOptions struct{ Picks, PicksSHA string }

func setKeep28Refresh(ctx context.Context, pool *pgxpool.Pool, enabled bool) error {
	tag, err := pool.Exec(ctx, `UPDATE r2_keep28_refresh_control SET enabled=$1 WHERE singleton`, enabled)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("refresh control missing")
	}
	return nil
}
func runKeep28RefreshControl(ctx context.Context, cfg config.Config, args []string) {
	if len(args) != 1 || (args[0] != "on" && args[0] != "off") {
		log.Fatal("usage: stoaramactl r2-inverse refresh on|off (controller scheduling only)")
	}
	enabled := args[0] == "on"
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	if err := setKeep28Refresh(ctx, pool, enabled); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("{\"refresh_enabled\":%t}\n", enabled)
}

// This is a refresh hint, not deletion custody: timestamps may be expired.
// Every other part of the exact inventory/sidecar identity remains mandatory.
const finishRefreshExactSQL = `n.connection_id=13 AND n.clip_id=c.id AND n.state='present'
 AND n.relative_path=c.display_path AND n.size_bytes=c.size_bytes AND n.sha256=lower(c.sha256)
 AND n.verified_at IS NOT NULL AND n.server_received_at IS NOT NULL
 AND n.verified_at<=clock_timestamp()+interval '5 minutes'
 AND n.sidecar_relative_path=c.display_path||'.stoarama.json' AND n.sidecar_size_bytes>0 AND n.sidecar_sha256 ~ '^[a-f0-9]{64}$'
 AND LEAST(n.verified_at,n.server_received_at)+interval '24 hours'<=clock_timestamp()+interval '1 hour'`

func feedKeep28Refresh(ctx context.Context, pool *pgxpool.Pool, o finishRefreshOptions, limit int, status *finishStatus) (int, error) {
	if limit <= 0 || o.Picks == "" {
		return 0, nil
	}
	var enabled bool
	if err := pool.QueryRow(ctx, `SELECT enabled FROM r2_keep28_refresh_control`).Scan(&enabled); err != nil {
		return 0, err
	}
	if !enabled {
		return 0, nil
	}
	picks, err := loadR2RetirePicks(o.Picks)
	if err != nil {
		return 0, err
	}
	if !r2RetireLowerHex64(o.PicksSHA) || picks.SHA256 != o.PicksSHA {
		return 0, errors.New("refresh picks digest differs")
	}
	policy, windows, err := api.Keep28Policy()
	if err != nil {
		return 0, err
	}
	init, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer rollbackFinishCandidateTx(init)
	if err = inverseProtectionBarrier(ctx, init); err != nil {
		return 0, err
	}
	var epoch int64
	if err = init.QueryRow(ctx, `SELECT epoch FROM r2_inverse_ke_epoch`).Scan(&epoch); err != nil {
		return 0, err
	}
	// KE folder discovery is shared with B2 and protected by its catalog epoch.
	protection, err := loadInverseApplyProtection(ctx, init, "stoarama", picks, windows)
	if err != nil {
		return 0, err
	}
	if err = init.Commit(ctx); err != nil {
		return 0, err
	}
	catalog, err := beginFinishCandidateTx(ctx, pool)
	if err != nil {
		return 0, err
	}
	defer rollbackFinishCandidateTx(catalog)
	rows, err := catalog.Query(ctx, `SELECT q.item,coalesce(c.etag,'') FROM nas_r2_copy_items q JOIN recording_clips c ON c.id=q.source_id
 JOIN nas_inventory_files n ON n.clip_id=c.id AND n.connection_id=13
 WHERE c.bucket='stoarama' AND q.connection_id=13 AND q.kind='raw' AND q.state='verified' AND q.blocked_reason='' AND q.policy_sha256=$1
 AND `+finishRefreshExactSQL+` ORDER BY LEAST(n.verified_at,n.server_received_at),c.id LIMIT $2`, policy, finishTarget)
	if err != nil {
		return 0, err
	}
	type candidate struct {
		item api.NASR2CopyItem
		etag string
	}
	var candidates []candidate
	for rows.Next() {
		var data []byte
		var c candidate
		if err = rows.Scan(&data, &c.etag); err != nil {
			rows.Close()
			return 0, err
		}
		if err = json.Unmarshal(data, &c.item); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	if err = catalog.Commit(ctx); err != nil {
		return 0, err
	}
	admitted := 0
	var bytes int64
	for _, c := range candidates {
		if admitted >= limit {
			break
		}
		if c.item.SizeBytes > nasCopyBatchBytes-bytes {
			continue
		}
		ok, err := queueKeep28Refresh(ctx, pool, o, protection, epoch, c.item, c.etag, policy, status)
		if err != nil {
			return admitted, err
		}
		if status.RefreshHold == finishRefreshDisabled {
			break
		}
		if ok {
			admitted++
			bytes += c.item.SizeBytes
		} else {
			status.RefreshHeldItems++
		}
	}
	return admitted, nil
}
func queueKeep28Refresh(ctx context.Context, pool *pgxpool.Pool, o finishRefreshOptions, p inverseProtection, epoch int64, i api.NASR2CopyItem, etag, policy string, status *finishStatus) (bool, error) {
	if api.ValidateNASR2CopyItem(i) != nil {
		return false, nil
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer rollbackFinishCandidateTx(tx)
	if _, err = tx.Exec(ctx, `SET LOCAL statement_timeout='10s'; SET LOCAL lock_timeout='2s'; SET LOCAL work_mem='16MB'`); err != nil {
		return false, err
	}
	if err = inverseProtectionBarrier(ctx, tx); err != nil {
		return false, err
	}
	if err = inverseProtectionLocks(ctx, tx, false); err != nil {
		return false, err
	}
	var current int64
	if err = tx.QueryRow(ctx, `SELECT epoch FROM r2_inverse_ke_epoch`).Scan(&current); err != nil {
		return false, err
	}
	if current != epoch {
		return false, errors.New("refresh catalog changed; retry with fresh picks/catalog")
	}
	if _, err = tx.Exec(ctx, `SELECT lock_nas_r2_copy_key($1,$2)`, i.Bucket, i.ObjectKey); err != nil {
		return false, err
	}
	e := inverseEntry{ObjectInfo: r2.ObjectInfo{Key: i.ObjectKey, ETag: etag, SizeBytes: i.SizeBytes}, Class: inverseKeep28}
	target, err := loadInverseRawTarget(ctx, tx, i.Bucket, p, inverseNASKeep28, e)
	if errors.Is(err, errInverseRawIneligible) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var recording int64
	if err = tx.QueryRow(ctx, `SELECT recording_id FROM recording_clips WHERE id=$1`, target.ID).Scan(&recording); err != nil {
		return false, err
	}
	if target.ID != i.SourceID || recording != i.RecordingID || target.StreamID != i.StreamID || target.Path != i.RelativePath || target.Size != i.SizeBytes || target.SHA != i.SHA256 || !target.Identity.Start.Equal(i.StartAt) || !target.Identity.End.Equal(i.EndAt) {
		return false, nil
	}
	var held bool
	if err = tx.QueryRow(ctx, inverseRawHeldSQL, target.ID, i.Bucket, i.ObjectKey, "", target.Path, target.SHA, target.Size).Scan(&held); err != nil || held {
		return false, err
	}
	var exact bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_clips c JOIN nas_inventory_files n ON n.clip_id=c.id WHERE c.id=$1 AND `+finishRefreshExactSQL+`)`, i.SourceID).Scan(&exact); err != nil || !exact {
		return false, err
	}
	data, _ := json.Marshal(i)
	var id int64
	err = tx.QueryRow(ctx, `SELECT id FROM nas_r2_copy_items WHERE connection_id=13 AND kind='raw' AND source_id=$1 AND item=$2::jsonb AND state='verified' AND blocked_reason='' AND policy_sha256=$3 FOR UPDATE`, i.SourceID, data, policy).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var total, refresh int
	if err = tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE state IN ('refresh_pending','refresh_leased')) FROM nas_r2_copy_items WHERE connection_id=13 AND state IN ('pending','leased','refresh_pending','refresh_leased')`).Scan(&total, &refresh); err != nil {
		return false, err
	}
	if total >= finishTarget || refresh >= finishRefreshTarget {
		return false, nil
	}
	if err = finishCleanupValve(ctx, tx, status); err != nil {
		return false, err
	}
	if status.NASSpaceClientVersion != api.NASStorageBudgetClientVersion {
		status.RefreshHold = finishLateClientUnknown
		return false, nil
	}
	if status.NASStorageBudgetAt == nil || !finishProofFresh(status.NASSpaceCheckedAt, *status.NASStorageBudgetAt, finishLateDebitWindow) {
		status.RefreshHold = finishLateProofStale
		return false, nil
	}
	if status.CleanupReason != "nas_headroom_sufficient" {
		status.RefreshHold = finishLateSpaceLow
		return false, nil
	}
	var charged int64
	if err = tx.QueryRow(ctx, finishLateDebitsSQL, status.NASSpaceCheckedAt.Add(-finishLateDebitWindow)).Scan(&charged); err != nil {
		return false, err
	}
	status.QueueReservedBytes = charged
	if i.SizeBytes > max(int64(0), *status.NASFreeBytes-api.NASR2CopyFloor-api.NASR2CopyRangeBytes-charged) {
		status.RefreshHold = finishLateBudgetHeld
		return false, nil
	}
	var enabled bool
	if err = tx.QueryRow(ctx, `SELECT enabled FROM r2_keep28_refresh_control FOR SHARE`).Scan(&enabled); err != nil || !enabled {
		return false, err
	}
	picks, err := loadR2RetirePicks(o.Picks)
	if err != nil {
		return false, err
	}
	if picks.SHA256 != o.PicksSHA {
		return false, errors.New("refresh picks changed")
	}
	_, err = tx.Exec(ctx, `UPDATE nas_r2_copy_items SET state='refresh_pending',work_mode='verify_existing',lease_token=NULL,lease_expires_at=NULL,range_until=NULL,range_id=NULL,range_length=0,updated_at=clock_timestamp() WHERE id=$1`, id)
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
