package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
)

type r2RetireSingleDeleteStore interface {
	DeleteObjectOnce(context.Context, string) error
}

// These writers hold credentials outside database admission. Prefixes are code
// inventory, never operator-supplied evidence. Empty means bucket-wide authority.
// Until operator/release credentials are provider-confined, ALL keys overlap;
// stopping a process or submitting an attestation cannot narrow that authority.
func r2RetireCheckUngatedPrefix(key string) error {
	writers := [...]struct{ family, prefix string }{
		{"repair", "repaired/"},
		{"releases-probes", ""}, // configurable release/probe destinations
		{"operator-cli", ""},    // AWS CLI/curl/rclone and untracked credential holders
	}
	for _, w := range writers {
		if strings.HasPrefix(key, w.prefix) {
			return fmt.Errorf("key overlaps ungated %s writer authority; provider prefix confinement required", w.family)
		}
	}
	return nil
}

type r2RetireMultipartInventory interface{ RequireNoMultipartUploads(context.Context) error }

func r2RetireCheckMultipart(ctx context.Context, store r2RetireStore) error {
	inventory, ok := store.(r2RetireMultipartInventory)
	if !ok {
		return errors.New("HEAD-guard mode requires live ListMultipartUploads inventory")
	}
	return inventory.RequireNoMultipartUploads(ctx)
}

func r2RetireCheckWriterFence(ctx context.Context, q r2RetireQuerier, bucket, endpoint string) error {
	var fenced bool
	err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM r2_retirement_writer_fences
 WHERE bucket=$1 AND endpoint_sha256=$2 AND drain_not_before<clock_timestamp())`, bucket, endpoint).Scan(&fenced)
	if err != nil {
		return fmt.Errorf("R2 writer fence unavailable (migration 0187 required): %w", err)
	}
	if !fenced {
		return errors.New("R2 writer fence missing, scope differs or legacy capabilities have not drained")
	}
	return r2RetireCheckStoppedWriters(ctx, q, bucket)
}

func r2RetireLockStoppedWriters(ctx context.Context, tx pgx.Tx, bucket string) error {
	// A durable fence survives transaction failures and crashes. The apply session
	// lock also prevents explicit deactivation through its SQL transition trigger.
	if _, err := tx.Exec(ctx, `SELECT singleton FROM r2_retirement_writer_fences FOR SHARE`); err != nil {
		return err
	}
	return r2RetireCheckStoppedWriters(ctx, tx, bucket)
}

func r2RetireCheckStoppedWriters(ctx context.Context, q r2RetireQuerier, bucket string) error {
	var drained bool
	if err := q.QueryRow(ctx, `SELECT r2_retirement_writers_drained($1)`, bucket).Scan(&drained); err != nil {
		return fmt.Errorf("check all R2 writer queues and capabilities: %w", err)
	}
	if !drained {
		return errors.New("R2 writers have active/queued work or unexpired PUT capabilities (including transfer/stitch/joined jobs)")
	}
	return nil
}

func r2RetireValidateGuardHead(h r2.ObjectHead, it r2RetireItem, catalogETag string, minAge time.Duration, now time.Time) error {
	// Trim only transport ETag quotes. An empty identity is never authority.
	expected := strings.Trim(it.ETag, `"`)
	if expected == "" || strings.Trim(catalogETag, `"`) != expected || strings.Trim(h.ETag, `"`) != expected {
		return errors.New("HEAD/manifest/catalog ETag identity differs or is missing")
	}
	if h.SizeBytes != it.SizeBytes || (h.SHA256Metadata != "" && !strings.EqualFold(h.SHA256Metadata, it.SHA256)) {
		return errors.New("HEAD size/SHA metadata differs from manifest/catalog")
	}
	if minAge < 7*24*time.Hour || h.LastModified.IsZero() || !h.LastModified.Before(now.Add(-minAge)) {
		return errors.New("HEAD LastModified missing, future or too recent")
	}
	return nil
}

// This runs AFTER the reservation and fsynced intent, immediately before the
// single wire DELETE. HEAD and live vetoes are observations, not a provider
// compare-and-delete: an ungated credential holder can still race.
func (a *r2RetireApplier) headGuard(ctx context.Context, tx pgx.Tx, it r2RetireItem) error {
	if err := checkR2RetireDeleteProofMode(a.opts.DeleteProofPath, a.opts.DeleteProofSHA256, a.header.Bucket, a.store.EndpointSHA256(), true); err != nil {
		return err
	}
	if err := a.checkWriterState(ctx, tx, it.ObjectKey); err != nil {
		return err
	}
	// Collated catalog currently stores only size and SHA. Do not manufacture
	// catalog ETag authority from a live listing or silently waive this check.
	if it.Kind != "raw" {
		return errors.New("HEAD-guard mode requires a catalog ETag; collated catalog has none")
	}
	var etag string
	if err := tx.QueryRow(ctx, `SELECT etag FROM recording_clips WHERE id=$1`, it.SourceID).Scan(&etag); err != nil {
		return err
	}
	head, err := a.store.Head(ctx, it.ObjectKey)
	if err != nil {
		return fmt.Errorf("final guarded HEAD: %w", err)
	}
	if err := r2RetireValidateGuardHead(head, it, etag, a.opts.MinObjectAge, time.Now()); err != nil {
		return err
	}
	if err := r2RetireCheckStoppedWriters(ctx, tx, a.header.Bucket); err != nil {
		return err
	}
	// Refresh every protection set too: fingerprint caching is not deletion
	// authority in this mode. Per-item live evidence is then reloaded under fences.
	sets, err := loadR2RetireSets(ctx, tx, a.digest, true, a.scope.Class)
	if err != nil {
		return err
	}
	latest, err := r2RetireLoadOne(ctx, tx, it.Kind, it.SourceID, a.scope, sets)
	if err != nil {
		return err
	}
	if latest.RecordingID != it.RecordingID || latest.StreamID != it.StreamID || latest.Bucket != it.Bucket || latest.ObjectKey != it.ObjectKey || latest.SizeBytes != it.SizeBytes || latest.SHA256 != it.SHA256 || latest.RelativePath != it.RelativePath || !latest.StartAt.Equal(it.StartAt) || !latest.EndAt.Equal(it.EndAt) {
		return errors.New("catalog identity changed during guarded HEAD")
	}
	latest.Facts.Purged, latest.Facts.Retired = false, false // our uncommitted receipt
	picks, err := loadR2RetirePicks(a.picks.Path)
	if err != nil {
		return err
	}
	if picks.SHA256 != a.header.PicksSHA256 {
		return errors.New("owner picks changed during guarded HEAD")
	}
	reason, choice, eligible := r2RetireDecideForScope(latest, a.scope, a.windows, picks.Choices)
	if !eligible || reason != it.Reason || choice != it.Choice {
		return fmt.Errorf("live veto after guarded HEAD: %s", reason)
	}
	if err := a.ledger.write(r2RetireLedgerLine{Event: "head_guard_passed", ManifestSHA256: a.digest, Kind: it.Kind, SourceID: it.SourceID, ObjectKey: it.ObjectKey, ETag: head.ETag, LastModified: head.LastModified, Detail: "provider_unconditional_delete_with_head_guard; residual_toctou=true"}); err != nil {
		return err
	}
	// Expiry or evidence revocation while waiting for HEAD/DB must veto too.
	if err := checkR2RetireDeleteProofMode(a.opts.DeleteProofPath, a.opts.DeleteProofSHA256, a.header.Bucket, a.store.EndpointSHA256(), true); err != nil {
		return err
	}
	if err := a.checkWriterState(ctx, tx, it.ObjectKey); err != nil {
		return err
	}
	return nil
}

// Raw catalog authority is loaded in one query for planning and preflight.
// Collated outputs have no ETag column and cannot authorize this mode.
func r2RetireCatalogETags(ctx context.Context, q r2RetireQuerier, accountID int64, bucket string) (map[string]string, error) {
	rows, err := q.Query(ctx, `SELECT c.object_key,c.etag FROM recording_clips c JOIN recordings r ON r.id=c.recording_id WHERE r.account_id=$1 AND c.bucket=$2`, accountID, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	etags := map[string]string{}
	for rows.Next() {
		var key, etag string
		if err := rows.Scan(&key, &etag); err != nil {
			return nil, err
		}
		etags[key] = strings.Trim(etag, `"`)
	}
	return etags, rows.Err()
}

func r2RetireGuardCatalogReason(it r2RetireCandidate, etags map[string]string) string {
	if it.Kind != "raw" || etags[it.ObjectKey] == "" {
		return "catalog_etag_unavailable"
	}
	if strings.Trim(it.ETag, `"`) == "" || strings.Trim(it.ETag, `"`) != etags[it.ObjectKey] {
		return "catalog_etag_mismatch"
	}
	return ""
}

func (a *r2RetireApplier) checkWriterState(ctx context.Context, q r2RetireQuerier, key string) error {
	if err := r2RetireCheckWriterFence(ctx, q, a.header.Bucket, a.store.EndpointSHA256()); err != nil {
		return err
	}
	if err := r2RetireCheckMultipart(ctx, a.store); err != nil {
		return err
	}
	return r2RetireCheckUngatedPrefix(key)
}
