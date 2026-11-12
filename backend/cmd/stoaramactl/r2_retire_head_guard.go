package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
)

type r2RetireSingleDeleteStore interface {
	DeleteObjectOnce(context.Context, string) error
}

// Must stay in sync with ops/jelly/r2-retire/HEAD-GUARD.md. Direct credential
// holders are outside the DB fence: an operator must stop and drain them.
var r2RetireWriterInventory = [...]string{
	"recorder", "capture", "collation", "joined-publish", "v85r-stitch",
	"repair", "nas-restore-redownload", "r2-select", "thumbnails",
	"transfer", "frames-inference-survey-import", "exports", "releases-probes",
	"operator-cli",
}

type r2RetireWriterDrain struct {
	StoppedAt               time.Time `json:"stopped_at"`
	DrainedAt               time.Time `json:"drained_at"`
	CapabilitiesExpiredAt   time.Time `json:"capabilities_expired_at"`
	MultipartUploadsDrained bool      `json:"multipart_uploads_drained"`
	DeployedRevision        string    `json:"deployed_revision"`
	Evidence                string    `json:"evidence"`
}

type r2RetireWriterProof struct {
	Bucket         string    `json:"bucket"`
	EndpointSHA256 string    `json:"endpoint_sha256"`
	ManifestSHA256 string    `json:"manifest_sha256"`
	CreatedAt      time.Time `json:"created_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	// Explicit operator commitment to keep all inventoried writers stopped
	// through the run. This cannot fence a direct credential holder atomically.
	KeepStoppedThroughExpiry bool                           `json:"keep_stopped_through_expiry"`
	ActiveWriterPrefixes     []string                       `json:"active_writer_prefixes"`
	Writers                  map[string]r2RetireWriterDrain `json:"writers"`
}

func checkR2RetireWriterProof(opts r2RetireApplyOptions, h r2RetireHeader, digest, endpoint, key string) error {
	if !r2RetireLowerHex64(opts.WriterProofSHA256) {
		return errors.New("HEAD-guard mode requires --writer-quiescence-proof and --expected-writer-proof-sha256")
	}
	raw, err := os.ReadFile(opts.WriterProofPath)
	if err != nil {
		return errors.New("writer quiescence evidence unavailable")
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != opts.WriterProofSHA256 {
		return errors.New("writer quiescence evidence changed; stop and review again")
	}
	var p r2RetireWriterProof
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return errors.New("invalid writer quiescence evidence")
	}
	if dec.Decode(new(any)) != io.EOF {
		return errors.New("invalid trailing writer evidence")
	}
	now := time.Now()
	if p.Bucket != h.Bucket || p.EndpointSHA256 != endpoint || p.ManifestSHA256 != digest ||
		p.CreatedAt.IsZero() || p.CreatedAt.After(now) || now.Sub(p.CreatedAt) > 24*time.Hour ||
		!p.ExpiresAt.After(now) || p.ExpiresAt.After(p.CreatedAt.Add(24*time.Hour)) ||
		!p.KeepStoppedThroughExpiry || p.ActiveWriterPrefixes == nil || len(p.Writers) != len(r2RetireWriterInventory) {
		return errors.New("writer evidence scope/age/inventory/stop commitment differs")
	}
	for _, name := range r2RetireWriterInventory {
		w, ok := p.Writers[name]
		if !ok || w.StoppedAt.IsZero() || w.DrainedAt.Before(w.StoppedAt) || w.DrainedAt.After(p.CreatedAt) ||
			w.CapabilitiesExpiredAt.IsZero() || w.CapabilitiesExpiredAt.After(w.DrainedAt) ||
			!w.MultipartUploadsDrained || strings.TrimSpace(w.DeployedRevision) == "" || strings.TrimSpace(w.Evidence) == "" {
			return fmt.Errorf("writer %s lacks stopped/drained capability and multipart evidence", name)
		}
	}
	for _, prefix := range p.ActiveWriterPrefixes {
		if prefix == "" || strings.TrimSpace(prefix) != prefix {
			return errors.New("invalid active writer prefix")
		}
		// Literal byte prefixes (including partial path components) fail closed.
		if key != "" && strings.HasPrefix(key, prefix) {
			return errors.New("key overlaps an active writer prefix")
		}
	}
	return nil
}

// SHARE conflicts with INSERT/UPDATE/DELETE (including admissions and presign
// intent creation). Hold these locks until the guarded DELETE and receipt commit.
// A consumed URL can still overwrite, so expiry is checked regardless of status.
func r2RetireLockStoppedWriters(ctx context.Context, tx pgx.Tx, bucket string) error {
	if _, err := tx.Exec(ctx, `LOCK TABLE recordings,recording_jobs,streams,capture_session_leases,recording_upload_intents,upload_intents IN SHARE MODE`); err != nil {
		return fmt.Errorf("lock recording/capture/upload admissions: %w", err)
	}
	return r2RetireCheckStoppedWriters(ctx, tx, bucket)
}

func r2RetireCheckStoppedWriters(ctx context.Context, q r2RetireQuerier, bucket string) error {
	var active bool
	err := q.QueryRow(ctx, `SELECT
 EXISTS(SELECT 1 FROM recordings WHERE status='active')
 OR EXISTS(SELECT 1 FROM recording_jobs WHERE status IN ('pending','leased'))
 OR EXISTS(SELECT 1 FROM streams WHERE recording_enabled)
 OR EXISTS(SELECT 1 FROM capture_session_leases WHERE lease_expires_at>clock_timestamp())
 OR EXISTS(SELECT 1 FROM recording_upload_intents WHERE bucket=$1 AND expires_at>clock_timestamp())
 OR EXISTS(SELECT 1 FROM upload_intents WHERE bucket=$1 AND expires_at>clock_timestamp())`, bucket).Scan(&active)
	if err != nil {
		return fmt.Errorf("check stopped recording/capture/upload state: %w", err)
	}
	if active {
		return errors.New("HEAD-guard mode requires stopped recordings, capture and expired upload capabilities")
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
// compare-and-delete: a writer ignoring the shutdown commitment can still race.
func (a *r2RetireApplier) headGuard(ctx context.Context, tx pgx.Tx, it r2RetireItem) error {
	if err := checkR2RetireDeleteProofMode(a.opts.DeleteProofPath, a.opts.DeleteProofSHA256, a.header.Bucket, a.store.EndpointSHA256(), true); err != nil {
		return err
	}
	if err := checkR2RetireWriterProof(a.opts, a.header, a.digest, a.store.EndpointSHA256(), it.ObjectKey); err != nil {
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
	if err := checkR2RetireWriterProof(a.opts, a.header, a.digest, a.store.EndpointSHA256(), it.ObjectKey); err != nil {
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
