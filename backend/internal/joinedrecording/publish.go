package joinedrecording

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

// WorkerClaim is a renewable publication lease for an already sealed hour.
// It is intentionally distinct from the expired source-only preflight claim.
type WorkerClaim struct {
	ProtocolVersion          int                    `json:"protocol_version"`
	HourID                   string                 `json:"hour_id"`
	LeaseID                  string                 `json:"lease_id"`
	OperationToken           string                 `json:"operation_token"`
	LeaseExpires             time.Time              `json:"lease_expires_at"`
	StorageAuthority         string                 `json:"storage_authority"`
	StorageBucket            string                 `json:"storage_bucket"`
	Plan                     BatchPlan              `json:"plan"`
	Allocation               HourManifestAllocation `json:"allocation"`
	AllocationLedger         StreamDayAllocation    `json:"allocation_ledger"`
	HourManifest             HourManifest           `json:"hour_manifest"`
	MediaArtifactIDs         []int64                `json:"media_artifact_ids"`
	HourManifestArtifactID   int64                  `json:"hour_manifest_artifact_id"`
	HourManifestExpectedSize int64                  `json:"hour_manifest_expected_size_bytes"`
	HourManifestExpectedSHA  string                 `json:"hour_manifest_expected_sha256"`
}

type PublishedOutput struct {
	ArtifactID int64  `json:"artifact_id"`
	ObjectKey  string `json:"object_key"`
	ETag       string `json:"etag"`
	VersionID  string `json:"version_id,omitempty"`
	SizeBytes  int64  `json:"size_bytes"`
	SHA256     string `json:"sha256"`
	Created    bool   `json:"-"`
}

type PublishedHour struct {
	HourID                string            `json:"hour_id"`
	RecordingID           int64             `json:"recording_id"`
	LocalDate             string            `json:"local_date"`
	LocalHour             int               `json:"local_hour"`
	Outputs               []PublishedOutput `json:"outputs"`
	HourManifestObjectKey string            `json:"hour_manifest_object_key"`
	HourManifestETag      string            `json:"hour_manifest_etag"`
	HourManifestVersionID string            `json:"hour_manifest_version_id,omitempty"`
	HourManifestSizeBytes int64             `json:"hour_manifest_size_bytes"`
	HourManifestSHA256    string            `json:"hour_manifest_sha256"`
}

type verifiedHourScratch struct {
	HourID            string
	SourceClaimSHA256 string
	OriginLeaseID     string
	Directory         string
	Built             []BuiltOutput
	Quarantine        []QuarantineEvidence
}

type SealedHourScratch struct {
	verified           verifiedHourScratch
	publicationLeaseID string
}

func bindSealedHourScratch(verified verifiedHourScratch, claim WorkerClaim) (SealedHourScratch, error) {
	if err := claim.Validate(time.Now().UTC()); err != nil || verified.HourID != claim.HourID || verified.SourceClaimSHA256 != claim.Plan.SourceClaimSHA256 || !validLeaseID(verified.OriginLeaseID) || !filepath.IsAbs(verified.Directory) || filepath.Base(verified.Directory) != verified.OriginLeaseID || len(verified.Built) != len(claim.Plan.Outputs) {
		return SealedHourScratch{}, fmt.Errorf("verified scratch differs from sealed hour")
	}
	for i, built := range verified.Built {
		output := claim.Plan.Outputs[i]
		if !SafeScratchOutput(built.Path, verified.Directory) || built.SizeBytes != output.ExpectedSize || built.SHA256 != output.ExpectedSHA || built.SourceCount != len(output.Sources) || validatePassedVerification(built.Verification) != nil {
			return SealedHourScratch{}, fmt.Errorf("verified scratch part %d differs from sealed output", i+1)
		}
	}
	return SealedHourScratch{verified: verified, publicationLeaseID: claim.LeaseID}, nil
}

// BindRebuiltSealedHourScratch is for a reclaimed publication lease after the
// prior worker's private scratch was lost. It accepts only scratch rooted in
// the current lease's task directory and never references the prior lease.
func BindRebuiltSealedHourScratch(claim WorkerClaim, scratchRoot, directory string, built []BuiltOutput, quarantine []QuarantineEvidence) (SealedHourScratch, error) {
	want, err := claim.ScratchDir(scratchRoot)
	if err != nil || filepath.Clean(directory) != filepath.Clean(want) {
		return SealedHourScratch{}, fmt.Errorf("rebuilt scratch is outside current publication lease")
	}
	verified := verifiedHourScratch{HourID: claim.HourID, SourceClaimSHA256: claim.Plan.SourceClaimSHA256, OriginLeaseID: claim.LeaseID, Directory: directory, Built: built, Quarantine: quarantine}
	return bindSealedHourScratch(verified, claim)
}

// BindReclaimedGapOnlyHourScratch reconstructs the only publication scratch
// that needs no source bytes: an empty, canonical gap-only hour. Any source,
// output, quarantine, or stale scratch entry fails closed.
func BindReclaimedGapOnlyHourScratch(claim WorkerClaim, scratchRoot string) (SealedHourScratch, error) {
	if err := claim.Validate(time.Now().UTC()); err != nil || !claim.Plan.GapOnly || len(claim.Plan.Sources) != 0 || len(claim.Plan.QuarantinedSources) != 0 || len(claim.Plan.Outputs) != 0 || len(claim.MediaArtifactIDs) != 0 {
		return SealedHourScratch{}, fmt.Errorf("only a canonical gap-only hour can rebuild empty scratch")
	}
	directory, err := claim.ScratchDir(scratchRoot)
	if err != nil {
		return SealedHourScratch{}, err
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return SealedHourScratch{}, fmt.Errorf("create reclaimed gap-only scratch: %w", err)
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&0o077 != 0 {
		return SealedHourScratch{}, fmt.Errorf("reclaimed gap-only scratch is not a private directory")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		return SealedHourScratch{}, fmt.Errorf("reclaimed gap-only scratch is not empty")
	}
	return BindRebuiltSealedHourScratch(claim, scratchRoot, directory, nil, nil)
}

type FinalizeHour func(context.Context, WorkerClaim, PublishedHour) error
type ReadCapabilityResolver func(context.Context, WorkerClaim, int64) (ObjectReadCapability, error)
type CreateCapabilityResolver func(context.Context, WorkerClaim, int64) (ObjectCreateCapability, error)

func (c WorkerClaim) Validate(now time.Time) error {
	if c.ProtocolVersion != JoinedProtocolVersion || c.HourID != c.Plan.HourID || !validLeaseID(c.LeaseID) || !validOperationToken(c.OperationToken) || !c.LeaseExpires.After(now) || c.StorageAuthority == "" || c.StorageBucket == "" || ValidatePlan(c.Plan) != nil || c.HourManifestArtifactID <= 0 || c.HourManifestExpectedSize <= 0 || !lowerHex64(c.HourManifestExpectedSHA) || len(c.MediaArtifactIDs) != len(c.Plan.Outputs) {
		return fmt.Errorf("invalid or expired fenced joined hour publication claim")
	}
	seen := map[int64]bool{}
	for _, artifactID := range c.MediaArtifactIDs {
		if artifactID <= 0 || seen[artifactID] {
			return fmt.Errorf("joined media artifact identity differs")
		}
		seen[artifactID] = true
	}
	built := make([]BuiltOutput, len(c.HourManifest.Media))
	for i, media := range c.HourManifest.Media {
		built[i] = BuiltOutput{SizeBytes: media.SizeBytes, SHA256: media.SHA256, SourceCount: len(media.SourceClipIDs),
			Verification: media.Verification, SplitEvidence: append([]MaximalityEvidence(nil), media.MaximalityEvidence...)}
	}
	manifest, canonical, sha, err := BuildHourManifest(HourManifestInput{Plan: c.Plan, Allocation: c.Allocation,
		AllocationLedger: c.AllocationLedger, MediaArtifactIDs: c.MediaArtifactIDs, Built: built,
		QuarantineEvidence: c.HourManifest.QuarantineEvidence})
	if err != nil || int64(len(canonical)) != c.HourManifestExpectedSize || sha != c.HourManifestExpectedSHA ||
		!sameCanonical([]HourManifest{manifest}, []HourManifest{c.HourManifest}) {
		return fmt.Errorf("sealed joined hour manifest differs")
	}
	return nil
}

func (c WorkerClaim) ScratchDir(root string) (string, error) {
	if err := c.Validate(time.Now().UTC()); err != nil {
		return "", err
	}
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("scratch root is required")
	}
	return leaseScratchDir(root, c.LeaseID), nil
}

// PublishClaimedHour publishes the already-sealed scratch bytes create-only,
// then its precomputed canonical hour manifest. The callback must atomically
// finalize the exact unexpired publication token.
func publishClaimedHour(ctx context.Context, client CapabilityHTTPClient, claim WorkerClaim, scratch SealedHourScratch, resolveCreate CreateCapabilityResolver, resolveRead ReadCapabilityResolver, finalize FinalizeHour) (PublishedHour, error) {
	built, quarantine, scratchDir := scratch.verified.Built, scratch.verified.Quarantine, scratch.verified.Directory
	if client == nil || resolveCreate == nil || resolveRead == nil || finalize == nil {
		return PublishedHour{}, fmt.Errorf("capability client and fenced finalizer are required")
	}
	if err := claim.Validate(time.Now().UTC()); err != nil {
		return PublishedHour{}, err
	}
	if scratch.publicationLeaseID != claim.LeaseID {
		return PublishedHour{}, fmt.Errorf("scratch is not bound to current publication lease")
	}
	_, manifestJSON, manifestSHA, err := BuildHourManifest(HourManifestInput{Plan: claim.Plan, Allocation: claim.Allocation, AllocationLedger: claim.AllocationLedger, MediaArtifactIDs: claim.MediaArtifactIDs, Built: built, QuarantineEvidence: quarantine})
	if err != nil || int64(len(manifestJSON)) != claim.HourManifestExpectedSize || manifestSHA != claim.HourManifestExpectedSHA {
		return PublishedHour{}, fmt.Errorf("sealed hour manifest identity changed before publication")
	}
	type identity struct {
		size int64
		sha  string
	}
	identities := make([]identity, len(built))
	for i := range built {
		output := claim.Plan.Outputs[i]
		if built[i].SourceCount != len(output.Sources) || built[i].Verification.Status != "passed" || !SafeScratchOutput(built[i].Path, scratchDir) {
			return PublishedHour{}, fmt.Errorf("hour part %d is not a verified scratch artifact", i+1)
		}
		size, sha, identityErr := localIdentity(built[i].Path)
		if identityErr != nil || size != built[i].SizeBytes || sha != built[i].SHA256 || size != output.ExpectedSize || sha != output.ExpectedSHA || size > r2.MaxConditionalPutBytes {
			return PublishedHour{}, fmt.Errorf("hour part %d changed before publication", i+1)
		}
		identities[i] = identity{size: size, sha: sha}
	}
	published := PublishedHour{HourID: claim.HourID, RecordingID: claim.Plan.RecordingID, LocalDate: claim.Plan.LocalDate, LocalHour: claim.Plan.LocalHour, Outputs: make([]PublishedOutput, 0, len(built)), HourManifestObjectKey: claim.Plan.CoverageObjectKey}
	publishStarted := time.Now()
	for i := range built {
		create, resolveErr := resolveCreate(ctx, claim, claim.MediaArtifactIDs[i])
		if resolveErr != nil {
			emitStageTiming(ctx, "upload_verify", time.Since(publishStarted), resolveErr)
			return PublishedHour{}, fmt.Errorf("resolve exact media create capability: %w", resolveErr)
		}
		output, publishErr := publishHourPart(ctx, client, claim, create, claim.MediaArtifactIDs[i], claim.Plan.Outputs[i], built[i], identities[i].size, identities[i].sha, resolveRead)
		if publishErr != nil {
			emitStageTiming(ctx, "upload_verify", time.Since(publishStarted), publishErr)
			return PublishedHour{}, publishErr
		}
		published.Outputs = append(published.Outputs, output)
	}
	manifestCreate, err := resolveCreate(ctx, claim, claim.HourManifestArtifactID)
	if err != nil {
		emitStageTiming(ctx, "upload_verify", time.Since(publishStarted), err)
		return PublishedHour{}, fmt.Errorf("resolve exact hour-manifest create capability: %w", err)
	}
	_, err = putCreateOnlyCapability(ctx, client, claim.StorageAuthority, claim.StorageBucket, claim.HourManifestArtifactID, claim.Plan.CoverageObjectKey, "application/json", int64(len(manifestJSON)), manifestSHA, manifestCreate, bytes.NewReader(manifestJSON))
	if err != nil {
		emitStageTiming(ctx, "upload_verify", time.Since(publishStarted), err)
		return PublishedHour{}, err
	}
	manifestRead, err := resolveRead(ctx, claim, claim.HourManifestArtifactID)
	if err != nil {
		emitStageTiming(ctx, "upload_verify", time.Since(publishStarted), err)
		return PublishedHour{}, fmt.Errorf("resolve exact hour-manifest reread capability")
	}
	manifestHead, err := reconcileExactCapability(ctx, client, claim.StorageAuthority, claim.StorageBucket, claim.HourManifestArtifactID, claim.Plan.CoverageObjectKey, int64(len(manifestJSON)), manifestSHA, manifestRead.ETag, manifestRead.VersionID, manifestRead)
	if err != nil {
		emitStageTiming(ctx, "upload_verify", time.Since(publishStarted), err)
		return PublishedHour{}, err
	}
	published.HourManifestETag, published.HourManifestVersionID, published.HourManifestSizeBytes, published.HourManifestSHA256 = manifestHead.ETag, manifestHead.VersionID, int64(len(manifestJSON)), manifestSHA
	emitStageTiming(ctx, "upload_verify", time.Since(publishStarted), nil)
	finalizeStarted := time.Now()
	if err := finalize(ctx, claim, published); err != nil {
		emitStageTiming(ctx, "finalize", time.Since(finalizeStarted), err)
		return PublishedHour{}, fmt.Errorf("immutable joined hour verified but fenced database reconciliation remains pending: %w", err)
	}
	emitStageTiming(ctx, "finalize", time.Since(finalizeStarted), nil)
	if filepath.Base(scratchDir) != claim.LeaseID || filepath.Clean(scratchDir) != scratchDir {
		return PublishedHour{}, fmt.Errorf("refusing cleanup outside current lease scratch")
	}
	for _, output := range built {
		if !SafeScratchOutput(output.Path, scratchDir) {
			return PublishedHour{}, fmt.Errorf("refusing cleanup outside scratch")
		}
	}
	if err := os.RemoveAll(scratchDir); err != nil {
		return PublishedHour{}, fmt.Errorf("cleanup verified worker scratch: %w", err)
	}
	return published, nil
}

// PublishClaimedHourRenewing keeps the publication lease alive while each
// just-in-time capability and finalization call uses the newest same-lease
// operation token. The create capability itself may outlive the DB lease, but
// it cannot finalize or mint another capability.

func PublishClaimedHourRenewing(ctx context.Context, client CapabilityHTTPClient, storageAuthority string, claim WorkerClaim, scratch SealedHourScratch, heartbeat HeartbeatOperation, resolveCreate CreateCapabilityResolver, resolveRead ReadCapabilityResolver, finalize FinalizeHour) (PublishedHour, error) {
	if storageAuthority == "" || claim.StorageAuthority != storageAuthority {
		return PublishedHour{}, fmt.Errorf("hour publication authority differs from configured storage")
	}
	return publishClaimedHourRenewing(ctx, client, claim, scratch, heartbeat, resolveCreate, resolveRead, finalize, defaultRenewableRunner)
}

func publishClaimedHourRenewing(ctx context.Context, client CapabilityHTTPClient, claim WorkerClaim, scratch SealedHourScratch, heartbeat HeartbeatOperation, resolveCreate CreateCapabilityResolver, resolveRead ReadCapabilityResolver, finalize FinalizeHour, run renewableRunner) (PublishedHour, error) {
	var published PublishedHour
	initial := OperationCredentials{LeaseID: claim.LeaseID, OperationToken: claim.OperationToken, ExpiresAt: claim.LeaseExpires}
	err := run(ctx, initial, heartbeat, func(workCtx context.Context, current func() OperationCredentials) error {
		fresh := func() (WorkerClaim, error) { return claim.WithOperation(current()) }
		create := func(callCtx context.Context, _ WorkerClaim, artifactID int64) (ObjectCreateCapability, error) {
			if err := callCtx.Err(); err != nil {
				return ObjectCreateCapability{}, err
			}
			currentClaim, err := fresh()
			if err != nil {
				return ObjectCreateCapability{}, err
			}
			return resolveCreate(callCtx, currentClaim, artifactID)
		}
		read := func(callCtx context.Context, _ WorkerClaim, artifactID int64) (ObjectReadCapability, error) {
			if err := callCtx.Err(); err != nil {
				return ObjectReadCapability{}, err
			}
			currentClaim, err := fresh()
			if err != nil {
				return ObjectReadCapability{}, err
			}
			capability, err := resolveRead(callCtx, currentClaim, artifactID)
			if err == nil && capability.ExpiresAt.After(currentClaim.LeaseExpires) {
				return ObjectReadCapability{}, fmt.Errorf("artifact read capability outlives current publication lease")
			}
			return capability, err
		}
		finish := func(callCtx context.Context, _ WorkerClaim, output PublishedHour) error {
			if err := callCtx.Err(); err != nil {
				return err
			}
			currentClaim, err := fresh()
			if err != nil {
				return err
			}
			return finalize(callCtx, currentClaim, output)
		}
		var err error
		published, err = publishClaimedHour(workCtx, client, claim, scratch, create, read, finish)
		return err
	})
	return published, err
}

func publishHourPart(ctx context.Context, client CapabilityHTTPClient, claim WorkerClaim, capability ObjectCreateCapability, artifactID int64, output OutputPlan, built BuiltOutput, size int64, sha string, resolveRead ReadCapabilityResolver) (PublishedOutput, error) {
	f, err := os.Open(built.Path)
	if err != nil {
		return PublishedOutput{}, err
	}
	// The HTTP transport owns and closes request bodies passed to Do. Keep a
	// defensive close for clients that do not, but do not treat a second close
	// as a failed publication: net/http may already have closed this file after
	// the PUT completed successfully.
	defer func() { _ = f.Close() }()
	observation, putErr := putCreateOnlyCapability(ctx, client, claim.StorageAuthority, claim.StorageBucket, artifactID, output.ObjectKey, "video/mp4", size, sha, capability, f)
	if putErr != nil {
		return PublishedOutput{}, putErr
	}
	readCapability, err := resolveRead(ctx, claim, artifactID)
	if err != nil {
		return PublishedOutput{}, fmt.Errorf("resolve exact media reread capability")
	}
	head, err := reconcileExactCapability(ctx, client, claim.StorageAuthority, claim.StorageBucket, artifactID, output.ObjectKey, size, sha, readCapability.ETag, readCapability.VersionID, readCapability)
	if err != nil {
		return PublishedOutput{}, err
	}
	return PublishedOutput{ArtifactID: artifactID, ObjectKey: output.ObjectKey, ETag: head.ETag, VersionID: head.VersionID, SizeBytes: size, SHA256: sha, Created: observation.Created}, nil
}
