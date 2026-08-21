package joinedrecording

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
	"github.com/google/uuid"
)

type ImmutableStore interface {
	PutReaderIfAbsentVerified(context.Context, string, string, io.Reader, int64, string) (r2.ObjectHead, bool, error)
}

type WorkerClaim struct {
	TaskID        int64      `json:"task_id"`
	ClaimToken    string     `json:"claim_token"`
	LeaseExpires  time.Time  `json:"lease_expires_at"`
	BatchPlanSHA  string     `json:"batch_plan_sha256"`
	BatchID       string     `json:"batch_id"`
	CampaignID    string     `json:"campaign_id"`
	RecordingID   int64      `json:"recording_id"`
	SourcePlanSHA string     `json:"source_manifest_sha256"`
	ExpectedCount int        `json:"expected_campaign_output_count"`
	Output        OutputPlan `json:"output"`
}

type PublishedOutput struct {
	TaskID            int64        `json:"task_id"`
	PlanOrdinal       int          `json:"plan_ordinal"`
	ObjectKey         string       `json:"object_key"`
	ETag              string       `json:"etag"`
	VersionID         string       `json:"version_id,omitempty"`
	SizeBytes         int64        `json:"size_bytes"`
	SHA256            string       `json:"sha256"`
	CoverageObjectKey string       `json:"coverage_object_key"`
	CoverageETag      string       `json:"coverage_etag"`
	CoverageVersionID string       `json:"coverage_version_id,omitempty"`
	CoverageSizeBytes int64        `json:"coverage_size_bytes"`
	CoverageSHA256    string       `json:"coverage_sha256"`
	Verification      Verification `json:"verification"`
	Created           bool         `json:"-"`
}

type OutputCoverage struct {
	SchemaVersion       int          `json:"schema_version"`
	PolicyVersion       string       `json:"policy_version"`
	CampaignID          string       `json:"campaign_id"`
	BatchID             string       `json:"batch_id"`
	BatchPlanSHA256     string       `json:"batch_plan_sha256"`
	SourceManifestSHA   string       `json:"source_manifest_sha256"`
	ExpectedOutputCount int          `json:"expected_campaign_output_count"`
	RecordingID         int64        `json:"recording_id"`
	Output              OutputPlan   `json:"output"`
	MediaSizeBytes      int64        `json:"media_size_bytes"`
	MediaSHA256         string       `json:"media_sha256"`
	MediaETag           string       `json:"media_etag"`
	MediaVersionID      string       `json:"media_version_id,omitempty"`
	Verification        Verification `json:"verification"`
}

type FinalizeOutput func(context.Context, WorkerClaim, PublishedOutput) error

func (c WorkerClaim) Validate(now time.Time) error {
	prefix := "joined/" + c.BatchID + "/"
	if c.TaskID <= 0 || uuid.Validate(c.ClaimToken) != nil || !c.LeaseExpires.After(now) || !lowerHex64(c.BatchPlanSHA) || !lowerHex64(c.SourcePlanSHA) || c.ExpectedCount <= 0 || c.Output.Ordinal > c.ExpectedCount || c.BatchID != c.CampaignID || !safeCampaignID.MatchString(c.CampaignID) || c.RecordingID <= 0 || c.Output.Ordinal <= 0 || len(c.Output.Sources) == 0 || c.Output.ActualEnd.Sub(c.Output.ActualStart) > time.Hour || !c.Output.ActualEnd.After(c.Output.ActualStart) || !lowerHex64(c.Output.ContentID) || !strings.HasPrefix(c.Output.ObjectKey, prefix) || c.Output.CoverageKey != c.Output.ObjectKey+".coverage.json" {
		return fmt.Errorf("invalid or expired fenced joined claim")
	}
	seen := map[int64]bool{}
	for _, source := range c.Output.Sources {
		if source.RecordingID != c.RecordingID || seen[source.ClipID] || validateSource(source, c.RecordingID) != nil {
			return fmt.Errorf("claimed output crosses recordings")
		}
		seen[source.ClipID] = true
	}
	contentID, _, err := stitchcert.CanonicalSHA(struct {
		Policy  string       `json:"policy"`
		Sources []SourceClip `json:"sources"`
	}{PlanPolicyVersion, c.Output.Sources})
	if err != nil || contentID != c.Output.ContentID {
		return fmt.Errorf("claimed output content identity drifted")
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
	return filepath.Join(root, "task-"+strconv.FormatInt(c.TaskID, 10), c.ClaimToken), nil
}

// PublishClaimedOutput publishes exactly one sealed <=1h task. The finalize
// callback must perform a token-fenced DB UPDATE; if it rejects a stale lease,
// retrying a fresh claim reconciles the exact immutable R2 objects.
func PublishClaimedOutput(ctx context.Context, store ImmutableStore, claim WorkerClaim, built BuiltOutput, scratchDir string, finalize FinalizeOutput) (PublishedOutput, error) {
	if store == nil || finalize == nil {
		return PublishedOutput{}, fmt.Errorf("immutable store and fenced finalizer are required")
	}
	if err := claim.Validate(time.Now().UTC()); err != nil {
		return PublishedOutput{}, err
	}
	if built.SourceCount != len(claim.Output.Sources) || built.Verification.Status != "passed" || !SafeScratchOutput(built.Path, scratchDir) {
		return PublishedOutput{}, fmt.Errorf("claimed output is not a verified scratch artifact")
	}
	size, sha, err := localIdentity(built.Path)
	if err != nil || size != built.SizeBytes || sha != built.SHA256 || size > r2.MaxConditionalPutBytes {
		return PublishedOutput{}, fmt.Errorf("claimed output changed before publication")
	}
	f, err := os.Open(built.Path)
	if err != nil {
		return PublishedOutput{}, err
	}
	head, created, putErr := store.PutReaderIfAbsentVerified(ctx, claim.Output.ObjectKey, "video/mp4", f, size, sha)
	closeErr := f.Close()
	if putErr != nil {
		return PublishedOutput{}, putErr
	}
	if closeErr != nil {
		return PublishedOutput{}, closeErr
	}
	coverage := OutputCoverage{SchemaVersion: 1, PolicyVersion: PlanPolicyVersion, CampaignID: claim.CampaignID, BatchID: claim.BatchID, BatchPlanSHA256: claim.BatchPlanSHA, SourceManifestSHA: claim.SourcePlanSHA, ExpectedOutputCount: claim.ExpectedCount, RecordingID: claim.RecordingID, Output: claim.Output, MediaSizeBytes: size, MediaSHA256: sha, MediaETag: head.ETag, MediaVersionID: head.VersionID, Verification: built.Verification}
	coverageSHA, coverageJSON, err := stitchcert.CanonicalSHA(coverage)
	if err != nil {
		return PublishedOutput{}, err
	}
	coverageHead, _, err := store.PutReaderIfAbsentVerified(ctx, claim.Output.CoverageKey, "application/json", bytes.NewReader(coverageJSON), int64(len(coverageJSON)), coverageSHA)
	if err != nil {
		return PublishedOutput{}, err
	}
	published := PublishedOutput{TaskID: claim.TaskID, PlanOrdinal: claim.Output.Ordinal, ObjectKey: claim.Output.ObjectKey, ETag: head.ETag, VersionID: head.VersionID, SizeBytes: size, SHA256: sha, CoverageObjectKey: claim.Output.CoverageKey, CoverageETag: coverageHead.ETag, CoverageVersionID: coverageHead.VersionID, CoverageSizeBytes: int64(len(coverageJSON)), CoverageSHA256: coverageSHA, Verification: built.Verification, Created: created}
	if err := finalize(ctx, claim, published); err != nil {
		return PublishedOutput{}, fmt.Errorf("immutable joined objects verified but fenced database reconciliation remains pending: %w", err)
	}
	if !SafeScratchOutput(built.Path, scratchDir) {
		return PublishedOutput{}, fmt.Errorf("refusing cleanup outside scratch")
	}
	if err := os.Remove(built.Path); err != nil && !os.IsNotExist(err) {
		return PublishedOutput{}, fmt.Errorf("cleanup verified worker scratch: %w", err)
	}
	return published, nil
}
