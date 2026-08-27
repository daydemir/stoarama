package joinedrecording

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

const JoinedProtocolVersion = 1

const (
	WorkScopeCanary       = "canary"
	WorkScopeSingleCanary = "canary_single"
	WorkScopeAllowlist50  = "allowlist_50"
	WorkScopeFrozenBatch  = "frozen_batch"
)

func IsCanaryWorkScope(scope string) bool {
	return scope == WorkScopeCanary || scope == WorkScopeSingleCanary || scope == WorkScopeAllowlist50
}

// WorkScopeIdentity is the exact rollout authority shared by worker, API, and
// signed claim tokens. Canary order is intentional and covered by the digest.
type WorkScopeIdentity struct {
	WorkScope           string   `json:"work_scope,omitempty"`
	CanaryHourIDs       []string `json:"canary_hour_ids,omitempty"`
	CanaryHourIDsSHA256 string   `json:"canary_hour_ids_sha256,omitempty"`
}

func NewWorkScopeIdentity(batchID, workScope string, canaryHourIDs []string) (WorkScopeIdentity, error) {
	identity := WorkScopeIdentity{WorkScope: workScope, CanaryHourIDs: slices.Clone(canaryHourIDs)}
	if IsCanaryWorkScope(workScope) {
		identity.CanaryHourIDsSHA256 = canaryHourIDsSHA256(identity.CanaryHourIDs)
	}
	if err := identity.Validate(batchID); err != nil {
		return WorkScopeIdentity{}, err
	}
	return identity, nil
}

func (s WorkScopeIdentity) Validate(batchID string) error {
	if !ValidBatchID(batchID) {
		return fmt.Errorf("invalid joined work scope batch")
	}
	switch s.WorkScope {
	case WorkScopeCanary, WorkScopeSingleCanary, WorkScopeAllowlist50:
		wantCount := 3
		if s.WorkScope == WorkScopeSingleCanary {
			wantCount = 1
		} else if s.WorkScope == WorkScopeAllowlist50 {
			wantCount = 50
		}
		if len(s.CanaryHourIDs) != wantCount || !lowerHex64(s.CanaryHourIDsSHA256) ||
			s.CanaryHourIDsSHA256 != canaryHourIDsSHA256(s.CanaryHourIDs) {
			return fmt.Errorf("invalid joined canary work scope")
		}
		seen := make(map[string]bool, len(s.CanaryHourIDs))
		prefix := batchID + "__recording-"
		for _, hourID := range s.CanaryHourIDs {
			if !strings.HasPrefix(hourID, prefix) || seen[hourID] {
				return fmt.Errorf("invalid joined canary work scope")
			}
			seen[hourID] = true
		}
	case WorkScopeFrozenBatch:
		if len(s.CanaryHourIDs) != 0 || s.CanaryHourIDsSHA256 != "" {
			return fmt.Errorf("invalid joined frozen-batch work scope")
		}
	default:
		return fmt.Errorf("invalid joined work scope")
	}
	return nil
}

func (s WorkScopeIdentity) Equal(other WorkScopeIdentity) bool {
	return s.WorkScope == other.WorkScope && s.CanaryHourIDsSHA256 == other.CanaryHourIDsSHA256 &&
		slices.Equal(s.CanaryHourIDs, other.CanaryHourIDs)
}

// SHA256 binds the exact validated rollout scope used to authorize work.
func (s WorkScopeIdentity) SHA256(batchID string) (string, error) {
	digest, _, err := s.Canonical(batchID)
	return digest, err
}

// Canonical returns the exact bytes and digest persisted by scope authorization.
func (s WorkScopeIdentity) Canonical(batchID string) (string, []byte, error) {
	if err := s.Validate(batchID); err != nil {
		return "", nil, err
	}
	digest, canonical, err := stitchcert.CanonicalSHA(s)
	if err != nil {
		return "", nil, fmt.Errorf("canonical joined work scope: %w", err)
	}
	return digest, canonical, nil
}

func canaryHourIDsSHA256(hourIDs []string) string {
	canonical, _ := json.Marshal(hourIDs)
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

type WorkerBootstrapRequest struct {
	ProtocolVersion int    `json:"protocol_version"`
	BatchID         string `json:"batch_id"`
	WorkScopeIdentity
}

type WorkerBootstrapResponse struct {
	ProtocolVersion int       `json:"protocol_version"`
	BatchID         string    `json:"batch_id"`
	ClaimToken      string    `json:"claim_token"`
	ExpiresAt       time.Time `json:"expires_at"`
	WorkScopeIdentity
}

type WorkClaimRequest struct {
	ProtocolVersion       int    `json:"protocol_version"`
	BatchID               string `json:"batch_id"`
	WorkerID              string `json:"worker_id"`
	ScratchAvailableBytes int64  `json:"scratch_available_bytes,omitempty"`
	TaskBudgetBytes       int64  `json:"task_budget_bytes,omitempty"`
}

// ClaimAdmissionRequest changes only admission of future joined claims. It
// does not revoke operation tokens or alter already-leased work.
type ClaimAdmissionRequest struct {
	ProtocolVersion            int    `json:"protocol_version"`
	BatchID                    string `json:"batch_id"`
	ClaimsPaused               bool   `json:"claims_paused"`
	ExpectedActiveClaimsSHA256 string `json:"expected_active_claims_sha256,omitempty"`
	MaxNewClaims               int    `json:"max_new_claims,omitempty"`
}

type ClaimAdmissionStatus struct {
	ProtocolVersion         int       `json:"protocol_version"`
	BatchID                 string    `json:"batch_id"`
	ClaimsPaused            bool      `json:"claims_paused"`
	ActiveHourLeases        int64     `json:"active_hour_leases"`
	ActivePublicationLeases int64     `json:"active_publication_leases"`
	ActiveLeaseCount        int64     `json:"active_lease_count"`
	ActiveClaimsSHA256      string    `json:"active_claims_sha256,omitempty"`
	OneShotClaimsRemaining  int       `json:"one_shot_claims_remaining,omitempty"`
	UpdatedAt               time.Time `json:"updated_at"`
}

const JoinedScratchFixedBytes int64 = 256 << 20

func RequiredScratchBudgetBytes(sourceBytes int64) (int64, error) {
	if sourceBytes < 0 || sourceBytes > (1<<62)-JoinedScratchFixedBytes/2 {
		return 0, fmt.Errorf("invalid joined source byte count")
	}
	return sourceBytes*2 + JoinedScratchFixedBytes, nil
}

type WorkFailureRequest struct {
	ProtocolVersion int    `json:"protocol_version"`
	ScopeKind       string `json:"scope_kind"`
	ScopeID         string `json:"scope_id"`
	FailureClass    string `json:"failure_class"`
	ReasonCode      string `json:"reason_code"`
}

type WorkFailureResponse struct {
	ProtocolVersion int        `json:"protocol_version"`
	State           string     `json:"state"`
	AttemptCount    int        `json:"attempt_count"`
	NextAttemptAt   *time.Time `json:"next_attempt_at,omitempty"`
}

type LeaseStatusRequest struct {
	ProtocolVersion int      `json:"protocol_version"`
	BatchID         string   `json:"batch_id"`
	LeaseIDs        []string `json:"lease_ids"`
}

type LeaseStatus struct {
	LeaseID   string     `json:"lease_id"`
	Active    bool       `json:"active"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

type LeaseStatusResponse struct {
	ProtocolVersion int           `json:"protocol_version"`
	Leases          []LeaseStatus `json:"leases"`
}

type HeartbeatRequest struct {
	ProtocolVersion int    `json:"protocol_version"`
	ScopeKind       string `json:"scope_kind"`
	ScopeID         string `json:"scope_id"`
}

type HeartbeatResponse struct {
	ProtocolVersion int       `json:"protocol_version"`
	ScopeKind       string    `json:"scope_kind"`
	ScopeID         string    `json:"scope_id"`
	LeaseID         string    `json:"lease_id"`
	OperationToken  string    `json:"operation_token"`
	ExpiresAt       time.Time `json:"expires_at"`
}

type SourceCapabilityRequest struct {
	ProtocolVersion int    `json:"protocol_version"`
	HourID          string `json:"hour_id"`
	ClipID          int64  `json:"clip_id"`
	Operation       string `json:"operation"`
}

// SealHourRequest contains only worker-derived evidence. The backend loads
// and exact-compares the frozen batch, ledger, source, naming, policy, and tool
// facts before it constructs the canonical plan and manifest.
type SealHourRequest struct {
	ProtocolVersion   int                  `json:"protocol_version"`
	HourID            string               `json:"hour_id"`
	SourceClaimSHA256 string               `json:"source_claim_sha256"`
	AccountedSources  []SourceClip         `json:"accounted_sources"`
	Media             []SealHourMedia      `json:"media"`
	Quarantine        []QuarantineEvidence `json:"quarantine_evidence"`
}

type SealHourMedia struct {
	Ordinal            int                  `json:"ordinal"`
	SourceClipIDs      []int64              `json:"source_clip_ids"`
	SizeBytes          int64                `json:"size_bytes"`
	SHA256             string               `json:"sha256"`
	Verification       Verification         `json:"verification"`
	MaximalityEvidence []MaximalityEvidence `json:"maximality_evidence"`
}

// SealHourResponse is the exact sealed publication claim. An alias avoids a
// second hour schema drifting from the worker's publication contract.
type SealHourResponse = WorkerClaim

type ArtifactCapabilityRequest struct {
	ProtocolVersion int    `json:"protocol_version"`
	ScopeKind       string `json:"scope_kind"`
	ScopeID         string `json:"scope_id"`
	ArtifactID      int64  `json:"artifact_id"`
	Operation       string `json:"operation"`
}

// PublicationClaimRequest uses the same operator-selected batch and bounded
// worker identity as a preflight claim. The backend chooses the highest
// priority sealed ledger, hour, or final index; callers cannot choose a scope.
type PublicationClaimRequest = WorkClaimRequest

type PublicationClaimResponse struct {
	ProtocolVersion int                         `json:"protocol_version"`
	Kind            string                      `json:"kind"`
	Ledger          *LedgerPublicationClaim     `json:"ledger,omitempty"`
	Hour            *WorkerClaim                `json:"hour,omitempty"`
	BatchIndex      *BatchIndexPublicationClaim `json:"batch_index,omitempty"`
}

type FinalizeLedgerRequest struct {
	ProtocolVersion int             `json:"protocol_version"`
	Published       PublishedLedger `json:"published"`
}

type FinalizeHourRequest struct {
	ProtocolVersion int           `json:"protocol_version"`
	Published       PublishedHour `json:"published"`
}

type FinalizeBatchIndexRequest struct {
	ProtocolVersion int                 `json:"protocol_version"`
	Published       PublishedBatchIndex `json:"published"`
}

func (r WorkerBootstrapRequest) Validate() error {
	if r.ProtocolVersion != JoinedProtocolVersion || !safeBatchID.MatchString(r.BatchID) || r.WorkScopeIdentity.Validate(r.BatchID) != nil {
		return fmt.Errorf("invalid joined worker bootstrap request")
	}
	return nil
}

func (r WorkerBootstrapResponse) Validate(now time.Time) error {
	if r.ProtocolVersion != JoinedProtocolVersion || !safeBatchID.MatchString(r.BatchID) || !validOperationToken(r.ClaimToken) ||
		!r.ExpiresAt.After(now) || r.WorkScopeIdentity.Validate(r.BatchID) != nil {
		return fmt.Errorf("invalid joined worker bootstrap response")
	}
	return nil
}

func (r WorkClaimRequest) Validate() error {
	if r.ProtocolVersion != JoinedProtocolVersion || !safeBatchID.MatchString(r.BatchID) || !validWorkerID(r.WorkerID) ||
		r.ScratchAvailableBytes < 0 || r.TaskBudgetBytes < 0 ||
		((r.ScratchAvailableBytes == 0) != (r.TaskBudgetBytes == 0)) {
		return fmt.Errorf("invalid joined work claim request")
	}
	return nil
}

func (r ClaimAdmissionRequest) Validate() error {
	if r.ProtocolVersion != JoinedProtocolVersion || !safeBatchID.MatchString(r.BatchID) {
		return fmt.Errorf("invalid joined claim admission request")
	}
	oneShot := r.ExpectedActiveClaimsSHA256 != "" || r.MaxNewClaims != 0
	if oneShot && (!r.ClaimsPaused || r.MaxNewClaims != 1 || !lowerHex64(r.ExpectedActiveClaimsSHA256)) {
		return fmt.Errorf("invalid joined one-shot claim admission request")
	}
	return nil
}

func (s ClaimAdmissionStatus) Validate() error {
	if s.ProtocolVersion != JoinedProtocolVersion || !safeBatchID.MatchString(s.BatchID) || s.UpdatedAt.IsZero() ||
		s.ActiveHourLeases < 0 || s.ActivePublicationLeases < 0 ||
		s.ActiveLeaseCount != s.ActiveHourLeases+s.ActivePublicationLeases ||
		(s.ActiveClaimsSHA256 != "" && !lowerHex64(s.ActiveClaimsSHA256)) ||
		s.OneShotClaimsRemaining < 0 || s.OneShotClaimsRemaining > 1 {
		return fmt.Errorf("invalid joined claim admission status")
	}
	return nil
}

func (r WorkFailureRequest) Validate() error {
	if r.ProtocolVersion != JoinedProtocolVersion || !validScope(r.ScopeKind, r.ScopeID) ||
		(r.FailureClass != "transient" && r.FailureClass != "resource" && r.FailureClass != "deterministic") ||
		!safeReasonCode(r.ReasonCode) {
		return fmt.Errorf("invalid joined work failure request")
	}
	return nil
}

func (r LeaseStatusRequest) Validate() error {
	if r.ProtocolVersion != JoinedProtocolVersion || !safeBatchID.MatchString(r.BatchID) || len(r.LeaseIDs) == 0 || len(r.LeaseIDs) > 256 {
		return fmt.Errorf("invalid joined lease status request")
	}
	seen := make(map[string]bool, len(r.LeaseIDs))
	for _, raw := range r.LeaseIDs {
		if !validLeaseID(raw) || seen[raw] {
			return fmt.Errorf("invalid joined lease status request")
		}
		seen[raw] = true
	}
	return nil
}

func safeReasonCode(value string) bool {
	if value == "" || len(value) > 80 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, c := range value[1:] {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

func (r HeartbeatRequest) Validate() error {
	if r.ProtocolVersion != JoinedProtocolVersion || !validScope(r.ScopeKind, r.ScopeID) {
		return fmt.Errorf("invalid joined heartbeat request")
	}
	return nil
}

func (r HeartbeatResponse) Validate(now time.Time) error {
	if r.ProtocolVersion != JoinedProtocolVersion || !validScope(r.ScopeKind, r.ScopeID) || !validLeaseID(r.LeaseID) || !validOperationToken(r.OperationToken) || !r.ExpiresAt.After(now) {
		return fmt.Errorf("invalid joined heartbeat response")
	}
	return nil
}

func validWorkerID(workerID string) bool {
	return len(workerID) > 0 && len(workerID) <= 256 && strings.TrimSpace(workerID) == workerID && visibleASCII(workerID)
}

func validScope(kind, id string) bool {
	if strings.TrimSpace(id) == "" || len(id) > 1024 {
		return false
	}
	switch kind {
	case "hour", "ledger", "batch_index":
		return true
	default:
		return false
	}
}

func (r SealHourRequest) Validate(recordingID int64, toolIdentity string) error {
	if r.ProtocolVersion != JoinedProtocolVersion || r.HourID == "" || !lowerHex64(r.SourceClaimSHA256) || len(r.AccountedSources) == 0 || !lowerHex64(toolIdentity) {
		return fmt.Errorf("invalid joined hour seal request")
	}
	claimSHA, _, err := sourceClaimSHA(r.AccountedSources)
	if err != nil || claimSHA != r.SourceClaimSHA256 {
		return fmt.Errorf("joined hour seal source claim differs")
	}
	accounted, disposed := map[int64]bool{}, map[int64]bool{}
	for _, source := range r.AccountedSources {
		if accounted[source.ClipID] || validatePreflightSource(source, recordingID) != nil {
			return fmt.Errorf("joined hour seal source differs")
		}
		accounted[source.ClipID] = true
	}
	for i, media := range r.Media {
		if media.Ordinal != i+1 || media.SizeBytes <= 0 || !lowerHex64(media.SHA256) || len(media.SourceClipIDs) == 0 || validatePassedVerification(media.Verification) != nil {
			return fmt.Errorf("joined hour seal media differs")
		}
		for _, clipID := range media.SourceClipIDs {
			if !accounted[clipID] || disposed[clipID] {
				return fmt.Errorf("joined hour seal media source assignment differs")
			}
			disposed[clipID] = true
		}
		for _, evidence := range media.MaximalityEvidence {
			candidateSources, sourceErr := sourceSubsetByIDs(r.AccountedSources, evidence.CandidateClipIDs)
			expectedClaim, claimErr := candidateSourceClaimSHA(candidateSources)
			if sourceErr != nil || claimErr != nil || validateMaximalityEvidence(evidence, toolIdentity, expectedClaim) != nil {
				return fmt.Errorf("joined hour seal maximality differs")
			}
		}
	}
	for _, evidence := range r.Quarantine {
		quarantineSources, sourceErr := sourceSubsetByIDs(r.AccountedSources, evidence.SourceClipIDs)
		expectedClaim, claimErr := candidateSourceClaimSHA(quarantineSources)
		if sourceErr != nil || claimErr != nil {
			return fmt.Errorf("joined hour seal quarantine differs")
		}
		if evidenceErr := validateQuarantineEvidence(evidence, toolIdentity, expectedClaim); evidenceErr != nil {
			return fmt.Errorf("joined hour seal quarantine differs: %w", evidenceErr)
		}
		for _, clipID := range evidence.SourceClipIDs {
			if !accounted[clipID] || disposed[clipID] {
				return fmt.Errorf("joined hour seal quarantine source assignment differs")
			}
			disposed[clipID] = true
		}
	}
	if len(disposed) != len(accounted) {
		return fmt.Errorf("joined hour seal omits accounted sources")
	}
	return nil
}

func (r ArtifactCapabilityRequest) Validate() error {
	if r.ProtocolVersion != JoinedProtocolVersion || !validScope(r.ScopeKind, r.ScopeID) || r.ArtifactID <= 0 || (r.Operation != "put" && r.Operation != "read") {
		return fmt.Errorf("invalid joined artifact capability request")
	}
	return nil
}

func (r PublicationClaimResponse) Validate(now time.Time) error {
	present := 0
	if r.Ledger != nil {
		present++
	}
	if r.Hour != nil {
		present++
	}
	if r.BatchIndex != nil {
		present++
	}
	if r.ProtocolVersion != JoinedProtocolVersion || present != 1 {
		return fmt.Errorf("invalid joined publication claim response")
	}
	switch r.Kind {
	case "ledger":
		if r.Ledger == nil {
			return fmt.Errorf("joined publication claim kind differs")
		}
		_, _, err := r.Ledger.Validate(now)
		return err
	case "hour":
		if r.Hour == nil {
			return fmt.Errorf("joined publication claim kind differs")
		}
		return r.Hour.Validate(now)
	case "batch_index":
		if r.BatchIndex == nil {
			return fmt.Errorf("joined publication claim kind differs")
		}
		_, _, err := r.BatchIndex.Validate(now)
		return err
	default:
		return fmt.Errorf("invalid joined publication claim kind")
	}
}

func (r FinalizeLedgerRequest) Validate() error {
	if r.ProtocolVersion != JoinedProtocolVersion || r.Published.ArtifactID <= 0 || !safeObjectKey(r.Published.ObjectKey) || r.Published.SizeBytes <= 0 || !lowerHex64(r.Published.SHA256) || !validObjectIdentity(r.Published.ETag, r.Published.VersionID) {
		return fmt.Errorf("invalid joined ledger finalize request")
	}
	return nil
}

func (r FinalizeHourRequest) Validate() error {
	if r.ProtocolVersion != JoinedProtocolVersion || r.Published.HourID == "" || r.Published.RecordingID <= 0 || r.Published.LocalHour < 1 || r.Published.LocalHour > 12 || !safeObjectKey(r.Published.HourManifestObjectKey) || r.Published.HourManifestSizeBytes <= 0 || !lowerHex64(r.Published.HourManifestSHA256) || !validObjectIdentity(r.Published.HourManifestETag, r.Published.HourManifestVersionID) {
		return fmt.Errorf("invalid joined hour finalize request")
	}
	seen := map[int64]bool{}
	for _, output := range r.Published.Outputs {
		if output.ArtifactID <= 0 || seen[output.ArtifactID] || !safeObjectKey(output.ObjectKey) || output.SizeBytes <= 0 || !lowerHex64(output.SHA256) || !validObjectIdentity(output.ETag, output.VersionID) {
			return fmt.Errorf("invalid joined hour output finalize request")
		}
		seen[output.ArtifactID] = true
	}
	return nil
}

func (r FinalizeBatchIndexRequest) Validate() error {
	if r.ProtocolVersion != JoinedProtocolVersion || r.Published.ArtifactID <= 0 || !safeObjectKey(r.Published.ObjectKey) || r.Published.SizeBytes <= 0 || !lowerHex64(r.Published.SHA256) || !validObjectIdentity(r.Published.ETag, r.Published.VersionID) {
		return fmt.Errorf("invalid joined batch-index finalize request")
	}
	return nil
}

func (r SourceCapabilityRequest) Validate() error {
	if r.ProtocolVersion != JoinedProtocolVersion || r.HourID == "" || r.ClipID <= 0 || (r.Operation != "head" && r.Operation != "get") {
		return fmt.Errorf("invalid joined source capability request")
	}
	return nil
}
