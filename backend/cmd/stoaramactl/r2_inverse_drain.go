package main

import (
	"errors"
	"strings"
	"time"
)

type inverseDrainProof string

const (
	inversePUTCompletionProof  inverseDrainProof = "accepted_put_completion_v1"
	inverseOwnerFenceProof     inverseDrainProof = "owner_fence_policy_20261118_v1"
	inverseOwnerDecisionSHA256                   = "5996aaeeaf90d128ee0ead06be876fe0365d41c5a7610f9f72c7ad09a83bdccb"
)

type inverseFenceResidual string

// Owner acceptance of a PUT racing final HEAD/unconditional DELETE or
// reappearing afterward. This never claims external PUT completion.
const inverseApprovedFenceResidual inverseFenceResidual = "owner_accepted_head_delete_overwrite_reappearance_v1"

type inverseFencePolicyEvidence struct {
	PartitionSHA256    string                   `json:"partition_sha256"`
	SummarySHA256      string                   `json:"summary_sha256"`
	LastModifiedBefore time.Time                `json:"last_modified_before"`
	OwnerDecision      inverseEvidenceReference `json:"owner_decision"`
	ResidualAcceptance inverseFenceResidual     `json:"residual_acceptance"`
}

type inverseEvidenceReference struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func (r inverseEvidenceReference) verify() error {
	if strings.TrimSpace(r.Path) == "" || !r2RetireLowerHex64(r.SHA256) {
		return errors.New("pinned controller evidence reference required")
	}
	digest, err := inverseFileSHA(r.Path)
	if err != nil {
		return err
	}
	if digest != r.SHA256 {
		return errors.New("controller evidence digest changed")
	}
	return nil
}

func inverseRetirementCutoff() time.Time {
	return time.Date(2026, 11, 12, 14, 28, 47, 0, time.UTC)
}

func inverseLastModifiedAllowed(modified, partition, now time.Time) bool {
	return !modified.IsZero() && modified.Before(inverseRetirementCutoff()) && modified.Before(partition) && modified.Before(now)
}

func (e inverseDrainEvidence) validateProof(o inverseApplyOptions) error {
	switch e.Proof {
	case inversePUTCompletionProof:
		if e.FencePolicy != nil || strings.TrimSpace(e.AcceptedPUTCompletion) == "" || strings.TrimSpace(e.DeployedWriterCoverage) == "" {
			return errors.New("accepted-PUT completion evidence missing or mixed with owner policy")
		}
	case inverseOwnerFenceProof:
		p := e.FencePolicy
		if p == nil || e.AcceptedPUTCompletion != "" || e.DeployedWriterCoverage != "" {
			return errors.New("owner fence policy must not claim PUT completion or writer coverage")
		}
		if e.Bucket != "stoarama" || !e.FenceActivatedAt.Truncate(time.Second).Equal(inverseRetirementCutoff()) || !p.LastModifiedBefore.Equal(inverseRetirementCutoff()) || !e.DrainNotBefore.After(e.FenceActivatedAt) {
			return errors.New("owner fence policy cutoff or activation differs")
		}
		if !r2RetireLowerHex64(p.PartitionSHA256) || p.PartitionSHA256 != o.ManifestSHA || !r2RetireLowerHex64(p.SummarySHA256) || p.SummarySHA256 != o.SummarySHA || p.ResidualAcceptance != inverseApprovedFenceResidual || p.OwnerDecision.SHA256 != inverseOwnerDecisionSHA256 {
			return errors.New("owner fence policy partition binding, decision or race acceptance missing")
		}
		if err := p.OwnerDecision.verify(); err != nil {
			return err
		}
	default:
		return errors.New("explicit supported writer proof required")
	}
	return nil
}
