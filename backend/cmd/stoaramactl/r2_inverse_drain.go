package main

import (
	"errors"
	"strings"
	"time"
)

type inverseDrainProof string

const (
	inversePUTCompletionProof  inverseDrainProof = "accepted_put_completion_v1"
	inverseNon28KeyPolicyProof inverseDrainProof = "non28_key_policy_v1"
)

type inverseKeyPolicyResidual string

// This is an explicit B1-only acceptance of an already-issued PUT racing the
// final HEAD/unconditional DELETE. It is not evidence of remote completion.
const inverseApprovedKeyResidual inverseKeyPolicyResidual = "approved_key_overwrite_reappearance_delivery_order_v1"

type inverseKeyPolicyEvidence struct {
	PartitionSHA256                 string                   `json:"partition_sha256"`
	SummarySHA256                   string                   `json:"summary_sha256"`
	IssuanceBoundEvidence           inverseEvidenceReference `json:"issuance_bound_evidence"`
	OperatorSigningEvidence         inverseEvidenceReference `json:"operator_signing_evidence"`
	OperatorSigningStoppedAt        time.Time                `json:"operator_signing_stopped_at"`
	OperatorSigningObservedThrough  time.Time                `json:"operator_signing_observed_through"`
	OperatorLastCapabilityExpiresAt time.Time                `json:"operator_last_capability_expires_at"`
	ResidualAcceptance              inverseKeyPolicyResidual `json:"residual_acceptance"`
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

func (e inverseDrainEvidence) validateProof(o inverseApplyOptions) error {
	switch e.Proof {
	case inversePUTCompletionProof:
		if e.KeyPolicy != nil || strings.TrimSpace(e.AcceptedPUTCompletion) == "" {
			return errors.New("accepted-PUT completion evidence missing or mixed with key-policy proof")
		}
	case inverseNon28KeyPolicyProof:
		p := e.KeyPolicy
		if o.Lane != inverseOutsideKeep28 || p == nil || e.AcceptedPUTCompletion != "" {
			return errors.New("key-policy proof is exclusive to B1 non-keep28 RAW")
		}
		if !r2RetireLowerHex64(p.PartitionSHA256) || p.PartitionSHA256 != o.ManifestSHA || !r2RetireLowerHex64(p.SummarySHA256) || p.SummarySHA256 != o.SummarySHA || p.ResidualAcceptance != inverseApprovedKeyResidual {
			return errors.New("key-policy partition binding, issuance evidence or residual acceptance missing")
		}
		for _, ref := range []inverseEvidenceReference{p.IssuanceBoundEvidence, p.OperatorSigningEvidence} {
			if err := ref.verify(); err != nil {
				return err
			}
		}
		// The controller must substantiate the unchanged seven-day gate and the
		// separate two-hour repair/operator signing window, not restamp expiry
		// as proof that accepted requests have completed.
		if !e.DrainNotBefore.Equal(e.FenceActivatedAt.Add(7*24*time.Hour+2*time.Minute)) || p.OperatorSigningStoppedAt.IsZero() || p.OperatorSigningStoppedAt.After(e.DrainNotBefore.Add(-2*time.Hour)) || p.OperatorSigningObservedThrough.IsZero() || !p.OperatorSigningObservedThrough.Equal(e.ObservedAt) || p.OperatorLastCapabilityExpiresAt.IsZero() || p.OperatorLastCapabilityExpiresAt.After(e.DrainNotBefore) {
			return errors.New("key-policy fence or operator capability window not substantiated")
		}
	default:
		return errors.New("explicit supported writer proof required")
	}
	return nil
}
