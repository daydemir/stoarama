package joinedrecording

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

const (
	Tier1FrozenAt       = "2026-08-21T06:59:07.534131Z"
	Tier1RecordingIDSHA = "6038d4a23be9b0b5c2bb29ea933743a5ceb7f06b8875e417a3f16b44051ebd71"
	PlannerAdvisoryLock = "recording_joined_output_planner"
)

var Tier1RecordingIDs = []int64{377, 335, 337, 355, 385, 350, 382, 384, 348, 403, 380, 379, 383, 404, 401, 408, 406, 409, 422, 418, 419, 413, 420, 428, 423, 425, 416, 421, 437, 440, 429, 431, 439}

type CompletionEvidence struct {
	RecordingID int64     `json:"recording_id"`
	JobID       int64     `json:"job_id"`
	WindowEnd   time.Time `json:"window_end"`
	CompletedAt time.Time `json:"completed_at"`
	QualityTier string    `json:"quality_tier"`
}

type CampaignManifest struct {
	CampaignID          string               `json:"campaign_id"`
	Tier                int                  `json:"tier"`
	FrozenAt            string               `json:"frozen_at"`
	RecordingIDs        []int64              `json:"recording_ids"`
	RecordingIDSHA256   string               `json:"recording_ids_sha256"`
	CompletionEvidence  []CompletionEvidence `json:"completion_evidence"`
	ExpectedOutputCount int                  `json:"expected_output_count"`
	PlanSHA256          string               `json:"plan_sha256"`
}

func Tier1Payload() []byte {
	var b strings.Builder
	for _, id := range Tier1RecordingIDs {
		b.WriteString(strconv.FormatInt(id, 10))
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

func ValidateTier1Campaign(m CampaignManifest) error {
	if !safeCampaignID.MatchString(m.CampaignID) || m.Tier != 1 || m.FrozenAt != Tier1FrozenAt || m.RecordingIDSHA256 != Tier1RecordingIDSHA || len(m.RecordingIDs) != len(Tier1RecordingIDs) || len(m.CompletionEvidence) != len(Tier1RecordingIDs) {
		return fmt.Errorf("campaign does not match frozen tier-1 identity")
	}
	for i, id := range Tier1RecordingIDs {
		if m.RecordingIDs[i] != id || m.CompletionEvidence[i].RecordingID != id || m.CompletionEvidence[i].JobID <= 0 || m.CompletionEvidence[i].WindowEnd.IsZero() || m.CompletionEvidence[i].CompletedAt.Before(m.CompletionEvidence[i].WindowEnd) || m.CompletionEvidence[i].QualityTier != "good+" {
			return fmt.Errorf("invalid frozen tier-1 evidence at ordinal %d", i+1)
		}
		if i > 0 && m.CompletionEvidence[i].CompletedAt.Before(m.CompletionEvidence[i-1].CompletedAt) {
			return fmt.Errorf("tier-1 completion evidence is not in frozen oldest-first order")
		}
	}
	sum := sha256.Sum256(Tier1Payload())
	if hex.EncodeToString(sum[:]) != Tier1RecordingIDSHA {
		return fmt.Errorf("compiled tier-1 identity hash mismatch")
	}
	if m.ExpectedOutputCount <= 0 || !lowerHex64(m.PlanSHA256) {
		return fmt.Errorf("campaign is not sealed")
	}
	return nil
}

func SealCampaign(m CampaignManifest, plans []BatchPlan) (CampaignManifest, error) {
	if len(plans) == 0 {
		return CampaignManifest{}, fmt.Errorf("cannot seal empty campaign")
	}
	identity := m
	identity.ExpectedOutputCount = 1
	identity.PlanSHA256 = strings.Repeat("a", 64)
	if err := ValidateTier1Campaign(identity); err != nil {
		return CampaignManifest{}, err
	}
	m.ExpectedOutputCount = 0
	m.PlanSHA256 = ""
	ranks := map[int64]int{}
	for i, id := range Tier1RecordingIDs {
		ranks[id] = i
	}
	seenRecordings := map[int64]bool{}
	seenObjects := map[string]bool{}
	seenSources := map[int64]bool{}
	lastRank := -1
	for _, plan := range plans {
		rank, ok := ranks[plan.RecordingID]
		if !ok || rank < lastRank || plan.CampaignID != m.CampaignID || ValidatePlan(plan) != nil {
			return CampaignManifest{}, fmt.Errorf("campaign contains unsealed batch")
		}
		lastRank = rank
		seenRecordings[plan.RecordingID] = true
		for _, output := range plan.Outputs {
			if seenObjects[output.ObjectKey] {
				return CampaignManifest{}, fmt.Errorf("tier-1 campaign repeats an output key")
			}
			seenObjects[output.ObjectKey] = true
			for _, source := range output.Sources {
				if seenSources[source.ClipID] {
					return CampaignManifest{}, fmt.Errorf("tier-1 campaign repeats a source clip")
				}
				seenSources[source.ClipID] = true
			}
		}
		m.ExpectedOutputCount += plan.ExpectedOutputCount
	}
	if len(seenRecordings) != len(Tier1RecordingIDs) {
		return CampaignManifest{}, fmt.Errorf("tier-1 campaign omits frozen recordings")
	}
	digest, _, err := stitchcert.CanonicalSHA(struct {
		CampaignID         string               `json:"campaign_id"`
		Tier               int                  `json:"tier"`
		FrozenAt           string               `json:"frozen_at"`
		RecordingIDs       []int64              `json:"recording_ids"`
		RecordingIDSHA256  string               `json:"recording_ids_sha256"`
		CompletionEvidence []CompletionEvidence `json:"completion_evidence"`
		BatchPlanSHA256    []string             `json:"batch_plan_sha256"`
	}{m.CampaignID, m.Tier, m.FrozenAt, m.RecordingIDs, m.RecordingIDSHA256, m.CompletionEvidence, batchDigests(plans)})
	if err != nil {
		return CampaignManifest{}, err
	}
	m.PlanSHA256 = digest
	return m, nil
}

func batchDigests(plans []BatchPlan) []string {
	out := make([]string, len(plans))
	for i := range plans {
		out[i] = plans[i].PlanSHA256
	}
	return out
}
