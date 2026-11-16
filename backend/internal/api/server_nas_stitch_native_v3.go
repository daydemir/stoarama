package api

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/stitchproof"
)

// Each genuine attempt gets a new owned directory. A restart or later reviewed
// attempt cannot overwrite the audits/publication of an interrupted run. The
// original task bytes, run ID and deadline must be preserved by the NAS ledger.
func boundedNASStitchNativeV3(p nasStitchPlan, sourceSHA, runID string) (nasStitchProof, error) {
	if !nasStitchReadbackUUID(runID) {
		return nasStitchProof{}, errors.New("canonical native run ID required")
	}
	proof, err := boundedNASStitchProof(p, sourceSHA)
	if err != nil {
		return proof, err
	}
	if strings.Count(proof.Plan.OutputFolder, "_V85r_proof_keep28_v2/") != 1 {
		return nasStitchProof{}, errors.New("native source namespace differs")
	}
	proof.Plan.OutputFolder = strings.Replace(proof.Plan.OutputFolder, "_V85r_proof_keep28_v2/", "_V85r_proof_keep28_v3/", 1) + "/run-" + runID
	raw, err := json.Marshal(proof.Plan)
	if err != nil {
		return nasStitchProof{}, err
	}
	var canonical any
	if err = json.Unmarshal(raw, &canonical); err != nil {
		return nasStitchProof{}, err
	}
	raw, err = json.Marshal(canonical)
	if err != nil {
		return nasStitchProof{}, err
	}
	proof.PlanSHA = cleanupSHA(raw)
	return proof, nil
}

// Exact task bytes are independent of logical plan identity. Activation/expiry
// live outside the plan and must never be authenticated by PlanSHA alone.
func validateNASStitchReadbackTask(raw []byte, sha, activation, run string, producer stitchproof.ClientVersion, source nasStitchPlan, sourceSHA string) (nasStitchProof, error) {
	var task nasStitchProof
	if len(raw) == 0 || len(raw) > 1024*1024 || !stitchSHA(sha) || cleanupSHA(raw) != sha || stitchproof.DecodeExact(raw, &task) != nil {
		return task, errors.New("original native task bytes differ")
	}
	expected, err := boundedNASStitchProof(source, sourceSHA)
	if producer == stitchproof.ClientCopyV3 {
		if !nasStitchReadbackUUID(activation) || task.ActivationID != activation {
			return task, errors.New("original native activation differs")
		}
		expected, err = boundedNASStitchNativeV3(source, sourceSHA, run)
	} else if producer != stitchproof.ClientNativeV2 || activation != "" || task.ActivationID != "" {
		return task, errors.New("original producer activation protocol differs")
	}
	actualPlan, _ := json.Marshal(task.Plan)
	expectedPlan, _ := json.Marshal(expected.Plan)
	var fields map[string]json.RawMessage
	var expiry string
	json.Unmarshal(raw, &fields)
	json.Unmarshal(fields["expires_at"], &expiry)
	if _, timeErr := parseNASStitchReadbackUTC(expiry); timeErr != nil || err != nil || task.Kind != expected.Kind || task.ID != expected.ID ||
		task.SourcePlanSHA != sourceSHA || task.PlanSHA != expected.PlanSHA || string(actualPlan) != string(expectedPlan) {
		return task, errors.New("original task plan/expiry binding differs")
	}
	return task, nil
}
