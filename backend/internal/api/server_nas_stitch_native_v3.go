package api

import (
	"encoding/json"
	"errors"
	"strings"
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
