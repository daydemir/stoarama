package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// This read-only proof has no lease, catalogue admission or retirement authority.
// Changing its pair requires another reviewed release, not an operator selector.
const nasStitchProofSourceSHA = "0dfa6b172f33bd55e409729ecd517d492cdee03427b028ac226788e530a72726"

type nasStitchProof struct {
	Kind          string        `json:"kind"`
	ID            int64         `json:"id"`
	SourcePlanSHA string        `json:"source_plan_sha256"`
	PlanSHA       string        `json:"plan_sha256"`
	Expires       time.Time     `json:"expires_at"`
	Plan          nasStitchPlan `json:"plan"`
}

func boundedNASStitchProof(p nasStitchPlan, sourceSHA string) (nasStitchProof, error) {
	source, err := json.Marshal(p)
	if err != nil || cleanupSHA(source) != sourceSHA {
		return nasStitchProof{}, errors.New("proof source bytes differ from immutable plan identity")
	}
	if sourceSHA != nasStitchProofSourceSHA || p.Policy != nasStitchPolicy || p.Revision != nasStitchRevision || p.CorpusSHA != nasStitchCorpusSHA || p.StreamID != 17208 || p.RecordingID != 444 || len(p.Clips) != 60 || len(p.FeatureInputs) != 60 || len(p.Frozen) != 59 || len(p.Joins) != 59 || len(p.Eligible) != 60 || !p.Joins[0] || !p.Eligible[0] || !p.Eligible[1] {
		return nasStitchProof{}, errors.New("reviewed proof source plan differs")
	}
	policy, err := cleanupEmbeddedPolicy()
	if err != nil || p.KeepSHA != policy.SHA {
		return nasStitchProof{}, errors.New("proof keep policy differs")
	}
	expectedIDs := [2]int64{373218, 373262}
	expectedSizes := [2]int64{11371311, 11249068}
	expectedSHAs := [2]string{"7bfea81fc9dd6fc83d15759b061a05b82f9b7a70a618e728d9a4e490a0534505", "600d2998f6799386ccf07be1687074e3a761babdd435ad0f79ce068fa7f166a4"}
	for n, c := range p.Clips[:2] {
		if c.SourceID != expectedIDs[n] || c.SizeBytes != expectedSizes[n] || c.SHA256 != expectedSHAs[n] {
			return nasStitchProof{}, errors.New("reviewed proof pair differs")
		}
	}
	edge, err := parseNASStitchEdge(p.Frozen[0])
	if err != nil || !edge.Nomination || edge.Status != "APPROVED85_nomination_requires_runtime_native_join_checks" {
		return nasStitchProof{}, errors.New("proof frozen seam not nominated")
	}
	p.Clips = p.Clips[:2]
	p.FeatureInputs = p.FeatureInputs[:2]
	p.Frozen = p.Frozen[:1]
	p.Eligible = p.Eligible[:2]
	p.Joins = p.Joins[:1]
	if !strings.Contains(p.OutputFolder, "_V85r_keep28/") {
		return nasStitchProof{}, errors.New("proof source namespace differs")
	}
	p.OutputFolder = strings.Replace(p.OutputFolder, "_V85r_keep28/", "_V85r_proof_keep28_v2/", 1) + "/proof-" + sourceSHA[:16]
	raw, err := json.Marshal(p)
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
	return nasStitchProof{Kind: "nas-only-v85r-proof-v1", ID: 1, SourcePlanSHA: sourceSHA, PlanSHA: cleanupSHA(raw), Plan: p}, nil
}

func (s *Server) nasStitchProof(ctx context.Context) (nasStitchProof, error) {
	if s.cfg.NASStitchProofTaskID != 1 || s.cfg.NASStitchEnabled || s.cfg.NASStitchDeletionEnabled {
		return nasStitchProof{}, errors.New("isolated proof selector inactive")
	}
	var raw []byte
	var sourceSHA string
	var until time.Time
	err := s.pool.QueryRow(ctx, `SELECT t.plan,t.plan_sha256,s.approved_until FROM nas_stitch_tasks t JOIN nas_stitch_scopes s ON s.id=t.scope_id WHERE t.id=1 AND t.recording_id=444 AND s.id=1 AND s.stream_id=17208 AND s.start_at<='2026-08-09T09:00:52.52Z' AND s.end_at>='2026-08-09T09:02:52.52Z' AND NOT s.paused AND NOT s.deletion_approved AND s.approved_until>now() AND NOT EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams)`).Scan(&raw, &sourceSHA, &until)
	if err != nil {
		return nasStitchProof{}, errors.New("proof scope or cleanup admission unavailable")
	}
	var p nasStitchPlan
	if json.Unmarshal(raw, &p) != nil {
		return nasStitchProof{}, errors.New("proof source plan unreadable")
	}
	result, err := boundedNASStitchProof(p, sourceSHA)
	if err != nil {
		return result, err
	}
	// Admission validity is short-lived; it does not renew or impersonate task1's lease.
	result.Expires = time.Now().UTC().Add(15 * time.Minute)
	if until.Before(result.Expires) {
		result.Expires = until
	}
	return result, nil
}

func (s *Server) handleAccountNASStitchProof(w http.ResponseWriter, r *http.Request) {
	cid, ok := s.nasRestoreConnection(r.Context(), w, r)
	if !ok {
		return
	}
	if cid != 13 {
		util.WriteError(w, 403, "proof connection not approved")
		return
	}
	s.writeNASStitchProof(w, r)
}
func (s *Server) handleAdminNASStitchProof(w http.ResponseWriter, r *http.Request) {
	s.writeNASStitchProof(w, r)
}
func (s *Server) writeNASStitchProof(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := cleanupExecutionContext(r.Context())
	defer cancel()
	proof, err := s.nasStitchProof(ctx)
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	util.WriteJSON(w, 200, proof)
}

func stitchProofSelector(selector int, connectionID int64, version string) int {
	if selector == 1 && connectionID == 13 && nasClientSupportsStitchProof(version) {
		return 1
	}
	return 0
}

// Receipt validation is read-only and grants neither writer nor deletion authority.
func (s *Server) handleAdminNASStitchProofVerify(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := cleanupExecutionContext(r.Context())
	defer cancel()
	var req struct {
		Result     nasStitchResult `json:"result"`
		ReceiptSHA string          `json:"receipt_sha256"`
		RunID      string          `json:"run_id"`
		ClientSHA  string          `json:"client_artifact_sha256"`
	}
	if util.DecodeJSON(r, &req) != nil || !stitchSHA(req.ReceiptSHA) || !stitchSHA(req.ClientSHA) || req.RunID == "" {
		util.WriteError(w, 400, "bounded proof validation request required")
		return
	}
	result := req.Result
	var raw []byte
	var sourceSHA string
	if s.pool.QueryRow(ctx, `SELECT t.plan,t.plan_sha256 FROM nas_stitch_tasks t JOIN nas_stitch_scopes s ON s.id=t.scope_id WHERE t.id=1 AND t.recording_id=444 AND s.id=1 AND s.stream_id=17208 AND s.start_at<='2026-08-09T09:00:52.52Z' AND s.end_at>='2026-08-09T09:02:52.52Z' AND NOT s.paused AND NOT s.deletion_approved AND s.approved_until>now()`).Scan(&raw, &sourceSHA) != nil {
		util.WriteError(w, 409, "proof source unavailable")
		return
	}
	var plan nasStitchPlan
	if json.Unmarshal(raw, &plan) != nil {
		util.WriteError(w, 409, "proof source unreadable")
		return
	}
	proof, err := boundedNASStitchProof(plan, sourceSHA)
	if err != nil || result.PlanSHA != proof.PlanSHA || len(result.Outputs) != 1 || len(result.Outputs[0].Inputs) != 2 || len(result.LeftOut) != 0 || len(result.Repairs) != 0 || result.Error != "" || result.Token != "" {
		util.WriteError(w, 409, "proof receipt binding differs")
		return
	}
	included, err := validateNASStitchResult(proof.Plan, result)
	if err != nil || len(included) != 2 {
		util.WriteError(w, 409, "complete native proof inclusion required")
		return
	}
	var status []byte
	var boot string
	if s.pool.QueryRow(ctx, `SELECT nas_stitch_status,client_boot_id FROM connections WHERE id=13 AND client_version=$1 AND nas_stitch_status_at>now()-interval '2 minutes' AND last_seen_at>now()-interval '2 minutes'`, nasCleanupProofClientVersion).Scan(&status, &boot) != nil {
		util.WriteError(w, 409, "fresh NAS proof readback unavailable")
		return
	}
	var readback struct {
		Kind       string `json:"kind"`
		State      string `json:"state"`
		At         string `json:"readback_at"`
		Boot       string `json:"readback_boot_id"`
		ReceiptSHA string `json:"receipt_sha256"`
		RunID      string `json:"run_id"`
		ClientSHA  string `json:"client_artifact_sha256"`
		OutputSHA  string `json:"output_sha256"`
		OutputSize int64  `json:"output_size_bytes"`
	}
	if json.Unmarshal(status, &readback) != nil {
		util.WriteError(w, 409, "NAS readback unreadable")
		return
	}
	at, err := time.Parse(time.RFC3339Nano, readback.At)
	now := time.Now().UTC()
	if err != nil || at.Before(now.Add(-2*time.Minute)) || at.After(now.Add(5*time.Minute)) || readback.Kind != "nas-only-v85r-proof-v1" || readback.State != "proof_registered_locally" || readback.Boot != boot || readback.ReceiptSHA != req.ReceiptSHA || readback.RunID != req.RunID || readback.ClientSHA != req.ClientSHA || readback.OutputSHA != result.Outputs[0].SHA || readback.OutputSize != result.Outputs[0].Size {
		util.WriteError(w, 409, "NAS proof readback binding differs or expired")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"verified": true, "kind": "nas-only-v85r-proof-v1", "plan_sha256": proof.PlanSHA, "output_sha256": result.Outputs[0].SHA, "registered_locally_only": true, "grants_retirement": false, "validated_at": now, "nas_readback_at": at, "client_boot_id": boot, "run_id": req.RunID, "receipt_sha256": req.ReceiptSHA, "client_artifact_sha256": req.ClientSHA})
}
