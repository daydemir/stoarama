package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/daydemir/stoarama/backend/internal/stitchproof"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type nasStitchReadbackBinding struct {
	NativeTaskSHA       string                   `json:"native_task_sha256"`
	NativeActivationID  string                   `json:"native_activation_id,omitempty"`
	NativeRunID         string                   `json:"native_run_id"`
	NativeBundleSHA     string                   `json:"native_bundle_sha256"`
	NativePlanSHA       string                   `json:"native_plan_sha256"`
	ProducerSHA         string                   `json:"producer_profile_sha256"`
	ReaderSHA           string                   `json:"reader_profile_sha256"`
	PairSHA             string                   `json:"profile_pair_sha256"`
	ReadbackSHA         string                   `json:"readback_receipt_sha256"`
	Reader              nasStitchReaderExecution `json:"reader_execution"`
	HeartbeatAt         nasStitchWireTime        `json:"reader_heartbeat_received_at"`
	Stable              bool                     `json:"reader_release_stable"`
	Candidate           bool                     `json:"reader_release_candidate"`
	InventoryMismatches int64                    `json:"reader_inventory_mismatch_count"`
	Server              nasStitchServerExecution `json:"server_execution"`
	Scope               stitchproof.PayloadScope `json:"scope"`
	Outputs             []stitchproof.Media      `json:"outputs"`
	Policy              stitchproof.Policy       `json:"policy"`
	Tools               stitchproof.PayloadTools `json:"tools"`
	LocalOnly           bool                     `json:"registered_locally_only"`
	GrantsRetirement    bool                     `json:"grants_retirement"`
}

type nasStitchReadbackValidation struct {
	Schema      string            `json:"schema"`
	ValidatedAt nasStitchWireTime `json:"validated_at"`
	nasStitchReadbackBinding
}

type nasStitchReadbackCurrent struct {
	Schema        string            `json:"schema"`
	CheckedAt     nasStitchWireTime `json:"checked_at"`
	StatusAt      nasStitchWireTime `json:"status_received_at"`
	VerifiedAt    string            `json:"readback_verified_at"`
	ID            string            `json:"readback_id"`
	ApprovedUntil nasStitchWireTime `json:"source_scope_approved_until"`
	Integrity     string            `json:"integrity"`
	nasStitchReadbackBinding
}

func nasStitchReadbackScope(planSHA string) stitchproof.PayloadScope {
	return stitchproof.PayloadScope{StreamID: 17208, RecordingID: 444, SourceTaskID: 1, SourcePlanSHA256: nasStitchProofSourceSHA, ProofPlanSHA256: planSHA, ClipIDs: []int64{373218, 373262}}
}

func bindNASStitchReadback(receipt nasStitchReadbackReceipt, sha string, snapshot nasStitchReadbackSnapshot, server nasStitchServerExecution) nasStitchReadbackBinding {
	return nasStitchReadbackBinding{NativeTaskSHA: receipt.NativeTaskSHA, NativeActivationID: receipt.NativeActivationID, NativeRunID: receipt.NativeRunID, NativeBundleSHA: receipt.NativeBundleSHA, NativePlanSHA: receipt.NativePlanSHA,
		ProducerSHA: receipt.ProducerProfileSHA, ReaderSHA: receipt.ReaderProfileSHA, PairSHA: receipt.PairSHA, ReadbackSHA: sha, Reader: receipt.Reader,
		HeartbeatAt: nasStitchWireTime(snapshot.Facts.HeartbeatAt.Time), Stable: snapshot.Facts.Host.Release.Stable,
		Candidate: snapshot.Facts.Host.Release.Candidate, InventoryMismatches: snapshot.Facts.InventoryMismatches.Int64,
		Server: server, Scope: nasStitchReadbackScope(receipt.NativePlanSHA), Outputs: receipt.Outputs, Policy: receipt.Policy, Tools: receipt.Tools, LocalOnly: true, GrantsRetirement: false}
}

func acceptedNASStitchReadback(receipt nasStitchReadbackReceipt) (stitchproof.AcceptedPair, error) {
	var key stitchproof.PairKey
	var err error
	key.Producer, err = stitchproof.ParseDigest(receipt.ProducerProfileSHA)
	if err != nil {
		return stitchproof.AcceptedPair{}, err
	}
	key.Reader, err = stitchproof.ParseDigest(receipt.ReaderProfileSHA)
	if err != nil {
		return stitchproof.AcceptedPair{}, err
	}
	key.Pair, err = stitchproof.ParseDigest(receipt.PairSHA)
	if err != nil {
		return stitchproof.AcceptedPair{}, err
	}
	pair, err := stitchproof.LookupAcceptedPair(key)
	if err != nil {
		return pair, errors.New("producer/current-reader pair is not accepted")
	}
	if (pair.ProducerIdentity().Version() == stitchproof.ClientCopyV3 && !nasStitchReadbackUUID(receipt.NativeActivationID)) ||
		(pair.ProducerIdentity().Version() == stitchproof.ClientNativeV2 && receipt.NativeActivationID != "") {
		return pair, errors.New("producer activation protocol differs")
	}
	r := pair.ReaderIdentity()
	if pair.Key() != key || string(r.Version()) != receipt.Reader.Version || r.ArtifactSHA256().String() != receipt.Reader.ArtifactSHA || r.SizeBytes() != receipt.Reader.ArtifactSize {
		return stitchproof.AcceptedPair{}, errors.New("current reader differs from sealed profile")
	}
	return pair, nil
}

// Admission is metadata-only and never a task1 lease. Native and readback use
// distinct modes. Changing mode cannot turn missing preserved evidence into work.
func (s *Server) handleAccountNASStitchEvidenceAdmission(w http.ResponseWriter, r *http.Request) {
	mode := nasStitchProofMode(s.cfg.NASStitchProofTaskID)
	purpose := nasConnectionMetadata
	if mode == nasStitchProofNative {
		purpose = nasConnectionProof
	}
	cid, ok := s.nasConnection(r.Context(), w, r, purpose)
	if !ok {
		return
	}
	if cid != 13 {
		util.WriteError(w, 403, "proof connection not approved")
		return
	}
	query, err := parseNASStitchAdmissionQuery(r, mode)
	if err != nil || s.cfg.NASStitchEnabled || s.cfg.NASStitchDeletionEnabled ||
		(mode == nasStitchProofNative && (!nasStitchReadbackUUID(s.cfg.NASStitchProofActivationID) || query.activation != s.cfg.NASStitchProofActivationID)) {
		util.WriteError(w, 409, "isolated evidence mode inactive or request differs")
		return
	}
	snapshot, err := s.nasStitchReadbackSnapshot(r.Context())
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	if snapshot.Frozen {
		util.WriteError(w, 409, "proof yielded to cleanup")
		return
	}
	status, err := decodeNASStitchReaderStatus(snapshot.Status["stitch_reader"], snapshot)
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	pair, err := selectNASStitchReadbackPair(mode, status.Reader.Version, status.Reader.ArtifactSHA, status.Reader.ArtifactSize, query.producer)
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	components, err := nasStitchPairComponents(pair)
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	response, err := buildNASStitchAdmissionResponse(snapshot, components, mode, query)
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	writeNASStitchWireJSON(w, response)
}

type nasStitchAdmissionQuery struct{ run, producer, activation string }

func urlParseNASStitchQuery(r *http.Request) (url.Values, error) {
	if len(r.URL.RawQuery) > 256 {
		return nil, errors.New("bounded fixed query required")
	}
	return url.ParseQuery(r.URL.RawQuery)
}
func parseNASStitchAdmissionQuery(r *http.Request, mode nasStitchProofMode) (nasStitchAdmissionQuery, error) {
	var value nasStitchAdmissionQuery
	q, err := urlParseNASStitchQuery(r)
	if err != nil {
		return value, err
	}
	if mode == nasStitchProofNative && len(q) == 2 && len(q["run_id"]) == 1 && nasStitchReadbackUUID(q.Get("run_id")) && len(q["activation_id"]) == 1 && nasStitchReadbackUUID(q.Get("activation_id")) {
		value.run, value.activation = q.Get("run_id"), q.Get("activation_id")
		return value, nil
	}
	if mode == nasStitchProofReadback && len(q) == 1 && len(q["producer_profile_sha256"]) == 1 && stitchSHA(q.Get("producer_profile_sha256")) {
		value.producer = q.Get("producer_profile_sha256")
		return value, nil
	}
	return value, errors.New("exact mode-specific admission identity required")
}

func (s *Server) handleAdminNASStitchReadbackStatus(w http.ResponseWriter, r *http.Request) {
	if s.cfg.NASStitchEnabled || s.cfg.NASStitchDeletionEnabled {
		util.WriteError(w, 409, "isolated proof contract inactive")
		return
	}
	q, err := urlParseNASStitchQuery(r)
	if err != nil || len(q) != 1 || len(q["receipt_sha256"]) != 1 || !stitchSHA(q.Get("receipt_sha256")) {
		util.WriteError(w, 400, "exact receipt SHA required")
		return
	}
	snapshot, err := s.nasStitchReadbackSnapshot(r.Context())
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	status, err := decodeNASStitchReaderStatus(snapshot.Status["stitch_reader"], snapshot)
	if err != nil || status.Native == nil {
		util.WriteError(w, 409, "current preserved native evidence unavailable")
		return
	}
	n := status.Native
	for _, entry := range status.History {
		if entry.SHA != q.Get("receipt_sha256") {
			continue
		}
		receipt := nasStitchReadbackReceipt{NativeTaskSHA: n.TaskSHA, NativeActivationID: n.ActivationID, Schema: nasStitchReadbackSchema, NativeRunID: n.RunID, NativeBundleSHA: n.BundleSHA, NativePlanSHA: n.PlanSHA,
			ProducerProfileSHA: n.ProducerSHA, ReaderProfileSHA: n.ReaderSHA, PairSHA: n.PairSHA, ID: entry.ID, VerifiedAt: entry.VerifiedAt,
			Reader: status.Reader, Outputs: n.Outputs, Policy: n.Policy, Tools: n.Tools, NativeRegistration: n.RegistrationSHA}
		receipt.StartedAt = receipt.VerifiedAt // projection only; current-context does not emit a started_at
		projected, _ := json.Marshal(receipt)
		if _, err = decodeNASStitchReadback(projected); err != nil {
			util.WriteError(w, 409, "preserved current-context fields differ")
			return
		}
		if _, err = acceptedNASStitchReadback(receipt); err != nil {
			util.WriteError(w, 409, err.Error())
			return
		}
		if err = validateNASStitchReadbackReader(receipt, status.Reader, snapshot.Facts, snapshot.Now); err != nil {
			util.WriteError(w, 409, err.Error())
			return
		}
		server, err := nasStitchReadbackServerExecution()
		if err != nil {
			util.WriteError(w, 409, err.Error())
			return
		}
		value := nasStitchReadbackCurrent{Schema: "stoarama.nas-stitch-current-context.v1", CheckedAt: nasStitchWireTime(snapshot.Now),
			StatusAt: nasStitchWireTime(snapshot.Facts.StatusAt.Time), VerifiedAt: entry.VerifiedAt, ID: entry.ID,
			ApprovedUntil: nasStitchWireTime(snapshot.ApprovedUntil), Integrity: "clear", nasStitchReadbackBinding: bindNASStitchReadback(receipt, entry.SHA, snapshot, server)}
		w.Header().Set("Cache-Control", "private, no-store")
		writeNASStitchWireJSON(w, value)
		return
	}
	util.WriteError(w, 409, "requested readback absent from current reader history")
}

type nasStitchReadbackValidateRequest struct {
	NativeTaskB64   string `json:"native_task_b64"`
	NativeTaskSHA   string `json:"native_task_sha256"`
	Schema          string `json:"schema"`
	ReadbackB64     string `json:"readback_b64"`
	ReadbackSHA     string `json:"readback_receipt_sha256"`
	NativeResultB64 string `json:"native_result_b64"`
	NativeResultSHA string `json:"native_result_sha256"`
}

func decodeNASStitchReadbackComponent(text, sha string, limit int) ([]byte, error) {
	if len(text) == 0 || len(text) > base64.StdEncoding.EncodedLen(limit) || !stitchSHA(sha) {
		return nil, errors.New("bounded checksummed component required")
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(text)
	if err != nil || len(raw) == 0 || len(raw) > limit || base64.StdEncoding.EncodeToString(raw) != text || cleanupSHA(raw) != sha {
		return nil, errors.New("canonical component bytes differ")
	}
	return raw, nil
}

func (s *Server) handleAdminNASStitchReadbackValidate(w http.ResponseWriter, r *http.Request) {
	if s.cfg.NASStitchEnabled || s.cfg.NASStitchDeletionEnabled {
		util.WriteError(w, 409, "isolated proof contract inactive")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 2*1024*1024+1))
	var req nasStitchReadbackValidateRequest
	if err != nil || len(raw) > 2*1024*1024 || stitchproof.DecodeExact(raw, &req) != nil || req.Schema != "stoarama.nas-stitch-validation-request.v2" {
		util.WriteError(w, 400, "strict bounded validation request required")
		return
	}
	readback, err := decodeNASStitchReadbackComponent(req.ReadbackB64, req.ReadbackSHA, 65536)
	if err != nil {
		util.WriteError(w, 400, err.Error())
		return
	}
	resultRaw, err := decodeNASStitchReadbackComponent(req.NativeResultB64, req.NativeResultSHA, 65536)
	if err != nil {
		util.WriteError(w, 400, err.Error())
		return
	}
	taskRaw, err := decodeNASStitchReadbackComponent(req.NativeTaskB64, req.NativeTaskSHA, 1024*1024)
	if err != nil {
		util.WriteError(w, 400, err.Error())
		return
	}
	receipt, err := decodeNASStitchReadback(readback)
	if err != nil || receipt.NativeTaskSHA != req.NativeTaskSHA {
		util.WriteError(w, 400, "native readback/task binding differs")
		return
	}
	pair, err := acceptedNASStitchReadback(receipt)
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	snapshot, err := s.nasStitchReadbackSnapshot(r.Context())
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	status, err := decodeNASStitchReaderStatus(snapshot.Status["stitch_reader"], snapshot)
	if err != nil || status.Native == nil || status.Native.ResultSHA != req.NativeResultSHA || matchNASStitchReadbackStatus(receipt, req.ReadbackSHA, status) != nil || validateNASStitchReadbackReader(receipt, status.Reader, snapshot.Facts, snapshot.Now) != nil {
		util.WriteError(w, 409, "current native/readback relationship differs")
		return
	}
	proof, err := validateNASStitchReadbackTask(taskRaw, receipt.NativeTaskSHA, receipt.NativeActivationID, receipt.NativeRunID, pair.ProducerIdentity().Version(), snapshot.SourcePlan, snapshot.SourceSHA)
	if err != nil || proof.PlanSHA != receipt.NativePlanSHA {
		util.WriteError(w, 409, "original native plan differs")
		return
	}
	if err = validateNASStitchReadbackResult(resultRaw, proof.Plan, receipt); err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	server, err := nasStitchReadbackServerExecution()
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	value := nasStitchReadbackValidation{Schema: "stoarama.nas-stitch-validation.v2", ValidatedAt: nasStitchWireTime(snapshot.Now), nasStitchReadbackBinding: bindNASStitchReadback(receipt, req.ReadbackSHA, snapshot, server)}
	w.Header().Set("Cache-Control", "private, no-store")
	writeNASStitchWireJSON(w, value)
}

// Preserve the full original native result bytes/hash. Only project its extra
// media_seconds field into the unchanged catalogue-independent result validator.
func validateNASStitchReadbackResult(raw []byte, plan nasStitchPlan, receipt nasStitchReadbackReceipt) error {
	var value map[string]json.RawMessage
	if stitchproof.DecodeExact(raw, &value) != nil || len(value) != 7 {
		return errors.New("full native result fields differ")
	}
	for _, name := range []string{"policy", "plan_sha256", "outputs", "repairs", "left_out", "seams", "error"} {
		if _, ok := value[name]; !ok {
			return errors.New("full native result incomplete")
		}
	}
	var outputs []map[string]json.RawMessage
	if json.Unmarshal(value["outputs"], &outputs) != nil || len(outputs) != 1 {
		return errors.New("real two-input native join required")
	}
	var seconds float64
	if json.Unmarshal(outputs[0]["media_seconds"], &seconds) != nil || seconds <= 0 || seconds > 121 {
		return errors.New("bounded native media duration required")
	}
	delete(outputs[0], "media_seconds")
	value["outputs"], _ = json.Marshal(outputs)
	projected, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var result nasStitchResult
	if stitchproof.DecodeExact(projected, &result) != nil || result.PlanSHA != receipt.NativePlanSHA || result.Error != "" ||
		len(result.Outputs) != 1 || len(result.Outputs[0].Inputs) != 2 || len(result.LeftOut) != 0 || len(result.Repairs) != 0 || len(result.Seams) != 1 {
		return errors.New("complete native joined result required")
	}
	included, err := validateNASStitchResult(plan, result)
	if err != nil || len(included) != 2 || len(receipt.Outputs) != 1 {
		return errors.New("current-policy native result rejected")
	}
	o, p := result.Outputs[0], receipt.Outputs[0]
	if o.Path != p.RelativePath || o.SHA != p.SHA256 || o.Size != p.SizeBytes {
		return errors.New("preserved native output differs")
	}
	return nil
}
