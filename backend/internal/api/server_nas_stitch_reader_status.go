package api

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchproof"
)

type nasStitchReadbackSummary struct {
	ID         string `json:"id"`
	VerifiedAt string `json:"verified_at"`
	SHA        string `json:"sha256"`
}

type nasStitchNativeStatus struct {
	RunID           string                   `json:"run_id"`
	BundleSHA       string                   `json:"bundle_sha256"`
	PlanSHA         string                   `json:"plan_sha256"`
	TaskSHA         string                   `json:"task_sha256"`
	ResultSHA       string                   `json:"result_sha256"`
	ProducerSHA     string                   `json:"producer_profile_sha256"`
	ReaderSHA       string                   `json:"reader_profile_sha256"`
	PairSHA         string                   `json:"profile_pair_sha256"`
	RegistrationSHA string                   `json:"registration_sha256"`
	Outputs         []stitchproof.Media      `json:"outputs"`
	Policy          stitchproof.Policy       `json:"policy"`
	Tools           stitchproof.PayloadTools `json:"tools"`
}

type nasStitchReaderStatus struct {
	Schema    string                     `json:"schema"`
	Reader    nasStitchReaderExecution   `json:"reader_execution"`
	Integrity string                     `json:"integrity"`
	Native    *nasStitchNativeStatus     `json:"native,omitempty"`
	History   []nasStitchReadbackSummary `json:"history"`
}

func decodeNASStitchReaderStatus(raw json.RawMessage, snapshot nasStitchReadbackSnapshot) (nasStitchReaderStatus, error) {
	var value nasStitchReaderStatus
	if len(raw) == 0 || len(raw) > 8192 || stitchproof.DecodeExact(raw, &value) != nil ||
		value.Schema != "stoarama.nas-stitch-reader-status.v1" || value.Integrity != "clear" || value.History == nil || len(value.History) > 4 {
		return value, errors.New("current reader integrity/status unavailable")
	}
	r := value.Reader
	if !validNASStitchReaderExecution(r) || r.Version != snapshot.Version || r.BootID != snapshot.BootID {
		return value, errors.New("actual reader identity differs")
	}
	// Use the same actual inventory/release/freshness guard even at admission,
	// before a native result or a readback exists. This synthetic value is solely
	// an internal projection of DB time, not an exported readback observation.
	probe := nasStitchReadbackReceipt{Reader: r, VerifiedAt: snapshot.Now.Format(time.RFC3339Nano)}
	if err := validateNASStitchReadbackReader(probe, r, snapshot.Facts, snapshot.Now); err != nil {
		return value, err
	}
	if value.Native == nil {
		if len(value.History) != 0 {
			return value, errors.New("readback history without preserved native binding")
		}
		return value, nil
	}
	n := value.Native
	if !nasStitchReadbackUUID(n.RunID) || len(n.Outputs) != 1 {
		return value, errors.New("preserved native status incomplete")
	}
	for _, sha := range []string{n.BundleSHA, n.PlanSHA, n.TaskSHA, n.ResultSHA, n.ProducerSHA, n.ReaderSHA, n.PairSHA, n.RegistrationSHA} {
		if !stitchSHA(sha) {
			return value, errors.New("preserved native digest unavailable")
		}
	}
	policy, err := nasStitchReadbackPolicy()
	if err != nil || n.Policy != policy || n.Tools != nasStitchReadbackTools() {
		return value, errors.New("preserved native policy/tool drift")
	}
	ids, shas := map[string]bool{}, map[string]bool{}
	var previous time.Time
	for _, entry := range value.History {
		at, err := parseNASStitchReadbackUTC(entry.VerifiedAt)
		if err != nil || !nasStitchReadbackUUID(entry.ID) || !stitchSHA(entry.SHA) || ids[entry.ID] || shas[entry.SHA] ||
			(!previous.IsZero() && at.Before(previous)) || at.After(snapshot.Now.Add(nasStitchReadbackFutureSkew)) {
			return value, errors.New("bounded readback history differs")
		}
		ids[entry.ID], shas[entry.SHA], previous = true, true, at
	}
	return value, nil
}

func matchNASStitchReadbackStatus(receipt nasStitchReadbackReceipt, rawSHA string, status nasStitchReaderStatus) error {
	n := status.Native
	if n == nil || n.RunID != receipt.NativeRunID || n.BundleSHA != receipt.NativeBundleSHA || n.PlanSHA != receipt.NativePlanSHA ||
		n.ProducerSHA != receipt.ProducerProfileSHA || n.ReaderSHA != receipt.ReaderProfileSHA || n.PairSHA != receipt.PairSHA ||
		n.RegistrationSHA != receipt.NativeRegistration || status.Reader != receipt.Reader || len(n.Outputs) != 1 ||
		n.Outputs[0] != receipt.Outputs[0] || n.Policy != receipt.Policy || n.Tools != receipt.Tools {
		return errors.New("current preserved-output binding differs")
	}
	for _, entry := range status.History {
		if entry.ID == receipt.ID && entry.VerifiedAt == receipt.VerifiedAt && entry.SHA == rawSHA {
			return nil
		}
	}
	return errors.New("requested readback absent from current bounded history")
}
