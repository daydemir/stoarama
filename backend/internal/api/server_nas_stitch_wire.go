package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// PostgreSQL timestamps have microsecond precision. Keep every instant exact
// while emitting the six fractional digits supported by the pinned Python 3.9
// reader. This type is only for newly generated stitch-protocol observations;
// copied native/readback timestamp strings remain untouched.
type nasStitchWireTime time.Time

func (value nasStitchWireTime) MarshalJSON() ([]byte, error) {
	instant := time.Time(value).UTC()
	if instant.IsZero() || instant.Year() < 1 || instant.Year() > 9999 || instant.Nanosecond()%1000 != 0 {
		return nil, errors.New("exact microsecond stitch timestamp required")
	}
	return json.Marshal(instant.Format("2006-01-02T15:04:05.000000Z"))
}

// A private wire view overrides only expiry; the underlying proof and its
// logical plan remain unchanged. Serialize this once, then reuse those bytes.
type nasStitchAdmissionTask struct {
	nasStitchProof
	Expires nasStitchWireTime `json:"expires_at"`
}

type nasStitchAdmissionResponse struct {
	Schema       string                     `json:"schema"`
	Mode         nasStitchProofMode         `json:"mode"`
	Profiles     nasStitchProfileComponents `json:"profiles"`
	Expires      nasStitchWireTime          `json:"expires_at"`
	Task         json.RawMessage            `json:"task,omitempty"`
	TaskB64      string                     `json:"task_b64,omitempty"`
	TaskSHA      string                     `json:"task_sha256,omitempty"`
	NativeRunID  string                     `json:"native_run_id,omitempty"`
	ActivationID string                     `json:"activation_id,omitempty"`
}

// Called only after the handler's request, source, cleanup and profile gates.
// This pure response producer neither performs admission nor creates a lease.
func buildNASStitchAdmissionResponse(snapshot nasStitchReadbackSnapshot, components nasStitchProfileComponents, mode nasStitchProofMode, query nasStitchAdmissionQuery) (nasStitchAdmissionResponse, error) {
	duration := 120 * time.Second
	switch mode {
	case nasStitchProofNative:
		duration = 15 * time.Minute
	case nasStitchProofReadback:
	default:
		return nasStitchAdmissionResponse{}, errors.New("active stitch response mode required")
	}
	expires := snapshot.Now.Add(duration)
	if snapshot.ApprovedUntil.Before(expires) {
		expires = snapshot.ApprovedUntil
	}
	response := nasStitchAdmissionResponse{Schema: "stoarama.nas-stitch-admission.v1", Mode: mode, Profiles: components, Expires: nasStitchWireTime(expires)}
	if mode == nasStitchProofNative {
		proof, err := boundedNASStitchNativeV3(snapshot.SourcePlan, snapshot.SourceSHA, query.run)
		if err != nil {
			return response, err
		}
		proof.Expires, proof.ActivationID = expires, query.activation
		raw, err := json.Marshal(nasStitchAdmissionTask{nasStitchProof: proof, Expires: response.Expires})
		if err != nil {
			return response, errors.New("native task bytes unavailable")
		}
		response.Task, response.TaskB64, response.TaskSHA = raw, base64.StdEncoding.EncodeToString(raw), cleanupSHA(raw)
		response.NativeRunID, response.ActivationID = query.run, query.activation
	}
	return response, nil
}

// The shared JSON writer commits status before encoding. These responses must
// instead refuse an unrepresentable timestamp before sending any success bytes.
func writeNASStitchWireJSON(w http.ResponseWriter, value any) {
	raw, err := json.Marshal(value)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "exact stitch response encoding unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(raw, '\n'))
}
