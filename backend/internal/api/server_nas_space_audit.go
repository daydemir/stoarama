package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const nasSpaceAuditSummaryLimit = 1 << 20
const nasSpaceAuditArtifactLimit int64 = 2 << 30
const nasSpaceAuditHeadEvidence = "head_size_and_uploader_declared_sha256_metadata_not_fresh_payload_hash"

var nasSpaceAuditUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
var nasSpaceAuditSHA = regexp.MustCompile(`^[0-9a-f]{64}$`)

type nasSpaceAuditRequest struct {
	AuditID           string          `json:"audit_id"`
	State             string          `json:"state"`
	ClientVersion     string          `json:"client_version"`
	Summary           json.RawMessage `json:"summary"`
	ArtifactSizeBytes int64           `json:"artifact_size_bytes"`
	ArtifactSHA256    string          `json:"artifact_sha256"`
}
type nasSpaceAuditStored struct {
	State, ClientVersion string
	Summary              json.RawMessage
	ObjectKey            string
	SizeBytes            int64
	SHA256               string
}

func validateNASSpaceAuditRequest(req nasSpaceAuditRequest) error {
	if !nasSpaceAuditUUID.MatchString(req.AuditID) {
		return errors.New("invalid audit_id")
	}
	switch req.State {
	case "scanning", "uploading", "complete", "failed":
	default:
		return errors.New("invalid state")
	}
	if len(req.ClientVersion) == 0 || len(req.ClientVersion) > 64 || !relayArtifactName.MatchString(req.ClientVersion) {
		return errors.New("invalid client_version")
	}
	var obj map[string]json.RawMessage
	if len(req.Summary) > nasSpaceAuditSummaryLimit || json.Unmarshal(req.Summary, &obj) != nil || obj == nil {
		return errors.New("summary must be an object of at most 1 MiB")
	}
	if req.ArtifactSizeBytes < 0 || req.ArtifactSizeBytes > nasSpaceAuditArtifactLimit {
		return errors.New("invalid artifact_size_bytes")
	}
	if req.ArtifactSizeBytes == 0 {
		if req.ArtifactSHA256 != "" {
			return errors.New("artifact size and SHA must be declared together")
		}
	} else if !nasSpaceAuditSHA.MatchString(req.ArtifactSHA256) {
		return errors.New("invalid artifact_sha256")
	}
	if (req.State == "uploading" || req.State == "complete") && req.ArtifactSizeBytes == 0 {
		return errors.New("artifact declaration required")
	}
	if req.State == "scanning" && req.ArtifactSizeBytes != 0 {
		return errors.New("scanning cannot declare an artifact")
	}
	return nil
}
func nasSpaceAuditJSONEqual(a, b []byte) bool {
	var x, y any
	da, db := json.NewDecoder(bytes.NewReader(a)), json.NewDecoder(bytes.NewReader(b))
	da.UseNumber()
	db.UseNumber()
	return da.Decode(&x) == nil && db.Decode(&y) == nil && reflect.DeepEqual(x, y)
}

// A declaration freezes at uploading, even when subsequent transfer attempts fail.
func validateNASSpaceAuditTransition(old nasSpaceAuditStored, req nasSpaceAuditRequest) error {
	if old.State == "complete" {
		if req.State != "complete" || req.ClientVersion != old.ClientVersion || req.ArtifactSizeBytes != old.SizeBytes || req.ArtifactSHA256 != old.SHA256 || !nasSpaceAuditJSONEqual(old.Summary, req.Summary) {
			return errors.New("completed audit is immutable")
		}
		return nil
	}
	if old.SizeBytes == 0 && req.State == "failed" && req.ArtifactSizeBytes != 0 {
		return errors.New("only uploading can freeze an artifact declaration")
	}
	if req.State == "complete" && old.SizeBytes == 0 {
		return errors.New("upload declaration required before completion")
	}
	if old.SizeBytes > 0 {
		if req.State == "scanning" {
			return errors.New("cannot restart a frozen audit")
		}
		if req.State != "failed" || req.ArtifactSizeBytes != 0 {
			if req.ArtifactSizeBytes != old.SizeBytes || req.ArtifactSHA256 != old.SHA256 {
				return errors.New("artifact declaration is immutable")
			}
		}
	}
	return nil
}
func validateNASSpaceAuditHead(head r2.ObjectHead, old nasSpaceAuditStored) error {
	if head.SizeBytes != old.SizeBytes || head.SHA256Metadata != old.SHA256 {
		return errors.New("artifact HEAD does not match frozen size and declared SHA metadata")
	}
	return nil
}

func (s *Server) handleAccountConnectionSpaceAudit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, nasSpaceAuditSummaryLimit+4096)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req nasSpaceAuditRequest
	if dec.Decode(&req) != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid bounded audit request")
		return
	}
	if dec.Decode(new(any)) != io.EOF {
		util.WriteError(w, http.StatusBadRequest, "expected one audit request")
		return
	}
	if err := validateNASSpaceAuditRequest(req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 40*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "begin space audit failed")
		return
	}
	defer tx.Rollback(ctx)
	connectionID, ok := s.nasUploadProbeConnection(ctx, tx, w, r)
	if !ok {
		return
	}
	var old nasSpaceAuditStored
	err = tx.QueryRow(ctx, `SELECT state,client_version,summary,artifact_object_key,artifact_size_bytes,artifact_sha256 FROM nas_space_audits WHERE connection_id=$1 AND audit_id=$2 FOR UPDATE`, connectionID, req.AuditID).Scan(&old.State, &old.ClientVersion, &old.Summary, &old.ObjectKey, &old.SizeBytes, &old.SHA256)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 500, "load space audit failed")
		return
	}
	if err := validateNASSpaceAuditTransition(old, req); err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	response := map[string]any{"ok": true, "audit_id": req.AuditID, "state": req.State}
	if old.State == "complete" {
		response["verification_evidence"] = nasSpaceAuditHeadEvidence
		util.WriteJSON(w, 200, response)
		return
	}
	if req.State == "uploading" {
		if s.r2 == nil {
			util.WriteError(w, 503, "audit artifact storage unavailable")
			return
		}
		if old.SizeBytes == 0 {
			old.SizeBytes = req.ArtifactSizeBytes
			old.SHA256 = req.ArtifactSHA256
			old.ObjectKey = fmt.Sprintf("nas-space-audits/%d/%s/%s.jsonl.gz", connectionID, req.AuditID, old.SHA256)
		}
		signed, err := s.r2.PresignPutCreateOnlyMetadataRequest(ctx, old.ObjectKey, "application/gzip", old.SizeBytes, old.SHA256, 30*time.Minute)
		if err != nil {
			util.WriteError(w, 503, "sign audit upload failed")
			return
		}
		headers := map[string]string{}
		for name, values := range signed.Headers {
			if http.CanonicalHeaderKey(name) != "Host" && len(values) > 0 {
				headers[http.CanonicalHeaderKey(name)] = strings.Join(values, ",")
			}
		}
		response["upload"] = map[string]any{"url": signed.URL, "method": signed.Method, "headers": headers, "size_bytes": old.SizeBytes}
	}
	if req.State == "complete" {
		if s.r2 == nil {
			util.WriteError(w, 503, "audit artifact storage unavailable")
			return
		}
		head, err := s.r2.Head(ctx, old.ObjectKey)
		if err != nil {
			util.WriteError(w, 409, "audit artifact HEAD unavailable")
			return
		}
		if err := validateNASSpaceAuditHead(head, old); err != nil {
			util.WriteError(w, 409, err.Error())
			return
		}
		response["verification_evidence"] = nasSpaceAuditHeadEvidence
	}
	_, err = tx.Exec(ctx, `INSERT INTO nas_space_audits(connection_id,audit_id,state,client_version,summary,artifact_object_key,artifact_size_bytes,artifact_sha256,completed_at) VALUES($1,$2,$3,$4,$5::jsonb,$6,$7,$8,CASE WHEN $3='complete' THEN now() END) ON CONFLICT(connection_id,audit_id) DO UPDATE SET state=EXCLUDED.state,client_version=EXCLUDED.client_version,summary=EXCLUDED.summary,artifact_object_key=EXCLUDED.artifact_object_key,artifact_size_bytes=EXCLUDED.artifact_size_bytes,artifact_sha256=EXCLUDED.artifact_sha256,updated_at=now(),completed_at=EXCLUDED.completed_at`, connectionID, req.AuditID, req.State, req.ClientVersion, string(req.Summary), old.ObjectKey, old.SizeBytes, old.SHA256)
	if err != nil {
		util.WriteError(w, 500, "store space audit failed")
		return
	}
	if tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "commit space audit failed")
		return
	}
	util.WriteJSON(w, 200, response)
}
