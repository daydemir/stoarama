package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const nasSelectedVerifyLeaseTTL = 30 * time.Minute

var nasSelectedVerifyOutcomes = map[string]bool{"match": true, "missing": true, "conflict": true, "unverified": true}

type nasSelectedVerifyLeaseRequest struct {
	ClientVersion   string `json:"client_version"`
	MaxTasks        int    `json:"max_tasks"`
	OriginalWitness bool   `json:"original_witness,omitempty"`
}
type nasSelectedVerifyTask struct {
	ItemID          int64                      `json:"item_id"`
	Attempt         int                        `json:"attempt"`
	Kind            string                     `json:"kind"`
	RelativePath    string                     `json:"relative_path"`
	SizeBytes       int64                      `json:"size_bytes"`
	SHA256          string                     `json:"sha256"`
	PlanSHA256      string                     `json:"plan_sha256"`
	ExpiresAt       time.Time                  `json:"expires_at"`
	OriginalWitness *nasOriginalWitnessContext `json:"original_witness,omitempty"`
}
type nasSelectedVerifyLeaseResponse struct {
	RetryAfterSec int                     `json:"retry_after_sec"`
	Tasks         []nasSelectedVerifyTask `json:"tasks"`
}
type nasSelectedVerifyResultRequest struct {
	Attempt         int                      `json:"attempt"`
	PlanSHA256      string                   `json:"plan_sha256"`
	Outcome         string                   `json:"outcome"`
	Error           string                   `json:"error"`
	DurationMS      int64                    `json:"duration_ms"`
	ClientVersion   string                   `json:"client_version"`
	OriginalWitness *nasOriginalWitnessProof `json:"original_witness,omitempty"`
}

func validateNASSelectedVerifyResult(req nasSelectedVerifyResultRequest) error {
	if req.Attempt < 1 {
		return errors.New("attempt must be positive")
	}
	if len(req.PlanSHA256) != 64 || !lowerHex(req.PlanSHA256) {
		return errors.New("invalid plan_sha256")
	}
	if !nasSelectedVerifyOutcomes[req.Outcome] {
		return errors.New("invalid outcome")
	}
	if req.DurationMS < 0 || req.DurationMS > int64((24*time.Hour)/time.Millisecond) {
		return errors.New("invalid duration_ms")
	}
	if len(req.Error) > 500 {
		return errors.New("error is too long")
	}
	if req.ClientVersion != "" && (len(req.ClientVersion) > 64 || !relayArtifactName.MatchString(req.ClientVersion)) {
		return errors.New("invalid client_version")
	}
	return nil
}

func (s *Server) handleAccountConnectionSelectedVerifyLease(w http.ResponseWriter, r *http.Request) {
	var req nasSelectedVerifyLeaseRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, 400, err.Error())
		return
	}
	if req.MaxTasks < 1 || req.MaxTasks > 64 {
		util.WriteError(w, 400, "max_tasks must be between 1 and 64")
		return
	}
	if req.ClientVersion != "" && (len(req.ClientVersion) > 64 || !relayArtifactName.MatchString(req.ClientVersion)) {
		util.WriteError(w, 400, "invalid client_version")
		return
	}
	ctx := r.Context()
	connectionID, ok := s.nasRestoreConnection(ctx, w, r)
	if !ok {
		return
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "begin selected verify lease failed")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `UPDATE nas_selected_verify_items i SET state=CASE WHEN attempts>=3 THEN 'unverified' ELSE 'pending' END,
      completed_at=CASE WHEN attempts>=3 THEN now() END, lease_expires_at=NULL,
      outcome_error=CASE WHEN attempts>=3 THEN 'lease expired three times' ELSE outcome_error END
      FROM nas_selected_verify_plans p WHERE i.plan_id=p.id AND p.connection_id=$1 AND i.state='leased' AND i.lease_expires_at<now()`, connectionID); err != nil {
		util.WriteError(w, 500, "expire selected verify leases failed")
		return
	}
	rows, err := tx.Query(ctx, `WITH picked AS (
      SELECT i.id FROM nas_selected_verify_items i JOIN nas_selected_verify_plans p ON p.id=i.plan_id
      WHERE p.connection_id=$1 AND i.state='pending' AND (NOT p.original_witness OR $5) ORDER BY p.original_witness DESC,i.id LIMIT $2 FOR UPDATE OF i SKIP LOCKED)
      UPDATE nas_selected_verify_items i SET state='leased',attempts=attempts+1,leased_at=now(),lease_expires_at=now()+greatest($3::interval,(i.size_bytes/4194304.0+120)*interval '1 second'),
        client_version=CASE WHEN $4<>'' THEN $4 ELSE client_version END
      FROM picked, nas_selected_verify_plans p WHERE i.id=picked.id AND p.id=i.plan_id
      RETURNING i.id,i.attempts,i.kind,i.relative_path,i.size_bytes,i.sha256,p.plan_sha256,i.lease_expires_at,p.original_witness,p.manifest->(i.ordinal-1)`,
		connectionID, req.MaxTasks, fmt.Sprintf("%d seconds", int(nasSelectedVerifyLeaseTTL.Seconds())), req.ClientVersion, req.OriginalWitness)
	if err != nil {
		util.WriteError(w, 500, "lease selected verify items failed")
		return
	}
	tasks := []nasSelectedVerifyTask{}
	for rows.Next() {
		var task nasSelectedVerifyTask
		var witnessMode bool
		var manifest []byte
		if err := rows.Scan(&task.ItemID, &task.Attempt, &task.Kind, &task.RelativePath, &task.SizeBytes, &task.SHA256, &task.PlanSHA256, &task.ExpiresAt, &witnessMode, &manifest); err != nil {
			rows.Close()
			util.WriteError(w, 500, "scan selected verify item failed")
			return
		}
		if witnessMode {
			var item struct {
				OriginalWitness *nasOriginalWitnessContext `json:"original_witness"`
			}
			if err := json.Unmarshal(manifest, &item); err != nil || task.Kind != "raw" || validateNASOriginalWitnessContext(item.OriginalWitness) != nil {
				rows.Close()
				util.WriteError(w, 500, "invalid immutable original witness context")
				return
			}
			task.OriginalWitness = item.OriginalWitness
		}
		tasks = append(tasks, task)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		util.WriteError(w, 500, "iterate selected verify items failed")
		return
	}
	if err := tx.Commit(ctx); err != nil {
		util.WriteError(w, 500, "commit selected verify lease failed")
		return
	}
	retry := 120
	if len(tasks) > 0 {
		retry = 0
	}
	util.WriteJSON(w, 200, nasSelectedVerifyLeaseResponse{RetryAfterSec: retry, Tasks: tasks})
}

func (s *Server) handleAccountConnectionSelectedVerifyResult(w http.ResponseWriter, r *http.Request) {
	itemID, ok := parseInt64Path(w, r, "itemId")
	if !ok {
		return
	}
	var req nasSelectedVerifyResultRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, 400, err.Error())
		return
	}
	if err := validateNASSelectedVerifyResult(req); err != nil {
		util.WriteError(w, 400, err.Error())
		return
	}
	ctx := r.Context()
	connectionID, ok := s.nasRestoreConnection(ctx, w, r)
	if !ok {
		return
	}
	var witnessMode bool
	var manifest []byte
	err := s.pool.QueryRow(ctx, `SELECT p.original_witness,p.manifest->(i.ordinal-1) FROM nas_selected_verify_items i JOIN nas_selected_verify_plans p ON p.id=i.plan_id WHERE i.id=$1 AND p.connection_id=$2`, itemID, connectionID).Scan(&witnessMode, &manifest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, 409, "selected verify item unavailable")
		} else {
			util.WriteError(w, 500, "load original witness context failed")
		}
		return
	}
	var proof any
	if witnessMode && req.Outcome == "match" {
		var item struct {
			OriginalWitness *nasOriginalWitnessContext `json:"original_witness"`
		}
		if err := json.Unmarshal(manifest, &item); err != nil || validateNASOriginalWitness(req.OriginalWitness, item.OriginalWitness) != nil {
			util.WriteError(w, 400, "valid original presentation evidence is required for this item")
			return
		}
		encoded, _ := json.Marshal(req.OriginalWitness)
		proof = string(encoded)
	} else if req.OriginalWitness != nil {
		util.WriteError(w, 400, "unexpected original witness evidence")
		return
	}
	tag, err := s.pool.Exec(ctx, `UPDATE nas_selected_verify_items i SET state=$4,completed_at=now(),lease_expires_at=NULL,
      outcome_error=$5,duration_ms=$6,client_version=CASE WHEN $7<>'' THEN $7 ELSE i.client_version END,original_witness=$9::jsonb
      FROM nas_selected_verify_plans p WHERE i.id=$1 AND i.plan_id=p.id AND p.connection_id=$2 AND p.plan_sha256=$3
        AND i.state='leased' AND i.lease_expires_at>=now() AND i.attempts=$8`, itemID, connectionID, req.PlanSHA256, req.Outcome, strings.TrimSpace(req.Error), req.DurationMS, req.ClientVersion, req.Attempt, proof)
	if err != nil {
		util.WriteError(w, 500, "record selected verify result failed")
		return
	}
	if tag.RowsAffected() != 1 {
		var same bool
		err = s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_selected_verify_items i JOIN nas_selected_verify_plans p ON p.id=i.plan_id WHERE i.id=$1 AND p.connection_id=$2 AND p.plan_sha256=$3 AND i.attempts=$4 AND i.state=$5 AND i.outcome_error=$6 AND i.duration_ms=$7 AND i.original_witness IS NOT DISTINCT FROM $8::jsonb)`, itemID, connectionID, req.PlanSHA256, req.Attempt, req.Outcome, strings.TrimSpace(req.Error), req.DurationMS, proof).Scan(&same)
		if err != nil {
			util.WriteError(w, 500, "reconcile selected verify result failed")
			return
		}
		if !same {
			util.WriteError(w, 409, "selected verify lease is no longer current")
			return
		}
	}
	util.WriteJSON(w, 200, map[string]any{"ok": true, "state": req.Outcome})
}
