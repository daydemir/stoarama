package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const nasDurhamParallelHours = 4

const nasDurhamClaimSQL = `
SELECT h.id,h.attempt,h.plan,h.plan_sha256
 FROM nas_stitch_r2_hours h JOIN nas_stitch_scopes s ON s.id=h.scope_id
 WHERE h.state IN ('pending','leased')
  AND (h.state='pending' OR (h.lease_expires_at<=clock_timestamp()
   AND NOT EXISTS(SELECT 1 FROM nas_stitch_r2_stage_objects o
    WHERE o.hour_id=h.id AND o.attempt=h.attempt AND o.state<>'aborted')))
  AND NOT s.paused AND s.approved_until>clock_timestamp()
  AND NOT EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams f WHERE f.stream_id=s.stream_id)
  AND jsonb_array_length(h.plan->'clips') BETWEEN 1 AND 200
  AND nas_stitch_r2_has_exact_input_set(h.id)
 ORDER BY h.input_bytes DESC,h.hour_at,h.id LIMIT 1 FOR UPDATE OF h FOR SHARE OF s`

func (s *Server) handleDurhamStitchLease(w http.ResponseWriter, r *http.Request, caller nasStitchCaller) {
	ctx, cancel := cleanupExecutionContext(r.Context())
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 503, "Durham stitch queue unavailable")
		return
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasStitchFence); err != nil {
		util.WriteError(w, 503, "Durham stitch lane unavailable")
		return
	}
	var enabled bool
	if err := tx.QueryRow(ctx, `SELECT enabled FROM nas_stitch_r2_control WHERE singleton FOR SHARE`).Scan(&enabled); err != nil {
		util.WriteError(w, 503, "Durham stitch control unavailable")
		return
	}
	noTask := map[string]any{"task": nil, "retry_after_sec": 60}
	if !enabled {
		util.WriteJSON(w, 200, noTask)
		return
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err != nil {
		util.WriteError(w, 503, "Durham stitch scope unavailable")
		return
	}
	var active int
	err = tx.QueryRow(ctx, `SELECT count(*) FROM nas_stitch_r2_hours
 WHERE state='leased' AND lease_expires_at>clock_timestamp()`).Scan(&active)
	if err != nil {
		util.WriteError(w, 503, "Durham stitch queue unavailable")
		return
	}
	if active >= nasDurhamParallelHours {
		util.WriteJSON(w, 200, noTask)
		return
	}
	var t nasStitchTask
	var raw []byte
	err = tx.QueryRow(ctx, nasDurhamClaimSQL).Scan(&t.ID, &t.Attempt, &raw, &t.PlanSHA)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, 200, noTask)
		return
	}
	if err != nil || json.Unmarshal(raw, &t.Plan) != nil {
		util.WriteError(w, 503, "Durham stitch plan unavailable")
		return
	}
	if !caller.lockActive(ctx, tx) {
		util.WriteError(w, 403, "Durham key no longer active")
		return
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		util.WriteError(w, 503, "Durham stitch lease unavailable")
		return
	}
	t.Attempt++
	t.Token = hex.EncodeToString(nonce[:])
	err = tx.QueryRow(ctx, `WITH decision AS MATERIALIZED (SELECT clock_timestamp() AS at)
 UPDATE nas_stitch_r2_hours h SET state='leased',attempt=$2,lease_token=$3,lease_api_key_id=$4,
  lease_expires_at=least(d.at+interval '2 hours',s.approved_until,coalesce(k.expires_at,'infinity'))
 FROM nas_stitch_scopes s,account_api_keys k,decision d
 WHERE h.id=$1 AND s.id=h.scope_id AND k.id=$4
  AND NOT s.paused AND s.approved_until>d.at AND k.revoked_at IS NULL
  AND (k.expires_at IS NULL OR k.expires_at>d.at)
 RETURNING h.lease_expires_at`, t.ID, t.Attempt, t.Token, caller.keyID).Scan(&t.Expires)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, 200, noTask)
		return
	}
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 503, "Durham stitch lease unavailable")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"source": "r2", "task": t, "retry_after_sec": 0})
}

type durhamStitchOperation string

const (
	durhamStitchContinue durhamStitchOperation = "continue"
	durhamStitchRelease  durhamStitchOperation = "release"
	durhamStitchInput    durhamStitchOperation = "input"
	durhamStitchStart    durhamStitchOperation = "start_upload"
	durhamStitchPart     durhamStitchOperation = "upload_part"
	durhamStitchFinish   durhamStitchOperation = "finish_upload"
)

type durhamStitchCheckpoint struct {
	Operation durhamStitchOperation `json:"operation"`
	Attempt   int                   `json:"attempt"`
	Token     string                `json:"lease_token"`
	PlanSHA   string                `json:"plan_sha256"`
	ClipID    int64                 `json:"clip_id,omitempty"`
	ObjectID  int64                 `json:"object_id,omitempty"`
	Part      int32                 `json:"part_number,omitempty"`
	SHA       string                `json:"sha256,omitempty"`
	Path      string                `json:"relative_path,omitempty"`
	Bytes     int64                 `json:"size_bytes,omitempty"`
}

func lockDurhamStitchAttempt(ctx context.Context, tx pgx.Tx, caller nasStitchCaller, lease r2.StitchStageLease) (time.Duration, []byte, error) {
	var plan []byte
	err := tx.QueryRow(ctx, `SELECT h.plan FROM nas_stitch_r2_hours h
 JOIN nas_stitch_scopes s ON s.id=h.scope_id CROSS JOIN nas_stitch_r2_control control
 WHERE h.id=$1 AND h.attempt=$2 AND h.lease_api_key_id=$3
  AND h.lease_token=$4 AND h.plan_sha256=$5
 FOR UPDATE OF h FOR SHARE OF s,control`, lease.HourID, lease.Attempt, caller.keyID, lease.Token, lease.PlanSHA).Scan(&plan)
	if err != nil || !caller.lockActive(ctx, tx) {
		return 0, nil, errors.New("Durham stitch attempt is unavailable")
	}
	var now, expiry time.Time
	err = tx.QueryRow(ctx, `SELECT clock_timestamp(),least(h.lease_expires_at,s.approved_until,coalesce(k.expires_at,'infinity'))
 FROM nas_stitch_r2_hours h JOIN nas_stitch_scopes s ON s.id=h.scope_id
 JOIN account_api_keys k ON k.id=h.lease_api_key_id CROSS JOIN nas_stitch_r2_control control
 WHERE h.id=$1 AND h.state='leased' AND NOT s.paused AND k.revoked_at IS NULL AND control.enabled
  AND NOT EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams f WHERE f.stream_id=s.stream_id)`, lease.HourID).Scan(&now, &expiry)
	ttl := expiry.Sub(now).Truncate(time.Second)
	if err != nil || ttl < time.Second {
		return 0, nil, errors.New("Durham stitch attempt is inactive")
	}
	return ttl, plan, nil
}

func (s *Server) handleDurhamStitchCheckpoint(w http.ResponseWriter, r *http.Request, caller nasStitchCaller, id int64) {
	var req durhamStitchCheckpoint
	if util.DecodeJSON(r, &req) != nil || req.Attempt <= 0 || !stitchSHA(req.PlanSHA) || !stitchSHA(req.Token) {
		util.WriteError(w, 400, "exact Durham stitch attempt required")
		return
	}
	if s.r2 == nil {
		util.WriteError(w, 503, "Durham stitch store unavailable")
		return
	}
	lease := r2.StitchStageLease{HourID: id, APIKeyID: caller.keyID, Attempt: req.Attempt, Token: req.Token, PlanSHA: req.PlanSHA}
	ctx := r.Context()
	var response any
	var err error
	switch req.Operation {
	case durhamStitchPart:
		var cap r2.PresignedRequest
		cap, err = s.r2.PresignStitchStagePart(ctx, lease, req.ObjectID, req.Part, req.SHA)
		response = durhamStitchCapability(cap)
	case durhamStitchFinish:
		err = s.r2.FinishStitchStage(ctx, lease, req.ObjectID)
		response = map[string]bool{"staged": true}
	case durhamStitchContinue, durhamStitchRelease, durhamStitchInput, durhamStitchStart:
		response, err = s.durhamStitchCheckpoint(ctx, caller, lease, req)
	default:
		util.WriteError(w, 400, "unknown Durham stitch operation")
		return
	}
	if err != nil {
		util.WriteError(w, 409, "Durham stitch operation unavailable")
		return
	}
	util.WriteJSON(w, 200, response)
}

func durhamStitchCapability(cap r2.PresignedRequest) map[string]any {
	return map[string]any{"url": cap.URL, "method": cap.Method, "headers": cap.Headers}
}

func (s *Server) durhamStitchCheckpoint(ctx context.Context, caller nasStitchCaller, lease r2.StitchStageLease, req durhamStitchCheckpoint) (any, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if req.Operation == durhamStitchStart {
		// One budget decision across all four workers.
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasStitchFence); err != nil {
			return nil, err
		}
	}
	ttl, raw, err := lockDurhamStitchAttempt(ctx, tx, caller, lease)
	if err != nil {
		return nil, err
	}
	switch req.Operation {
	case durhamStitchContinue:
		return map[string]bool{"continue": true}, tx.Commit(ctx)
	case durhamStitchRelease:
		tag, err := tx.Exec(ctx, `UPDATE nas_stitch_r2_hours SET state='pending',lease_api_key_id=NULL,
 lease_expires_at=NULL,lease_token=NULL WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM nas_stitch_r2_stage_objects
  WHERE hour_id=$1 AND attempt=$2 AND state<>'aborted')`, lease.HourID, lease.Attempt)
		if err != nil || tag.RowsAffected() != 1 {
			return nil, errors.New("staging exists for the Durham attempt")
		}
		return map[string]bool{"released": true}, tx.Commit(ctx)
	case durhamStitchInput:
		var key, etag string
		err = tx.QueryRow(ctx, `SELECT i.object_key,i.etag FROM nas_stitch_r2_input_holds i
 JOIN recording_clips c ON c.id=i.catalog_clip_id AND c.bucket=i.bucket AND c.object_key=i.object_key
 WHERE i.hour_id=$1 AND i.catalog_clip_id=$2 AND i.bucket=$3 AND i.released_at IS NULL
  AND c.purged_at IS NULL AND c.etag=i.etag AND c.sha256=i.sha256 AND c.size_bytes=i.size_bytes`,
			lease.HourID, req.ClipID, s.r2.Bucket()).Scan(&key, &etag)
		if err != nil {
			return nil, err
		}
		cap, err := s.r2.PresignGetExactRequest(ctx, key, etag, "", min(ttl, 3*time.Minute))
		if err != nil {
			return nil, err
		}
		return durhamStitchCapability(cap), tx.Commit(ctx)
	case durhamStitchStart:
		var plan nasStitchPlan
		if json.Unmarshal(raw, &plan) != nil || !stitchSHA(req.SHA) || req.Bytes < 1 || req.Bytes > nasStitchMaxBytes+nasStitchMaxBytes/10 ||
			!validCleanupPath(req.Path) || !durhamStitchOutputPath(plan.OutputFolder, req.Path, req.SHA) {
			return nil, errors.New("invalid Durham stitch output identity")
		}
		var id int64
		err = tx.QueryRow(ctx, `SELECT id FROM nas_stitch_r2_stage_objects
 WHERE hour_id=$1 AND attempt=$2 AND relative_path=$3 AND sha256=$4 AND size_bytes=$5`,
			lease.HourID, lease.Attempt, req.Path, req.SHA, req.Bytes).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			err = tx.QueryRow(ctx, `
 INSERT INTO nas_stitch_r2_stage_objects(hour_id,attempt,api_key_id,bucket,endpoint_sha256,relative_path,sha256,size_bytes)
 SELECT $1,$2,$3,$4,$5,$6,$7,$8 FROM nas_stitch_r2_control c
 WHERE c.max_staging_bytes >= $8::bigint + (SELECT COALESCE(sum(o.size_bytes),0) FROM nas_stitch_r2_stage_objects o
  WHERE o.state<>'aborted' AND NOT EXISTS(SELECT 1 FROM nas_stitch_r2_stage_retirements d
   WHERE d.object_id=o.id AND d.absence_observed_at IS NOT NULL))
  AND (SELECT count(*) FROM nas_stitch_r2_stage_objects WHERE hour_id=$1 AND attempt=$2)<200
 RETURNING id`, lease.HourID, lease.Attempt, caller.keyID, s.r2.Bucket(), s.r2.EndpointSHA256(),
				req.Path, req.SHA, req.Bytes).Scan(&id)
		}
		if err != nil {
			return nil, err
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, err
		}
		return s.r2.StartStitchStage(ctx, lease, id)
	}
	return nil, errors.New("unknown Durham checkpoint operation")
}

var durhamOutputName = regexp.MustCompile(`^run-[0-9]{3}-[0-9a-f]{64}\.mp4$`)

func durhamStitchOutputPath(folder, name, sha string) bool {
	return path.Dir(name) == folder && durhamOutputName.MatchString(path.Base(name)) &&
		strings.HasSuffix(name, "-"+sha+".mp4")
}

type durhamStitchResult struct {
	Attempt int             `json:"attempt"`
	Result  nasStitchResult `json:"result"`
}

func (s *Server) handleDurhamStitchResult(w http.ResponseWriter, r *http.Request, caller nasStitchCaller, id int64) {
	var req durhamStitchResult
	if util.DecodeJSON(r, &req) != nil || req.Attempt < 1 || !stitchSHA(req.Result.PlanSHA) || !stitchSHA(req.Result.Token) {
		util.WriteError(w, 400, "exact Durham stitch result required")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 503, "Durham stitch result unavailable")
		return
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasStitchFence); err != nil {
		util.WriteError(w, 503, "Durham stitch result unavailable")
		return
	}
	var old []byte
	var state string
	err = tx.QueryRow(ctx, `SELECT staged_manifest,state FROM nas_stitch_r2_hours
 WHERE id=$1 AND attempt=$2 AND lease_api_key_id=$3 AND lease_token=$4 AND plan_sha256=$5 FOR UPDATE`,
		id, req.Attempt, caller.keyID, req.Result.Token, req.Result.PlanSHA).Scan(&old, &state)
	if err != nil || !caller.lockActive(ctx, tx) {
		util.WriteError(w, 409, "Durham stitch result identity differs")
		return
	}
	body, _ := json.Marshal(req.Result)
	if state != "leased" {
		if (state == "staged" || state == "held" || state == "certified") && nasSpaceAuditJSONEqual(old, body) {
			util.WriteJSON(w, 200, map[string]any{"recorded": true, "state": state})
			return
		}
		util.WriteError(w, 409, "Durham stitch result replay differs")
		return
	}
	lease := r2.StitchStageLease{HourID: id, APIKeyID: caller.keyID, Attempt: req.Attempt, Token: req.Result.Token, PlanSHA: req.Result.PlanSHA}
	_, raw, err := lockDurhamStitchAttempt(ctx, tx, caller, lease)
	var plan nasStitchPlan
	if err != nil || json.Unmarshal(raw, &plan) != nil {
		util.WriteError(w, 409, "Durham stitch result plan unavailable")
		return
	}
	if _, err = validateNASStitchProof(plan, req.Result, nasStitchOnDurham); err != nil {
		util.WriteError(w, 400, err.Error())
		return
	}
	covered := len(req.Result.Outputs) > 0 && len(req.Result.LeftOut) == 0 && len(req.Result.Repairs) == 0 && req.Result.Error == ""
	for _, seam := range req.Result.Seams {
		covered = covered && seam.Decision != "hold"
	}
	state = "held"
	digest := cleanupSHA(body)
	if covered {
		if err := s.queueDurhamStitchCopies(ctx, tx, lease, plan, req.Result, digest); err != nil {
			util.WriteError(w, 409, "Durham output staging does not cover the exact plan")
			return
		}
		state = "staged"
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_stitch_r2_hours SET state=$2,staged_manifest=$3,staged_manifest_sha256=$4
 WHERE id=$1`, id, state, string(body), digest); err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 503, "Durham stitch result commit unavailable")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"recorded": true, "state": state})
}

func (s *Server) queueDurhamStitchCopies(
	ctx context.Context, tx pgx.Tx, lease r2.StitchStageLease, plan nasStitchPlan, result nasStitchResult, digest string,
) error {
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM nas_stitch_r2_stage_objects
 WHERE hour_id=$1 AND attempt=$2 AND state='staged'`, lease.HourID, lease.Attempt).Scan(&count); err != nil {
		return err
	}
	if count != len(result.Outputs) {
		return errors.New("Durham staged output count differs")
	}
	first, last := plan.Clips[0], plan.Clips[len(plan.Clips)-1]
	for _, output := range result.Outputs {
		item := NASR2CopyItem{Kind: NASR2CopyStitchOutput, RecordingID: plan.RecordingID, StreamID: plan.StreamID,
			RelativePath: output.Path, SHA256: output.SHA, SizeBytes: output.Size, StartAt: first.StartAt, EndAt: last.EndAt}
		err := tx.QueryRow(ctx, `SELECT id,bucket,object_key FROM nas_stitch_r2_stage_objects
 WHERE hour_id=$1 AND attempt=$2 AND api_key_id=$3 AND relative_path=$4 AND sha256=$5 AND size_bytes=$6
  AND state='staged'`, lease.HourID, lease.Attempt, lease.APIKeyID, output.Path, output.SHA, output.Size).
			Scan(&item.SourceID, &item.Bucket, &item.ObjectKey)
		if err != nil {
			return err
		}
		if err := ValidateNASR2CopyItem(item); err != nil {
			return err
		}
		body, _ := json.Marshal(item)
		if _, err := tx.Exec(ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item)
 VALUES(13,'stitch_output',$1,$2,$3,$4)`, item.SourceID, digest, cleanupPolicySHA, string(body)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) handleDurhamStitchStatus(w http.ResponseWriter, r *http.Request, caller nasStitchCaller, id int64) {
	var req durhamStitchCheckpoint
	if util.DecodeJSON(r, &req) != nil || req.Attempt < 1 || !stitchSHA(req.Token) || !stitchSHA(req.PlanSHA) {
		util.WriteError(w, 400, "exact Durham attempt required")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 503, "Durham status unavailable")
		return
	}
	defer tx.Rollback(ctx)
	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM nas_stitch_r2_hours
 WHERE id=$1 AND attempt=$2 AND lease_api_key_id=$3 AND lease_token=$4 AND plan_sha256=$5 FOR SHARE`,
		id, req.Attempt, caller.keyID, req.Token, req.PlanSHA).Scan(&state)
	if err != nil || !caller.lockActive(ctx, tx) || tx.Commit(ctx) != nil {
		util.WriteError(w, 409, "Durham status identity differs")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"state": state, "ready": state == "certified"})
}
