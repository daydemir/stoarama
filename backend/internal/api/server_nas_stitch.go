package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/collation"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

const nasStitchPolicy = "v85r"
const nasStitchRevision = "v85r-rebuilt-1"
const nasStitchCorpusSHA = "5b2765ecb4f6483c8e48b5a6c958c7c0d8082764d4da57901626a22569c6346b"
const nasStitchReason = "stitched_included"
const nasStitchFence int64 = 7343226011110
const nasStitchMaxBytes int64 = 2500000000

type nasStitchEdge struct {
	SourceKey [2]struct {
		ID   int64
		SHA  string
		Size int64
	} `json:"-"`
	RecordingID int64              `json:"recording_id"`
	Nomination  bool               `json:"nomination"`
	Status      string             `json:"status"`
	Diagnostic  string             `json:"frozen_diagnostic_sha256"`
	RawKey      [2]json.RawMessage `json:"source_key"`
}

func parseNASStitchEdge(raw []byte) (nasStitchEdge, error) {
	var e nasStitchEdge
	if json.Unmarshal(raw, &e) != nil || e.RecordingID <= 0 || (e.Diagnostic != "" && !stitchSHA(e.Diagnostic)) || (e.Nomination && !stitchSHA(e.Diagnostic)) || e.Status == "" {
		return e, errors.New("incomplete frozen decision")
	}
	for n, k := range e.RawKey {
		var t []json.RawMessage
		if json.Unmarshal(k, &t) != nil || len(t) != 3 || json.Unmarshal(t[0], &e.SourceKey[n].ID) != nil || json.Unmarshal(t[1], &e.SourceKey[n].SHA) != nil || json.Unmarshal(t[2], &e.SourceKey[n].Size) != nil || e.SourceKey[n].ID <= 0 || !stitchSHA(e.SourceKey[n].SHA) || e.SourceKey[n].Size <= 0 {
			return e, errors.New("invalid ordered source key")
		}
	}
	if e.Nomination && strings.HasPrefix(e.Status, "REJECT_") {
		return e, errors.New("contradictory frozen nomination")
	}
	return e, nil
}
func stitchSHA(s string) bool { return len(s) == 64 && lowerHex(s) }

type nasStitchPlan struct {
	Policy      string           `json:"policy"`
	Revision    string           `json:"revision"`
	CorpusSHA   string           `json:"corpus_sha256"`
	KeepSHA     string           `json:"keep_policy_sha256"`
	StreamID    int64            `json:"stream_id"`
	RecordingID int64            `json:"recording_id"`
	Clips       []nasCleanupItem `json:"clips"`
	// Metadata bounds only; every joined seam also needs a V85r decision.
	Eligible      []bool            `json:"eligible"`
	Joins         []bool            `json:"joins"`
	OutputFolder  string            `json:"output_folder"`
	Frozen        []json.RawMessage `json:"frozen"`
	FeatureInputs []collation.Clip  `json:"feature_inputs"`
}
type nasStitchTask struct {
	ID      int64         `json:"id"`
	Attempt int           `json:"attempt"`
	Token   string        `json:"lease_token"`
	Expires time.Time     `json:"expires_at"`
	PlanSHA string        `json:"plan_sha256"`
	Plan    nasStitchPlan `json:"plan"`
}
type nasStitchDecision struct {
	Policy        string `json:"policy"`
	LeftID        int64  `json:"left_id"`
	RightID       int64  `json:"right_id"`
	Join          bool   `json:"join"`
	Nomination    bool   `json:"nomination"`
	Status        string `json:"status"`
	Decision      string `json:"decision"`
	Reason        string `json:"reason"`
	Frozen        bool   `json:"frozen"`
	DiagnosticSHA string `json:"diagnostic_sha256"`
}
type nasStitchProvenance struct {
	OctoberRecovered bool   `json:"october_ffmpeg_recovered"`
	Policy           string `json:"policy"`
	ManifestSHA      string `json:"manifest_sha256"`
	FFmpegVersion    string `json:"ffmpeg_version"`
	FFprobeVersion   string `json:"ffprobe_version"`
	FFmpegSHA        string `json:"ffmpeg_sha256"`
	FFprobeSHA       string `json:"ffprobe_sha256"`
}
type nasStitchInputProof struct {
	RetirementHeld bool            `json:"retirement_held"`
	ID             int64           `json:"clip_id"`
	SHA            string          `json:"sha256"`
	Size           int64           `json:"size_bytes"`
	PacketSHA      string          `json:"packet_chain_sha256"`
	Packets        int64           `json:"packets"`
	Frames         int64           `json:"frames"`
	Repaired       bool            `json:"repaired"`
	RawProof       json.RawMessage `json:"raw_proof"`
}
type nasStitchOutput struct {
	Policy      string                `json:"policy"`
	Provenance  nasStitchProvenance   `json:"provenance"`
	Path        string                `json:"relative_path"`
	SHA         string                `json:"sha256"`
	Size        int64                 `json:"size_bytes"`
	Inputs      []nasStitchInputProof `json:"inputs"`
	FullDecode  bool                  `json:"full_decode"`
	SeamDecode  bool                  `json:"seam_decode"`
	PacketChain bool                  `json:"packet_chain"`
	NativeAudit bool                  `json:"native_audit"`
	NoAudio     bool                  `json:"original_sources_no_audio"`
	ReadbackSHA string                `json:"readback_sha256"`
	Fsynced     bool                  `json:"fsynced"`
}
type nasStitchRepair struct {
	Policy       string              `json:"policy"`
	Provenance   nasStitchProvenance `json:"provenance"`
	ID           int64               `json:"clip_id"`
	OriginalSHA  string              `json:"original_sha256"`
	OriginalSize int64               `json:"original_size_bytes"`
	Path         string              `json:"relative_path"`
	SHA          string              `json:"sha256"`
	Size         int64               `json:"size_bytes"`
	Method       string              `json:"method"`
	FPS          string              `json:"fps"`
	FPSSource    string              `json:"fps_src"`
	Frames       int64               `json:"frames"`
	Duration     float64             `json:"duration_seconds"`
	Note         string              `json:"note"`
	Synthesized  bool                `json:"synthesized_timestamps"`
	Complete     bool                `json:"complete_original"`
	FullDecode   bool                `json:"full_decode"`
	Fsynced      bool                `json:"fsynced"`
	ReadbackSHA  string              `json:"readback_sha256"`
}
type nasStitchResult struct {
	Policy  string              `json:"policy"`
	Seams   []nasStitchDecision `json:"seams"`
	Repairs []nasStitchRepair   `json:"repairs"`

	Token   string            `json:"lease_token"`
	PlanSHA string            `json:"plan_sha256"`
	Outputs []nasStitchOutput `json:"outputs"`
	LeftOut []int64           `json:"left_out"`
	Error   string            `json:"error"`
}

func (s *Server) handleAdminNASStitchCorpus(w http.ResponseWriter, r *http.Request) {
	// Canonical *uncompressed* JSONL bytes only. No caller-supplied digest/policy.
	raw, err := io.ReadAll(io.LimitReader(r.Body, 64<<20+1))
	if err != nil || len(raw) > 64<<20 || cleanupSHA(raw) != nasStitchCorpusSHA {
		util.WriteError(w, 400, "exact approved frozen decision export bytes required; no corpus extension admitted")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, 500, "begin corpus import")
		return
	}
	defer tx.Rollback(r.Context())
	var present bool
	if err = tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM nas_stitch_corpus WHERE sha256=$1)`, nasStitchCorpusSHA).Scan(&present); err != nil {
		util.WriteError(w, 500, "corpus lookup failed")
		return
	}
	if present {
		util.WriteJSON(w, 200, map[string]any{"sha256": nasStitchCorpusSHA})
		return
	}
	scan := bufio.NewScanner(bytes.NewReader(raw))
	scan.Buffer(make([]byte, 4096), 1<<20)
	count, nominees := 0, 0
	_, err = tx.CopyFrom(r.Context(), pgx.Identifier{"nas_stitch_edges"}, []string{"left_id", "left_sha", "left_size", "right_id", "right_sha", "right_size", "recording_id", "diagnostic_sha", "nomination", "witness"}, pgx.CopyFromFunc(func() ([]any, error) {
		if !scan.Scan() {
			return nil, scan.Err()
		}
		e, err := parseNASStitchEdge(scan.Bytes())
		if err != nil {
			return nil, err
		}
		a, b := e.SourceKey[0], e.SourceKey[1]
		count++
		if e.Nomination {
			nominees++
		}
		return []any{a.ID, a.SHA, a.Size, b.ID, b.SHA, b.Size, e.RecordingID, e.Diagnostic, e.Nomination, json.RawMessage(bytes.Clone(scan.Bytes()))}, nil
	}))
	if err != nil {
		util.WriteError(w, 409, "frozen decision import conflict or incomplete row")
		return
	}
	if scan.Err() != nil || count != 73843 || nominees != 52462 {
		util.WriteError(w, 400, "frozen census differs")
		return
	}
	if _, err = tx.Exec(r.Context(), `INSERT INTO nas_stitch_corpus(sha256,body) VALUES($1,$2)`, nasStitchCorpusSHA, raw); err != nil || tx.Commit(r.Context()) != nil {
		util.WriteError(w, 500, "corpus commit failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"sha256": nasStitchCorpusSHA, "edges": count})
}
func (s *Server) handleAccountNASStitchCorpus(w http.ResponseWriter, r *http.Request) {
	cid, ok := s.nasRestoreConnection(r.Context(), w, r)
	if !ok {
		return
	}
	if cid != 13 {
		util.WriteError(w, 403, "stitch corpus not admitted for this connection")
		return
	}

	if !s.cfg.NASStitchEnabled {
		util.WriteError(w, 409, "stitch switch off")
		return
	}
	var raw []byte
	if s.pool.QueryRow(r.Context(), `SELECT body FROM nas_stitch_corpus WHERE sha256=$1`, nasStitchCorpusSHA).Scan(&raw) != nil || cleanupSHA(raw) != nasStitchCorpusSHA {
		util.WriteError(w, 409, "approved corpus unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Write(raw)
}
func (s *Server) handleAdminNASStitchScope(w http.ResponseWriter, r *http.Request) {
	var q struct {
		StreamID int64     `json:"stream_id"`
		Start    time.Time `json:"start_at"`
		End      time.Time `json:"end_at"`
		Until    time.Time `json:"approved_until"`
		Reason   string    `json:"reason"`
		Delete   bool      `json:"deletion_approved"`
	}
	if util.DecodeJSON(r, &q) != nil {
		util.WriteError(w, 400, "invalid scope")
		return
	}
	policy, err := cleanupEmbeddedPolicy()
	if err != nil {
		util.WriteError(w, 409, "keep policy unavailable")
		return
	}
	kw, ok := policy.Streams[q.StreamID]
	if !ok || q.Start.Before(kw.start) || q.End.After(kw.end) || !q.End.After(q.Start) || q.Until.Before(time.Now()) || q.Until.After(time.Now().Add(180*24*time.Hour)) || len(q.Reason) < 1 || len(q.Reason) > 500 {
		util.WriteError(w, 400, "exact kept-window pilot range, reason and approval expiry (<=180d) required")
		return
	}
	var id int64
	err = s.pool.QueryRow(r.Context(), `INSERT INTO nas_stitch_scopes(stream_id,start_at,end_at,reason,approved_until,deletion_approved) VALUES($1,$2,$3,$4,$5,$6) RETURNING id`, q.StreamID, q.Start, q.End, q.Reason, q.Until, q.Delete).Scan(&id)
	if err != nil {
		util.WriteError(w, 500, "scope insert failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id})
}
func (s *Server) handleAdminNASStitchPause(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	// Scope pause revokes new stitch work AND subsequent deletion decisions.
	if _, err := s.pool.Exec(r.Context(), `UPDATE nas_stitch_scopes SET paused=true WHERE id=$1`, id); err != nil {
		util.WriteError(w, 500, "scope pause failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"paused": true})
}
func (s *Server) handleAdminNASStitchStatus(w http.ResponseWriter, r *http.Request) {
	var raw []byte
	err := s.pool.QueryRow(r.Context(), `SELECT jsonb_build_object('policy','v85r','revision','v85r-rebuilt-1','scopes',coalesce((SELECT jsonb_agg(s ORDER BY id) FROM nas_stitch_scopes s),'[]'),'tasks',coalesce((SELECT jsonb_agg(t) FROM (SELECT id,scope_id,recording_id,hour_at,state,attempt,cleanup_plan_id,updated_at FROM nas_stitch_tasks ORDER BY id DESC LIMIT 100) t),'[]'),'client',(SELECT nas_stitch_status FROM connections WHERE id=13))`).Scan(&raw)
	if err != nil {
		util.WriteError(w, 500, "stitch status unavailable")
		return
	}
	util.WriteJSON(w, 200, json.RawMessage(raw))
}

func (s *Server) handleAccountNASStitchLease(w http.ResponseWriter, r *http.Request) {
	cid, ok := s.nasRestoreConnection(r.Context(), w, r)
	if !ok {
		return
	}
	if cid != 13 || !s.cfg.NASStitchEnabled {
		util.WriteJSON(w, 200, map[string]any{"task": nil, "retry_after_sec": 300})
		return
	}
	ctx, cancel := cleanupExecutionContext(r.Context())
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "begin stitch lease")
		return
	}
	var releaseFreeze func()
	defer func() {
		tx.Rollback(ctx)
		if releaseFreeze != nil {
			releaseFreeze()
		}
	}()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasStitchFence); err != nil {
		util.WriteError(w, 500, "stitch fence failed")
		return
	}
	// Serialize lease selection with cleanup activation, including committed
	// freeze publication. In-flight jobs also check this at bounded checkpoints.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err != nil {
		util.WriteError(w, 503, "cleanup source fence unavailable")
		return
	}
	if releaseFreeze, err = s.stitchAdvanceRetirement(ctx, tx); err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	var busy bool
	// One lane across clients/reboots; an ambiguous result expires back to the
	// *same* immutable task. Pending cleanup must drain before more media grows.
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_stitch_tasks WHERE (state='leased' AND lease_expires_at>now()) OR (state='verified' AND (cleanup_plan_id IS NULL OR EXISTS(SELECT 1 FROM nas_cleanup_items WHERE plan_id=cleanup_plan_id AND state<>'deleted')))) OR EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams)`).Scan(&busy)
	if err != nil {
		util.WriteError(w, 500, "stitch lane lookup failed")
		return
	}
	if busy {
		if tx.Commit(ctx) != nil {
			util.WriteError(w, 500, "stitch advance commit failed")
			return
		}
		util.WriteJSON(w, 200, map[string]any{"task": nil, "retry_after_sec": 60})
		return
	}
	var t nasStitchTask
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT t.id,t.plan,t.plan_sha256,t.attempt FROM nas_stitch_tasks t JOIN nas_stitch_scopes s ON s.id=t.scope_id WHERE NOT s.paused AND s.approved_until>now() AND NOT EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams f WHERE f.stream_id=s.stream_id) AND (t.state='pending' OR (t.state='leased' AND t.lease_expires_at<=now())) ORDER BY t.id LIMIT 1 FOR UPDATE OF t`).Scan(&t.ID, &raw, &t.PlanSHA, &t.Attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		t, err = s.stitchPlanNext(ctx, tx)
	} else if err == nil {
		err = json.Unmarshal(raw, &t.Plan)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 409, err.Error())
		return
	}
	if t.ID == 0 {
		util.WriteJSON(w, 200, map[string]any{"task": nil, "retry_after_sec": 300})
		return
	}
	t.Attempt++
	t.Expires = time.Now().UTC().Add(2 * time.Hour)
	nonce := make([]byte, 32)
	if _, err = rand.Read(nonce); err != nil {
		util.WriteError(w, 500, "lease entropy unavailable")
		return
	}
	t.Token = hex.EncodeToString(nonce)
	_, err = tx.Exec(ctx, `UPDATE nas_stitch_tasks SET state='leased',attempt=$2,lease_token=$3,lease_expires_at=$4,updated_at=now() WHERE id=$1`, t.ID, t.Attempt, t.Token, t.Expires)
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "stitch lease commit failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"task": t, "retry_after_sec": 0})
}

// A running builder owns only its new sibling directory, never source writers
// or the cleanup execution lane. Checkpoints pause it under the same committed
// source freeze that cleanup activation publishes before issuing any permits.
func (s *Server) handleAccountNASStitchCheckpoint(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := cleanupExecutionContext(r.Context())
	defer cancel()
	r = r.WithContext(ctx)
	cid, ok := s.nasRestoreConnection(ctx, w, r)
	if !ok {
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Token   string `json:"lease_token"`
		PlanSHA string `json:"plan_sha256"`
	}
	if cid != 13 || util.DecodeJSON(r, &req) != nil {
		util.WriteError(w, 400, "stitch checkpoint identity required")
		return
	}
	var allowed bool
	err := s.pool.QueryRow(ctx, `SELECT t.state='leased' AND t.lease_expires_at>now() AND NOT s.paused AND s.approved_until>now()
        AND NOT EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams f WHERE f.stream_id=s.stream_id)
        FROM nas_stitch_tasks t JOIN nas_stitch_scopes s ON s.id=t.scope_id
        WHERE t.id=$1 AND t.lease_token=$2 AND t.plan_sha256=$3`, id, req.Token, req.PlanSHA).Scan(&allowed)
	if err != nil {
		util.WriteError(w, 409, "stitch checkpoint identity unavailable")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"continue": allowed && s.cfg.NASStitchEnabled})
}

func (s *Server) stitchPlanNext(ctx context.Context, tx pgx.Tx) (nasStitchTask, error) {
	var t nasStitchTask
	var sid int64
	var hour, start, end time.Time
	var corpus bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_stitch_corpus WHERE sha256=$1)`, nasStitchCorpusSHA).Scan(&corpus); err != nil {
		return t, err
	}
	if !corpus {
		return t, errors.New("exact approved V85 corpus unavailable; HOLD")
	}
	err := tx.QueryRow(ctx, `SELECT s.id,s.stream_id,r.id,date_trunc('hour',c.clip_start_at),s.start_at,s.end_at
 FROM nas_stitch_scopes s JOIN recordings r ON r.stream_id=s.stream_id AND r.account_id=47
 JOIN recording_clips c ON c.recording_id=r.id JOIN nas_inventory_files n ON n.clip_id=c.id AND n.connection_id=13 AND n.state='present' AND n.sha256=c.sha256 AND n.size_bytes=c.size_bytes
 WHERE NOT s.paused AND s.approved_until>now() AND NOT EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams f WHERE f.stream_id=s.stream_id) AND c.clip_start_at>=s.start_at AND c.clip_end_at<=s.end_at
 AND NOT EXISTS(SELECT 1 FROM nas_stitch_tasks t WHERE t.recording_id=r.id AND t.hour_at=date_trunc('hour',c.clip_start_at))
 AND NOT EXISTS(SELECT 1 FROM recording_clips c2 LEFT JOIN nas_inventory_files n2 ON n2.clip_id=c2.id AND n2.connection_id=13 AND n2.state='present' AND n2.relative_path=c2.display_path AND n2.sha256=c2.sha256 AND n2.size_bytes=c2.size_bytes
 WHERE c2.recording_id=r.id AND c2.clip_start_at>=date_trunc('hour',c.clip_start_at) AND c2.clip_start_at<date_trunc('hour',c.clip_start_at)+interval '1 hour' AND c2.clip_start_at>=s.start_at AND c2.clip_end_at<=s.end_at AND n2.clip_id IS NULL)
 ORDER BY c.clip_start_at,c.id,s.id LIMIT 1`).Scan(&sid, &t.Plan.StreamID, &t.Plan.RecordingID, &hour, &start, &end)
	if err != nil {
		return t, err
	}
	policy, err := cleanupEmbeddedPolicy()
	if err != nil {
		return t, err
	}
	kw, ok := policy.Streams[t.Plan.StreamID]
	if !ok || start.Before(kw.start) || end.After(kw.end) {
		return t, errors.New("scope no longer inside keep policy")
	}
	rows, err := tx.Query(ctx, `SELECT c.id FROM recording_clips c WHERE c.recording_id=$1 AND c.clip_start_at>=$2 AND c.clip_start_at<$3 AND c.clip_start_at>=$4 AND c.clip_end_at<=$5 ORDER BY c.clip_start_at,c.id LIMIT 201`, t.Plan.RecordingID, hour, hour.Add(time.Hour), start, end)
	if err != nil {
		return t, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return t, err
	}
	if len(ids) == 0 || len(ids) > 200 {
		return t, errors.New("assembly unit outside 1..200 clip bound")
	}
	var total int64
	for _, id := range ids {
		if s.r2 == nil {
			return t, errors.New("recovery store unavailable")
		}
		item, err := cleanupCatalogItem(ctx, tx, "raw", id, s.r2.Bucket())
		if err != nil {
			return t, err
		}
		var present bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_inventory_files WHERE connection_id=13 AND clip_id=$1 AND state='present' AND relative_path=$2 AND size_bytes=$3 AND lower(sha256)=$4)`, id, item.RelativePath, item.SizeBytes, item.SHA256).Scan(&present); err != nil {
			return t, err
		}
		if !present {
			return t, errors.New("assembly unit awaits exact NAS inputs")
		}
		var feature collation.Clip
		feature.ClipID, feature.RecordingID, feature.SHA256, feature.SizeBytes, feature.StartUTC, feature.EndUTC = item.SourceID, item.RecordingID, item.SHA256, item.SizeBytes, item.StartAt, item.EndAt
		if err = tx.QueryRow(ctx, `SELECT coalesce(c.recording_job_id,0),coalesce(c.capture_attempt_id::text,''),coalesce(c.capture_lease_token::text,''),coalesce(c.capture_sequence,0),r.clip_duration_sec::float8 FROM recording_clips c JOIN recordings r ON r.id=c.recording_id WHERE c.id=$1`, id).Scan(&feature.JobID, &feature.CaptureAttemptID, &feature.CaptureLeaseToken, &feature.CaptureSequence, &feature.NominalSeconds); err != nil {
			return t, err
		}
		if feature.NominalSeconds <= 0 {
			return t, errors.New("V85r nominal duration missing")
		}
		t.Plan.FeatureInputs = append(t.Plan.FeatureInputs, feature)
		total += item.SizeBytes
		t.Plan.Clips = append(t.Plan.Clips, item)
	}
	if total > nasStitchMaxBytes {
		return t, errors.New("assembly unit exceeds 2.5GB cap")
	}
	for range t.Plan.Clips {
		t.Plan.Eligible = append(t.Plan.Eligible, true)
	}
	for n := 1; n < len(t.Plan.Clips); n++ {
		a, b := t.Plan.Clips[n-1], t.Plan.Clips[n]
		var raw []byte
		err = tx.QueryRow(ctx, `SELECT witness FROM nas_stitch_edges WHERE left_id=$1 AND left_sha=$2 AND left_size=$3 AND right_id=$4 AND right_sha=$5 AND right_size=$6 AND recording_id=$7`, a.SourceID, a.SHA256, a.SizeBytes, b.SourceID, b.SHA256, b.SizeBytes, t.Plan.RecordingID).Scan(&raw)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return t, err
		}
		t.Plan.Frozen = append(t.Plan.Frozen, json.RawMessage(raw))
		// Frozen negatives may never be promoted by a rebuilt measurement.
		join := a.EndAt.Equal(b.StartAt)
		if len(raw) > 0 {
			e, parseErr := parseNASStitchEdge(raw)
			if parseErr != nil {
				return t, parseErr
			}
			join = join && e.Nomination
		}
		t.Plan.Joins = append(t.Plan.Joins, join)
	}

	t.Plan.Policy = nasStitchPolicy
	t.Plan.Revision = nasStitchRevision
	t.Plan.CorpusSHA = nasStitchCorpusSHA
	t.Plan.KeepSHA = cleanupPolicySHA
	// New versioned sibling tree; never replace raw or previous joined output.
	first := strings.Split(t.Plan.Clips[0].RelativePath, "/")[0]
	t.Plan.OutputFolder = first + "_V85r_keep28/" + strconv.FormatInt(t.Plan.RecordingID, 10) + "/" + hour.UTC().Format("2006-01-02T15-04-05Z")
	raw, _ := json.Marshal(t.Plan)
	t.PlanSHA = cleanupSHA(raw)
	err = tx.QueryRow(ctx, `INSERT INTO nas_stitch_tasks(scope_id,recording_id,hour_at,plan,plan_sha256) VALUES($1,$2,$3,$4,$5) RETURNING id`, sid, t.Plan.RecordingID, hour, string(raw), t.PlanSHA).Scan(&t.ID)
	return t, err
}

func validateNASStitchResult(p nasStitchPlan, q nasStitchResult) ([]nasCleanupItem, error) {
	if p.Policy != nasStitchPolicy || p.Revision != nasStitchRevision || q.Policy != nasStitchPolicy || len(p.Eligible) != len(p.Clips) || len(p.Joins) != len(p.Clips)-1 || len(q.Seams) != len(p.Joins) || len(p.Frozen) != len(p.Joins) {
		return nil, errors.New("incomplete policy admission")
	}
	if len(q.Repairs) > len(p.Clips) {
		return nil, errors.New("unbounded repair results")
	}
	if len(q.Outputs) > len(p.Clips) || len(q.LeftOut) > len(p.Clips) || len(q.Error) > 1000 {
		return nil, errors.New("unbounded stitch result")
	}
	held := map[int64]bool{}
	for n, d := range q.Seams {
		a, b := p.Clips[n], p.Clips[n+1]
		if d.Policy != nasStitchPolicy || d.LeftID != a.SourceID || d.RightID != b.SourceID || len(d.Status) > 200 || len(d.Reason) > 200 {
			return nil, errors.New("V85r seam binding differs")
		}
		if d.Decision != "join" && d.Decision != "split" && d.Decision != "hold" {
			return nil, errors.New("V85r decision unknown")
		}
		if d.Decision == "split" && (d.Nomination || !strings.HasPrefix(d.Status, "REJECT_") && !strings.HasPrefix(d.Status, "SPLIT_")) {
			return nil, errors.New("ambiguous split must HOLD")
		}
		if d.Join != (d.Decision == "join") || (d.Join && (!p.Joins[n] || !d.Nomination || d.Status != "APPROVED85_nomination_requires_runtime_native_join_checks" || !stitchSHA(d.DiagnosticSHA))) {
			return nil, errors.New("V85r seam is not an unambiguous nomination")
		}
		if len(p.Frozen[n]) > 0 && string(p.Frozen[n]) != "null" {
			e, err := parseNASStitchEdge(p.Frozen[n])
			if err != nil {
				return nil, err
			}
			if d.Decision != "hold" && (e.Status != d.Status || e.Nomination != d.Nomination || !d.Frozen) {
				return nil, errors.New("October difference must HOLD")
			}
		}
		if d.Decision == "hold" {
			held[a.SourceID] = true
			held[b.SourceID] = true
		}
	}
	clips := map[int64]nasCleanupItem{}
	positions := map[int64]int{}
	seen := map[int64]bool{}
	paths := map[string]bool{}
	for n, c := range p.Clips {
		clips[c.SourceID] = c
		positions[c.SourceID] = n
	}
	var included []nasCleanupItem
	for _, o := range q.Outputs {
		if o.Policy != nasStitchPolicy || !validNASStitchProvenance(o.Provenance) || !validCleanupPath(o.Path) || path.Dir(o.Path) != p.OutputFolder || paths[o.Path] || !stitchSHA(o.SHA) || o.ReadbackSHA != o.SHA || o.Size <= 0 || o.Size > nasStitchMaxBytes+nasStitchMaxBytes/10 || !o.FullDecode || !o.SeamDecode || !o.PacketChain || !o.NativeAudit || !o.NoAudio || !o.Fsynced || len(o.Inputs) == 0 {
			return nil, errors.New("complete durable native output verification required")
		}
		paths[o.Path] = true
		prev := -1
		for _, proof := range o.Inputs {
			c, ok := clips[proof.ID]
			pos := positions[proof.ID]
			if !ok || !p.Eligible[pos] || seen[proof.ID] || proof.SHA != c.SHA256 || proof.Size != c.SizeBytes || !stitchSHA(proof.PacketSHA) || proof.Packets <= 0 || proof.Frames != proof.Packets || proof.Repaired || (prev >= 0 && (pos != prev+1 || !q.Seams[prev].Join)) {
				return nil, errors.New("missing, repaired, duplicate or unqualified inclusion proof")
			}
			// Freeze the exact physical NAS identity for existing cleanup machinery.
			var physical struct {
				Identity [5]int64 `json:"identity"`
				Sidecar  struct {
					Identity [5]int64 `json:"identity"`
				} `json:"sidecar"`
			}
			if json.Unmarshal(proof.RawProof, &physical) != nil {
				return nil, errors.New("raw physical proof missing")
			}
			c.Frozen = physical.Identity
			c.FrozenSidecar = physical.Sidecar.Identity
			if !validCleanupProof(proof.RawProof, c) {
				return nil, errors.New("raw physical proof differs")
			}
			c.PermitReason = nasStitchReason
			c.StitchOutputPath = o.Path
			c.StitchOutputSHA = o.SHA
			c.StitchOutputSize = o.Size
			if held[proof.ID] && !proof.RetirementHeld {
				return nil, errors.New("held seam source must retain raw")
			}
			if !proof.RetirementHeld {
				included = append(included, c)
			}
			seen[proof.ID] = true
			prev = pos
		}
	}
	for _, id := range q.LeftOut {
		if _, ok := clips[id]; !ok || seen[id] {
			return nil, errors.New("invalid left-out disposition")
		}
		seen[id] = true
	}
	repairSeen := map[int64]bool{}
	for _, r := range q.Repairs {
		c, ok := clips[r.ID]
		left := false
		for _, id := range q.LeftOut {
			left = left || id == r.ID
		}
		if r.Policy != nasStitchPolicy || !validNASStitchProvenance(r.Provenance) || !ok || !left || repairSeen[r.ID] || r.OriginalSHA != c.SHA256 || r.OriginalSize != c.SizeBytes || !validCleanupPath(r.Path) || path.Dir(r.Path) != p.OutputFolder || !strings.HasPrefix(path.Base(r.Path), "repaired-") || !stitchSHA(r.SHA) || r.Size <= 0 || r.Size > 32<<20 || r.ReadbackSHA != r.SHA || r.Method != "mdat_annexb_vui_prefix_v1" || r.FPSSource != "vui" || !r.Synthesized || r.Complete || !r.FullDecode || !r.Fsynced || r.Frames <= 0 || r.Duration < 1 || r.Duration > 60 || len(r.Note) > 1000 {
			return nil, errors.New("partial repair provenance incomplete or wrongly included")
		}
		repairSeen[r.ID] = true
	}
	if len(seen) != len(clips) {
		return nil, errors.New("every leaf needs a disposition")
	}
	return included, nil
}
func (s *Server) handleAccountNASStitchResult(w http.ResponseWriter, r *http.Request) {
	cid, ok := s.nasRestoreConnection(r.Context(), w, r)
	if !ok {
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var q nasStitchResult
	if util.DecodeJSON(r, &q) != nil {
		util.WriteError(w, 400, "invalid result")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "begin stitch result")
		return
	}
	defer tx.Rollback(ctx)
	var raw, old []byte
	var sha, token, state string
	var expiry time.Time
	var active bool
	err = tx.QueryRow(ctx, `SELECT t.plan,t.plan_sha256,coalesce(t.lease_token,''),t.state,t.lease_expires_at,t.result,NOT s.paused AND s.approved_until>now() FROM nas_stitch_tasks t JOIN nas_stitch_scopes s ON s.id=t.scope_id WHERE t.id=$1 FOR UPDATE OF t`, id).Scan(&raw, &sha, &token, &state, &expiry, &old, &active)
	body, _ := json.Marshal(q)
	if err != nil || cid != 13 || sha != q.PlanSHA || token != q.Token {
		util.WriteError(w, 409, "stitch result identity differs")
		return
	}
	if (state == "verified" || state == "held" || state == "completed_no_retirement") && nasSpaceAuditJSONEqual(old, body) {
		util.WriteJSON(w, 200, map[string]any{"recorded": true, "id": id})
		return
	}
	if !s.cfg.NASStitchEnabled || !active || state != "leased" || expiry.Before(time.Now()) {
		util.WriteError(w, 409, "stitch lease/scope inactive")
		return
	}
	var p nasStitchPlan
	if json.Unmarshal(raw, &p) != nil {
		util.WriteError(w, 500, "invalid task plan")
		return
	}
	_, err = validateNASStitchResult(p, q)
	if err != nil {
		util.WriteError(w, 400, err.Error())
		return
	}
	// Persist verified output evidence atomically. Cleanup admission waits for
	// the separate durable-local-receipt readiness ACK.
	for _, o := range q.Outputs {
		b, _ := json.Marshal(o)
		if _, err = tx.Exec(ctx, `INSERT INTO nas_stitch_outputs(task_id,relative_path,sha256,size_bytes,proof) VALUES($1,$2,$3,$4,$5)`, id, o.Path, o.SHA, o.Size, string(b)); err != nil {
			util.WriteError(w, 409, "output registration conflict")
			return
		}
	}
	for _, repair := range q.Repairs {
		b, _ := json.Marshal(repair)
		if _, err = tx.Exec(ctx, `INSERT INTO nas_stitch_repairs(task_id,clip_id,proof) VALUES($1,$2,$3)`, id, repair.ID, string(b)); err != nil {
			util.WriteError(w, 409, "repair registration conflict")
			return
		}
	}
	_, err = tx.Exec(ctx, `UPDATE nas_stitch_tasks SET state=CASE WHEN $3::boolean THEN 'verified' ELSE 'held' END,result=$2::jsonb,updated_at=now() WHERE id=$1`, id, string(body), len(q.Outputs) > 0)
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "stitch result commit failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"recorded": true, "id": id})
}

// The original cleanup permit, budget, R2 recovery and receipt path remains the
// only unlink authority. Scope approval can advance its dry-run plan unattended.
func (s *Server) stitchAdvanceRetirement(ctx context.Context, tx pgx.Tx) (func(), error) {
	if !s.cfg.NASStitchEnabled || !s.cfg.NASStitchDeletionEnabled || !s.cfg.NASCleanupDeletionEnabled {
		return nil, nil
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err != nil {
		return nil, err
	}
	if _, err := cleanupLockConnection(ctx, tx, 13); err != nil {
		return nil, err
	}
	// Close only this lane's cleanly completed plan so the shared one-plan
	// cap permits its successor. Issued R2 recovery holds remain permanent.
	if _, err := tx.Exec(ctx, `UPDATE nas_cleanup_plans p SET mode='paused',approved_until=NULL,approval_reason='stitch retirement complete'
 WHERE p.mode='enabled' AND EXISTS(SELECT 1 FROM nas_stitch_tasks t WHERE t.cleanup_plan_id=p.id)
 AND NOT EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.state<>'deleted')`); err != nil {
		return nil, err
	}
	var id int64
	var until time.Time
	err := tx.QueryRow(ctx, `SELECT p.id,s.approved_until FROM nas_stitch_tasks t JOIN nas_stitch_scopes s ON s.id=t.scope_id JOIN nas_cleanup_plans p ON p.id=t.cleanup_plan_id WHERE t.state='verified' AND p.mode='dry_run' AND s.deletion_approved AND NOT s.paused AND s.approved_until>now() AND NOT EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.state<>'would_delete') ORDER BY t.id LIMIT 1 FOR UPDATE OF p`).Scan(&id, &until)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if code, _ := cleanupEnableGates(ctx, tx, id); code != 0 {
		return nil, nil
	}
	if until.After(time.Now().Add(nasCleanupMaxApproval)) {
		until = time.Now().Add(nasCleanupMaxApproval)
	}
	release, err := s.cleanupActivateRankings(ctx, tx, id)
	if err != nil {
		return release, err
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_cleanup_plans SET mode='enabled',approval_reason='owner scoped stitched_included',approved_until=$2 WHERE id=$1`, id, until); err != nil {
		return release, err
	}
	_, err = tx.Exec(ctx, `UPDATE nas_cleanup_items SET state='pending',lease_expires_at=NULL WHERE plan_id=$1`, id)
	return release, err
}
func (s *Server) stitchDeletionMaster(i nasCleanupItem) bool {
	return s.cfg.NASCleanupDeletionEnabled && (i.PermitReason == "" || (i.PermitReason == nasStitchReason && s.cfg.NASStitchEnabled && s.cfg.NASStitchDeletionEnabled))
}
func (s *Server) stitchInclusionCurrent(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, i nasCleanupItem) error {
	if i.Kind != "raw" || i.PermitReason != nasStitchReason || i.StitchTaskID <= 0 {
		return errors.New("unsupported stitched retirement")
	}
	if !s.cfg.NASStitchEnabled {
		return errors.New("NAS stitch switch off")
	}
	var plan, result []byte
	var active bool
	err := q.QueryRow(ctx, `SELECT t.plan,t.result,NOT s.paused AND s.deletion_approved AND s.approved_until>now() FROM nas_stitch_tasks t JOIN nas_stitch_scopes s ON s.id=t.scope_id JOIN nas_stitch_outputs o ON o.task_id=t.id WHERE t.id=$1 AND t.state='verified' AND o.relative_path=$2 AND o.sha256=$3 AND o.size_bytes=$4 FOR SHARE OF s,t,o`, i.StitchTaskID, i.StitchOutputPath, i.StitchOutputSHA, i.StitchOutputSize).Scan(&plan, &result, &active)
	if err != nil || !active {
		return errors.New("verified output or owner stream deletion approval absent")
	}
	var p nasStitchPlan
	var r nasStitchResult
	if json.Unmarshal(plan, &p) != nil || json.Unmarshal(result, &r) != nil {
		return errors.New("invalid stitch evidence")
	}
	items, err := validateNASStitchResult(p, r)
	if err != nil {
		return err
	}
	if err := deliveryNASUnheld(ctx, q, i); err != nil {
		return err
	}
	for _, c := range items {
		if c.SourceID == i.SourceID && c.SHA256 == i.SHA256 && c.SizeBytes == i.SizeBytes && c.StitchOutputSHA == i.StitchOutputSHA && c.StitchOutputPath == i.StitchOutputPath && c.StreamID == i.StreamID && c.RelativePath == i.RelativePath {
			return nil
		}
	}
	return errors.New("raw clip has no full source inclusion proof")
}

func (s *Server) recordNASStitchStatus(ctx context.Context, cid int64, raw json.RawMessage) {
	if len(raw) == 0 || len(raw) > 8192 || s.pool == nil {
		return
	}
	var v map[string]any
	if json.Unmarshal(raw, &v) != nil || v == nil {
		return
	}
	raw, _ = json.Marshal(redactNASDiagnostics(v))
	_, _ = s.pool.Exec(ctx, `UPDATE connections SET nas_stitch_status=$2::jsonb,nas_stitch_status_at=now() WHERE id=$1`, cid, string(raw))
}

// Local registration receipts must be durable before cleanup work is visible.
// A crash/lost ACK repeats this transition; it never creates another plan.
func (s *Server) handleAccountNASStitchReady(w http.ResponseWriter, r *http.Request) {
	cid, ok := s.nasRestoreConnection(r.Context(), w, r)
	if !ok {
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Token   string `json:"lease_token"`
		PlanSHA string `json:"plan_sha256"`
		Ready   bool   `json:"outputs_ready"`
	}
	if util.DecodeJSON(r, &req) != nil || !req.Ready || cid != 13 {
		util.WriteError(w, 400, "durable local registration readiness required")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "begin stitch readiness")
		return
	}
	defer tx.Rollback(ctx)
	var plan, result []byte
	var state, token, sha string
	var active bool
	var pid *int64
	err = tx.QueryRow(ctx, `SELECT t.plan,t.result,t.state,t.lease_token,t.plan_sha256,t.cleanup_plan_id,NOT s.paused AND s.approved_until>now() FROM nas_stitch_tasks t JOIN nas_stitch_scopes s ON s.id=t.scope_id WHERE t.id=$1 FOR UPDATE OF t FOR SHARE OF s`, id).Scan(&plan, &result, &state, &token, &sha, &pid, &active)
	if err != nil || req.Token != token || req.PlanSHA != sha || (state != "verified" && state != "held" && state != "completed_no_retirement") {
		util.WriteError(w, 409, "stitch readiness identity differs")
		return
	}
	if pid != nil || state == "completed_no_retirement" {
		util.WriteJSON(w, 200, map[string]any{"ready": true, "id": id})
		return
	}
	var p nasStitchPlan
	var q nasStitchResult
	if json.Unmarshal(plan, &p) != nil || json.Unmarshal(result, &q) != nil {
		util.WriteError(w, 500, "stitch evidence unavailable")
		return
	}
	items, err := validateNASStitchResult(p, q)
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	// Holds can arrive after the immutable stitch plan/result. Retain those
	// sources while accepting the verified outputs and releasing the lane;
	// they must not strand every later stitch task at readiness.
	retire := items[:0]
	for _, item := range items {
		held, err := deliveryNASHeld(ctx, tx, item)
		if err != nil {
			util.WriteError(w, 503, err.Error())
			return
		}
		if !held {
			retire = append(retire, item)
		}
	}
	items = retire
	if len(items) == 0 {
		// Finishing without retirement grants no deletion authority. A scope
		// pause must not strand durable outputs in the shared media lane.
		if _, err = tx.Exec(ctx, `UPDATE nas_stitch_tasks SET state='completed_no_retirement',updated_at=now() WHERE id=$1`, id); err != nil {
			util.WriteError(w, 500, "non-retiring completion failed")
			return
		}
	} else {
		if !s.cfg.NASStitchEnabled || !active {
			util.WriteError(w, 409, "stitch scope inactive")
			return
		}
		for n := range items {
			items[n].StitchTaskID = id
		}
		planID, _, err := persistNASCleanupPlan(ctx, tx, fmt.Sprintf("v85-stitch-%d", id), items)
		if err != nil {
			util.WriteError(w, 409, "cleanup admission: "+err.Error())
			return
		}
		if _, err = tx.Exec(ctx, `UPDATE nas_stitch_tasks SET cleanup_plan_id=$2 WHERE id=$1`, id, planID); err != nil {
			util.WriteError(w, 500, "readiness link failed")
			return
		}
	}
	if tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "readiness commit failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"ready": true, "id": id})
}

func validNASStitchProvenance(p nasStitchProvenance) bool {
	return p.OctoberRecovered && p.Policy == nasStitchPolicy && p.ManifestSHA == nasStitchManifestSHA && p.FFmpegVersion == "N-126826-gc0e8b139fd-20260924" && p.FFprobeVersion == "N-126826-gc0e8b139fd-20260924" && p.FFmpegSHA == "2388de8e05ad71c913ee1a9c4504a1aa5de09ac100a1de92858dd9ec08e9f98b" && p.FFprobeSHA == "9b2bac862606a48f5bc36091d38e5334cf241538a6f4cc47c60f68e7973a365c"
}

// GET capabilities only; no cloud writes, no release or arbitrary object paths.
func (s *Server) handleAccountNASStitchTool(w http.ResponseWriter, r *http.Request) {
	cid, ok := s.nasRestoreConnection(r.Context(), w, r)
	if !ok {
		return
	}
	if cid != 13 || !s.cfg.NASStitchEnabled {
		util.WriteError(w, 403, "V85r tool download disabled")
		return
	}
	name := chi.URLParam(r, "name")
	var sha string
	var size int64
	switch name {
	case "ffmpeg":
		sha = "2388de8e05ad71c913ee1a9c4504a1aa5de09ac100a1de92858dd9ec08e9f98b"
		size = 174165384
	case "ffprobe":
		sha = "9b2bac862606a48f5bc36091d38e5334cf241538a6f4cc47c60f68e7973a365c"
		size = 173923656
	default:
		util.WriteError(w, 404, "unknown V85r tool")
		return
	}
	if s.r2 == nil {
		util.WriteError(w, 503, "V85r artifact storage unavailable")
		return
	}
	key := "experimental/v85-retired-WORK-custody/20261013/" + sha + ".bin"
	head, err := s.r2.Head(r.Context(), key)
	if err != nil || head.SizeBytes != size || head.ETag == "" {
		util.WriteError(w, 409, "pinned V85r tool unavailable")
		return
	}
	cap, err := s.r2.PresignGetExactRequest(r.Context(), key, head.ETag, head.VersionID, 15*time.Minute)
	if err != nil {
		util.WriteError(w, 503, "V85r tool capability unavailable")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"url": cap.URL, "if_match": "\"" + strings.Trim(head.ETag, "\"") + "\"", "sha256": sha, "size_bytes": size, "policy": nasStitchPolicy})
}
