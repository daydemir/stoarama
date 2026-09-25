package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// NAS host facts and one-shot media benchmark.
//
// Host facts ride the heartbeat so operators can see what the NAS container
// runs on. The benchmark measures, on one real hour of clips that are already
// on the NAS, what NAS-side collation would cost. The server freezes the exact
// clip list at first claim; the client reads those files read-only, writes a
// temporary remux under its state volume, measures, deletes the temp output and
// reports. A benchmark runs once per connection automatically; an operator can
// request another with `stoaramactl nas-benchmark request`.

const (
	nasBenchmarkDeadline       = 30 * time.Minute
	nasBenchmarkLease          = 45 * time.Minute
	nasBenchmarkMaxClaims      = 3
	nasBenchmarkIdleRetrySec   = 1800
	nasBenchmarkDisabledSec    = 3600
	nasBenchmarkMaxClips       = 200
	nasBenchmarkMaxResultBytes = 256 * 1024
	nasBenchmarkMaxError       = 1000
	nasHostMaxString           = 256
)

var nasBenchmarkStatuses = map[string]bool{"ok": true, "partial": true, "error": true, "unsupported_arch": true}

var nasHostToken = regexp.MustCompile(`^[A-Za-z0-9_.+-]{1,64}$`)

// nasHostFacts is what the NAS client can observe about its own container.
// Pointers are "not observable" (for example no cgroup limit file), which is
// different from zero.
type nasHostFacts struct {
	Machine                string   `json:"machine"`
	System                 string   `json:"system"`
	KernelRelease          string   `json:"kernel_release"`
	PythonVersion          string   `json:"python_version"`
	CPUModel               string   `json:"cpu_model"`
	CPUCount               int      `json:"cpu_count"`
	AffinityCPUs           int      `json:"affinity_cpus"`
	CgroupCPULimit         *float64 `json:"cgroup_cpu_limit"`
	MemTotalBytes          int64    `json:"mem_total_bytes"`
	MemAvailableBytes      int64    `json:"mem_available_bytes"`
	CgroupMemoryLimitBytes *int64   `json:"cgroup_memory_limit_bytes"`
	StateExecAllowed       *bool    `json:"state_exec_allowed"`
	Load1                  *float64 `json:"load1"`
}

func validateNASHostFacts(h *nasHostFacts) error {
	if h == nil {
		return nil
	}
	for _, token := range []string{h.Machine, h.System, h.PythonVersion} {
		if token != "" && !nasHostToken.MatchString(token) {
			return errors.New("invalid NAS host token")
		}
	}
	for _, text := range []string{h.KernelRelease, h.CPUModel} {
		if len(text) > nasHostMaxString {
			return errors.New("NAS host string is too long")
		}
		for _, r := range text {
			if r < 32 || r == 127 {
				return errors.New("NAS host string has control characters")
			}
		}
	}
	if h.CPUCount < 0 || h.CPUCount > 4096 || h.AffinityCPUs < 0 || h.AffinityCPUs > 4096 {
		return errors.New("invalid NAS host cpu count")
	}
	if h.MemTotalBytes < 0 || h.MemAvailableBytes < 0 {
		return errors.New("invalid NAS host memory")
	}
	if h.CgroupCPULimit != nil && (*h.CgroupCPULimit <= 0 || *h.CgroupCPULimit > 4096) {
		return errors.New("invalid NAS host cgroup cpu limit")
	}
	if h.CgroupMemoryLimitBytes != nil && *h.CgroupMemoryLimitBytes <= 0 {
		return errors.New("invalid NAS host cgroup memory limit")
	}
	if h.Load1 != nil && (*h.Load1 < 0 || *h.Load1 > 100000) {
		return errors.New("invalid NAS host load")
	}
	return nil
}

type nasBenchmarkClaimRequest struct {
	ClientVersion string        `json:"client_version"`
	Host          *nasHostFacts `json:"host,omitempty"`
}

type nasBenchmarkClip struct {
	ClipID       int64     `json:"clip_id"`
	RelativePath string    `json:"relative_path"`
	SizeBytes    int64     `json:"size_bytes"`
	SHA256       string    `json:"sha256"`
	ClipStartAt  time.Time `json:"clip_start_at"`
	ClipEndAt    time.Time `json:"clip_end_at"`
	DurationMS   int64     `json:"duration_ms"`
}

type nasBenchmarkPlan struct {
	RecordingID   int64              `json:"recording_id"`
	WindowStartAt time.Time          `json:"window_start_at"`
	WindowEndAt   time.Time          `json:"window_end_at"`
	Clips         []nasBenchmarkClip `json:"clips"`
}

type nasBenchmarkTask struct {
	BenchmarkID    int64     `json:"benchmark_id"`
	DeadlineSec    int       `json:"deadline_sec"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	nasBenchmarkPlan
}

type nasBenchmarkClaimResponse struct {
	Enabled       bool              `json:"enabled"`
	RetryAfterSec int               `json:"retry_after_sec"`
	Task          *nasBenchmarkTask `json:"task"`
}

type nasBenchmarkResultRequest struct {
	ClientVersion string          `json:"client_version"`
	Status        string          `json:"status"`
	Error         string          `json:"error"`
	Host          *nasHostFacts   `json:"host,omitempty"`
	Result        json.RawMessage `json:"result"`
}

func validateNASBenchmarkResult(req nasBenchmarkResultRequest) error {
	if !nasBenchmarkStatuses[req.Status] {
		return errors.New("invalid benchmark status")
	}
	if len(req.Error) > nasBenchmarkMaxError {
		return errors.New("benchmark error is too long")
	}
	if req.Status != "ok" && req.Error == "" {
		return errors.New("a non-ok benchmark requires an error")
	}
	if req.ClientVersion != "" && (len(req.ClientVersion) > 64 || !relayArtifactName.MatchString(req.ClientVersion)) {
		return errors.New("invalid client_version")
	}
	if err := validateNASHostFacts(req.Host); err != nil {
		return err
	}
	if len(req.Result) == 0 || len(req.Result) > nasBenchmarkMaxResultBytes {
		return errors.New("benchmark result is required and must be at most 256 KiB")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(req.Result, &object); err != nil || object == nil {
		return errors.New("benchmark result must be a JSON object")
	}
	return nil
}

// validateNASBenchmarkPlan rejects a plan the client could not safely execute.
func validateNASBenchmarkPlan(plan nasBenchmarkPlan) error {
	if len(plan.Clips) == 0 || len(plan.Clips) > nasBenchmarkMaxClips {
		return fmt.Errorf("benchmark hour has %d clips", len(plan.Clips))
	}
	for _, clip := range plan.Clips {
		if clip.RelativePath == "" || clip.SizeBytes <= 0 || len(clip.SHA256) != 64 || !lowerHex(clip.SHA256) {
			return fmt.Errorf("benchmark clip %d is incomplete", clip.ClipID)
		}
	}
	return nil
}

func (s *Server) handleAccountConnectionBenchmarkClaim(w http.ResponseWriter, r *http.Request) {
	var req nasBenchmarkClaimRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.ClientVersion != "" && (len(req.ClientVersion) > 64 || !relayArtifactName.MatchString(req.ClientVersion)) {
		util.WriteError(w, http.StatusBadRequest, "invalid client_version")
		return
	}
	if err := validateNASHostFacts(req.Host); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin benchmark claim failed")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	connectionID, ok := s.nasUploadProbeConnection(ctx, tx, w, r)
	if !ok {
		return
	}
	principal, _ := accountPrincipalFromContext(ctx)
	if !s.cfg.NASBenchmarkEnabled {
		util.WriteJSON(w, http.StatusOK, nasBenchmarkClaimResponse{RetryAfterSec: nasBenchmarkDisabledSec})
		return
	}
	// First contact: one automatic benchmark per connection, ever.
	if _, err := tx.Exec(ctx, `
		INSERT INTO nas_benchmarks (connection_id,requested_by)
		SELECT $1,'auto' WHERE NOT EXISTS (SELECT 1 FROM nas_benchmarks WHERE connection_id=$1)`, connectionID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "request first benchmark failed")
		return
	}
	// An expired claim (client restarted or self-updated mid-run) is re-offered
	// a bounded number of times.
	if _, err := tx.Exec(ctx, `
		UPDATE nas_benchmarks SET
		  state=CASE WHEN claim_count >= $2 THEN 'abandoned' ELSE 'requested' END,
		  error=CASE WHEN claim_count >= $2 THEN 'lease expired without a report' ELSE error END
		WHERE connection_id=$1 AND state='claimed' AND lease_expires_at < now()`, connectionID, nasBenchmarkMaxClaims); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "expire benchmark claims failed")
		return
	}
	var benchmarkID int64
	var pinnedRecording *int64
	var pinnedStart *time.Time
	var planRaw []byte
	err = tx.QueryRow(ctx, `
		SELECT id,recording_id,window_start_at,plan FROM nas_benchmarks
		WHERE connection_id=$1 AND state='requested' ORDER BY id LIMIT 1 FOR UPDATE`, connectionID).
		Scan(&benchmarkID, &pinnedRecording, &pinnedStart, &planRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit benchmark claim failed")
			return
		}
		util.WriteJSON(w, http.StatusOK, nasBenchmarkClaimResponse{Enabled: true, RetryAfterSec: nasBenchmarkIdleRetrySec})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load benchmark request failed")
		return
	}
	var plan nasBenchmarkPlan
	if len(planRaw) > 0 {
		if err := json.Unmarshal(planRaw, &plan); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "decode benchmark plan failed")
			return
		}
	} else {
		plan, err = selectNASBenchmarkPlan(ctx, tx, principal.AccountID, connectionID, pinnedRecording, pinnedStart)
		if err == nil {
			err = validateNASBenchmarkPlan(plan)
		}
		if err != nil {
			message := err.Error()
			if len(message) > nasBenchmarkMaxError {
				message = message[:nasBenchmarkMaxError]
			}
			if _, uerr := tx.Exec(ctx, `UPDATE nas_benchmarks SET state='abandoned',error=$2 WHERE id=$1`, benchmarkID, message); uerr != nil {
				util.WriteError(w, http.StatusInternalServerError, "record benchmark plan failure failed")
				return
			}
			if err := tx.Commit(ctx); err != nil {
				util.WriteError(w, http.StatusInternalServerError, "commit benchmark claim failed")
				return
			}
			util.WriteJSON(w, http.StatusOK, nasBenchmarkClaimResponse{Enabled: true, RetryAfterSec: nasBenchmarkIdleRetrySec})
			return
		}
		encoded, err := json.Marshal(plan)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, "encode benchmark plan failed")
			return
		}
		planRaw = encoded
	}
	var leaseExpires time.Time
	if err := tx.QueryRow(ctx, `
		UPDATE nas_benchmarks SET state='claimed',plan=$2,claim_count=claim_count+1,claimed_at=now(),
		  lease_expires_at=now()+$3::interval,client_version=$4
		WHERE id=$1 RETURNING lease_expires_at`,
		benchmarkID, planRaw, fmt.Sprintf("%d seconds", int(nasBenchmarkLease.Seconds())), req.ClientVersion).Scan(&leaseExpires); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "claim benchmark failed")
		return
	}
	if err := tx.Commit(ctx); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit benchmark claim failed")
		return
	}
	util.WriteJSON(w, http.StatusOK, nasBenchmarkClaimResponse{
		Enabled: true, RetryAfterSec: nasBenchmarkIdleRetrySec,
		Task: &nasBenchmarkTask{
			BenchmarkID: benchmarkID, DeadlineSec: int(nasBenchmarkDeadline.Seconds()),
			LeaseExpiresAt: leaseExpires, nasBenchmarkPlan: plan,
		},
	})
}

// selectNASBenchmarkPlan picks one hour whose clips are all present on this
// NAS with the database size and sha256. Without a pin it takes the most
// recent complete single-job hour between one and four days old, walking each
// recording through its (recording_id, clip_start_at) index.
func selectNASBenchmarkPlan(ctx context.Context, q pgx.Tx, accountID, connectionID int64, pinnedRecording *int64, pinnedStart *time.Time) (nasBenchmarkPlan, error) {
	var plan nasBenchmarkPlan
	if pinnedRecording != nil && pinnedStart != nil {
		plan.RecordingID, plan.WindowStartAt = *pinnedRecording, pinnedStart.UTC()
	} else {
		err := q.QueryRow(ctx, `
			SELECT r.id, x.h FROM recordings r
			CROSS JOIN LATERAL (
			  SELECT date_trunc('hour', c.clip_start_at) AS h FROM recording_clips c
			  LEFT JOIN nas_inventory_files n ON n.connection_id=$2 AND n.clip_id=c.id AND n.state='present'
			    AND n.size_bytes=c.size_bytes AND lower(n.sha256)=lower(c.sha256)
			  WHERE c.recording_id=r.id AND c.purged_at IS NULL
			    AND c.clip_start_at >= now()-interval '4 days' AND c.clip_start_at < now()-interval '1 day'
			  GROUP BY 1
			  HAVING count(*) BETWEEN 55 AND 61 AND count(n.clip_id)=count(*) AND count(DISTINCT c.recording_job_id)=1
			) x
			WHERE r.account_id=$1
			ORDER BY x.h DESC, r.id LIMIT 1`, accountID, connectionID).Scan(&plan.RecordingID, &plan.WindowStartAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return plan, errors.New("no complete hour on the NAS in the last four days")
		}
		if err != nil {
			return plan, fmt.Errorf("select benchmark hour: %w", err)
		}
		plan.WindowStartAt = plan.WindowStartAt.UTC()
	}
	plan.WindowEndAt = plan.WindowStartAt.Add(time.Hour)
	rows, err := q.Query(ctx, `
		SELECT c.id,n.relative_path,c.size_bytes,lower(c.sha256),c.clip_start_at,c.clip_end_at,c.duration_ms
		FROM recording_clips c
		JOIN recordings r ON r.id=c.recording_id AND r.account_id=$1
		JOIN nas_inventory_files n ON n.connection_id=$2 AND n.clip_id=c.id AND n.state='present'
		  AND n.size_bytes=c.size_bytes AND lower(n.sha256)=lower(c.sha256)
		WHERE c.recording_id=$3 AND c.purged_at IS NULL AND c.clip_start_at >= $4 AND c.clip_start_at < $5
		ORDER BY c.clip_start_at, c.id LIMIT $6`,
		accountID, connectionID, plan.RecordingID, plan.WindowStartAt, plan.WindowEndAt, nasBenchmarkMaxClips+1)
	if err != nil {
		return plan, fmt.Errorf("load benchmark clips: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var clip nasBenchmarkClip
		if err := rows.Scan(&clip.ClipID, &clip.RelativePath, &clip.SizeBytes, &clip.SHA256, &clip.ClipStartAt, &clip.ClipEndAt, &clip.DurationMS); err != nil {
			return plan, fmt.Errorf("scan benchmark clip: %w", err)
		}
		plan.Clips = append(plan.Clips, clip)
	}
	if err := rows.Err(); err != nil {
		return plan, fmt.Errorf("load benchmark clips: %w", err)
	}
	return plan, nil
}

func (s *Server) handleAccountConnectionBenchmarkResult(w http.ResponseWriter, r *http.Request) {
	benchmarkID, ok := parseInt64Path(w, r, "benchmarkId")
	if !ok {
		return
	}
	var req nasBenchmarkResultRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateNASBenchmarkResult(req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	var host []byte
	if req.Host != nil {
		encoded, err := json.Marshal(req.Host)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, "invalid host facts")
			return
		}
		host = encoded
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin benchmark result failed")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	connectionID, ok := s.nasUploadProbeConnection(ctx, tx, w, r)
	if !ok {
		return
	}
	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM nas_benchmarks WHERE id=$1 AND connection_id=$2 FOR UPDATE`, benchmarkID, connectionID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "benchmark not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load benchmark failed")
		return
	}
	if state == "reported" {
		util.WriteError(w, http.StatusConflict, "benchmark already reported")
		return
	}
	// A late report after the lease was re-offered or abandoned is still the
	// measurement we wanted; accept it.
	if _, err := tx.Exec(ctx, `
		UPDATE nas_benchmarks SET state='reported',reported_at=now(),status=$2,error=$3,host=$4,result=$5,
		  client_version=CASE WHEN $6<>'' THEN $6 ELSE client_version END
		WHERE id=$1`, benchmarkID, req.Status, req.Error, host, []byte(req.Result), req.ClientVersion); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "record benchmark result failed")
		return
	}
	if host != nil {
		if _, err := tx.Exec(ctx, `UPDATE connections SET nas_host=$2,nas_host_reported_at=now() WHERE id=$1`, connectionID, host); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "record host facts failed")
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit benchmark result failed")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}
