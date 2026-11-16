package api

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const nasStitchReadbackServiceID = "srv-d6usqn94tr6s73d94cd0"

type nasStitchServerExecution struct {
	Repository string `json:"repository"`
	Commit     string `json:"commit_sha"`
	ServiceID  string `json:"service_id"`
	InstanceID string `json:"instance_id"`
}

func nasStitchReadbackServerExecution() (nasStitchServerExecution, error) {
	// Actual runtime identity only. A Render instance is never a deployment ID.
	value := nasStitchServerExecution{Repository: os.Getenv("RENDER_GIT_REPO_SLUG"), Commit: os.Getenv("RENDER_GIT_COMMIT"),
		ServiceID: os.Getenv("RENDER_SERVICE_ID"), InstanceID: os.Getenv("RENDER_INSTANCE_ID")}
	if value.Repository != "daydemir/stoarama" || value.ServiceID != nasStitchReadbackServiceID ||
		len(value.Commit) != 40 || !lowerHex(value.Commit) || value.InstanceID == "" || len(value.InstanceID) > 256 ||
		strings.IndexFunc(value.InstanceID, func(r rune) bool { return r < 33 || r == 127 }) >= 0 {
		return nasStitchServerExecution{}, errors.New("actual server execution identity unavailable")
	}
	return value, nil
}

type nasStitchReadbackSnapshot struct {
	Now           time.Time
	ApprovedUntil time.Time
	SourcePlan    nasStitchPlan
	SourceSHA     string
	Version       string
	BootID        string
	Status        map[string]json.RawMessage
	Facts         nasStitchReadbackFacts
	Frozen        bool
}

// One bounded PK-source read. No lease, catalogue, cleanup or retirement writes.
const nasStitchReadbackSnapshotSQL = `SELECT clock_timestamp(),t.plan,t.plan_sha256,s.approved_until,
 c.client_version,c.client_boot_id,c.last_seen_at,c.nas_host,c.nas_host_reported_at,
 c.nas_stitch_status,c.nas_stitch_status_at,c.inventory_mismatches,
 EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams)
 FROM connections c CROSS JOIN nas_stitch_tasks t JOIN nas_stitch_scopes s ON s.id=t.scope_id
 WHERE c.id=13 AND c.account_id=47 AND c.kind='nas_pull' AND t.id=1 AND t.recording_id=444
 AND s.id=1 AND s.stream_id=17208 AND s.start_at<='2026-08-09T09:00:52.52Z'
 AND s.end_at>='2026-08-09T09:02:52.52Z' AND NOT s.paused AND NOT s.deletion_approved
 AND s.approved_until>clock_timestamp() AND octet_length(t.plan::text)<=1048576`

func (s *Server) nasStitchReadbackSnapshot(ctx context.Context) (nasStitchReadbackSnapshot, error) {
	var value nasStitchReadbackSnapshot
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return value, errors.New("read-only proof observation unavailable")
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL statement_timeout='5000ms';SET LOCAL lock_timeout='500ms';SET LOCAL timezone='UTC'`); err != nil {
		return value, errors.New("bounded read-only proof observation unavailable")
	}
	var plan, host, status []byte
	err = tx.QueryRow(ctx, nasStitchReadbackSnapshotSQL).Scan(&value.Now, &plan, &value.SourceSHA, &value.ApprovedUntil,
		&value.Version, &value.BootID, &value.Facts.HeartbeatAt, &host, &value.Facts.HostAt,
		&status, &value.Facts.StatusAt, &value.Facts.InventoryMismatches, &value.Frozen)
	if err != nil || json.Unmarshal(plan, &value.SourcePlan) != nil || json.Unmarshal(host, &value.Facts.Host) != nil ||
		len(status) == 0 || len(status) > 8192 || json.Unmarshal(status, &value.Status) != nil || value.Status == nil {
		return value, errors.New("current scoped proof facts unavailable")
	}
	if err = tx.Commit(ctx); err != nil {
		return value, errors.New("read-only proof observation incomplete")
	}
	value.Now, value.ApprovedUntil = value.Now.UTC(), value.ApprovedUntil.UTC()
	if value.Now.IsZero() || !value.ApprovedUntil.After(value.Now) || validateNASHostFacts(&value.Facts.Host) != nil {
		return value, errors.New("proof source/host observation invalid")
	}
	// Reuse the unchanged native-v1 source validator; the v3 namespace wrapper
	// subsequently changes only the owned run path and its logical plan digest.
	if _, err = boundedNASStitchProof(value.SourcePlan, value.SourceSHA); err != nil {
		return value, errors.New("immutable proof source metadata differs")
	}
	return value, nil
}
