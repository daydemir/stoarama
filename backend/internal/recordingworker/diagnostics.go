package recordingworker

import (
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

var (
	diagnosticURLRe        = regexp.MustCompile(`https?://\S+`)
	diagnosticBearerRe     = regexp.MustCompile(`(?i)\b(bearer\s+)[A-Za-z0-9._~+/-]+=*`)
	diagnosticTokenFieldRe = regexp.MustCompile(`(?i)\b(token|signature|credential|access_key|secret_key)=\S+`)
)

type RelayDiagnostics struct {
	mu      sync.Mutex
	current map[int64]*jobDiagnostic
	last    *jobDiagnostic
}

type jobDiagnostic struct {
	JobID         int64
	RecordingID   int64
	Stage         string
	LastError     string
	StartedAt     time.Time
	StageAt       time.Time
	FinishedAt    *time.Time
	SegmentCount  int
	LastSegmentAt *time.Time
}

func (d *RelayDiagnostics) Start(job recordingapi.RecordingJob) {
	if d == nil {
		return
	}
	now := time.Now().UTC()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.current == nil {
		d.current = make(map[int64]*jobDiagnostic)
	}
	d.current[job.JobID] = &jobDiagnostic{
		JobID:       job.JobID,
		RecordingID: job.RecordingID,
		Stage:       "leased",
		StartedAt:   now,
		StageAt:     now,
	}
}

func (d *RelayDiagnostics) Stage(jobID int64, stage string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if j := d.current[jobID]; j != nil {
		j.Stage = strings.TrimSpace(stage)
		j.StageAt = time.Now().UTC()
	}
}

func (d *RelayDiagnostics) Error(jobID int64, stage string, err error) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if j := d.current[jobID]; j != nil {
		j.Stage = strings.TrimSpace(stage)
		j.LastError = sanitizeDiagnosticError(err)
		j.StageAt = time.Now().UTC()
	}
}

func (d *RelayDiagnostics) Segment(jobID int64, at time.Time) {
	if d == nil {
		return
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	at = at.UTC()
	d.mu.Lock()
	defer d.mu.Unlock()
	if j := d.current[jobID]; j != nil {
		j.Stage = "segment_ingested"
		j.StageAt = time.Now().UTC()
		j.SegmentCount++
		j.LastSegmentAt = &at
	}
}

func (d *RelayDiagnostics) Finish(jobID int64, stage string, err error) {
	if d == nil {
		return
	}
	now := time.Now().UTC()
	d.mu.Lock()
	defer d.mu.Unlock()
	j := d.current[jobID]
	if j == nil {
		return
	}
	delete(d.current, jobID)
	cp := *j
	cp.Stage = strings.TrimSpace(stage)
	cp.StageAt = now
	cp.FinishedAt = &now
	if err != nil {
		cp.LastError = sanitizeDiagnosticError(err)
	}
	d.last = &cp
}

func (d *RelayDiagnostics) Snapshot() map[string]any {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	var cur *jobDiagnostic
	for _, j := range d.current {
		if cur == nil || j.StageAt.After(cur.StageAt) {
			cur = j
		}
	}
	var curOut any
	if cur != nil {
		curOut = diagnosticMap(cur)
	}
	var lastOut any
	if d.last != nil {
		lastOut = diagnosticMap(d.last)
	}
	return map[string]any{
		"current": curOut,
		"last":    lastOut,
	}
}

func diagnosticMap(j *jobDiagnostic) map[string]any {
	if j == nil {
		return nil
	}
	out := map[string]any{
		"job_id":        j.JobID,
		"recording_id":  j.RecordingID,
		"stage":         j.Stage,
		"stage_at":      j.StageAt.UTC().Format(time.RFC3339Nano),
		"started_at":    j.StartedAt.UTC().Format(time.RFC3339Nano),
		"segment_count": j.SegmentCount,
	}
	if j.LastError != "" {
		out["last_error"] = j.LastError
	}
	if j.FinishedAt != nil {
		out["finished_at"] = j.FinishedAt.UTC().Format(time.RFC3339Nano)
	}
	if j.LastSegmentAt != nil {
		out["last_segment_at"] = j.LastSegmentAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func sanitizeDiagnosticError(err error) string {
	if err == nil {
		return ""
	}
	s := strings.TrimSpace(err.Error())
	if s == "" {
		return ""
	}
	s = diagnosticURLRe.ReplaceAllStringFunc(s, sanitizeDiagnosticURL)
	s = diagnosticBearerRe.ReplaceAllString(s, "${1}[redacted]")
	s = diagnosticTokenFieldRe.ReplaceAllString(s, "${1}=[redacted]")
	if len(s) > 500 {
		s = s[:500] + "..."
	}
	return s
}

func sanitizeDiagnosticURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "[url]"
	}
	p := u.EscapedPath()
	if len(p) > 120 || strings.Contains(u.Host, "googlevideo.com") {
		base := path.Base(u.Path)
		if base == "." || base == "/" || base == "" {
			p = "/..."
		} else {
			p = "/.../" + base
		}
	}
	out := u.Scheme + "://" + u.Host + p
	if u.RawQuery != "" {
		out += "?[query]"
	}
	return out
}
