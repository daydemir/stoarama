package api

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// NAS app remote diagnostics (owner-approved, 2026-11). The app uploads a
// bounded, already-redacted ring of recent log lines; the API re-applies the
// credential redactions, stores each upload append-only and keeps 7 days /
// 2000 uploads per connection. Operators read them with
// `stoaramactl nas-client logs`.
const (
	nasClientLogMaxLines     = 500
	nasClientLogMaxLineBytes = 4096
	nasClientLogMaxBytes     = 512 * 1024
	nasClientLogKeepUploads  = 2000
)

var nasClientLogRedactions = []struct {
	re   *regexp.Regexp
	with string
}{
	{regexp.MustCompile(`(?i)\b(authorization)(\s*[:=]\s*)(?:bearer\s+|basic\s+)?\S+`), `${1}${2}[REDACTED]`},
	{regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`), `${1} [REDACTED]`},
	{regexp.MustCompile(`(?i)\b((?:https?|s3)://[^\s?#'"<>]+)\?[^\s'"<>]*`), `${1}?[REDACTED]`},
	{regexp.MustCompile(`\b(?:sir|sk|rk|pk|ghp|gho)_[A-Za-z0-9_-]{8,}\b`), `[REDACTED]`},
}

func redactNASClientLogLine(s string) string {
	for _, r := range nasClientLogRedactions {
		s = r.re.ReplaceAllString(s, r.with)
	}
	return s
}

type nasClientLogLine struct {
	Seq     int64  `json:"seq"`
	At      string `json:"at"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

func (s *Server) handleAccountNASClientLogs(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientVersion  string             `json:"client_version"`
		BootID         string             `json:"boot_id"`
		JournalEntries *int               `json:"journal_entries"`
		Quarantined    *int               `json:"quarantined"`
		DroppedLines   int                `json:"dropped_lines"`
		Lines          []nasClientLogLine `json:"lines"`
	}
	if util.DecodeJSON(r, &req) != nil || len(req.ClientVersion) < 1 || len(req.ClientVersion) > 64 || len(req.BootID) > 64 || len(req.Lines) > nasClientLogMaxLines || req.DroppedLines < 0 {
		util.WriteError(w, 400, "client version, boot id and at most 500 log lines required")
		return
	}
	total := 0
	for n, line := range req.Lines {
		if len(line.Message) > nasClientLogMaxLineBytes || len(line.Level) > 16 || len(line.At) > 40 {
			util.WriteError(w, 400, "log line exceeds bounds")
			return
		}
		req.Lines[n].Message = redactNASClientLogLine(line.Message)
		total += len(line.Message) + len(line.Level) + len(line.At)
	}
	if total > nasClientLogMaxBytes {
		util.WriteError(w, 400, "log upload exceeds bounds")
		return
	}
	ctx := r.Context()
	cid, ok := s.nasRestoreConnection(ctx, w, r)
	if !ok {
		return
	}
	lines, _ := json.Marshal(req.Lines)
	if _, err := s.pool.Exec(ctx, `INSERT INTO nas_client_logs(connection_id,client_version,boot_id,journal_entries,quarantined,dropped_lines,lines) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)`,
		cid, req.ClientVersion, req.BootID, req.JournalEntries, req.Quarantined, req.DroppedLines, string(lines)); err != nil {
		util.WriteError(w, 500, "client log upload failed")
		return
	}
	// Retention: 7 days, at most nasClientLogKeepUploads uploads per connection.
	_, _ = s.pool.Exec(ctx, `DELETE FROM nas_client_logs WHERE connection_id=$1 AND (received_at<now()-interval '7 days' OR id<=(SELECT id FROM nas_client_logs WHERE connection_id=$1 ORDER BY id DESC OFFSET $2 LIMIT 1))`, cid, nasClientLogKeepUploads)
	util.WriteJSON(w, 200, map[string]any{"ok": true, "stored_lines": len(req.Lines)})
}

// handleAdminNASClientLogs returns uploaded lines (oldest first) for one
// connection: ?since=1h (Go duration, default 1h, max 7 days), ?grep= (case
// insensitive substring), ?limit= (default 2000, max 10000).
func (s *Server) handleAdminNASClientLogs(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	since := time.Hour
	if v := r.URL.Query().Get("since"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 || d > 7*24*time.Hour {
			util.WriteError(w, 400, "since must be a duration up to 168h")
			return
		}
		since = d
	}
	limit := 2000
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 10000 {
			util.WriteError(w, 400, "limit must be 1..10000")
			return
		}
		limit = n
	}
	grep := strings.ToLower(r.URL.Query().Get("grep"))
	rows, err := s.pool.Query(r.Context(), `SELECT received_at,client_version,boot_id,coalesce(journal_entries,-1),coalesce(quarantined,-1),dropped_lines,lines FROM nas_client_logs WHERE connection_id=$1 AND received_at>now()-$2::bigint*interval '1 second' ORDER BY id`, id, int64(since/time.Second))
	if err != nil {
		util.WriteError(w, 500, "client logs unavailable")
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	uploads := 0
	var last map[string]any
	for rows.Next() {
		var received time.Time
		var version, boot string
		var journal, quarantined, dropped int
		var raw []byte
		if err := rows.Scan(&received, &version, &boot, &journal, &quarantined, &dropped, &raw); err != nil {
			util.WriteError(w, 500, "client logs unavailable")
			return
		}
		uploads++
		last = map[string]any{"received_at": received, "client_version": version, "boot_id": boot, "journal_entries": journal, "quarantined": quarantined, "dropped_lines": dropped}
		var lines []nasClientLogLine
		_ = json.Unmarshal(raw, &lines)
		for _, line := range lines {
			if grep != "" && !strings.Contains(strings.ToLower(line.Message), grep) && !strings.Contains(strings.ToLower(line.Level), grep) {
				continue
			}
			out = append(out, map[string]any{"at": line.At, "level": line.Level, "message": line.Message, "client_version": version, "boot_id": boot})
		}
	}
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	util.WriteJSON(w, 200, map[string]any{"connection_id": id, "uploads": uploads, "latest_upload": last, "lines": out})
}
