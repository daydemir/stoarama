package api

import (
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// NAS app remote diagnostics (owner-approved, 2026-11). The app uploads a
// bounded, already-redacted ring of recent log lines; the API re-applies the
// credential redactions, stores each upload append-only and keeps 7 days. Operators read them with
// `stoaramactl nas-client logs`.
const (
	nasClientLogMaxLines     = 500
	nasClientLogMaxLineBytes = 4096
	nasClientLogMaxBytes     = 512 * 1024
)

// Keep these patterns and their order in sync with the NAS client's _REDACTIONS.
var nasClientLogRedactions = []struct {
	re   *regexp.Regexp
	with string
}{
	{regexp.MustCompile(`(?is)([\[(]\s*\\*["']\s*[A-Za-z0-9_.-]*(?:token|secret|passw(?:or)?d|key|signature|credential|authorization|cookie|dsn|database_url|x-amz-[a-z-]+)[A-Za-z0-9_.-]*\s*\\*["']\s*,\s*)[\s\S]*`), `${1}[REDACTED]`},
	// Match arbitrary escaped quotes; redact the whole tail of sensitive
	// assignments so arrays, objects and Digest parameters cannot leak.
	{regexp.MustCompile(`(?is)(\b[A-Za-z0-9_.-]*(?:token|secret|passw(?:or)?d|key|signature|credential|authorization|cookie|dsn|database_url|x-amz-[a-z-]+)[A-Za-z0-9_.-]*(?:\\*["'])?\s*[=:]\s*)[\s\S]*`), `${1}[REDACTED]`},
	{regexp.MustCompile(`(?i)([\"']?\b[A-Za-z0-9_.-]*(?:token|secret|passw(?:or)?d|key|signature|credential|authorization|cookie|dsn|database_url|x-amz-[a-z-]+)[A-Za-z0-9_.-]*[\"']?\s*[=:]\s*)(\[REDACTED\]|\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|(?:bearer\s+|basic\s+)?[^\s,;&}\]]+)`), `${1}[REDACTED]`},
	{regexp.MustCompile(`(?i)(\bauthorization[\"']?\s*[:=]\s*[\"']?)(?:bearer\s+|basic\s+)?[^\s\"',}]+`), `${1}[REDACTED]`},
	{regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`), `${1} [REDACTED]`},
	{regexp.MustCompile(`(?i)\b(?:postgres(?:ql)?|mysql|redis(?:s)?)://[^\s'\"<>]+`), `[REDACTED]`},
	{regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)[^/\s'\"<>]*@`), `${1}[REDACTED]@`},
	{regexp.MustCompile(`(?i)\b((?:https?|s3)://[^\s?#'\"<>]+)\?[^\s'\"<>]*`), `${1}?[REDACTED]`},
	{regexp.MustCompile(`\beyJ[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+){0,2}`), `[REDACTED]`},
	{regexp.MustCompile(`\btskey-[A-Za-z0-9_-]+`), `[REDACTED]`},
	{regexp.MustCompile(`\bAKIA[A-Z0-9]{16}\b`), `[REDACTED]`},
	{regexp.MustCompile(`\b100\.(?:6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\.[0-9]{1,3}\.[0-9]{1,3}\b`), `[REDACTED]`},
	{regexp.MustCompile(`(?i)\bfd7a:115c:a1e0(?::[0-9a-f]{0,4})+(?:%[A-Za-z0-9_.-]+)?`), `[REDACTED]`},
	{regexp.MustCompile(`(?i)\b[A-Za-z0-9.-]+\.ts\.net\b`), `[REDACTED]`},
	{regexp.MustCompile(`\b(?:sir|sk|rk|pk|ghp|gho|xox[abp])_[A-Za-z0-9_-]{8,}\b`), `[REDACTED]`},
}

var nasDiagnosticSecretName = regexp.MustCompile(`(?i)^[A-Za-z0-9_.-]*(?:token|secret|passw(?:or)?d|key|signature|credential|authorization|cookie|dsn|database_url|x-amz-[a-z-]+)[A-Za-z0-9_.-]*$`)

var nasClientLogPath = regexp.MustCompile(`(^|[^\w:/.])(/[^\s'"<>,;)]+)`)

var nasDiagnosticUnicodeEscape = regexp.MustCompile(`\\+u([0-9a-fA-F]{4})`)

func normalizeNASDiagnosticText(s string) string {
	// Each pass shortens the input, including nested JSON backslash escapes.
	for {
		decoded := nasDiagnosticUnicodeEscape.ReplaceAllStringFunc(s, func(escape string) string {
			n, _ := strconv.ParseUint(escape[len(escape)-4:], 16, 16)
			return string(rune(n))
		})
		if decoded == s {
			return s
		}
		s = decoded
	}
}

func redactNASClientLogLine(s string) string {
	s = normalizeNASDiagnosticText(s)
	for _, r := range nasClientLogRedactions {
		s = r.re.ReplaceAllString(s, r.with)
	}
	return nasClientLogPath.ReplaceAllStringFunc(s, func(match string) string {
		parts := nasClientLogPath.FindStringSubmatch(match)
		for _, route := range []string{"/account/", "/api/", "/nas/", "/service/"} {
			if strings.HasPrefix(parts[2], route) {
				return match
			}
		}
		return parts[1] + "<path>"
	})
}

// Redact recursively after parsing so JSON quotes/escapes remain valid.
func redactNASDiagnostics(value any) any {
	switch v := value.(type) {
	case string:
		return redactNASClientLogLine(v)
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, x := range v {
			if nasDiagnosticSecretName.MatchString(normalizeNASDiagnosticText(k)) {
				out[redactNASClientLogLine(k)] = "[REDACTED]"
			} else {
				out[redactNASClientLogLine(k)] = redactNASDiagnostics(x)
			}
		}
		return out
	case []any:
		if len(v) >= 2 {
			if name, ok := v[0].(string); ok && nasDiagnosticSecretName.MatchString(normalizeNASDiagnosticText(name)) {
				return []any{redactNASClientLogLine(name), "[REDACTED]"}
			}
		}
		for i, x := range v {
			v[i] = redactNASDiagnostics(x)
		}
	}
	return value
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
		req.Lines[n].Level = redactNASClientLogLine(line.Level)
		req.Lines[n].At = redactNASClientLogLine(line.At)
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
		cid, redactNASClientLogLine(req.ClientVersion), redactNASClientLogLine(req.BootID), req.JournalEntries, req.Quarantined, req.DroppedLines, string(lines)); err != nil {
		util.WriteError(w, 500, "client log upload failed")
		return
	}
	// Time is the retention boundary: an upload storm must never evict
	// another upload from within the seven-day diagnostic window.
	_, _ = s.pool.Exec(ctx, `DELETE FROM nas_client_logs WHERE received_at<now()-interval '7 days'`)
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
	// Metadata is independent of the line filter. Fetch at most one upload.
	var uploads int
	var last map[string]any
	var received time.Time
	var version, boot string
	var journal, quarantined, dropped int
	err := s.pool.QueryRow(r.Context(), `SELECT count(*) OVER(),received_at,client_version,boot_id,coalesce(journal_entries,-1),coalesce(quarantined,-1),dropped_lines FROM nas_client_logs WHERE connection_id=$1 AND received_at>now()-$2::bigint*interval '1 second' ORDER BY id DESC LIMIT 1`, id, int64(since/time.Second)).Scan(&uploads, &received, &version, &boot, &journal, &quarantined, &dropped)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 500, "client logs unavailable")
		return
	}
	if err == nil {
		last = map[string]any{"received_at": received, "client_version": version, "boot_id": boot, "journal_entries": journal, "quarantined": quarantined, "dropped_lines": dropped}
	}
	// Expand and filter lines in PostgreSQL, select the newest LIMIT there,
	// then return them oldest first. The application never loads the window.
	rows, err := s.pool.Query(r.Context(), `SELECT at,level,message,client_version,boot_id FROM (
      SELECT l.id,x.n,x.line->>'at' AS at,x.line->>'level' AS level,x.line->>'message' AS message,l.client_version,l.boot_id
      FROM nas_client_logs l CROSS JOIN LATERAL jsonb_array_elements(l.lines) WITH ORDINALITY x(line,n)
      WHERE l.connection_id=$1 AND l.received_at>now()-$2::bigint*interval '1 second'
       AND ($3='' OR strpos(lower(x.line->>'message'),$3)>0 OR strpos(lower(x.line->>'level'),$3)>0)
      ORDER BY l.id DESC,x.n DESC LIMIT $4
    ) recent ORDER BY id,n`, id, int64(since/time.Second), grep, limit)
	if err != nil {
		util.WriteError(w, 500, "client logs unavailable")
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var at, level, message, version, boot string
		if err := rows.Scan(&at, &level, &message, &version, &boot); err != nil {
			util.WriteError(w, 500, "client logs unavailable")
			return
		}
		out = append(out, map[string]any{"at": at, "level": level, "message": redactNASClientLogLine(message), "client_version": version, "boot_id": boot})
	}
	if rows.Err() != nil {
		util.WriteError(w, 500, "client logs unavailable")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"connection_id": id, "uploads": uploads, "latest_upload": last, "lines": out})
}

// The monotonic generation survives lost heartbeat responses and old clients.
// Release affects local receipt replay only; it grants no deletion authority.
func (s *Server) handleAdminNASReleaseQuarantine(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var generation int64
	err := s.pool.QueryRow(r.Context(), `UPDATE connections SET nas_cleanup_release_generation=coalesce(nas_cleanup_release_generation,0)+1 WHERE id=$1 RETURNING nas_cleanup_release_generation`, id).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 404, "connection not found")
		return
	}
	if err != nil {
		util.WriteError(w, 500, "quarantine release failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"connection_id": id, "cleanup_release_generation": generation})
}
