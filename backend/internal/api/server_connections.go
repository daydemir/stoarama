package api

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// The two pull endpoints that carry numeric path params. r.URL.Path is reliable in
// middleware (chi's RoutePattern is not, for nested-group middleware), so the
// allowlist matches on the raw path with precompiled, anchored regexps for the
// param routes and literal+method checks for the rest. Default is DENY: any account
// route not in this allowlist is 403d for a pull-scoped key automatically.
var (
	pullDownloadPathRe = regexp.MustCompile(`^/api/v1/account/recordings/\d+/clips/\d+/download$`)
	pullReleasePathRe  = regexp.MustCompile(`^/api/v1/account/recordings/\d+/clips/\d+/release$`)
)

// pullPathAllowed reports whether a pull-scoped key may call (method, path). It is
// the single source of truth for pull confinement and is exercised directly by the
// table tests. The 4 allowed shapes (list + download + release + heartbeat):
//   - GET  /api/v1/account/clips                                        (cursor list)
//   - POST /api/v1/account/connections/heartbeat                       (heartbeat)
//   - GET  /api/v1/account/recordings/{id}/clips/{clipId}/download      (presign)
//   - POST /api/v1/account/recordings/{id}/clips/{clipId}/release       (release one clip)
//
// The pull key can RELEASE a clip (detach it from the org, keeping the R2 object)
// but can NOT hard-delete: the old DELETE .../clips/{clipId} allowance is removed,
// so a leaked NAS key can never destroy recorded content.
func pullPathAllowed(method, path string) bool {
	switch {
	case method == http.MethodGet && path == "/api/v1/account/clips":
		return true
	case method == http.MethodPost && path == "/api/v1/account/connections/heartbeat":
		return true
	case method == http.MethodGet && pullDownloadPathRe.MatchString(path):
		return true
	case method == http.MethodPost && pullReleasePathRe.MatchString(path):
		return true
	default:
		return false
	}
}

// confineAccountScope is registered immediately after requireAccountAuth on the
// account group, so the principal is already in context. A session or full/read
// key passes through untouched; a pull-scoped key is allowed ONLY on the 4 pull
// endpoints and 403d everywhere else. Default-DENY means a newly added account
// route is automatically out of reach for a leaked NAS key.
func (s *Server) confineAccountScope(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := accountPrincipalFromContext(r.Context())
		if !ok {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if !isPullScopedPrincipal(principal) {
			next.ServeHTTP(w, r)
			return
		}
		if !pullPathAllowed(r.Method, r.URL.Path) {
			util.WriteError(w, http.StatusForbidden, "this key is limited to the NAS pull")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func clampPollIntervalSec(v int) int {
	if v == 0 {
		return 60
	}
	if v < 10 {
		return 10
	}
	if v > 3600 {
		return 3600
	}
	return v
}

const nasPythonImage = "python:3.13-slim-bookworm@sha256:9d7f287598e1a5a978c015ee176d8216435aaf335ed69ac3c38dd1bbb10e8d64"

// This bootstrap is intentionally tiny and frozen in the compose file. It refreshes
// the durable client from the checksum-verified release channel when possible, but always falls
// back to the last valid local client if the release endpoint is unavailable.
const nasClientBootstrap = `import hashlib,json,os,sys,urllib.request
state='/state'
current='/state/stoarama_pull.py'
previous='/state/stoarama_pull.previous.py'
runtime='/state/runtime.json'
candidate='/state/stoarama_pull.py.candidate'
base='https://stoarama.com/nas/download/'

def atomic(path, data):
    temp=path+'.new'
    with open(temp,'wb') as output:
        output.write(data); output.flush(); os.fsync(output.fileno())
    os.replace(temp,path)

def fetch_latest():
    with urllib.request.urlopen(base+'latest.json',timeout=30) as response:
        manifest=json.load(response)
    artifact=str(manifest.get('artifact',''))
    expected=str(manifest.get('sha256','')).lower()
    if not artifact or '/' in artifact or '\\' in artifact or len(expected)!=64:
        raise RuntimeError('invalid NAS release manifest')
    with urllib.request.urlopen(base+artifact,timeout=30) as response:
        source=response.read()
    if hashlib.sha256(source).hexdigest()!=expected:
        raise RuntimeError('NAS client checksum mismatch')
    compile(source,artifact,'exec')
    return source

os.makedirs(state,exist_ok=True)
recovered=False
try:
    with open(runtime,'r') as status_file:
        status=json.load(status_file)
    if os.path.exists(previous) and status.get('exit') in ('running','self_update'):
        with open(previous,'rb') as old_file:
            source=old_file.read()
        compile(source,previous,'exec')
        atomic(current,source)
        if os.path.exists(candidate): os.unlink(candidate)
        recovered=True
        print('NAS bootstrap restored previous client after unclean run',file=sys.stderr,flush=True)
except (FileNotFoundError,ValueError,TypeError):
    pass
if not recovered:
  try:
    source=fetch_latest()
  except Exception as exc:
    if not os.path.exists(current):
        raise
    with open(current,'rb') as existing:
        compile(existing.read(),current,'exec')
    print('NAS bootstrap update skipped: %s' % exc,file=sys.stderr,flush=True)
  else:
    old=None
    if os.path.exists(current):
        with open(current,'rb') as existing:
            old=existing.read()
    if old != source:
        if old is not None:
            atomic(previous,old)
        atomic(current,source)
os.execv(sys.executable,[sys.executable,current,'run'])`

func connectionComposeSnippet(apiBase, token string, connectionID int64, pollIntervalSec int) string {
	bootstrap := "      " + strings.ReplaceAll(nasClientBootstrap, "\n", "\n      ")
	return fmt.Sprintf(`services:
  stoarama-pull:
    image: %s
    restart: always
    environment:
      STOARAMA_API_BASE: "%s"
      STOARAMA_API_KEY: "%s"
      STOARAMA_CONNECTION_ID: "%d"
      STOARAMA_OUTPUT_DIR: "/clips"
      STOARAMA_STATE_DIR: "/state"
      STOARAMA_POLL_INTERVAL_SEC: "%d"
      STOARAMA_UPDATE_MANIFEST_URL: "https://stoarama.com/nas/download/latest.json"
      STOARAMA_DRY_RUN: "0"
      PYTHONUNBUFFERED: "1"
    command:
      - python3
      - -c
      - |
%s
    volumes:
      - /volume1/stoarama-clips:/clips
      - /volume1/stoarama-state:/state
`, nasPythonImage, apiBase, token, connectionID, pollIntervalSec, bootstrap)
}

// connectionPublicAPIBase is the public /api/v1 base the NAS pull client targets:
// the user-facing host stoarama.com. Used only for the copyable compose snippet.
const connectionPublicAPIBase = "https://stoarama.com/api/v1"

type connectionCreateRequest struct {
	Label           string `json:"label"`
	PollIntervalSec int    `json:"poll_interval_sec"`
}

// handleAccountConnectionsCreate mints a stoarama.pull-scoped key and inserts a
// connection row referencing it, in ONE tx, then returns the sir_ token ONCE plus a
// ready-to-paste docker-compose snippet. Member-visible (session group), no owner
// gate. The minted key can do nothing but the pull loop (confineAccountScope).
func (s *Server) handleAccountConnectionsCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req connectionCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	label := strings.TrimSpace(req.Label)
	if label == "" {
		label = "NAS"
	}
	pollInterval := clampPollIntervalSec(req.PollIntervalSec)

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin connection tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var exists bool
	if err := tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM connections WHERE account_id=$1 AND kind='nas_pull')`, principal.AccountID).Scan(&exists); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check connection: %v", err))
		return
	}
	if exists {
		util.WriteError(w, http.StatusConflict, "this account already has a NAS connection")
		return
	}

	keyID, prefix, token, err := mintAccountAPIKey(r.Context(), tx, principal.AccountID, label, accountScopePull, nil)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mint pull key: %v", err))
		return
	}
	var connID int64
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO connections (account_id, kind, label, api_key_id, poll_interval_sec, created_by)
		VALUES ($1, 'nas_pull', $2, $3, $4, $5)
		RETURNING id
	`, principal.AccountID, label, keyID, pollInterval, principal.AccountID).Scan(&connID); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			util.WriteError(w, http.StatusConflict, "this account already has a NAS connection")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create connection: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, &keyID, "connection_created", "account", accountActorLabel(principal, ""), map[string]any{
		"connection_id": connID,
		"label":         label,
		"key_prefix":    prefix,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("audit connection: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit connection: %v", err))
		return
	}

	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"id":                connID,
		"label":             label,
		"poll_interval_sec": pollInterval,
		"token":             token,
		"compose_snippet":   connectionComposeSnippet(connectionPublicAPIBase, token, connID, pollInterval),
	})
}

// handleAccountConnectionsList returns the account's connections with a derived
// health: 'never' until the first heartbeat, then 'healthy' if last_seen_at is
// within 3x the poll interval else 'stale'. Never returns the token.
func (s *Server) handleAccountConnectionsList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, label, last_seen_at, clips_pulled, bytes_pulled, last_cursor_id,
		       poll_interval_sec, client_version, client_started_at, client_boot_id,
		       client_phase, client_previous_exit, client_last_success_at,
		       client_last_error, client_last_error_at, last_outage_class,
		       last_outage_started_at, last_outage_recovered_at,
		       last_outage_failure_count, created_at
		FROM connections
		WHERE account_id=$1
		ORDER BY created_at DESC, id DESC
	`, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list connections: %v", err))
		return
	}
	defer rows.Close()
	now := time.Now().UTC()
	items := make([]map[string]any, 0, 8)
	for rows.Next() {
		var (
			id              int64
			label           string
			lastSeenAt      *time.Time
			clipsPulled     int64
			bytesPulled     int64
			lastCursorID    int64
			pollIntervalSec int
			clientVersion   string
			clientStartedAt *time.Time
			clientBootID    string
			clientPhase     string
			previousExit    string
			lastSuccessAt   *time.Time
			lastError       string
			lastErrorAt     *time.Time
			outageClass     string
			outageStartedAt *time.Time
			outageRecovered *time.Time
			outageFailures  int
			createdAt       time.Time
		)
		if err := rows.Scan(&id, &label, &lastSeenAt, &clipsPulled, &bytesPulled, &lastCursorID,
			&pollIntervalSec, &clientVersion, &clientStartedAt, &clientBootID, &clientPhase,
			&previousExit, &lastSuccessAt, &lastError, &lastErrorAt, &outageClass,
			&outageStartedAt, &outageRecovered, &outageFailures, &createdAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan connection: %v", err))
			return
		}
		health := "never"
		var lastSeen any
		if lastSeenAt != nil {
			lastSeen = lastSeenAt.UTC()
			staleAfter := time.Duration(pollIntervalSec) * 3 * time.Second
			if now.Sub(*lastSeenAt) <= staleAfter {
				health = "healthy"
			} else {
				health = "stale"
			}
		}
		items = append(items, map[string]any{
			"id":                        id,
			"label":                     label,
			"last_seen_at":              lastSeen,
			"clips_pulled":              clipsPulled,
			"bytes_pulled":              bytesPulled,
			"last_cursor_id":            lastCursorID,
			"poll_interval_sec":         pollIntervalSec,
			"health":                    health,
			"created_at":                createdAt.UTC(),
			"client_version":            clientVersion,
			"client_started_at":         clientStartedAt,
			"client_boot_id":            clientBootID,
			"client_phase":              clientPhase,
			"client_previous_exit":      previousExit,
			"client_last_success_at":    lastSuccessAt,
			"client_last_error":         lastError,
			"client_last_error_at":      lastErrorAt,
			"last_outage_class":         outageClass,
			"last_outage_started_at":    outageStartedAt,
			"last_outage_recovered_at":  outageRecovered,
			"last_outage_failure_count": outageFailures,
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate connections: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

// handleAccountConnectionRotate mints a fresh pull key, points the connection at it,
// and revokes the old key, in ONE tx. Returns the new token ONCE plus a refreshed
// compose snippet. 404 if the connection is not owned by the caller's account.
func (s *Server) handleAccountConnectionRotate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin rotate tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var label string
	var oldKeyID int64
	var pollInterval int
	err = tx.QueryRow(r.Context(), `
		SELECT label, api_key_id, poll_interval_sec
		FROM connections
		WHERE id=$1 AND account_id=$2
		FOR UPDATE
	`, id, principal.AccountID).Scan(&label, &oldKeyID, &pollInterval)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "connection not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load connection: %v", err))
		return
	}

	newKeyID, prefix, token, err := mintAccountAPIKey(r.Context(), tx, principal.AccountID, label, accountScopePull, nil)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mint pull key: %v", err))
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE connections SET api_key_id=$1, updated_at=now() WHERE id=$2 AND account_id=$3
	`, newKeyID, id, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("point connection at new key: %v", err))
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE account_api_keys SET revoked_at=COALESCE(revoked_at, now()), updated_at=now()
		WHERE id=$1 AND account_id=$2
	`, oldKeyID, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke old key: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, &newKeyID, "connection_rotated", "account", accountActorLabel(principal, ""), map[string]any{
		"connection_id": id,
		"old_key_id":    oldKeyID,
		"key_prefix":    prefix,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("audit rotate: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit rotate: %v", err))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"id":                id,
		"label":             label,
		"poll_interval_sec": pollInterval,
		"token":             token,
		"compose_snippet":   connectionComposeSnippet(connectionPublicAPIBase, token, id, pollInterval),
	})
}

// handleAccountConnectionDelete revokes the connection's key and deletes the row.
// 404 if not owned by the caller's account.
func (s *Server) handleAccountConnectionDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin delete tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var keyID int64
	err = tx.QueryRow(r.Context(), `
		DELETE FROM connections WHERE id=$1 AND account_id=$2 RETURNING api_key_id
	`, id, principal.AccountID).Scan(&keyID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "connection not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete connection: %v", err))
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE account_api_keys SET revoked_at=COALESCE(revoked_at, now()), updated_at=now()
		WHERE id=$1 AND account_id=$2
	`, keyID, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke connection key: %v", err))
		return
	}
	if err := s.insertAccountAuthEventTx(r.Context(), tx, principal.AccountID, &keyID, "connection_deleted", "account", accountActorLabel(principal, ""), map[string]any{
		"connection_id": id,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("audit delete: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit delete: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type connectionHeartbeatRequest struct {
	CursorID           int64                      `json:"cursor_id"`
	ClipsPulled        int64                      `json:"clips_pulled"`
	BytesPulled        int64                      `json:"bytes_pulled"`
	ClientVersion      string                     `json:"client_version"`
	ClientStartedAt    *time.Time                 `json:"client_started_at"`
	ClientBootID       string                     `json:"client_boot_id"`
	ClientPhase        string                     `json:"client_phase"`
	ClientPreviousExit string                     `json:"client_previous_exit"`
	ClientLastSuccess  *time.Time                 `json:"client_last_success_at"`
	ClientLastError    string                     `json:"client_last_error"`
	ClientLastErrorAt  *time.Time                 `json:"client_last_error_at"`
	LastOutage         *connectionHeartbeatOutage `json:"last_outage"`
}

type connectionHeartbeatOutage struct {
	Class        string     `json:"class"`
	StartedAt    *time.Time `json:"started_at"`
	RecoveredAt  *time.Time `json:"recovered_at"`
	FailureCount int        `json:"failure_count"`
}

var connectionPhases = map[string]bool{"starting": true, "idle": true, "draining": true, "updating": true, "blocked": true, "degraded": true}
var connectionPreviousExits = map[string]bool{"unknown": true, "clean": true, "self_update": true, "unclean_process": true, "unclean_reboot": true}
var connectionOutageClasses = map[string]bool{"dns_failed": true, "timeout": true, "connection": true, "http": true, "other": true}

func validateConnectionHeartbeat(req connectionHeartbeatRequest) error {
	if req.CursorID < 0 || req.ClipsPulled < 0 || req.BytesPulled < 0 {
		return errors.New("cursor_id, clips_pulled, and bytes_pulled must be non-negative")
	}
	if req.ClientVersion == "" {
		return nil // Backward compatibility for the old NAS client during rollout.
	}
	if len(req.ClientVersion) > 64 || !relayArtifactName.MatchString(req.ClientVersion) {
		return errors.New("invalid client_version")
	}
	if len(req.ClientBootID) > 128 || !connectionPhases[req.ClientPhase] || !connectionPreviousExits[req.ClientPreviousExit] {
		return errors.New("invalid NAS client telemetry")
	}
	if len(req.ClientLastError) > 1000 {
		return errors.New("client_last_error is too long")
	}
	if req.LastOutage != nil {
		if !connectionOutageClasses[req.LastOutage.Class] || req.LastOutage.FailureCount < 1 || req.LastOutage.StartedAt == nil {
			return errors.New("invalid last_outage")
		}
	}
	return nil
}

// handleAccountConnectionHeartbeat is called by the pull client with its scoped
// key. It resolves the connection by the calling api_key_id (+ account_id) and
// advances last_seen_at/last_cursor_id and the monotonic clips_pulled. A session
// principal (no api_key_id) or a key with no connection row gets 403; the heartbeat
// is machine-only.
func (s *Server) handleAccountConnectionHeartbeat(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if principal.APIKeyID == nil {
		util.WriteError(w, http.StatusForbidden, "heartbeat requires a NAS pull key")
		return
	}
	var req connectionHeartbeatRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateConnectionHeartbeat(req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	var outageClass string
	var outageStartedAt, outageRecoveredAt *time.Time
	var outageFailureCount int
	if req.LastOutage != nil {
		outageClass = req.LastOutage.Class
		outageStartedAt = req.LastOutage.StartedAt
		outageRecoveredAt = req.LastOutage.RecoveredAt
		outageFailureCount = req.LastOutage.FailureCount
	}
	ct, err := s.pool.Exec(r.Context(), `
		UPDATE connections
		SET last_seen_at=now(),
		    last_cursor_id=GREATEST(last_cursor_id, $1),
		    clips_pulled=GREATEST(clips_pulled, $2),
		    bytes_pulled=GREATEST(bytes_pulled, $3),
		    client_version=CASE WHEN $4 <> '' THEN $4 ELSE client_version END,
		    client_started_at=CASE WHEN $4 <> '' THEN $5 ELSE client_started_at END,
		    client_boot_id=CASE WHEN $4 <> '' THEN $6 ELSE client_boot_id END,
		    client_phase=CASE WHEN $4 <> '' THEN $7 ELSE client_phase END,
		    client_previous_exit=CASE WHEN $4 <> '' THEN $8 ELSE client_previous_exit END,
		    client_last_success_at=CASE WHEN $4 <> '' THEN $9 ELSE client_last_success_at END,
		    client_last_error=CASE WHEN $4 <> '' THEN $10 ELSE client_last_error END,
		    client_last_error_at=CASE WHEN $4 <> '' THEN $11 ELSE client_last_error_at END,
		    last_outage_class=CASE WHEN $12 <> '' THEN $12 ELSE last_outage_class END,
		    last_outage_started_at=CASE WHEN $12 <> '' THEN $13 ELSE last_outage_started_at END,
		    last_outage_recovered_at=CASE WHEN $12 <> '' THEN $14 ELSE last_outage_recovered_at END,
		    last_outage_failure_count=CASE WHEN $12 <> '' THEN $15 ELSE last_outage_failure_count END,
		    updated_at=now()
		WHERE api_key_id=$16 AND account_id=$17
	`, req.CursorID, req.ClipsPulled, req.BytesPulled, req.ClientVersion,
		req.ClientStartedAt, req.ClientBootID, req.ClientPhase, req.ClientPreviousExit,
		req.ClientLastSuccess, req.ClientLastError, req.ClientLastErrorAt,
		outageClass, outageStartedAt, outageRecoveredAt, outageFailureCount,
		*principal.APIKeyID, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("heartbeat: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusForbidden, "no connection for this key")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}
