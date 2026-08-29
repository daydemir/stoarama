package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	sharedRecordingsCookie             = "stoarama_shared_recordings_read"
	sharedRecordingsSessionTTL         = 24 * time.Hour
	sharedRecordingsRateWindow         = 15 * time.Minute
	sharedRecordingsMaxFailures        = 5
	sharedRecordingsMaxClients         = 4096
	sharedRecordingsListLimit          = 500
	sharedRecordingsVisibleStatusesSQL = "rec.status IN ('active','paused','completed')"
)

type sharedRecording struct {
	ID                int64                       `json:"id"`
	Name              string                      `json:"name"`
	Status            string                      `json:"status"`
	Mode              string                      `json:"mode"`
	CronExpr          string                      `json:"cron_expr"`
	CronTimezone      string                      `json:"cron_timezone"`
	ClipDurationSec   int                         `json:"clip_duration_sec"`
	DailyWindowStart  string                      `json:"daily_window_start"`
	DailyWindowEnd    string                      `json:"daily_window_end"`
	ActiveWeekdays    uint8                       `json:"active_weekdays"`
	StartAt           time.Time                   `json:"start_at"`
	EndAt             *time.Time                  `json:"end_at"`
	NextFireAt        *time.Time                  `json:"next_fire_at"`
	LastClipAt        *time.Time                  `json:"last_clip_at"`
	CapturedClipCount int64                       `json:"captured_clip_count"`
	ExpectedClipCount int64                       `json:"expected_clip_count"`
	CaptureHealth     recordingCaptureHealthState `json:"capture_health"`
	StreamID          *int64                      `json:"stream_id"`
	StreamName        *string                     `json:"stream_name"`
	StreamLocation    *string                     `json:"stream_location"`
	SourceKind        string                      `json:"source_kind"`
	CaptureVia        string                      `json:"capture_via"`
	RelayNodeName     *string                     `json:"relay_node_name"`
	HasRelayOnline    bool                        `json:"has_relay_online"`
	HasRelayAssigned  bool                        `json:"has_relay_assigned"`
	CaptureHealthBins []recordingHealthBin        `json:"capture_health_bins"`
	TimelineHealth    *recordingTimelineHealth    `json:"timeline_health"`
	JoinedReadyMS     int64                       `json:"joined_ready_ms"`
	SourceDurationMS  int64                       `json:"source_duration_ms"`
	JoinedPercent     *int                        `json:"joined_percent"`
	Naming            sharedRecordingNaming       `json:"naming"`
}

// sharedRecordingNaming is the public allowlist for recording folder data.
// Keep this typed instead of forwarding the raw JSONB metadata so a future
// private naming field cannot cross the shared-recordings boundary by accident.
type sharedRecordingNaming struct {
	Profile    string                   `json:"profile"`
	FolderName string                   `json:"folder_name"`
	Metadata   recordingnaming.Metadata `json:"metadata"`
}

type sharedRecordingsLimiter struct {
	mu         sync.Mutex
	failures   map[string][]time.Time
	nextExpiry time.Time
}

func newSharedRecordingsLimiter() *sharedRecordingsLimiter {
	return &sharedRecordingsLimiter{failures: map[string][]time.Time{}}
}

func (l *sharedRecordingsLimiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	failures := l.recent(key, now)
	if failures != nil {
		return len(failures) < sharedRecordingsMaxFailures
	}
	if len(l.failures) < sharedRecordingsMaxClients {
		return true
	}
	if now.Before(l.nextExpiry) {
		return false
	}
	l.pruneExpired(now)
	return len(l.failures) < sharedRecordingsMaxClients
}

func (l *sharedRecordingsLimiter) fail(key string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	failures := l.recent(key, now)
	if failures == nil && len(l.failures) >= sharedRecordingsMaxClients {
		if now.Before(l.nextExpiry) {
			return
		}
		l.pruneExpired(now)
		if len(l.failures) >= sharedRecordingsMaxClients {
			return
		}
	}
	if len(failures) >= sharedRecordingsMaxFailures {
		return
	}
	l.failures[key] = append(failures, now)
	expiry := now.Add(sharedRecordingsRateWindow)
	if l.nextExpiry.IsZero() || expiry.Before(l.nextExpiry) {
		l.nextExpiry = expiry
	}
}

func (l *sharedRecordingsLimiter) clear(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.failures, key)
}

func (l *sharedRecordingsLimiter) recent(key string, now time.Time) []time.Time {
	failures := l.failures[key]
	if failures == nil {
		return nil
	}
	failures = recentSharedRecordingFailures(failures, now.Add(-sharedRecordingsRateWindow))
	if len(failures) == 0 {
		delete(l.failures, key)
		return nil
	}
	l.failures[key] = failures
	return failures
}

func (l *sharedRecordingsLimiter) pruneExpired(now time.Time) {
	cutoff := now.Add(-sharedRecordingsRateWindow)
	for key, failures := range l.failures {
		failures = recentSharedRecordingFailures(failures, cutoff)
		if len(failures) == 0 {
			delete(l.failures, key)
			continue
		}
		l.failures[key] = failures
	}
	l.recomputeNextExpiry()
}

func (l *sharedRecordingsLimiter) recomputeNextExpiry() {
	l.nextExpiry = time.Time{}
	for _, failures := range l.failures {
		expiry := failures[0].Add(sharedRecordingsRateWindow)
		if l.nextExpiry.IsZero() || expiry.Before(l.nextExpiry) {
			l.nextExpiry = expiry
		}
	}
}

func recentSharedRecordingFailures(failures []time.Time, cutoff time.Time) []time.Time {
	recent := failures[:0]
	for _, at := range failures {
		if at.After(cutoff) {
			recent = append(recent, at)
		}
	}
	return recent
}

func (s *Server) sharedRecordingsEnabled() bool {
	return s.cfg.SharedRecordingsAccountID > 0 && (s.cfg.SharedRecordingsPublic || s.cfg.SharedRecordingsPassword != "")
}

func (s *Server) requireSharedRecordingsAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if !s.sharedRecordingsEnabled() {
			http.NotFound(w, r)
			return
		}
		if s.cfg.SharedRecordingsPublic {
			next.ServeHTTP(w, r)
			return
		}
		cookie, err := r.Cookie(sharedRecordingsCookie)
		if err != nil || !validSharedRecordingsToken(cookie.Value, s.cfg.SharedRecordingsCookieSigningKey, time.Now()) {
			util.WriteError(w, http.StatusUnauthorized, "shared recordings password required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleSharedRecordingsUnlock(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !s.sharedRecordingsEnabled() || s.cfg.SharedRecordingsPublic {
		http.NotFound(w, r)
		return
	}
	key := sharedRecordingsRequesterIP(r, s.cfg.SharedRecordingsProxyCIDRs)
	now := time.Now().UTC()
	if !s.sharedRecordingsLimiter.allow(key, now) {
		util.WriteError(w, http.StatusTooManyRequests, "too many password attempts; try again later")
		return
	}
	var input struct {
		Password string `json:"password"`
	}
	if err := util.DecodeJSON(r, &input); err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid request")
		return
	}
	want := sha256.Sum256([]byte(s.cfg.SharedRecordingsPassword))
	got := sha256.Sum256([]byte(input.Password))
	if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
		s.sharedRecordingsLimiter.fail(key, now)
		util.WriteError(w, http.StatusUnauthorized, "invalid password")
		return
	}
	s.sharedRecordingsLimiter.clear(key)
	expires := now.Add(sharedRecordingsSessionTTL)
	http.SetCookie(w, &http.Cookie{
		Name:     sharedRecordingsCookie,
		Value:    sharedRecordingsToken(expires, s.cfg.SharedRecordingsCookieSigningKey),
		Path:     "/",
		Expires:  expires,
		MaxAge:   int(sharedRecordingsSessionTTL.Seconds()),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleSharedRecordingsLogout(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	http.SetCookie(w, &http.Cookie{
		Name:     sharedRecordingsCookie,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleSharedRecordingClips exposes only the media-browser fields needed by the
// public read-only page. It intentionally omits object keys, destination IDs,
// naming paths, capture generation tokens, and every mutation affordance.
func (s *Server) handleSharedRecordingClips(w http.ResponseWriter, r *http.Request) {
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var ownerOK bool
	if err := s.pool.QueryRow(r.Context(), `
		SELECT EXISTS(SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2 AND status <> 'canceled')
	`, recordingID, s.cfg.SharedRecordingsAccountID).Scan(&ownerOK); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load recording")
		return
	}
	if !ownerOK {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}
	var total int64
	if err := s.pool.QueryRow(r.Context(), `SELECT count(*) FROM recording_clips WHERE recording_id=$1`, recordingID).Scan(&total); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "count clips")
		return
	}
	limit := parseIntQuery(r, "limit", 100, 1, 500)
	offset := parseIntQuery(r, "offset", 0, 0, 1<<30)
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, clip_start_at, clip_end_at, size_bytes, duration_ms, actual_fps,
		       purged_at IS NOT NULL, released_at IS NOT NULL
		FROM recording_clips
		WHERE recording_id=$1
		ORDER BY clip_start_at DESC, id DESC
		LIMIT $2 OFFSET $3
	`, recordingID, limit, offset)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "list clips")
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var id, sizeBytes, durationMS int64
		var startAt, endAt time.Time
		var actualFPS *float64
		var purged, released bool
		if err := rows.Scan(&id, &startAt, &endAt, &sizeBytes, &durationMS, &actualFPS, &purged, &released); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "scan clip")
			return
		}
		items = append(items, map[string]any{
			"id": id, "clip_start_at": startAt.UTC(), "clip_end_at": endAt.UTC(),
			"size_bytes": sizeBytes, "duration_ms": durationMS, "actual_fps": actualFPS,
			"purged": purged, "released": released,
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "iterate clips")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items, "total": total, "limit": limit, "offset": offset})
}

func (s *Server) handleSharedRecordingClipDownload(w http.ResponseWriter, r *http.Request) {
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	clipID, ok := parseInt64Path(w, r, "clipId")
	if !ok {
		return
	}
	s.writeRecordingClipDownload(w, r, s.cfg.SharedRecordingsAccountID, recordingID, clipID, false)
}

func (s *Server) handleSharedRecordingsList(w http.ResponseWriter, r *http.Request) {
	items, err := s.loadSharedRecordings(r, 0)
	if err != nil {
		log.Printf("shared recordings list failed account_id=%d: %v", s.cfg.SharedRecordingsAccountID, err)
		util.WriteError(w, http.StatusInternalServerError, "list recordings")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"recordings": items})
}

func (s *Server) handleSharedRecordingGet(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	items, err := s.loadSharedRecordings(r, id)
	if err != nil {
		log.Printf("shared recording load failed account_id=%d recording_id=%d: %v", s.cfg.SharedRecordingsAccountID, id, err)
		util.WriteError(w, http.StatusInternalServerError, "load recording")
		return
	}
	if len(items) == 0 {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}
	util.WriteJSON(w, http.StatusOK, items[0])
}

func sharedRecordingsRequesterIP(r *http.Request, trustedProxies []netip.Prefix) string {
	peer := requestPeerIP(r.RemoteAddr)
	if !addressInPrefixes(peer, trustedProxies) {
		return peer.String()
	}
	if ip, err := netip.ParseAddr(strings.TrimSpace(r.Header.Get("CF-Connecting-IP"))); err == nil {
		return ip.Unmap().String()
	}
	forwarded := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	for i := len(forwarded) - 1; i >= 0; i-- {
		ip, err := netip.ParseAddr(strings.TrimSpace(forwarded[i]))
		if err == nil && !addressInPrefixes(ip, trustedProxies) {
			return ip.Unmap().String()
		}
	}
	return peer.String()
}

func requestPeerIP(remoteAddr string) netip.Addr {
	if addr, err := netip.ParseAddrPort(strings.TrimSpace(remoteAddr)); err == nil {
		return addr.Addr().Unmap()
	}
	addr, _ := netip.ParseAddr(strings.TrimSpace(remoteAddr))
	return addr.Unmap()
}

func addressInPrefixes(addr netip.Addr, prefixes []netip.Prefix) bool {
	for _, prefix := range prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

func (s *Server) handleSharedRecordingCaptureHealth(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	s.writeRecordingCaptureHealth(w, r, s.cfg.SharedRecordingsAccountID, id, true)
}

func (s *Server) sharedRecordingPrincipalRequest(r *http.Request) *http.Request {
	ctx := context.WithValue(r.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: s.cfg.SharedRecordingsAccountID, AuthType: "shared"})
	return r.WithContext(ctx)
}

func (s *Server) handleSharedRecordingJoinedList(w http.ResponseWriter, r *http.Request) {
	s.handleAccountRecordingJoinedList(w, s.sharedRecordingPrincipalRequest(r))
}

func (s *Server) handleSharedRecordingJoinedDownload(w http.ResponseWriter, r *http.Request) {
	s.handleAccountRecordingJoinedDownload(w, s.sharedRecordingPrincipalRequest(r))
}

func (s *Server) handleSharedRecordingJoinedFolder(w http.ResponseWriter, r *http.Request) {
	s.handleAccountRecordingJoinedFolder(w, s.sharedRecordingPrincipalRequest(r))
}

func (s *Server) loadSharedRecordings(r *http.Request, recordingID int64) ([]sharedRecording, error) {
	query := recordingListSelectSQL + `
		WHERE rec.account_id=$1 AND ` + sharedRecordingsVisibleStatusesSQL
	args := []any{s.cfg.SharedRecordingsAccountID}
	if recordingID > 0 {
		query += ` AND rec.id=$2`
		args = append(args, recordingID)
		query += ` ORDER BY rec.created_at DESC, rec.id DESC LIMIT 1`
	} else {
		query += ` ORDER BY rec.created_at DESC, rec.id DESC LIMIT ` + strconv.Itoa(sharedRecordingsListLimit)
	}
	rows, err := s.pool.Query(r.Context(), query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []sharedRecording{}
	for rows.Next() {
		raw, err := scanRecordingListRow(rows, s.billing != nil)
		if err != nil {
			return nil, err
		}
		item, err := sharedRecordingFrom(raw)
		if err != nil {
			return nil, err
		}
		item.CaptureHealthBins = []recordingHealthBin{}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if recordingID > 0 && len(items) == 0 {
		return items, nil
	}
	// The shared page uses the same fast baseline as the authenticated page.
	// Slow health and joined aggregates load through separate scoped endpoints.
	return items, nil
}

func sharedRecordingFrom(raw map[string]any) (sharedRecording, error) {
	encoded, err := json.Marshal(raw)
	if err != nil {
		return sharedRecording{}, fmt.Errorf("encode shared recording: %w", err)
	}
	var item sharedRecording
	if err := json.Unmarshal(encoded, &item); err != nil {
		return sharedRecording{}, fmt.Errorf("decode shared recording: %w", err)
	}
	return item, nil
}

func sharedRecordingsToken(expires time.Time, signingKey string) string {
	expiry := strconv.FormatInt(expires.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(signingKey))
	_, _ = mac.Write([]byte(expiry))
	return expiry + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func validSharedRecordingsToken(token, signingKey string, now time.Time) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return false
	}
	expires, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || now.Unix() >= expires || expires > now.Add(sharedRecordingsSessionTTL).Unix() {
		return false
	}
	expected := sharedRecordingsToken(time.Unix(expires, 0), signingKey)
	return hmac.Equal([]byte(token), []byte(expected))
}
