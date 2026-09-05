package api

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const (
	joinedArchiveAudience     = "joined-folder-archive-v1"
	joinedArchiveTTL          = 5 * time.Minute
	joinedArchiveMaxFiles     = 4000
	joinedArchiveMaxBytes     = int64(256 << 30)
	joinedArchiveMaxTokenSize = 4096
)

type joinedArchiveClaims struct {
	Version     int      `json:"v"`
	Audience    string   `json:"aud"`
	AccountID   int64    `json:"account_id"`
	RecordingID int64    `json:"recording_id"`
	Folder      []string `json:"folder,omitempty"`
	ExpiresAt   int64    `json:"exp"`
	Nonce       string   `json:"nonce"`
	ClientScope string   `json:"client_scope"`
}

type joinedArchiveArtifact struct {
	BatchID      string `json:"batch_id"`
	ETag         string `json:"etag"`
	VersionID    string `json:"version_id"`
	ContentType  string `json:"content_type"`
	RelativePath string `json:"relative_path"`
	SizeBytes    int64  `json:"size_bytes"`
	SHA256       string `json:"sha256"`
}

type joinedArchiveManifest struct {
	SchemaVersion int                     `json:"schema_version"`
	ArchiveName   string                  `json:"archive_name"`
	ExpiresAt     time.Time               `json:"expires_at"`
	TotalBytes    int64                   `json:"total_bytes"`
	RateScope     string                  `json:"rate_scope"`
	CapabilityID  string                  `json:"capability_id"`
	Files         []joinedArchiveArtifact `json:"files"`
}

func (s *Server) joinedArchiveConfigured() bool {
	return s.cfg.JoinedArchiveWorkerURL != "" && len(s.cfg.JoinedArchiveCapabilityKey) >= 32 && len(s.cfg.JoinedArchiveWorkerToken) >= 32
}

func joinedArchiveOpaqueID(key, domain, value string) string {
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write([]byte(domain + "\x00" + value))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func joinedArchiveWorkerAuthorized(want, got string) bool {
	wantHash := sha256.Sum256([]byte(strings.TrimSpace(want)))
	gotHash := sha256.Sum256([]byte(strings.TrimSpace(got)))
	return len(strings.TrimSpace(want)) >= 32 && subtle.ConstantTimeCompare(wantHash[:], gotHash[:]) == 1
}

func canonicalJoinedArchivePath(relative string) (string, bool) {
	if relative == "" || len(relative) > 1024 || strings.HasPrefix(relative, "/") || strings.ContainsAny(relative, "\\:<>\"|?*\x00\r\n") {
		return "", false
	}
	for _, part := range strings.Split(relative, "/") {
		if part == "" || part == "." || part == ".." || strings.TrimRight(part, " .") != part || joinedArchiveWindowsReserved(part) {
			return "", false
		}
	}
	clean := path.Clean(relative)
	return clean, clean == relative
}

func joinedArchiveWindowsReserved(segment string) bool {
	base := strings.ToUpper(strings.SplitN(segment, ".", 2)[0])
	if base == "CON" || base == "PRN" || base == "AUX" || base == "NUL" {
		return true
	}
	return len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && base[3] >= '1' && base[3] <= '9'
}

func validateJoinedArchive(artifacts []joinedArchiveArtifact) (int64, error) {
	if len(artifacts) == 0 {
		return 0, errors.New("joined folder is empty")
	}
	if len(artifacts) > joinedArchiveMaxFiles {
		return 0, errors.New("joined folder has too many files")
	}
	seen := make(map[string]struct{}, len(artifacts))
	root := ""
	var total int64
	for _, artifact := range artifacts {
		name, ok := canonicalJoinedArchivePath(artifact.RelativePath)
		if !ok || artifact.ContentType != "video/mp4" || !strings.HasSuffix(strings.ToLower(name), ".mp4") || !joinedrecording.ValidBatchID(artifact.BatchID) || !lowerHex64(artifact.SHA256) || artifact.ETag == "" {
			return 0, errors.New("invalid joined archive artifact")
		}
		entryRoot := strings.SplitN(name, "/", 2)[0]
		if root == "" {
			root = entryRoot
		} else if entryRoot != root {
			return 0, errors.New("joined archive has multiple recording roots")
		}
		portable := strings.ToLower(name)
		if _, duplicate := seen[portable]; duplicate || portable == "joined-files.json" {
			return 0, fmt.Errorf("ambiguous joined archive path %q", name)
		}
		seen[portable] = struct{}{}
		if artifact.SizeBytes <= 0 || artifact.SizeBytes > joinedArchiveMaxBytes-total {
			return 0, errors.New("joined folder exceeds archive size limit")
		}
		total += artifact.SizeBytes
	}
	return total, nil
}

func scopeJoinedArchive(all []joinedArchiveArtifact, active []string) ([]joinedArchiveArtifact, string, bool) {
	rootName := ""
	for _, artifact := range all {
		rootName = joinedCanonicalRootFromPath(artifact.RelativePath)
		if rootName != "" {
			break
		}
	}
	if len(active) == 0 {
		if rootName == "" {
			rootName = "joined-recording"
		}
		return append([]joinedArchiveArtifact(nil), all...), rootName, len(all) > 0
	}
	selected := make([]joinedArchiveArtifact, 0)
	for _, artifact := range all {
		parts := joinedFolderFileParts(artifact.RelativePath)
		if len(parts) < len(active)+1 {
			continue
		}
		matches := true
		for index, want := range active {
			if parts[index] != want {
				matches = false
				break
			}
		}
		if matches {
			selected = append(selected, artifact)
		}
	}
	return selected, active[len(active)-1], len(selected) > 0
}

func safeJoinedArchiveFilename(name string, recordingID int64) string {
	var safe strings.Builder
	for _, char := range strings.TrimSpace(name) {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-' {
			safe.WriteRune(char)
		} else if char == ' ' {
			safe.WriteByte('_')
		}
	}
	if safe.Len() == 0 {
		return fmt.Sprintf("recording-%d-joined.zip", recordingID)
	}
	return safe.String() + ".zip"
}

func mintJoinedArchiveCapability(key string, claims joinedArchiveClaims) (string, error) {
	if len(key) < 32 || claims.AccountID <= 0 || claims.RecordingID <= 0 || claims.ExpiresAt <= 0 || len(claims.Nonce) < 16 || len(claims.ClientScope) < 32 {
		return "", errors.New("invalid joined archive capability")
	}
	claims.Version = 1
	claims.Audience = joinedArchiveAudience
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	signature := joinedArchiveOpaqueID(key, joinedArchiveAudience, encoded)
	return encoded + "." + signature, nil
}

func verifyJoinedArchiveCapability(key, token string, now time.Time) (joinedArchiveClaims, error) {
	var claims joinedArchiveClaims
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(key) < 32 || len(token) > joinedArchiveMaxTokenSize || len(parts) != 2 {
		return claims, errors.New("invalid joined archive capability")
	}
	want := joinedArchiveOpaqueID(key, joinedArchiveAudience, parts[0])
	if !hmac.Equal([]byte(want), []byte(parts[1])) {
		return claims, errors.New("invalid joined archive capability")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || json.Unmarshal(payload, &claims) != nil || claims.Version != 1 || claims.Audience != joinedArchiveAudience ||
		claims.AccountID <= 0 || claims.RecordingID <= 0 || claims.ExpiresAt <= now.Unix() || claims.ExpiresAt > now.Add(10*time.Minute).Unix() ||
		len(claims.Nonce) < 16 || len(claims.ClientScope) < 32 {
		return joinedArchiveClaims{}, errors.New("invalid or expired joined archive capability")
	}
	if folder, ok := parseJoinedFolderPath(strings.Join(claims.Folder, "/")); !ok || len(folder) != len(claims.Folder) {
		return joinedArchiveClaims{}, errors.New("invalid joined archive folder scope")
	}
	return claims, nil
}

func joinedArchiveWorkerURL(base, token string) (string, error) {
	worker, err := url.Parse(strings.TrimSpace(base))
	if err != nil || worker.Scheme != "https" || worker.Host == "" || worker.User != nil || (worker.Path != "" && worker.Path != "/") || worker.RawQuery != "" || worker.Fragment != "" {
		return "", errors.New("joined archive worker is unavailable")
	}
	worker.Path = "/archive"
	query := worker.Query()
	query.Set("token", token)
	worker.RawQuery = query.Encode()
	return worker.String(), nil
}

func (s *Server) joinedArchiveArtifacts(ctx context.Context, accountID, recordingID int64) ([]joinedArchiveArtifact, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2 AND status IN ('active','paused','completed'))`, recordingID, accountID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, pgx.ErrNoRows
	}
	rows, err := s.pool.Query(ctx, `SELECT
		(SELECT b.batch_id FROM recording_joined_batches b WHERE b.id=a.batch_record_id AND b.account_id=a.account_id),
		a.etag,a.version_id,a.content_type,a.relative_path,a.expected_size_bytes,a.expected_sha256 `+
		recordingJoinedPublishedArtifactScopeSQL+`
		  AND a.artifact_kind='media'
		ORDER BY a.relative_path,a.id
		LIMIT $3`, recordingID, accountID, joinedArchiveMaxFiles+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	artifacts := make([]joinedArchiveArtifact, 0)
	for rows.Next() {
		var artifact joinedArchiveArtifact
		if err := rows.Scan(&artifact.BatchID, &artifact.ETag, &artifact.VersionID,
			&artifact.ContentType, &artifact.RelativePath, &artifact.SizeBytes, &artifact.SHA256); err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, rows.Err()
}

func (s *Server) handleAccountRecordingJoinedFolderArchive(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !s.joinedArchiveConfigured() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined archive worker is unavailable")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	folder, ok := parseJoinedFolderPath(r.URL.Query().Get("folder"))
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "invalid joined folder")
		return
	}
	artifacts, err := s.joinedArchiveArtifacts(r.Context(), principal.AccountID, recordingID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined folder is unavailable")
		return
	}
	selected, _, found := scopeJoinedArchive(artifacts, folder)
	if !found {
		util.WriteError(w, http.StatusNotFound, "joined folder not found")
		return
	}
	if _, err := validateJoinedArchive(selected); err != nil {
		util.WriteError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined archive capability is unavailable")
		return
	}
	requester := sharedRecordingsRequesterIP(r, s.cfg.SharedRecordingsProxyCIDRs)
	expiresAt := time.Now().UTC().Add(joinedArchiveTTL)
	token, err := mintJoinedArchiveCapability(s.cfg.JoinedArchiveCapabilityKey, joinedArchiveClaims{
		AccountID: principal.AccountID, RecordingID: recordingID, Folder: folder, ExpiresAt: expiresAt.Unix(),
		Nonce:       base64.RawURLEncoding.EncodeToString(nonce),
		ClientScope: joinedArchiveOpaqueID(s.cfg.JoinedArchiveCapabilityKey, "joined-archive-client-v1", requester),
	})
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined archive capability is unavailable")
		return
	}
	location, err := joinedArchiveWorkerURL(s.cfg.JoinedArchiveWorkerURL, token)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined archive worker is unavailable")
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	http.Redirect(w, r, location, http.StatusTemporaryRedirect)
}

func (s *Server) handleSharedRecordingJoinedFolderArchive(w http.ResponseWriter, r *http.Request) {
	s.handleAccountRecordingJoinedFolderArchive(w, s.sharedRecordingPrincipalRequest(r))
}

func (s *Server) handleJoinedArchiveManifest(w http.ResponseWriter, r *http.Request) {
	authorization, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !bearer || !s.joinedArchiveConfigured() || !joinedArchiveWorkerAuthorized(s.cfg.JoinedArchiveWorkerToken, authorization) {
		util.WriteError(w, http.StatusUnauthorized, "archive worker authentication failed")
		return
	}
	now := time.Now().UTC()
	claims, err := verifyJoinedArchiveCapability(s.cfg.JoinedArchiveCapabilityKey, r.URL.Query().Get("token"), now)
	if err != nil {
		w.Header().Set("Cache-Control", "private, no-store")
		util.WriteError(w, http.StatusGone, "invalid or expired joined archive capability")
		return
	}
	artifacts, err := s.joinedArchiveArtifacts(r.Context(), claims.AccountID, claims.RecordingID)
	if err != nil {
		util.WriteError(w, http.StatusNotFound, "joined archive scope not found")
		return
	}
	selected, folderName, found := scopeJoinedArchive(artifacts, claims.Folder)
	if !found {
		util.WriteError(w, http.StatusNotFound, "joined folder not found")
		return
	}
	sort.SliceStable(selected, func(i, j int) bool { return selected[i].RelativePath < selected[j].RelativePath })
	total, err := validateJoinedArchive(selected)
	if err != nil {
		util.WriteError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	util.WriteJSON(w, http.StatusOK, joinedArchiveManifest{
		SchemaVersion: 1,
		ArchiveName:   safeJoinedArchiveFilename(folderName, claims.RecordingID),
		ExpiresAt:     time.Unix(claims.ExpiresAt, 0).UTC(),
		TotalBytes:    total,
		RateScope:     joinedArchiveOpaqueID(s.cfg.JoinedArchiveCapabilityKey, "joined-archive-rate-v1", fmt.Sprintf("%d:%s", claims.AccountID, claims.ClientScope)),
		CapabilityID:  joinedArchiveOpaqueID(s.cfg.JoinedArchiveCapabilityKey, "joined-archive-capability-v1", claims.Nonce),
		Files:         selected,
	})
}
