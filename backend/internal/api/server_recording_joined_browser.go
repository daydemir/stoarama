package api

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const (
	recordingJoinedBrowserDefaultLimit = 100
	recordingJoinedBrowserMaxLimit     = 500
	recordingJoinedFolderMaxFiles      = 5000
)

type recordingJoinedFile struct {
	ArtifactID    int64      `json:"artifact_id"`
	HourID        string     `json:"hour_id"`
	Kind          string     `json:"kind"`
	ContentType   string     `json:"content_type"`
	RelativePath  string     `json:"relative_path"`
	SizeBytes     int64      `json:"size_bytes"`
	SHA256        string     `json:"sha256"`
	PublishedAt   *time.Time `json:"published_at,omitempty"`
	LocalDate     string     `json:"local_date"`
	DeliveryHour  int        `json:"delivery_hour"`
	ScheduledFrom time.Time  `json:"scheduled_start_at"`
	ScheduledTo   time.Time  `json:"scheduled_end_at"`
	DownloadPath  string     `json:"download_path"`
}

type recordingJoinedList struct {
	RecordingID int64                 `json:"recording_id"`
	Files       []recordingJoinedFile `json:"files"`
	Total       int64                 `json:"total"`
	Limit       int                   `json:"limit"`
	Offset      int                   `json:"offset"`
	FolderPath  string                `json:"folder_path"`
}

// recordingJoinedAPIRoot returns the account-scoped API namespace without
// exposing any storage authority or credential.
func (s *Server) recordingJoinedAPIRoot(principal accountPrincipal) string {
	if principal.AuthType == "shared" {
		return "/api/v1/shared/" + s.cfg.SharedRecordingsSlug
	}
	return "/api/v1/account"
}

// recordingJoinedPageRoot returns the browser route for the principal's view.
func (s *Server) recordingJoinedPageRoot(principal accountPrincipal) string {
	if principal.AuthType == "shared" {
		return "/shared/" + s.cfg.SharedRecordingsSlug + "/recordings"
	}
	return "/recordings"
}

const recordingJoinedPublishedArtifactScopeSQL = `
	FROM recording_joined_artifacts a
	JOIN recording_joined_hours h
	  ON h.id=a.hour_record_id
	 AND h.batch_record_id=a.batch_record_id
	 AND h.account_id=a.account_id
	 AND h.state='sealed'
	JOIN recordings rec
	  ON rec.id=h.recording_id
	 AND rec.account_id=h.account_id
	 AND rec.status IN ('active','paused','completed')
	WHERE rec.id=$1
	  AND rec.account_id=$2
	  AND a.published_at IS NOT NULL
	  AND a.etag IS NOT NULL AND a.etag<>''
	  AND a.version_id IS NOT NULL
	  AND (a.artifact_kind='media' OR a.publication_state='published')
	  AND EXISTS (
	    SELECT 1 FROM recording_joined_artifacts manifest
	    WHERE manifest.hour_record_id=h.id
	      AND manifest.batch_record_id=h.batch_record_id
	      AND manifest.account_id=h.account_id
	      AND manifest.artifact_kind='hour_manifest'
	      AND manifest.publication_state='published'
	      AND manifest.published_at IS NOT NULL
	      AND manifest.etag IS NOT NULL AND manifest.etag<>''
	      AND manifest.version_id IS NOT NULL
	  )`

// recordingJoinedFiles lists only artifacts from an account-owned, sealed hour
// whose immutable identity and published hour manifest are complete.
func (s *Server) recordingJoinedFiles(ctx context.Context, principal accountPrincipal, recordingID int64, kinds []string, limit, offset int) (recordingJoinedList, error) {
	var recordingExists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2 AND status IN ('active','paused','completed')
	)`, recordingID, principal.AccountID).Scan(&recordingExists); err != nil {
		return recordingJoinedList{}, err
	}
	if !recordingExists {
		return recordingJoinedList{}, pgx.ErrNoRows
	}

	publishedScope := recordingJoinedPublishedArtifactScopeSQL + `
		AND a.artifact_kind=ANY($3::text[])`

	var total int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) `+publishedScope, recordingID, principal.AccountID, kinds).Scan(&total); err != nil {
		return recordingJoinedList{}, err
	}
	rows, err := s.pool.Query(ctx, `SELECT a.id,h.hour_id,a.artifact_kind,a.content_type,a.relative_path,
		a.expected_size_bytes,a.expected_sha256,a.published_at,h.local_date::text,h.delivery_hour,
		h.scheduled_start_at,h.scheduled_end_at `+publishedScope+`
		ORDER BY h.local_date,h.delivery_hour,a.ordinal,a.id
		LIMIT $4 OFFSET $5`, recordingID, principal.AccountID, kinds, limit, offset)
	if err != nil {
		return recordingJoinedList{}, err
	}
	defer rows.Close()
	root := s.recordingJoinedAPIRoot(principal)
	capacity := limit
	if total < int64(capacity) {
		capacity = int(total)
	}
	files := make([]recordingJoinedFile, 0, capacity)
	for rows.Next() {
		var file recordingJoinedFile
		if err := rows.Scan(&file.ArtifactID, &file.HourID, &file.Kind, &file.ContentType, &file.RelativePath,
			&file.SizeBytes, &file.SHA256, &file.PublishedAt, &file.LocalDate, &file.DeliveryHour,
			&file.ScheduledFrom, &file.ScheduledTo); err != nil {
			return recordingJoinedList{}, err
		}
		file.DownloadPath = fmt.Sprintf("%s/recordings/%d/joined/%d/download", root, recordingID, file.ArtifactID)
		files = append(files, file)
	}
	if err := rows.Err(); err != nil {
		return recordingJoinedList{}, err
	}
	return recordingJoinedList{
		RecordingID: recordingID,
		Files:       files,
		Total:       total,
		Limit:       limit,
		Offset:      offset,
		FolderPath:  fmt.Sprintf("%s/recordings/%d/joined/folder", root, recordingID),
	}, nil
}

// handleAccountRecordingJoinedList returns only published joined video parts.
// It never returns R2 object keys, endpoints, credentials, or presigned URLs.
func (s *Server) handleAccountRecordingJoinedList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	limit := parseIntQuery(r, "limit", recordingJoinedBrowserDefaultLimit, 1, recordingJoinedBrowserMaxLimit)
	offset := parseIntQuery(r, "offset", 0, 0, 1<<30)
	result, err := s.recordingJoinedFiles(r.Context(), principal, recordingID, []string{"media"}, limit, offset)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "list joined recording files")
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	util.WriteJSON(w, http.StatusOK, result)
}

type joinedFolderPage struct {
	RecordingID int64
	BackPath    string
	Crumbs      []joinedFolderCrumb
	Folders     []joinedFolderEntry
	Files       []joinedFolderFile
	Total       int64
}

type joinedFolderCrumb struct {
	Name    string
	Path    string
	Current bool
}

type joinedFolderEntry struct {
	Name  string
	Count int
	Path  string
}

type joinedFolderFile struct {
	Name         string
	DownloadPath string
	LocalDate    string
	Size         string
}

var joinedFolderTemplate = template.Must(template.New("joined-folder").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Joined clips</title><style>
:root{color-scheme:dark;font-family:ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d0f13;color:#f3f5f8}*{box-sizing:border-box}body{max-width:1120px;margin:0 auto;padding:32px 24px 56px}a{color:#a8ceff}a:focus-visible{outline:2px solid #8bbcff;outline-offset:3px}.back{display:inline-flex;margin-bottom:22px;color:#aeb8c7;font-size:14px;text-decoration:none}.back:hover{color:#f3f5f8}.head{display:flex;gap:24px;align-items:end;justify-content:space-between;margin-bottom:24px}.head h1{margin:0 0 7px;font-size:clamp(26px,4vw,38px);line-height:1.12;letter-spacing:-.025em}.note{max-width:70ch;margin:0;color:#aeb8c7;font-size:14px;line-height:1.55}.count{flex:0 0 auto;color:#aeb8c7;font:13px ui-monospace,SFMono-Regular,Menlo,monospace}.browser{overflow:hidden;border:1px solid #303744;border-radius:14px;background:#15191f;box-shadow:0 12px 32px rgba(0,0,0,.22)}.crumbs{display:flex;align-items:center;gap:7px;min-height:48px;padding:8px 14px;overflow-x:auto;border-bottom:1px solid #303744;white-space:nowrap}.crumbs a,.crumbs span{font-size:14px}.crumbs span{color:#f3f5f8;font-weight:650}.sep{color:#667085}.entry{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:16px;min-height:64px;padding:11px 14px;border-bottom:1px solid #292f39}.entry:last-child{border-bottom:0}.folder{display:flex;align-items:center;gap:12px;min-width:0;color:#f3f5f8;font-weight:650;text-decoration:none}.folder:hover .name{text-decoration:underline;text-underline-offset:3px}.folder-icon{position:relative;flex:0 0 auto;width:24px;height:17px;border-radius:4px;background:#5e9de7}.folder-icon:before{content:"";position:absolute;top:-5px;left:2px;width:10px;height:6px;border-radius:4px 4px 0 0;background:inherit}.name{min-width:0;overflow-wrap:anywhere}.meta{margin-top:5px;color:#aeb8c7;font-size:13px;line-height:1.45}.actions{display:flex;gap:8px}.button{display:inline-flex;align-items:center;justify-content:center;min-height:38px;padding:7px 13px;border:1px solid #4d596b;border-radius:8px;color:#dceaff;text-decoration:none}.button:hover{border-color:#78aeea;background:#202a36}.empty{padding:28px 18px;color:#aeb8c7;text-align:center}@media(max-width:640px){body{padding:20px 14px 40px}.head{align-items:start;flex-direction:column;gap:8px}.entry{grid-template-columns:1fr;gap:10px;padding:14px}.actions{justify-content:flex-start}.button{min-height:44px;flex:1}.count{white-space:normal}}
</style></head><body><a class="back" href="{{.BackPath}}">← Back to recording</a><header class="head"><div><h1>Joined clips</h1><p class="note">Browse the published continuous clips for this recording. Separate files mark a detected gap or incompatible source layout.</p></div><div class="count">{{.Total}} clip{{if ne .Total 1}}s{{end}}</div></header>
<main class="browser"><nav class="crumbs" aria-label="Joined clip folders">{{range $index, $crumb := .Crumbs}}{{if $index}}<span class="sep" aria-hidden="true">/</span>{{end}}{{if $crumb.Current}}<span aria-current="page">{{$crumb.Name}}</span>{{else}}<a href="{{$crumb.Path}}">{{$crumb.Name}}</a>{{end}}{{end}}</nav>
<div>{{range .Folders}}<div class="entry"><a class="folder" href="{{.Path}}"><span class="folder-icon" aria-hidden="true"></span><span class="name">{{.Name}}</span></a><span class="meta">{{.Count}} clip{{if ne .Count 1}}s{{end}}</span></div>{{end}}{{range .Files}}<div class="entry"><div><div class="name">{{.Name}}</div><div class="meta">{{.LocalDate}} · {{.Size}}</div></div><div class="actions"><a class="button" href="{{.DownloadPath}}?disposition=inline" target="_blank" rel="noopener noreferrer">View</a><a class="button" href="{{.DownloadPath}}?disposition=attachment">Download</a></div></div>{{end}}{{if and (not .Folders) (not .Files)}}<div class="empty">No joined clips are published in this folder yet.</div>{{end}}</div></main></body></html>`))

// handleAccountRecordingJoinedFolder renders a same-origin folder view for
// people who do not have R2 access. It uses the same account/cohort scope as the
// joined clip browser and exposes only delivery-relative paths.
func (s *Server) handleAccountRecordingJoinedFolder(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	activePath, ok := parseJoinedFolderPath(r.URL.Query().Get("folder"))
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "invalid joined folder")
		return
	}
	result, err := s.recordingJoinedFiles(r.Context(), principal, recordingID,
		[]string{"media"}, recordingJoinedBrowserMaxLimit, 0)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "list joined recording folder")
		return
	}
	if result.Total > recordingJoinedFolderMaxFiles {
		util.WriteError(w, http.StatusServiceUnavailable, "joined folder is too large to browse")
		return
	}
	for offset := len(result.Files); int64(offset) < result.Total; offset += recordingJoinedBrowserMaxLimit {
		next, nextErr := s.recordingJoinedFiles(r.Context(), principal, recordingID,
			[]string{"media"}, recordingJoinedBrowserMaxLimit, offset)
		if nextErr != nil {
			util.WriteError(w, http.StatusInternalServerError, "list joined recording folder")
			return
		}
		if len(next.Files) == 0 {
			break
		}
		result.Files = append(result.Files, next.Files...)
	}
	folders, files, found := joinedFolderEntries(result.FolderPath, result.Files, activePath)
	if !found {
		util.WriteError(w, http.StatusNotFound, "joined folder not found")
		return
	}
	crumbs := joinedFolderCrumbs(result.FolderPath, activePath)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; media-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	if err := joinedFolderTemplate.Execute(w, joinedFolderPage{
		RecordingID: recordingID,
		BackPath:    fmt.Sprintf("%s/%d", s.recordingJoinedPageRoot(principal), recordingID),
		Crumbs:      crumbs,
		Folders:     folders,
		Files:       files,
		Total:       result.Total,
	}); err != nil {
		return
	}
}

var joinedFolderMonths = map[string]bool{
	"January": true, "February": true, "March": true, "April": true,
	"May": true, "June": true, "July": true, "August": true,
	"September": true, "October": true, "November": true, "December": true,
}

func parseJoinedFolderPath(raw string) ([]string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, true
	}
	if len(raw) > 128 {
		return nil, false
	}
	parts := strings.Split(raw, "/")
	if len(parts) > 2 {
		return nil, false
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, "\\\x00\r\n") {
			return nil, false
		}
	}
	return parts, true
}

func joinedFolderFileParts(relativePath string) []string {
	parts := strings.FieldsFunc(strings.TrimSpace(relativePath), func(r rune) bool { return r == '/' || r == '\\' })
	for i, part := range parts {
		if joinedFolderMonths[part] {
			return parts[i:]
		}
	}
	return parts
}

func joinedFolderPath(root string, parts []string) string {
	if len(parts) == 0 {
		return root
	}
	return root + "?folder=" + url.QueryEscape(strings.Join(parts, "/"))
}

func joinedFolderCrumbs(root string, active []string) []joinedFolderCrumb {
	crumbs := []joinedFolderCrumb{{Name: "Joined clips", Path: root, Current: len(active) == 0}}
	for i, name := range active {
		crumbs = append(crumbs, joinedFolderCrumb{
			Name: name, Path: joinedFolderPath(root, active[:i+1]), Current: i == len(active)-1,
		})
	}
	return crumbs
}

func joinedFolderEntries(root string, source []recordingJoinedFile, active []string) ([]joinedFolderEntry, []joinedFolderFile, bool) {
	folderIndex := make(map[string]int)
	folders := make([]joinedFolderEntry, 0)
	files := make([]joinedFolderFile, 0)
	matched := len(active) == 0
	for _, file := range source {
		parts := joinedFolderFileParts(file.RelativePath)
		if len(parts) == 0 || len(parts) <= len(active) {
			continue
		}
		matches := true
		for i, want := range active {
			if parts[i] != want {
				matches = false
				break
			}
		}
		if !matches {
			continue
		}
		matched = true
		if len(parts) > len(active)+1 {
			name := parts[len(active)]
			if index, exists := folderIndex[name]; exists {
				folders[index].Count++
			} else {
				folderIndex[name] = len(folders)
				folders = append(folders, joinedFolderEntry{Name: name, Count: 1, Path: joinedFolderPath(root, append(append([]string{}, active...), name))})
			}
			continue
		}
		files = append(files, joinedFolderFile{
			Name: parts[len(parts)-1], DownloadPath: file.DownloadPath, LocalDate: file.LocalDate,
			Size: joinedFolderSize(file.SizeBytes),
		})
	}
	return folders, files, matched
}

func joinedFolderSize(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	}
	if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(bytes)/(1024*1024))
	}
	return fmt.Sprintf("%.1f GB", float64(bytes)/(1024*1024*1024))
}

type joinedOutputRangeStore interface {
	OpenExactRange(context.Context, string, string, string, int64, int64) (io.ReadCloser, error)
}

type joinedByteRange struct {
	start int64
	end   int64
}

// parseJoinedByteRange accepts one RFC 7233 byte range. Unsupported units and
// multipart forms are ignored; malformed or unsatisfiable byte ranges fail.
func parseJoinedByteRange(raw string, size int64) (*joinedByteRange, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	unit, value, found := strings.Cut(raw, "=")
	if !found || !strings.EqualFold(strings.TrimSpace(unit), "bytes") || strings.Contains(value, ",") {
		return nil, nil
	}
	if size <= 0 {
		return nil, errors.New("invalid byte range")
	}
	parts := strings.Split(strings.TrimSpace(value), "-")
	if len(parts) != 2 {
		return nil, errors.New("invalid byte range")
	}
	if parts[0] == "" {
		suffix, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffix <= 0 {
			return nil, errors.New("invalid byte range")
		}
		if suffix > size {
			suffix = size
		}
		return &joinedByteRange{start: size - suffix, end: size - 1}, nil
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= size {
		return nil, errors.New("invalid byte range")
	}
	end := size - 1
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || end < start {
			return nil, errors.New("invalid byte range")
		}
		if end >= size {
			end = size - 1
		}
	}
	return &joinedByteRange{start: start, end: end}, nil
}

// openJoinedExactRange uses native ranged reads when available and preserves the
// exact-generation ETag fence in the compatibility fallback.
func openJoinedExactRange(ctx context.Context, store joinedOutputObjectStore, objectKey, etag, versionID string, byteRange *joinedByteRange) (io.ReadCloser, error) {
	if byteRange == nil {
		return store.OpenExact(ctx, objectKey, etag, versionID)
	}
	if ranged, ok := store.(joinedOutputRangeStore); ok {
		return ranged.OpenExactRange(ctx, objectKey, etag, versionID, byteRange.start, byteRange.end)
	}
	body, err := store.OpenExact(ctx, objectKey, etag, versionID)
	if err != nil {
		return nil, err
	}
	if _, err := io.CopyN(io.Discard, body, byteRange.start); err != nil {
		body.Close()
		return nil, err
	}
	return body, nil
}

// handleAccountRecordingJoinedDownload streams one exact published generation
// through Stoarama. If R2 versioning is off, If-Match still binds the GET to the
// stored strong ETag. Single-byte ranges support normal MP4 seeking.
func (s *Server) handleAccountRecordingJoinedDownload(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	artifactID, ok := parseInt64Path(w, r, "joinedId")
	if !ok {
		return
	}
	disposition := strings.TrimSpace(r.URL.Query().Get("disposition"))
	if disposition == "" {
		disposition = "inline"
	}
	if disposition != "inline" && disposition != "attachment" {
		util.WriteError(w, http.StatusBadRequest, "disposition must be inline or attachment")
		return
	}
	var objectKey, etag, versionID, contentType, sha256, relativePath string
	var sizeBytes int64
	err := s.pool.QueryRow(r.Context(), `SELECT a.object_key,a.etag,a.version_id,a.content_type,
		a.expected_size_bytes,a.expected_sha256,a.relative_path `+recordingJoinedPublishedArtifactScopeSQL+`
		  AND a.id=$3
		  AND a.artifact_kind IN ('hour_manifest','media')`, recordingID, principal.AccountID, artifactID).Scan(
		&objectKey, &etag, &versionID, &contentType, &sizeBytes, &sha256, &relativePath)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "joined recording file not found")
		return
	}
	store := s.joinedOutputStore()
	if err != nil || store == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording file is unavailable")
		return
	}
	head, err := store.Head(r.Context(), objectKey)
	if err != nil || head.SizeBytes != sizeBytes || head.ETag != etag || head.VersionID != versionID {
		util.WriteError(w, http.StatusConflict, "joined recording file identity changed")
		return
	}
	var stillCurrent bool
	err = s.pool.QueryRow(r.Context(), `SELECT EXISTS(
		SELECT 1 `+recordingJoinedPublishedArtifactScopeSQL+`
		  AND a.id=$3 AND a.object_key=$4 AND a.etag=$5 AND a.version_id=$6
		  AND a.expected_size_bytes=$7 AND a.expected_sha256=$8 AND a.content_type=$9
		  AND a.artifact_kind IN ('hour_manifest','media')
	)`, recordingID, principal.AccountID, artifactID, objectKey, etag, versionID,
		sizeBytes, sha256, contentType).Scan(&stillCurrent)
	if err != nil || !stillCurrent {
		util.WriteError(w, http.StatusConflict, "joined recording file changed while opening")
		return
	}
	etagHeader := `"` + etag + `"`
	rangeHeader := r.Header.Get("Range")
	if ifRange := strings.TrimSpace(r.Header.Get("If-Range")); ifRange != "" && ifRange != etagHeader {
		rangeHeader = ""
	}
	byteRange, err := parseJoinedByteRange(rangeHeader, sizeBytes)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", sizeBytes))
		util.WriteError(w, http.StatusRequestedRangeNotSatisfiable, "invalid byte range")
		return
	}
	body, err := openJoinedExactRange(r.Context(), store, objectKey, etag, versionID, byteRange)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "joined recording file identity changed")
		return
	}
	defer body.Close()
	responseBytes := sizeBytes
	status := http.StatusOK
	if byteRange != nil {
		responseBytes = byteRange.end - byteRange.start + 1
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", byteRange.start, byteRange.end, sizeBytes))
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.FormatInt(responseBytes, 10))
	w.Header().Set("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": path.Base(relativePath)}))
	w.Header().Set("ETag", etagHeader)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = io.CopyN(w, body, responseBytes)
}
