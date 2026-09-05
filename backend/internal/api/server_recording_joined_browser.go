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
	RootPath    string
	RootName    string
	Crumbs      []joinedFolderCrumb
	Folders     []joinedFolderEntry
	Files       []joinedFolderFile
	MediaTotal  int
	ArchivePath string
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
	Type         string
	TypeClass    string
}

type joinedFolderRootEntry struct {
	RecordingID int64
	Name        string
	ClipCount   int64
	Size        string
	Path        string
	ArchivePath string
}

type joinedFolderRootPage struct {
	BackPath string
	Folders  []joinedFolderRootEntry
}

var joinedFolderTemplate = template.Must(template.New("joined-folder").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Joined clips</title><style>
:root{color-scheme:dark;font-family:ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d0f13;color:#f3f5f8}*{box-sizing:border-box}body{max-width:1280px;margin:0 auto;padding:22px 20px 48px}a{color:#a8ceff}a:focus-visible{outline:2px solid #8bbcff;outline-offset:2px}.topbar,.head,.actions{display:flex;align-items:center}.topbar{min-height:44px;gap:18px;margin-bottom:14px}.topbar a{color:#aeb8c7;font-size:13px;text-decoration:none}.topbar a:hover{color:#f3f5f8}.head{gap:18px;align-items:end;justify-content:space-between;margin-bottom:16px}.head h1{max-width:100%;margin:0 0 5px;font:650 clamp(20px,3vw,28px)/1.2 ui-monospace,SFMono-Regular,Menlo,monospace;letter-spacing:-.025em;overflow-wrap:anywhere}.note{max-width:72ch;margin:0;color:#aeb8c7;font-size:13px;line-height:1.45}.head-actions{display:flex;flex:0 0 auto;align-items:center;gap:9px}.count{color:#aeb8c7;font:12px ui-monospace,SFMono-Regular,Menlo,monospace;white-space:nowrap}.browser{overflow:hidden;border:1px solid #303744;border-radius:10px;background:#15191f}.crumbs{display:flex;align-items:center;gap:6px;min-height:44px;padding:0 12px;overflow-x:auto;border-bottom:1px solid #303744;white-space:nowrap}.crumbs a,.crumbs span{display:inline-flex;align-items:center;min-height:40px;font-size:13px}.crumbs span{color:#f3f5f8;font-weight:650}.crumbs .sep{min-height:0;color:#667085}.entry{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:14px;min-height:48px;padding:6px 12px;border-bottom:1px solid #292f39}.entry:last-child{border-bottom:0}.folder{display:flex;align-items:center;gap:10px;min-width:0;min-height:36px;color:#f3f5f8;font-size:13px;font-weight:650;text-decoration:none}.folder:hover .name{text-decoration:underline;text-underline-offset:3px}.folder-icon{position:relative;flex:0 0 auto;width:19px;height:13px;border-radius:3px;background:#5e9de7}.folder-icon:before{content:"";position:absolute;top:-4px;left:2px;width:8px;height:5px;border-radius:3px 3px 0 0;background:inherit}.file-main{display:grid;grid-template-columns:auto minmax(0,1fr);align-items:center;gap:9px;min-width:0}.name{min-width:0;font-size:13px;overflow-wrap:anywhere}.meta{margin-top:2px;color:#aeb8c7;font-size:11px;line-height:1.35}.type{min-width:42px;padding:3px 6px;border:1px solid #475568;border-radius:5px;color:#b9c6d8;font:700 10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;text-align:center}.type.mp4{border-color:#376891;color:#9fd1ff}.type.json{border-color:#6b5935;color:#f1d18d}.actions{gap:6px}.button{display:inline-flex;align-items:center;justify-content:center;min-height:32px;padding:5px 10px;border:1px solid #4d596b;border-radius:6px;color:#dceaff;font-size:12px;text-decoration:none}.button:hover{border-color:#78aeea;background:#202a36}.button.primary{border-color:#4f86bf;background:#1c4168;color:#fff}.empty{padding:24px 16px;color:#aeb8c7;text-align:center}@media(max-width:680px){body{padding:14px 10px 36px}.topbar{gap:12px;overflow-x:auto;white-space:nowrap}.head{align-items:start;flex-direction:column;gap:10px}.head-actions{width:100%;justify-content:space-between}.entry{gap:8px;padding:7px 9px}.button{min-height:40px;padding:7px 9px}.actions{flex-wrap:wrap;justify-content:flex-end}.count{white-space:normal}.file-main{align-items:start}}
</style></head><body><nav class="topbar" aria-label="Joined recordings navigation"><a href="{{.BackPath}}">Back to recording</a><a href="{{.RootPath}}">All joined recordings</a></nav><header class="head"><div><h1>{{.RootName}}</h1><p class="note">Published continuous parts. Separate MP4 files mark a detected gap or incompatible source layout.</p></div><div class="head-actions"><span class="count">{{.MediaTotal}} MP4</span>{{if .ArchivePath}}<a class="button primary" href="{{.ArchivePath}}">Download folder ZIP</a>{{end}}</div></header>
<main class="browser"><nav class="crumbs" aria-label="Joined clip folders">{{range $index, $crumb := .Crumbs}}{{if $index}}<span class="sep" aria-hidden="true">/</span>{{end}}{{if $crumb.Current}}<span aria-current="page">{{$crumb.Name}}</span>{{else}}<a href="{{$crumb.Path}}">{{$crumb.Name}}</a>{{end}}{{end}}</nav>
<div>{{range .Folders}}<div class="entry"><a class="folder" href="{{.Path}}"><span class="folder-icon" aria-hidden="true"></span><span class="name">{{.Name}}</span></a><span class="meta">{{.Count}} file{{if ne .Count 1}}s{{end}}</span></div>{{end}}{{range .Files}}<div class="entry"><div class="file-main"><span class="type {{.TypeClass}}">{{.Type}}</span><div><div class="name">{{.Name}}</div><div class="meta">{{.LocalDate}} · {{.Size}}</div></div></div><div class="actions"><a class="button" href="{{.DownloadPath}}?disposition=inline" target="_blank" rel="noopener noreferrer">View</a><a class="button" href="{{.DownloadPath}}?disposition=attachment">Download</a></div></div>{{end}}{{if and (not .Folders) (not .Files)}}<div class="empty">No joined files are published in this folder yet.</div>{{end}}</div></main></body></html>`))

var joinedFolderRootTemplate = template.Must(template.New("joined-folder-root").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Joined recordings</title><style>
:root{color-scheme:dark;font-family:ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d0f13;color:#f3f5f8}*{box-sizing:border-box}body{max-width:1280px;margin:0 auto;padding:22px 20px 48px}a{color:#a8ceff}a:focus-visible{outline:2px solid #8bbcff;outline-offset:2px}.back{display:inline-flex;align-items:center;min-height:44px;margin-bottom:14px;color:#aeb8c7;font-size:13px;text-decoration:none}.back:hover{color:#f3f5f8}h1{margin:0 0 5px;font-size:clamp(24px,3vw,32px);letter-spacing:-.025em}.note{max-width:70ch;margin:0 0 16px;color:#aeb8c7;font-size:13px;line-height:1.45}.browser{overflow:hidden;border:1px solid #303744;border-radius:10px;background:#15191f}.entry{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:14px;min-height:52px;padding:7px 12px;border-bottom:1px solid #292f39}.entry:last-child{border-bottom:0}.folder{display:flex;align-items:center;gap:10px;min-width:0;min-height:38px;color:#f3f5f8;font:650 13px ui-monospace,SFMono-Regular,Menlo,monospace;text-decoration:none}.folder:hover .name{text-decoration:underline;text-underline-offset:3px}.folder-icon{position:relative;flex:0 0 auto;width:19px;height:13px;border-radius:3px;background:#5e9de7}.folder-icon:before{content:"";position:absolute;top:-4px;left:2px;width:8px;height:5px;border-radius:3px 3px 0 0;background:inherit}.name{min-width:0;overflow-wrap:anywhere}.meta{margin-top:3px;color:#aeb8c7;font:11px ui-monospace,SFMono-Regular,Menlo,monospace}.actions{display:flex;gap:6px}.button{display:inline-flex;align-items:center;justify-content:center;min-height:32px;padding:5px 10px;border:1px solid #4d596b;border-radius:6px;color:#dceaff;font-size:12px;text-decoration:none}.button:hover{border-color:#78aeea;background:#202a36}.empty{padding:24px 16px;color:#aeb8c7;text-align:center}@media(max-width:680px){body{padding:14px 10px 36px}.entry{grid-template-columns:1fr;gap:6px;padding:8px 9px}.actions{justify-content:flex-start}.button{min-height:40px}}
</style></head><body><a class="back" href="{{.BackPath}}">Back to recordings</a><h1>Joined recordings</h1><p class="note">Each folder uses its canonical delivery name. Open a folder to browse its published MP4 files.</p><main class="browser">{{range .Folders}}<div class="entry"><div><a class="folder" href="{{.Path}}"><span class="folder-icon" aria-hidden="true"></span><span class="name">{{.Name}}</span></a><div class="meta">{{.ClipCount}} MP4 · {{.Size}}</div></div>{{if .ArchivePath}}<div class="actions"><a class="button" href="{{.ArchivePath}}">Download ZIP</a></div>{{end}}</div>{{else}}<div class="empty">No joined recordings are published yet.</div>{{end}}</main></body></html>`))

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
	rootName := joinedCanonicalRootName(result.Files)
	if rootName == "" {
		rootName = fmt.Sprintf("Recording %d", recordingID)
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
		RootPath:    s.recordingJoinedAPIRoot(principal) + "/recordings/joined/folder",
		RootName:    rootName,
		Crumbs:      crumbs,
		Folders:     folders,
		Files:       files,
		MediaTotal:  joinedFolderEntryCount(folders, files),
		ArchivePath: joinedFolderArchivePath(result.FolderPath, activePath, s.joinedArchiveConfigured()),
	}); err != nil {
		return
	}
}

func joinedFolderEntryCount(folders []joinedFolderEntry, files []joinedFolderFile) int {
	total := len(files)
	for _, folder := range folders {
		total += folder.Count
	}
	return total
}

func (s *Server) handleAccountJoinedFolderRoot(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT h.recording_id,
		       min(split_part(a.relative_path,'/',1)) AS folder_name,
		       count(*) AS clip_count,
		       coalesce(sum(a.expected_size_bytes),0)
		FROM recording_joined_artifacts a
		JOIN recording_joined_hours h
		  ON h.id=a.hour_record_id AND h.batch_record_id=a.batch_record_id AND h.account_id=a.account_id
		JOIN recordings rec
		  ON rec.id=h.recording_id AND rec.account_id=h.account_id
		WHERE rec.account_id=$1
		  AND rec.status IN ('active','paused','completed')
		  AND h.state='sealed'
		  AND a.artifact_kind='media'
		  AND a.published_at IS NOT NULL
		  AND a.etag IS NOT NULL AND a.etag<>''
		  AND a.version_id IS NOT NULL
		  AND EXISTS (
		    SELECT 1 FROM recording_joined_artifacts manifest
		    WHERE manifest.hour_record_id=h.id
		      AND manifest.batch_record_id=h.batch_record_id
		      AND manifest.account_id=h.account_id
		      AND manifest.artifact_kind='hour_manifest'
		      AND manifest.publication_state='published'
		      AND manifest.published_at IS NOT NULL
		      AND manifest.etag IS NOT NULL AND manifest.etag<>''
		      AND manifest.version_id IS NOT NULL)
		GROUP BY h.recording_id
		ORDER BY folder_name,h.recording_id`, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "list joined recording folders")
		return
	}
	defer rows.Close()
	root := s.recordingJoinedAPIRoot(principal)
	entries := make([]joinedFolderRootEntry, 0)
	for rows.Next() {
		var entry joinedFolderRootEntry
		var sizeBytes int64
		if err := rows.Scan(&entry.RecordingID, &entry.Name, &entry.ClipCount, &sizeBytes); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "list joined recording folders")
			return
		}
		if strings.TrimSpace(entry.Name) == "" {
			entry.Name = fmt.Sprintf("Recording %d", entry.RecordingID)
		}
		entry.Size = joinedFolderSize(sizeBytes)
		entry.Path = fmt.Sprintf("%s/recordings/%d/joined/folder", root, entry.RecordingID)
		entry.ArchivePath = joinedFolderArchivePath(entry.Path, nil, s.joinedArchiveConfigured())
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "list joined recording folders")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	_ = joinedFolderRootTemplate.Execute(w, joinedFolderRootPage{
		BackPath: s.recordingJoinedPageRoot(principal),
		Folders:  entries,
	})
}

func (s *Server) handleSharedJoinedFolderRoot(w http.ResponseWriter, r *http.Request) {
	s.handleAccountJoinedFolderRoot(w, s.sharedRecordingPrincipalRequest(r))
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
	if len(parts) > 1 {
		return parts[1:]
	}
	return parts
}

func joinedCanonicalRootName(files []recordingJoinedFile) string {
	for _, file := range files {
		if root := joinedCanonicalRootFromPath(file.RelativePath); root != "" {
			return root
		}
	}
	return ""
}

func joinedCanonicalRootFromPath(relativePath string) string {
	parts := strings.FieldsFunc(strings.TrimSpace(relativePath), func(r rune) bool { return r == '/' || r == '\\' })
	if len(parts) > 1 && parts[0] != "." && parts[0] != ".." {
		return parts[0]
	}
	return ""
}

func joinedFolderPath(root string, parts []string) string {
	if len(parts) == 0 {
		return root
	}
	return root + "?folder=" + url.QueryEscape(strings.Join(parts, "/"))
}

func joinedFolderArchivePath(root string, parts []string, enabled bool) string {
	if !enabled {
		return ""
	}
	return joinedFolderPath(strings.TrimRight(root, "/")+"/archive", parts)
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
			Size: joinedFolderSize(file.SizeBytes), Type: joinedFolderFileType(file),
			TypeClass: strings.ToLower(joinedFolderFileType(file)),
		})
	}
	return folders, files, matched
}

func joinedFolderFileType(file recordingJoinedFile) string {
	extension := strings.TrimPrefix(strings.ToUpper(path.Ext(file.RelativePath)), ".")
	if extension != "" && len(extension) <= 8 {
		return extension
	}
	if file.Kind == "hour_manifest" || strings.Contains(strings.ToLower(file.ContentType), "json") {
		return "JSON"
	}
	if file.Kind == "media" {
		return "MP4"
	}
	return "FILE"
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
