package api

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"mime"
	"net/http"
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
	recordingJoinedFolderPageSize      = 250
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

// recordingJoinedFiles lists only artifacts from an account-owned, sealed hour
// whose immutable identity and published hour manifest are complete.
func (s *Server) recordingJoinedFiles(ctx context.Context, principal accountPrincipal, recordingID int64, kinds []string, limit, offset int) (recordingJoinedList, error) {
	var recordingExists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2 AND status<>'canceled'
	)`, recordingID, principal.AccountID).Scan(&recordingExists); err != nil {
		return recordingJoinedList{}, err
	}
	if !recordingExists {
		return recordingJoinedList{}, pgx.ErrNoRows
	}

	const publishedScope = `
		FROM recording_joined_artifacts a
		JOIN recording_joined_hours h
		  ON h.id=a.hour_record_id
		 AND h.batch_record_id=a.batch_record_id
		 AND h.account_id=a.account_id
		 AND h.state='sealed'
		JOIN recordings rec
		  ON rec.id=h.recording_id
		 AND rec.account_id=h.account_id
		 AND rec.status<>'canceled'
		WHERE rec.id=$1
		  AND rec.account_id=$2
		  AND a.artifact_kind=ANY($3::text[])
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
	Files       []recordingJoinedFile
	Total       int64
	First       int64
	Last        int64
	Previous    string
	Next        string
}

var joinedFolderTemplate = template.Must(template.New("joined-folder").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Joined recordings</title><style>
:root{color-scheme:dark;font-family:ui-sans-serif,system-ui,sans-serif;background:#101114;color:#f3f4f6}body{max-width:1080px;margin:0 auto;padding:24px}a{color:#9bc5ff}a:focus-visible{outline:2px solid #9bc5ff;outline-offset:3px}.head{display:flex;gap:12px;align-items:center;justify-content:space-between;flex-wrap:wrap}.button{display:inline-flex;align-items:center;min-height:36px;padding:0 14px;border:1px solid #586174;border-radius:8px;text-decoration:none}.note{color:#aeb5c2;font-size:14px;line-height:1.5}.list{margin:20px 0;border-top:1px solid #30343d}.file{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;padding:12px 0;border-bottom:1px solid #30343d}.path{overflow-wrap:anywhere}.meta{color:#aeb5c2;font-size:13px;margin-top:4px}.actions{display:flex;gap:8px;align-items:center}.pager{display:flex;gap:12px;align-items:center;justify-content:flex-end;flex-wrap:wrap}@media(max-width:640px){body{padding:16px}.file{grid-template-columns:1fr}.actions{justify-content:flex-start}}
</style></head><body><div class="head"><div><a href="{{.BackPath}}">Back to recording</a><h1>Joined recordings</h1></div><div>{{.First}}-{{.Last}} of {{.Total}}</div></div>
<p class="note">This view lists the published joined files in Stoarama's private R2 folder. Each file is read through an account-scoped, ETag-bound Stoarama URL. The bucket and its credentials remain private.</p>
<div class="list">{{range .Files}}<div class="file"><div><div class="path">{{.RelativePath}}</div><div class="meta">{{.Kind}} · {{.SizeBytes}} bytes · {{.LocalDate}} · hour {{printf "%02d" .DeliveryHour}}</div></div><div class="actions"><a class="button" href="{{.DownloadPath}}?disposition=inline" target="_blank" rel="noopener noreferrer">Open</a><a class="button" href="{{.DownloadPath}}?disposition=attachment">Download</a></div></div>{{else}}<p class="note">No joined files are published yet.</p>{{end}}</div>
<nav class="pager" aria-label="Joined folder pages">{{if .Previous}}<a class="button" href="{{.Previous}}">Previous</a>{{end}}{{if .Next}}<a class="button" href="{{.Next}}">Next</a>{{end}}</nav></body></html>`))

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
	pageNumber := parseIntQuery(r, "page", 1, 1, 1<<20)
	offset := (pageNumber - 1) * recordingJoinedFolderPageSize
	result, err := s.recordingJoinedFiles(r.Context(), principal, recordingID,
		[]string{"hour_manifest", "media"}, recordingJoinedFolderPageSize, offset)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "list joined recording folder")
		return
	}
	pagePath := result.FolderPath
	previous, next := "", ""
	if pageNumber > 1 {
		previous = pagePath + "?page=" + strconv.Itoa(pageNumber-1)
	}
	if int64(offset+len(result.Files)) < result.Total {
		next = pagePath + "?page=" + strconv.Itoa(pageNumber+1)
	}
	first, last := int64(0), int64(0)
	if len(result.Files) > 0 {
		first = int64(offset + 1)
		last = int64(offset + len(result.Files))
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; media-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	if err := joinedFolderTemplate.Execute(w, joinedFolderPage{
		RecordingID: recordingID,
		BackPath:    fmt.Sprintf("%s/%d", s.recordingJoinedPageRoot(principal), recordingID),
		Files:       result.Files,
		Total:       result.Total,
		First:       first,
		Last:        last,
		Previous:    previous,
		Next:        next,
	}); err != nil {
		return
	}
}

type joinedOutputRangeStore interface {
	OpenExactRange(context.Context, string, string, string, int64, int64) (io.ReadCloser, error)
}

type joinedByteRange struct {
	start int64
	end   int64
}

// parseJoinedByteRange accepts one RFC 7233 byte range and rejects multipart or
// unsatisfiable requests.
func parseJoinedByteRange(raw string, size int64) (*joinedByteRange, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if size <= 0 || !strings.HasPrefix(raw, "bytes=") || strings.Contains(raw, ",") {
		return nil, errors.New("invalid byte range")
	}
	parts := strings.Split(strings.TrimPrefix(raw, "bytes="), "-")
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
		a.expected_size_bytes,a.expected_sha256,a.relative_path
		FROM recording_joined_artifacts a
		JOIN recording_joined_hours h
		  ON h.id=a.hour_record_id
		 AND h.batch_record_id=a.batch_record_id
		 AND h.account_id=a.account_id
		 AND h.state='sealed'
		JOIN recordings rec
		  ON rec.id=h.recording_id
		 AND rec.account_id=h.account_id
		 AND rec.status<>'canceled'
		WHERE rec.id=$1 AND rec.account_id=$2 AND a.id=$3
		  AND a.artifact_kind IN ('hour_manifest','media')
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
		  )`, recordingID, principal.AccountID, artifactID).Scan(
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
		SELECT 1 FROM recording_joined_artifacts a
		JOIN recording_joined_hours h
		  ON h.id=a.hour_record_id AND h.batch_record_id=a.batch_record_id AND h.account_id=a.account_id AND h.state='sealed'
		JOIN recordings rec ON rec.id=h.recording_id AND rec.account_id=h.account_id AND rec.status<>'canceled'
		WHERE rec.id=$1 AND rec.account_id=$2 AND a.id=$3
		  AND a.object_key=$4 AND a.etag=$5 AND a.version_id=$6
		  AND a.expected_size_bytes=$7 AND a.expected_sha256=$8 AND a.content_type=$9
		  AND a.artifact_kind IN ('hour_manifest','media') AND a.published_at IS NOT NULL
		  AND (a.artifact_kind='media' OR a.publication_state='published')
		  AND EXISTS (
		    SELECT 1 FROM recording_joined_artifacts manifest
		    WHERE manifest.hour_record_id=h.id AND manifest.batch_record_id=h.batch_record_id
		      AND manifest.account_id=h.account_id AND manifest.artifact_kind='hour_manifest'
		      AND manifest.publication_state='published' AND manifest.published_at IS NOT NULL
		      AND manifest.etag IS NOT NULL AND manifest.etag<>'' AND manifest.version_id IS NOT NULL
		  )
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
