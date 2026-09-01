package api

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const (
	joinedArchiveMaxArtifacts  = 5000
	joinedArchiveMaxBytes      = int64(1 << 40)
	joinedArchiveHeadWorkers   = 8
	joinedArchiveHeadTimeout   = 30 * time.Second
	joinedArchiveObjectTimeout = 2 * time.Hour
)

var joinedArchiveSlots = make(chan struct{}, 4)

type joinedArchiveArtifact struct {
	ArtifactID   int64
	ObjectKey    string
	ETag         string
	VersionID    string
	ContentType  string
	RelativePath string
	SizeBytes    int64
	SHA256       string
}

func canonicalJoinedArchivePath(relative string) (string, bool) {
	if relative == "" || len(relative) > 1024 || strings.HasPrefix(relative, "/") || strings.ContainsAny(relative, "\\\x00\r\n") {
		return "", false
	}
	parts := strings.Split(relative, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", false
		}
	}
	clean := path.Clean(relative)
	return clean, clean == relative
}

func validateJoinedArchive(artifacts []joinedArchiveArtifact) error {
	if len(artifacts) == 0 {
		return errors.New("joined folder is empty")
	}
	if len(artifacts) > joinedArchiveMaxArtifacts {
		return errors.New("joined folder has too many files")
	}
	seen := make(map[string]struct{}, len(artifacts))
	var total int64
	for _, artifact := range artifacts {
		name, ok := canonicalJoinedArchivePath(artifact.RelativePath)
		if !ok {
			return fmt.Errorf("unsafe joined archive path for artifact %d", artifact.ArtifactID)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("duplicate joined archive path %q", name)
		}
		seen[name] = struct{}{}
		if name == "joined-files.json" {
			return errors.New("joined artifact conflicts with archive index")
		}
		if artifact.SizeBytes < 0 || artifact.SizeBytes > joinedArchiveMaxBytes-total {
			return errors.New("joined folder exceeds archive size limit")
		}
		total += artifact.SizeBytes
	}
	return nil
}

// scopeJoinedArchive preserves canonical delivery-relative names. A nested
// folder contains only artifacts whose canonical media delivery path is below
// that folder. Coverage manifests remain available from the recording root.
func scopeJoinedArchive(all []joinedArchiveArtifact, active []string) ([]joinedArchiveArtifact, string, bool) {
	rootName := ""
	for _, artifact := range all {
		parts := joinedFolderFileParts(artifact.RelativePath)
		if len(parts) >= 2 {
			original := strings.Split(artifact.RelativePath, "/")
			for index, part := range original {
				if joinedFolderMonths[part] && index > 0 {
					rootName = original[index-1]
					break
				}
			}
		}
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
		match := true
		for index, want := range active {
			if parts[index] != want {
				match = false
				break
			}
		}
		if match {
			selected = append(selected, artifact)
		}
	}
	return selected, active[len(active)-1], len(selected) > 0
}

func preflightJoinedArchive(ctx context.Context, store joinedOutputObjectStore, artifacts []joinedArchiveArtifact) error {
	if store == nil {
		return errors.New("joined output storage unavailable")
	}
	if err := validateJoinedArchive(artifacts); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan joinedArchiveArtifact)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	worker := func() {
		defer wg.Done()
		for artifact := range jobs {
			headCtx, headCancel := context.WithTimeout(ctx, joinedArchiveHeadTimeout)
			head, err := store.Head(headCtx, artifact.ObjectKey)
			headCancel()
			if err == nil && (head.ETag != artifact.ETag || head.VersionID != artifact.VersionID || head.SizeBytes != artifact.SizeBytes) {
				err = errors.New("joined artifact identity changed")
			}
			if err != nil {
				errOnce.Do(func() { firstErr = err; cancel() })
				return
			}
		}
	}
	workers := joinedArchiveHeadWorkers
	if len(artifacts) < workers {
		workers = len(artifacts)
	}
	for index := 0; index < workers; index++ {
		wg.Add(1)
		go worker()
	}
send:
	for _, artifact := range artifacts {
		select {
		case jobs <- artifact:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func streamJoinedArchive(ctx context.Context, destination io.Writer, store joinedOutputObjectStore, artifacts []joinedArchiveArtifact) error {
	archive := zip.NewWriter(destination)
	indexBytes, err := json.Marshal(struct {
		SchemaVersion int                      `json:"schema_version"`
		Files         []joinedArchiveIndexFile `json:"files"`
	}{SchemaVersion: 1, Files: joinedArchiveIndex(artifacts)})
	if err != nil {
		return err
	}
	indexHeader := &zip.FileHeader{Name: "joined-files.json", Method: zip.Store}
	indexHeader.SetMode(0o644)
	indexHeader.SetModTime(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC))
	indexEntry, err := archive.CreateHeader(indexHeader)
	if err != nil {
		return err
	}
	if _, err := indexEntry.Write(indexBytes); err != nil {
		return err
	}
	for _, artifact := range artifacts {
		if err := ctx.Err(); err != nil {
			return err
		}
		objectCtx, objectCancel := context.WithTimeout(ctx, joinedArchiveObjectTimeout)
		body, err := store.OpenExact(objectCtx, artifact.ObjectKey, artifact.ETag, artifact.VersionID)
		if err != nil {
			objectCancel()
			return err
		}
		header := &zip.FileHeader{Name: artifact.RelativePath, Method: zip.Store}
		header.SetMode(0o644)
		header.SetModTime(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC))
		entry, err := archive.CreateHeader(header)
		if err != nil {
			body.Close()
			objectCancel()
			return err
		}
		hash := sha256.New()
		written, copyErr := io.CopyN(io.MultiWriter(entry, hash), body, artifact.SizeBytes)
		closeErr := body.Close()
		objectCancel()
		if copyErr != nil || written != artifact.SizeBytes {
			if copyErr != nil {
				return copyErr
			}
			return io.ErrUnexpectedEOF
		}
		if closeErr != nil {
			return closeErr
		}
		if artifact.SHA256 != "" && hex.EncodeToString(hash.Sum(nil)) != artifact.SHA256 {
			return errors.New("joined artifact checksum changed")
		}
	}
	return archive.Close()
}

type joinedArchiveIndexFile struct {
	ArtifactID  int64  `json:"artifact_id"`
	Path        string `json:"path"`
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
}

func joinedArchiveIndex(artifacts []joinedArchiveArtifact) []joinedArchiveIndexFile {
	files := make([]joinedArchiveIndexFile, 0, len(artifacts))
	for _, artifact := range artifacts {
		files = append(files, joinedArchiveIndexFile{
			ArtifactID: artifact.ArtifactID, Path: artifact.RelativePath, ContentType: artifact.ContentType,
			SizeBytes: artifact.SizeBytes, SHA256: artifact.SHA256,
		})
	}
	return files
}

func safeJoinedArchiveFilename(name string, recordingID int64) string {
	name = strings.TrimSpace(name)
	var safe strings.Builder
	for _, char := range name {
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

func (s *Server) joinedArchiveArtifacts(ctx context.Context, principal accountPrincipal, recordingID int64) ([]joinedArchiveArtifact, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2 AND status IN ('active','paused','completed'))`, recordingID, principal.AccountID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, pgx.ErrNoRows
	}
	rows, err := s.pool.Query(ctx, `SELECT a.id,a.object_key,a.etag,a.version_id,a.content_type,a.relative_path,a.expected_size_bytes,a.expected_sha256 `+
		recordingJoinedPublishedArtifactScopeSQL+`
		  AND a.artifact_kind='media'
		ORDER BY a.relative_path,a.id
		LIMIT $3`, recordingID, principal.AccountID, joinedArchiveMaxArtifacts+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	artifacts := make([]joinedArchiveArtifact, 0)
	for rows.Next() {
		var artifact joinedArchiveArtifact
		if err := rows.Scan(&artifact.ArtifactID, &artifact.ObjectKey, &artifact.ETag, &artifact.VersionID,
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
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	activePath, ok := parseJoinedFolderPath(r.URL.Query().Get("folder"))
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "invalid joined folder")
		return
	}
	select {
	case joinedArchiveSlots <- struct{}{}:
		defer func() { <-joinedArchiveSlots }()
	default:
		util.WriteError(w, http.StatusTooManyRequests, "joined archive capacity is busy")
		return
	}
	ctx := r.Context()
	artifacts, err := s.joinedArchiveArtifacts(ctx, principal, recordingID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "joined folder is unavailable")
		return
	}
	artifacts, archiveName, found := scopeJoinedArchive(artifacts, activePath)
	if !found {
		util.WriteError(w, http.StatusNotFound, "joined folder not found")
		return
	}
	sort.SliceStable(artifacts, func(i, j int) bool {
		if artifacts[i].RelativePath == artifacts[j].RelativePath {
			return artifacts[i].ArtifactID < artifacts[j].ArtifactID
		}
		return artifacts[i].RelativePath < artifacts[j].RelativePath
	})
	if err := preflightJoinedArchive(ctx, s.joinedOutputStore(), artifacts); err != nil {
		util.WriteError(w, http.StatusConflict, "joined folder changed while preparing archive")
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": safeJoinedArchiveFilename(archiveName, recordingID)}))
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_ = streamJoinedArchive(ctx, w, s.joinedOutputStore(), artifacts)
}

func (s *Server) handleSharedRecordingJoinedFolderArchive(w http.ResponseWriter, r *http.Request) {
	s.handleAccountRecordingJoinedFolderArchive(w, s.sharedRecordingPrincipalRequest(r))
}
