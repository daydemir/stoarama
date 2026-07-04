package api

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/go-chi/chi/v5"
)

// relayReleasePrefix is the R2 key prefix under which the relay installer, the
// versioned relay binaries, the pinned yt-dlp/ffmpeg deps, and latest.json are
// published by scripts/release-relay.sh.
const relayReleasePrefix = "relay-releases/"

// relayArtifactName restricts a downloadable artifact to a single path segment of
// safe characters, so the {artifact} route can never traverse out of the release
// prefix or reach an unrelated R2 key. It excludes '/' entirely; the "." and ".."
// traversal names are rejected separately in handleRelayDownload.
var relayArtifactName = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// handleRelayInstallScript streams the public relay installer from R2. This is the
// target of the show-once install command: curl -fsSL <api>/relay/install.sh | bash.
func (s *Server) handleRelayInstallScript(w http.ResponseWriter, r *http.Request) {
	s.streamRelayArtifact(w, r, "install.sh", "text/x-shellscript; charset=utf-8", false)
}

// handleRelayUninstallScript streams the public relay uninstaller from R2. This is
// the target of the shown uninstall command: curl -fsSL <api>/relay/uninstall.sh | bash.
func (s *Server) handleRelayUninstallScript(w http.ResponseWriter, r *http.Request) {
	s.streamRelayArtifact(w, r, "uninstall.sh", "text/x-shellscript; charset=utf-8", false)
}

// handleRelayDownload streams a named relay artifact (binary tarball, pinned
// dependency, or latest.json) from R2 by exact name.
func (s *Server) handleRelayDownload(w http.ResponseWriter, r *http.Request) {
	artifact := strings.TrimSpace(chi.URLParam(r, "artifact"))
	if artifact == "" || artifact == "." || artifact == ".." ||
		strings.Contains(artifact, "/") || strings.Contains(artifact, "..") ||
		!relayArtifactName.MatchString(artifact) {
		util.WriteError(w, http.StatusBadRequest, "invalid artifact name")
		return
	}
	s.streamRelayArtifact(w, r, artifact, contentTypeForRelayArtifact(artifact), true)
}

func (s *Server) streamRelayArtifact(w http.ResponseWriter, r *http.Request, name, contentType string, asAttachment bool) {
	body, err := s.r2.Open(r.Context(), relayReleasePrefix+name)
	if err != nil {
		if r2.IsNotFound(err) {
			util.WriteError(w, http.StatusNotFound, "not found")
			return
		}
		// Do not echo the internal key prefix or raw S3 error to the client; log
		// the detail server-side and return a generic 404.
		log.Printf("relay artifact open failed key=%q: %v", relayReleasePrefix+name, err)
		util.WriteError(w, http.StatusNotFound, "not found")
		return
	}
	defer body.Close()
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "public, max-age=300")
	if asAttachment {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, body)
}

func contentTypeForRelayArtifact(name string) string {
	switch {
	case strings.HasSuffix(name, ".tar.gz"), strings.HasSuffix(name, ".tgz"):
		return "application/gzip"
	case strings.HasSuffix(name, ".json"):
		return "application/json"
	case strings.HasSuffix(name, ".sh"):
		return "text/x-shellscript; charset=utf-8"
	default:
		return "application/octet-stream"
	}
}
