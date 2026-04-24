package api

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func loadHTMLPage(name string) ([]byte, error) {
	candidates := []string{
		filepath.Join("backend/web", name),
		filepath.Join("web", name),
		filepath.Join("../backend/web", name),
		filepath.Join("../web", name),
	}
	for _, path := range candidates {
		if data, err := os.ReadFile(path); err == nil {
			return data, nil
		}
	}
	cwd, _ := os.Getwd()
	if cwd != "" {
		for _, rel := range candidates {
			path := filepath.Join(cwd, rel)
			if data, err := os.ReadFile(path); err == nil {
				return data, nil
			}
		}
	}
	return nil, fmt.Errorf("%s html not found", name)
}

func loadStreamsHTML() ([]byte, error) {
	return loadHTMLPage("streams.html")
}

func loadRecordingHTML() ([]byte, error) {
	return loadHTMLPage("recording.html")
}

func loadDocsHTML() ([]byte, error) {
	return loadHTMLPage("docs.html")
}

func loadAdminHTML() ([]byte, error) {
	return loadHTMLPage("admin.html")
}

func writeHTML(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) handleStreamsApp(w http.ResponseWriter, _ *http.Request) {
	writeHTML(w, s.streamsHTML)
}

func (s *Server) handleRecordingApp(w http.ResponseWriter, _ *http.Request) {
	writeHTML(w, s.recordingHTML)
}

func (s *Server) handleDocsApp(w http.ResponseWriter, _ *http.Request) {
	writeHTML(w, s.docsHTML)
}

func (s *Server) handleAdminApp(w http.ResponseWriter, _ *http.Request) {
	writeHTML(w, s.adminHTML)
}

func (s *Server) handleDocsRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/docs/getting-started", http.StatusFound)
}

func (s *Server) redirectLegacyRelayGuide(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/docs/self-serve", http.StatusMovedPermanently)
}

func (s *Server) redirectDashboard(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimSpace(r.URL.Path)
	target := "/streams"
	switch {
	case path == "/dashboard", path == "/dashboard/overview", path == "/dashboard/streams":
		target = "/streams"
	case path == "/dashboard/recording":
		target = "/recording"
	case strings.HasPrefix(path, "/dashboard/stream/"):
		id := strings.TrimPrefix(path, "/dashboard/stream/")
		id = strings.TrimSpace(id)
		if id != "" {
			target = "/streams/" + id
		}
	case path == "/dashboard/discovery", path == "/dashboard/pipelines", path == "/dashboard/servers":
		target = "/admin"
	}
	if raw := strings.TrimSpace(r.URL.RawQuery); raw != "" {
		target += "?" + raw
	}
	http.Redirect(w, r, target, http.StatusMovedPermanently)
}
