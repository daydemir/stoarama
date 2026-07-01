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

func loadDocsHTML() ([]byte, error) {
	return loadHTMLPage("docs.html")
}

func loadPricingHTML() ([]byte, error) {
	return loadHTMLPage("pricing.html")
}

func loadAdminHTML() ([]byte, error) {
	return loadHTMLPage("admin.html")
}

func loadRecordingsHTML() ([]byte, error) {
	return loadHTMLPage("recordings.html")
}

func loadOrgSettingsHTML() ([]byte, error) {
	return loadHTMLPage("org-settings.html")
}

func writeHTML(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) handleStreamsApp(w http.ResponseWriter, _ *http.Request) {
	writeHTML(w, s.streamsHTML)
}

func (s *Server) handleDocsApp(w http.ResponseWriter, _ *http.Request) {
	writeHTML(w, s.docsHTML)
}

func (s *Server) handlePricingApp(w http.ResponseWriter, _ *http.Request) {
	writeHTML(w, s.pricingHTML)
}

func (s *Server) handleAdminApp(w http.ResponseWriter, _ *http.Request) {
	writeHTML(w, s.adminHTML)
}

func (s *Server) handleRecordingsApp(w http.ResponseWriter, _ *http.Request) {
	writeHTML(w, s.recordingsHTML)
}

func (s *Server) handleOrgSettingsApp(w http.ResponseWriter, _ *http.Request) {
	writeHTML(w, s.orgSettingsHTML)
}

func (s *Server) handleKoreaApp(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if strings.TrimSpace(q.Get("korea_family")) == "" {
		q.Set("korea_family", "all")
	}
	if strings.TrimSpace(q.Get("recordable")) == "" && strings.TrimSpace(q.Get("capture_type")) == "" {
		q.Set("recordable", "1")
	}
	target := "/streams"
	if encoded := q.Encode(); encoded != "" {
		target += "?" + encoded
	}
	http.Redirect(w, r, target, http.StatusFound)
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
