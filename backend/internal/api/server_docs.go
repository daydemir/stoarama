package api

import (
	"fmt"
	"net/http"
	"os"
)

func loadYouTubeRelaySourceDocsHTML() ([]byte, error) {
	candidates := []string{
		"backend/web/docs_youtube_relay_source.html",
		"web/docs_youtube_relay_source.html",
		"../backend/web/docs_youtube_relay_source.html",
		"../web/docs_youtube_relay_source.html",
	}
	for _, path := range candidates {
		if data, err := os.ReadFile(path); err == nil {
			return data, nil
		}
	}
	return nil, fmt.Errorf("youtube relay source docs html not found")
}

func (s *Server) handleYouTubeRelaySourceDocs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(s.docsHTML)
}
