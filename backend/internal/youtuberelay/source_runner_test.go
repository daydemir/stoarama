package youtuberelay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRewriteRelayPlaylistRewritesSegmentURLs(t *testing.T) {
	t.Parallel()

	playlist := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXTINF:6,\nsegment0.ts\n#EXTINF:6,\nhttps://cdn.example.com/live/segment1.ts\n"
	rewritten, ok := rewriteRelayPlaylist("https://relay.example", 42, "shared-token", "https://upstream.example/live/index.m3u8", []byte(playlist))
	if !ok {
		t.Fatalf("rewriteRelayPlaylist returned ok=false")
	}
	out := string(rewritten)
	if !strings.Contains(out, "https://relay.example/relay/42/segment?token=shared-token&u=") {
		t.Fatalf("rewritten playlist missing relay segment URLs: %s", out)
	}
	if strings.Contains(out, "\nsegment0.ts\n") || strings.Contains(out, "https://cdn.example.com/live/segment1.ts") {
		t.Fatalf("rewritten playlist still contains upstream segment URLs: %s", out)
	}
}

func TestProbeRelayPlaylistRequiresRelaySegmentURLs(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("#EXTM3U\n#EXTINF:6,\nhttps://cdn.example.com/live/segment0.ts\n"))
	}))
	defer srv.Close()

	err := probeRelayPlaylist(context.Background(), srv.Client(), srv.URL)
	if err == nil || !strings.Contains(err.Error(), "did not rewrite segment urls") {
		t.Fatalf("expected rewrite validation error, got %v", err)
	}
}

func TestProbeRelayPlaylistFetchesSegmentThroughRelay(t *testing.T) {
	t.Parallel()

	var baseURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/relay/42/segment"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("segment-bytes"))
		default:
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = w.Write([]byte("#EXTM3U\n#EXTINF:6,\n" + baseURL + "/relay/42/segment?token=shared&u=https%3A%2F%2Fcdn.example.com%2Fsegment0.ts\n"))
		}
	}))
	defer srv.Close()
	baseURL = srv.URL

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := probeRelayPlaylist(ctx, srv.Client(), srv.URL); err != nil {
		t.Fatalf("probeRelayPlaylist returned error: %v", err)
	}
}
