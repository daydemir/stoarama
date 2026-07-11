package capture

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/netguard"
)

func TestYTDLPResolveArgs(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv("YT_DLP_FORMAT", "")
		t.Setenv("YT_DLP_FORMAT_SORT", "")
		got := ytDLPResolveArgs("https://www.youtube.com/watch?v=abc123")
		want := []string{"-g", "--no-warnings", "--no-playlist", "https://www.youtube.com/watch?v=abc123"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("ytDLPResolveArgs()=%v want=%v", got, want)
		}
	})

	t.Run("format and sort", func(t *testing.T) {
		t.Setenv("YT_DLP_FORMAT", "bestvideo[vcodec^=avc1]/bestvideo/best")
		t.Setenv("YT_DLP_FORMAT_SORT", "res,fps")
		got := ytDLPResolveArgs("https://www.youtube.com/watch?v=abc123")
		want := []string{
			"-g", "--no-warnings", "--no-playlist",
			"-f", "bestvideo[vcodec^=avc1]/bestvideo/best",
			"-S", "res,fps",
			"https://www.youtube.com/watch?v=abc123",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("ytDLPResolveArgs()=%v want=%v", got, want)
		}
	})
}

func TestFirstHTTPURLFromTimedOutYTDLPOutput(t *testing.T) {
	got := firstHTTPURL("signal: killed\nhttps://manifest.googlevideo.com/live/index.m3u8\n")
	if want := "https://manifest.googlevideo.com/live/index.m3u8"; got != want {
		t.Fatalf("firstHTTPURL()=%q want=%q", got, want)
	}
}

// TestResolveCaptureInputFailsClosedOnUnresolvedHLS proves the recorder capture
// path never hands a raw '!hls' marker to ffmpeg: when the indirect endpoint
// returns a body that yields no http(s) URL, the marker survives resolution and
// ResolveCaptureInput must return an error rather than the raw marker (which
// ffmpeg rejects with "Invalid data found", exit 183).
func TestResolveCaptureInputFailsClosedOnUnresolvedHLS(t *testing.T) {
	// Not parallel: overrides the package SSRF guards so the loopback httptest
	// server is reachable, then restores them via t.Cleanup.
	resolveValidateURL = func(string) (net.IP, error) { return net.IPv4(127, 0, 0, 1), nil }
	resolveDialControl = func(string, string, syscall.RawConn) error { return nil }
	t.Cleanup(func() {
		resolveValidateURL = netguard.ValidatePublicURL
		resolveDialControl = netguard.ControlReject
	})

	// Endpoint returns 200 with a body that contains no http(s) line, so
	// resolveIndirectURL reports ok=false and the '!hls' marker survives.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not a url\n"))
	}))
	defer server.Close()

	got, isImage, err := ResolveCaptureInput(context.Background(), "", server.URL+"/indirect!hls", "")
	if err == nil {
		t.Fatalf("expected fail-closed error, got url=%q isImage=%v", got, isImage)
	}
	if got != "" {
		t.Fatalf("expected empty url on failure, got %q", got)
	}
	if strings.Contains(strings.ToLower(err.Error()), "!hls") == false {
		t.Fatalf("error should name the unresolved marker, got %q", err.Error())
	}
}

func TestSkylineManifestFromHTMLBuildsAuthManifestURL(t *testing.T) {
	html := `<script>var player=new Clappr.Player({source:'livee.m3u8?a=6utedl4nm3v07ossuoijphlve1'});</script>`
	got := skylineManifestFromHTML(html)
	want := "https://hd-auth.skylinewebcams.com/live.m3u8?a=6utedl4nm3v07ossuoijphlve1"
	if got != want {
		t.Fatalf("skylineManifestFromHTML()=%q want %q", got, want)
	}
}

func TestResolveCaptureInputRefreshesSkylineManifestFromSourcePage(t *testing.T) {
	// Not parallel: overrides the package SSRF guards so the loopback httptest
	// server is reachable, then restores them via t.Cleanup.
	resolveValidateURL = func(string) (net.IP, error) { return net.IPv4(127, 0, 0, 1), nil }
	resolveDialControl = func(string, string, syscall.RawConn) error { return nil }
	t.Cleanup(func() {
		resolveValidateURL = netguard.ValidatePublicURL
		resolveDialControl = netguard.ControlReject
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<script>new Clappr.Player({source:"livee.m3u8?a=fresh-token"});</script>`))
	}))
	defer server.Close()

	got, isImage, err := ResolveCaptureInput(
		context.Background(),
		"SKYLINEWEBCAMS",
		"https://hd-auth.skylinewebcams.com/live.m3u8?a=stale-token",
		server.URL+"/webcam.html",
	)
	if err != nil {
		t.Fatalf("ResolveCaptureInput() error=%v", err)
	}
	if isImage {
		t.Fatalf("ResolveCaptureInput() isImage=true, want false")
	}
	want := "https://hd-auth.skylinewebcams.com/live.m3u8?a=fresh-token"
	if got != want {
		t.Fatalf("ResolveCaptureInput()=%q want %q", got, want)
	}
}

func TestResolveCaptureInputFailsClosedWhenSkylinePageHasNoManifest(t *testing.T) {
	// Not parallel: overrides the package SSRF guards so the loopback httptest
	// server is reachable, then restores them via t.Cleanup.
	resolveValidateURL = func(string) (net.IP, error) { return net.IPv4(127, 0, 0, 1), nil }
	resolveDialControl = func(string, string, syscall.RawConn) error { return nil }
	t.Cleanup(func() {
		resolveValidateURL = netguard.ValidatePublicURL
		resolveDialControl = netguard.ControlReject
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><body>no player source</body></html>`))
	}))
	defer server.Close()

	got, isImage, err := ResolveCaptureInput(
		context.Background(),
		"SKYLINEWEBCAMS",
		"https://hd-auth.skylinewebcams.com/live.m3u8?a=stale-token",
		server.URL+"/webcam.html",
	)
	if err == nil {
		t.Fatalf("expected fail-closed error, got url=%q isImage=%v", got, isImage)
	}
	if got != "" {
		t.Fatalf("expected empty url on failure, got %q", got)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "skyline") {
		t.Fatalf("error should name skyline resolution, got %q", err.Error())
	}
}

func TestEarthCamManifestCandidatesFromHTML(t *testing.T) {
	html := `<script>{"stream":"https:\/\/videos-3.earthcam.com\/fecnetwork\/15041.flv\/playlist.m3u8?t=abc&td=1"}</script>`
	got := earthCamManifestCandidatesFromHTML(html)
	want := []string{"https://videos-3.earthcam.com/fecnetwork/15041.flv/playlist.m3u8?t=abc&td=1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("earthCamManifestCandidatesFromHTML()=%v want %v", got, want)
	}
}

func TestShouldResolveEarthCamPageSkipsEarthCamYouTubeRows(t *testing.T) {
	if shouldResolveEarthCamPage("EARTHCAM", "https://www.youtube.com/watch?v=gUgn9Mn_VM8", "https://www.youtube.com/watch?v=gUgn9Mn_VM8") {
		t.Fatalf("EarthCam YouTube rows must use the YouTube resolver")
	}
	if !shouldResolveEarthCamPage("EARTHCAM", "https://earthcam.com/world/hungary/budapest/", "https://earthcam.com/world/hungary/budapest/") {
		t.Fatalf("EarthCam page rows should use the EarthCam page resolver")
	}
}

func TestResolveCaptureInputRefreshesEarthCamManifestFromSourcePage(t *testing.T) {
	resolveValidateURL = func(string) (net.IP, error) { return net.IPv4(127, 0, 0, 1), nil }
	resolveDialControl = func(string, string, syscall.RawConn) error { return nil }
	t.Cleanup(func() {
		resolveValidateURL = netguard.ValidatePublicURL
		resolveDialControl = netguard.ControlReject
	})

	escapeURL := func(s string) string { return strings.ReplaceAll(s, "/", `\/`) }
	var pageHTML string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/page":
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(pageHTML))
		case "/bad.m3u8":
			http.NotFound(w, r)
		case "/good.m3u8":
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nchunk.m3u8\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	pageHTML = `{"stream":"` + escapeURL(server.URL+"/bad.m3u8") + `"},{"stream":"` + escapeURL(server.URL+"/good.m3u8") + `"}`

	got, isImage, inputHeaders, err := ResolveCaptureInputWithHeaders(context.Background(), "EARTHCAM", "https://earthcam.com/example", server.URL+"/page")
	if err != nil {
		t.Fatalf("ResolveCaptureInput() error=%v", err)
	}
	if isImage {
		t.Fatalf("ResolveCaptureInput() isImage=true, want false")
	}
	if want := server.URL + "/good.m3u8"; got != want {
		t.Fatalf("ResolveCaptureInput()=%q want %q", got, want)
	}
	if !strings.Contains(inputHeaders, "Referer: "+server.URL+"/page") {
		t.Fatalf("ResolveCaptureInputWithHeaders() headers=%q", inputHeaders)
	}
}
