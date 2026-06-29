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
