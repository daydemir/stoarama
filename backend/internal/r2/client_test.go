package r2

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestSameAuthorityHTTPSRedirect(t *testing.T) {
	origin := &http.Request{URL: mustURL(t, "https://storage.example.test:443/original")}
	for _, tc := range []struct {
		name    string
		target  string
		allowed bool
	}{
		{name: "same authority", target: "https://storage.example.test:443/redirected", allowed: true},
		{name: "scheme case", target: "HTTPS://storage.example.test:443/redirected", allowed: true},
		{name: "http downgrade", target: "http://storage.example.test:443/redirected"},
		{name: "different host", target: "https://attacker.example.test:443/redirected"},
		{name: "subdomain", target: "https://child.storage.example.test:443/redirected"},
		{name: "different port", target: "https://storage.example.test:8443/redirected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := &http.Request{URL: mustURL(t, tc.target)}
			allowed := sameAuthorityHTTPSRedirect(request, []*http.Request{origin}) == nil
			if allowed != tc.allowed {
				t.Fatalf("redirect to %s allowed=%v want %v", tc.target, allowed, tc.allowed)
			}
		})
	}
}

func TestSameAuthorityHTTPSRedirectRejectsMissingOriginAndLoop(t *testing.T) {
	request := &http.Request{URL: mustURL(t, "https://storage.example.test/next")}
	if sameAuthorityHTTPSRedirect(request, nil) == nil {
		t.Fatal("redirect without an origin was allowed")
	}
	via := make([]*http.Request, 10)
	for i := range via {
		via[i] = request
	}
	if sameAuthorityHTTPSRedirect(request, via) == nil {
		t.Fatal("tenth redirect was allowed")
	}
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestOpenExactSendsConditionalGenerationIdentity(t *testing.T) {
	var ifMatch, version string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ifMatch, version = r.Header.Get("If-Match"), r.URL.Query().Get("versionId")
		_, _ = io.WriteString(w, "clip")
	}))
	defer server.Close()
	client, err := New(context.Background(), Config{AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	body, err := client.OpenExact(context.Background(), "clip.mp4", "abc123", "version-7")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	if _, err := io.ReadAll(body); err != nil {
		t.Fatal(err)
	}
	if ifMatch != `"abc123"` || version != "version-7" {
		t.Fatalf("If-Match=%q versionId=%q", ifMatch, version)
	}
	if strings.Contains(ifMatch, "clip") {
		t.Fatal("object key leaked into conditional identity")
	}
}
