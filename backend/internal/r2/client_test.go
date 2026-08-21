package r2

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPresignGetExactBindsConditionalGenerationIdentity(t *testing.T) {
	client, err := New(context.Background(), Config{AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: "https://storage.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := client.PresignGetExact(context.Background(), "clip.mp4", `"abc123"`, "version-7", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u := mustURL(t, raw)
	if u.Query().Get("versionId") != "version-7" {
		t.Fatalf("versionId=%q", u.Query().Get("versionId"))
	}
	if !strings.Contains(u.Query().Get("X-Amz-SignedHeaders"), "if-match") {
		t.Fatalf("signed headers do not bind If-Match: %q", u.Query().Get("X-Amz-SignedHeaders"))
	}
}

func TestPresignedExactCapabilitiesBindMethodAuthorityKeyAndHeaders(t *testing.T) {
	client, err := New(context.Background(), Config{AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: "https://storage.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	get, err := client.PresignGetExactRequest(context.Background(), "raw/exact clip.mp4", "abc123", "version-7", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	head, err := client.PresignHeadExactRequest(context.Background(), "raw/exact clip.mp4", "abc123", "version-7", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	put, err := client.PresignPutCreateOnlyRequest(context.Background(), "joined/batch/objects/"+strings.Repeat("a", 64)+".mp4", "video/mp4", 1234, strings.Repeat("a", 64), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, method, path string
		cap                PresignedRequest
		headers            map[string]string
	}{
		{name: "get", method: http.MethodGet, path: "/bucket/raw/exact%20clip.mp4", cap: get, headers: map[string]string{"If-Match": `"abc123"`}},
		{name: "head", method: http.MethodHead, path: "/bucket/raw/exact%20clip.mp4", cap: head, headers: map[string]string{"If-Match": `"abc123"`}},
		{name: "put", method: http.MethodPut, path: "/bucket/joined/batch/objects/" + strings.Repeat("a", 64) + ".mp4", cap: put, headers: map[string]string{
			"Content-Length": "1234", "Content-Type": "video/mp4", "If-None-Match": "*",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := mustURL(t, tc.cap.URL)
			if tc.cap.Method != tc.method || u.Scheme != "https" || u.Host != "storage.example.test" || u.EscapedPath() != tc.path {
				t.Fatalf("capability=%s %s%s, want %s https://storage.example.test%s", tc.cap.Method, u.Host, u.EscapedPath(), tc.method, tc.path)
			}
			if tc.name != "put" && u.Query().Get("versionId") != "version-7" {
				t.Fatalf("versionId=%q", u.Query().Get("versionId"))
			}
			if tc.name == "put" && u.Query().Get("X-Amz-Checksum-Sha256") != "qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo=" {
				t.Fatalf("checksum query is not bound: %q", u.Query().Get("X-Amz-Checksum-Sha256"))
			}
			for key, want := range tc.headers {
				if got := tc.cap.Headers.Get(key); got != want {
					t.Fatalf("%s=%q want %q (required=%v)", key, got, want, tc.cap.Headers)
				}
				if !strings.Contains(strings.ToLower(u.Query().Get("X-Amz-SignedHeaders")), strings.ToLower(key)) {
					t.Fatalf("%s is not signed: %q", key, u.Query().Get("X-Amz-SignedHeaders"))
				}
			}
		})
	}
}

func TestSameAuthorityHTTPSRedirect(t *testing.T) {
	origin := &http.Request{URL: mustURL(t, "https://storage.example.test:443/original")}
	for _, tc := range []struct {
		name, target string
		allowed      bool
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
