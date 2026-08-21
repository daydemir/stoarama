package r2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

func TestPutReaderIfAbsentVerifiedCreatesAndRereadsExactObject(t *testing.T) {
	body := []byte("joined-media")
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	var putIfNoneMatch, getIfMatch string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putIfNoneMatch = r.Header.Get("If-None-Match")
			got, _ := io.ReadAll(r.Body)
			if string(got) != string(body) {
				t.Fatalf("PUT body=%q", got)
			}
			w.Header().Set("ETag", `"etag-1"`)
		case http.MethodHead:
			w.Header().Set("ETag", `"etag-1"`)
			w.Header().Set("Content-Length", "12")
		case http.MethodGet:
			getIfMatch = r.Header.Get("If-Match")
			w.Header().Set("ETag", `"etag-1"`)
			_, _ = w.Write(body)
		default:
			t.Fatalf("method=%s", r.Method)
		}
	}))
	defer server.Close()
	client, err := New(context.Background(), Config{AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	head, created, err := client.PutReaderIfAbsentVerified(context.Background(), "joined/x.mp4", "video/mp4", strings.NewReader(string(body)), int64(len(body)), sha)
	if err != nil {
		t.Fatal(err)
	}
	if !created || head.ETag != "etag-1" || putIfNoneMatch != "*" || getIfMatch != `"etag-1"` {
		t.Fatalf("created=%v head=%+v if-none-match=%q if-match=%q", created, head, putIfNoneMatch, getIfMatch)
	}
}

func TestPutReaderIfAbsentVerifiedReconcilesOnlyExactExistingObject(t *testing.T) {
	body := []byte("joined-media")
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code></Error>`)
		case http.MethodHead:
			w.Header().Set("ETag", `"existing"`)
			w.Header().Set("Content-Length", "12")
		case http.MethodGet:
			_, _ = w.Write(body)
		}
	}))
	defer server.Close()
	client, err := New(context.Background(), Config{AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, created, err := client.PutReaderIfAbsentVerified(context.Background(), "joined/x.mp4", "video/mp4", strings.NewReader(string(body)), int64(len(body)), sha)
	if err != nil || created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	bad := strings.Repeat("0", 64)
	if _, _, err := client.PutReaderIfAbsentVerified(context.Background(), "joined/x.mp4", "video/mp4", strings.NewReader(string(body)), int64(len(body)), bad); err == nil {
		t.Fatal("mismatched existing object was accepted")
	}
}

func TestPutReaderIfAbsentVerifiedRejectsUnconditionalPutCeiling(t *testing.T) {
	client := &Client{}
	sha := strings.Repeat("a", 64)
	if _, _, err := client.PutReaderIfAbsentVerified(context.Background(), "joined/x.mp4", "video/mp4", strings.NewReader("x"), MaxConditionalPutBytes+1, sha); err == nil {
		t.Fatal("oversized single-part object was accepted")
	}
}
