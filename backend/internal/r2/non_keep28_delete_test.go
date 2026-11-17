package r2

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type testNonKeep28Guard struct {
	calls  atomic.Int32
	denyAt int32
}

func (g *testNonKeep28Guard) CheckNonKeep28Delete(_ context.Context, b NonKeep28DeleteBinding) error {
	if b.Bucket != "bucket" || b.EndpointSHA256 == "" || b.Key != "managed/test.mp4" {
		return errors.New("binding differs")
	}
	if n := g.calls.Add(1); g.denyAt > 0 && n >= g.denyAt {
		return errors.New("protection changed")
	}
	return nil
}
func TestR2InverseSingleWireAndChangedProtection(t *testing.T) {
	for _, mode := range []string{"success", "lost_response", "changed_protection", "missing_authority"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "DELETE" || r.URL.Path != "/bucket/managed/test.mp4" {
					t.Error("unexpected request", r.Method, r.URL.Path)
				}
				if mode == "lost_response" {
					c, _, _ := w.(http.Hijacker).Hijack()
					c.Close()
					return
				}
				w.WriteHeader(204)
			}))
			defer s.Close()
			c, err := New(context.Background(), Config{Endpoint: s.URL, Bucket: "bucket", AccessKey: "test", SecretKey: "test", Region: "auto"})
			if err != nil {
				t.Fatal(err)
			}
			guard := &testNonKeep28Guard{}
			if mode == "changed_protection" {
				guard.denyAt = 3
			}
			b := NonKeep28DeleteBinding{Bucket: "bucket", EndpointSHA256: c.EndpointSHA256(), Key: "managed/test.mp4", ETag: "etag", SourceID: 1, StreamID: 1, SizeBytes: 100, LastModified: time.Now().Add(-8 * 24 * time.Hour), ManifestSHA256: strings.Repeat("a", 64), PolicySHA256: strings.Repeat("b", 64), PicksSHA256: strings.Repeat("c", 64), DrainSHA256: strings.Repeat("d", 64)}
			authority, err := AuthorizeNonKeep28Delete(context.Background(), b, guard)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "missing_authority" {
				authority = &NonKeep28DeleteAuthority{}
			}
			err = c.DeleteNonKeep28Once(context.Background(), authority)
			// Repeated calls and value copies may never create a second attempt,
			// after success, a protection veto or an ambiguous lost response.
			clone := *authority
			if retryErr := c.DeleteNonKeep28Once(context.Background(), authority); retryErr == nil {
				t.Fatal("authority replay accepted")
			}
			if retryErr := c.DeleteNonKeep28Once(context.Background(), &clone); retryErr == nil {
				t.Fatal("copied authority replay accepted")
			}

			want := int32(1)
			if mode == "changed_protection" || mode == "missing_authority" {
				want = 0
			}
			if calls.Load() != want {
				t.Fatalf("wire calls %d want %d", calls.Load(), want)
			}
			if (mode == "success") != (err == nil) {
				t.Fatal("unexpected result", err)
			}
		})
	}
}
