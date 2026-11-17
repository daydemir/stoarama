package r2

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type testNonKeep28Guard struct {
	calls  atomic.Int32
	denyAt int32
}

func (g *testNonKeep28Guard) CheckNonKeep28Delete(_ context.Context, bucket, endpoint, key string) error {
	if bucket != "bucket" || endpoint == "" || key != "managed/test.mp4" {
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
				guard.denyAt = 2
			}
			var authority NonKeep28DeleteGuard = guard
			if mode == "missing_authority" {
				authority = nil
			}
			err = c.DeleteNonKeep28Once(context.Background(), "managed/test.mp4", authority)
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
