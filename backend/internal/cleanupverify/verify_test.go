package cleanupverify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

type fakeStore struct {
	body       []byte
	head       r2.ObjectHead
	after      *r2.ObjectHead
	headCalls  int
	openedETag string
}

func (f *fakeStore) Head(context.Context, string) (r2.ObjectHead, error) {
	f.headCalls++
	if f.headCalls > 1 && f.after != nil {
		return *f.after, nil
	}
	return f.head, nil
}
func (f *fakeStore) OpenIfMatch(_ context.Context, _ string, etag, version string) (r2.ObjectReader, error) {
	f.openedETag = etag
	return r2.ObjectReader{Body: io.NopCloser(bytes.NewReader(f.body)), ETag: f.head.ETag, VersionID: version, SizeBytes: int64(len(f.body))}, nil
}

func sha(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }

func TestVerifyStreamsExactStableObject(t *testing.T) {
	body := []byte("native media bytes")
	f := &fakeStore{body: body, head: r2.ObjectHead{ETag: "etag-1", SizeBytes: int64(len(body)), VersionID: "v1"}}
	got, err := Verify(context.Background(), f, "clips/one.mp4", int64(len(body)), sha(body), 1024)
	if err != nil || got.SHA256 != sha(body) || f.openedETag != "etag-1" {
		t.Fatalf("got=%+v etag=%q err=%v", got, f.openedETag, err)
	}
}

func TestVerifyFailsClosedOnBoundsShortBodySHAAndDrift(t *testing.T) {
	body := []byte("abc")
	for _, tc := range []struct {
		name string
		f    *fakeStore
		size int64
		sha  string
		cap  int64
		code string
	}{
		{"oversize", &fakeStore{body: body, head: r2.ObjectHead{ETag: "e", SizeBytes: 3}}, 3, sha(body), 2, "object_too_large"},
		{"short", &fakeStore{body: body[:2], head: r2.ObjectHead{ETag: "e", SizeBytes: 3}}, 3, sha(body), 10, "get_identity_mismatch"},
		{"sha", &fakeStore{body: body, head: r2.ObjectHead{ETag: "e", SizeBytes: 3}}, 3, sha([]byte("xyz")), 10, "content_sha_mismatch"},
		{"drift", &fakeStore{body: body, head: r2.ObjectHead{ETag: "e", SizeBytes: 3}, after: &r2.ObjectHead{ETag: "changed", SizeBytes: 3}}, 3, sha(body), 10, "object_changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Verify(context.Background(), tc.f, "k", tc.size, tc.sha, tc.cap)
			if err == nil || ErrorCode(err) != tc.code {
				t.Fatalf("err=%v code=%s", err, ErrorCode(err))
			}
		})
	}
}
