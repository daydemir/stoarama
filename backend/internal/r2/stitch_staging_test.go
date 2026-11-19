package r2

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestStitchPartCapabilityExact(t *testing.T) {
	ctx := context.Background()
	client, err := New(ctx, Config{Bucket: "bucket", Endpoint: "https://storage.example.test", AccessKey: "fake", SecretKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	sha := sha256.Sum256([]byte("part"))
	digest := base64.StdEncoding.EncodeToString(sha[:])
	key := "stitch-staging/00000000-0000-0000-0000-000000000001/" + strings.Repeat("a", 64) + ".mp4"
	cap, err := client.presignStitchPart(ctx, StitchStageUpload{key: key, upload: "provider-upload"}, 2, digest, 4, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(cap.URL)
	if err != nil {
		t.Fatal(err)
	}
	if cap.Method != "PUT" || u.Host != "storage.example.test" || u.Path != "/bucket/"+key ||
		u.Query().Get("uploadId") != "provider-upload" || u.Query().Get("partNumber") != "2" ||
		u.Query().Get("X-Amz-Expires") != "60" || cap.Headers.Get("Content-Length") != "4" ||
		cap.Headers.Get("X-Amz-Checksum-Sha256") != digest {
		t.Fatal("signed exact staging grant differs")
	}
}

func TestStitchMultipartExemptionRequiresAllPagesAndExactUpload(t *testing.T) {
	for _, second := range []string{"known", "unknown", "managed", "fail", "truncated"} {
		t.Run(second, func(t *testing.T) {
			const key = "stitch-staging/00000000-0000-0000-0000-000000000001/one.mp4"
			const key2 = "stitch-staging/00000000-0000-0000-0000-000000000002/two.mp4"
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/xml")
				if calls == 1 {
					fmt.Fprintf(w, `<ListMultipartUploadsResult><Upload><Key>%s</Key><UploadId>a</UploadId></Upload>
<IsTruncated>true</IsTruncated><NextKeyMarker>%s</NextKeyMarker><NextUploadIdMarker>a</NextUploadIdMarker></ListMultipartUploadsResult>`, key, key)
					return
				}
				if req.URL.Query().Get("key-marker") != key || req.URL.Query().Get("upload-id-marker") != "a" {
					t.Error("multipart continuation was not bound")
				}
				chosen, id := key2, "b"
				switch second {
				case "unknown":
					id = "other"
				case "managed":
					chosen = "managed/file.mp4"
				case "fail":
					w.WriteHeader(403)
					fmt.Fprint(w, `<Error><Code>AccessDenied</Code></Error>`)
					return
				case "truncated":
					fmt.Fprint(w, `<ListMultipartUploadsResult><IsTruncated>true</IsTruncated></ListMultipartUploadsResult>`)
					return
				}
				fmt.Fprintf(w, `<ListMultipartUploadsResult><Upload><Key>%s</Key><UploadId>%s</UploadId></Upload>
<IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`, chosen, id)
			}))
			defer server.Close()
			client, err := New(context.Background(), Config{Bucket: "bucket", Endpoint: server.URL, AccessKey: "fake", SecretKey: "fake"})
			if err != nil {
				t.Fatal(err)
			}
			err = client.RequireNoMultipartOutsideStitchStaging(context.Background(), func(_ context.Context, object, id string) (bool, error) {
				switch object {
				case key:
					return id == "a", nil
				case key2:
					return id == "b", nil
				default:
					return false, errors.New("nonstaging key reached exact lookup")
				}
			})
			if (err == nil) != (second == "known") || calls != 2 {
				t.Fatalf("multipart decision %v, calls %d", err, calls)
			}
		})
	}
}

func TestStitchStageDeleteChecksFenceAtQuotedUnversionedWireRequest(t *testing.T) {
	const key = "stitch-staging/00000000-0000-0000-0000-000000000001/output.mp4"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/bucket/"+key ||
			r.Header.Get("If-Match") != `"stage-etag"` || r.URL.Query().Has("versionId") {
			t.Error("staging DELETE differs from the exact generation")
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	ctx := context.Background()
	client, err := New(ctx, Config{Bucket: "bucket", Endpoint: server.URL, AccessKey: "fake", SecretKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	denied := errors.New("fake fence unavailable")
	if err := client.deleteStitchStageExact(ctx, key, "stage-etag", func(context.Context) error {
		return denied
	}); !errors.Is(err, denied) {
		t.Fatalf("staging request ignored the transport fence: %v", err)
	}
	if err := client.deleteStitchStageExact(ctx, key, "stage-etag", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
}
