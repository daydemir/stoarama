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

// The stage path no longer owns a DELETE transport: it reuses the audited RAW
// transport, which is tested with the RAW lanes. These tests pin the two parts
// that stay here: the exact-key gate and the strict absence readback.
func TestStitchStageKeyShapeIsExact(t *testing.T) {
	sha := strings.Repeat("a", 64)
	ok := "stitch-staging/00000000-0000-0000-0000-000000000001/" + sha + ".mp4"
	for _, tc := range []struct {
		key, sha string
		want     bool
	}{
		{ok, sha, true},
		{"managed/acct-47/raw.mp4", sha, false},
		{"stitch-staging/" + strings.Repeat("b", 64) + ".mp4", sha, false},
		{"stitch-staging/00000000-0000-0000-0000-000000000001/" + strings.Repeat("a", 64) + ".mp4", strings.Repeat("b", 64), false},
		{"stitch-staging/00000000-0000-0000-0000-000000000001/../" + sha + ".mp4", sha, false},
		{"stitch-staging/00000000-0000-0000-0000-000000000001/" + sha + ".mp4/extra", sha, false},
	} {
		if got := ValidStitchStageKey(tc.key, tc.sha); got != tc.want {
			t.Fatalf("ValidStitchStageKey(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestStitchStageAbsenceIsStrictAboutProviderHeaders(t *testing.T) {
	const key = "stitch-staging/00000000-0000-0000-0000-000000000001/output.mp4"
	for _, tc := range []struct {
		name    string
		status  int
		headers map[string]string
		want    bool
	}{
		{"plain 404", http.StatusNotFound, nil, true},
		{"no such key header", http.StatusNotFound, map[string]string{"x-amz-error-code": "NoSuchKey"}, true},
		{"delete marker", http.StatusNotFound, map[string]string{"x-amz-delete-marker": "true"}, false},
		{"conflicting error", http.StatusNotFound, map[string]string{"x-amz-error-code": "NoSuchBucket"}, false},
		{"object still present", http.StatusOK, nil, false},
		{"service error", http.StatusServiceUnavailable, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodHead || r.URL.Path != "/bucket/"+key {
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
				}
				for k, v := range tc.headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tc.status)
			}))
			defer server.Close()
			client, err := New(context.Background(), Config{Bucket: "bucket", Endpoint: server.URL, AccessKey: "fake", SecretKey: "fake"})
			if err != nil {
				t.Fatal(err)
			}
			if err := client.requireStitchStageAbsent(context.Background(), key); (err == nil) != tc.want {
				t.Fatalf("requireStitchStageAbsent = %v, want confirmed %v", err, tc.want)
			}
		})
	}
}

func TestStitchStageMultipartInventoryMustBeEmptyAndExact(t *testing.T) {
	const key = "stitch-staging/00000000-0000-0000-0000-000000000001/output.mp4"
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{"empty exact", "<ListMultipartUploadsResult><Bucket>bucket</Bucket><Prefix>" + key + "</Prefix><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>", true},
		{"one live upload", "<ListMultipartUploadsResult><Bucket>bucket</Bucket><Prefix>" + key + "</Prefix><IsTruncated>false</IsTruncated><Upload><Key>" + key + "</Key><UploadId>x</UploadId></Upload></ListMultipartUploadsResult>", false},
		{"truncated", "<ListMultipartUploadsResult><Bucket>bucket</Bucket><Prefix>" + key + "</Prefix><IsTruncated>true</IsTruncated></ListMultipartUploadsResult>", false},
		{"different prefix", "<ListMultipartUploadsResult><Bucket>bucket</Bucket><Prefix>stitch-staging/other</Prefix><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>", false},
		{"different bucket", "<ListMultipartUploadsResult><Bucket>other</Bucket><Prefix>" + key + "</Prefix><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			client, err := New(context.Background(), Config{Bucket: "bucket", Endpoint: server.URL, AccessKey: "fake", SecretKey: "fake"})
			if err != nil {
				t.Fatal(err)
			}
			if err := client.stitchStageNoMultipart(context.Background(), key); (err == nil) != tc.want {
				t.Fatalf("stitchStageNoMultipart = %v, want drained %v", err, tc.want)
			}
		})
	}
}

// Publication is the only step that can make a held stage eligible for NAS
// release, so it must refuse when the exact key could still be recreated by an
// unfinished upload, even though the object itself is already absent. A HEAD
// that answers 404 is not enough: a live or truncated multipart inventory for
// the same exact key keeps the row held.
func TestStitchStageAbsenceIsNotPublishedWhileTheExactKeyCanStillAppear(t *testing.T) {
	key := "stitch-staging/00000000-0000-0000-0000-000000000001/" + strings.Repeat("a", 64) + ".mp4"
	for _, tc := range []struct {
		name    string
		listing string
		want    bool
	}{
		{"empty inventory publishes", "<ListMultipartUploadsResult><Bucket>bucket</Bucket><Prefix>" + key + "</Prefix><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>", true},
		{"live upload withholds", "<ListMultipartUploadsResult><Bucket>bucket</Bucket><Prefix>" + key + "</Prefix><IsTruncated>false</IsTruncated><Upload><Key>" + key + "</Key><UploadId>live</UploadId></Upload></ListMultipartUploadsResult>", false},
		{"truncated listing withholds", "<ListMultipartUploadsResult><Bucket>bucket</Bucket><Prefix>" + key + "</Prefix><IsTruncated>true</IsTruncated></ListMultipartUploadsResult>", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			heads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodGet:
					w.Header().Set("Content-Type", "application/xml")
					fmt.Fprint(w, tc.listing)
				case http.MethodHead:
					// The object is genuinely absent at the provider.
					heads++
					w.WriteHeader(http.StatusNotFound)
				default:
					t.Errorf("publication sent an unexpected %s", r.Method)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			client, err := New(context.Background(), Config{Bucket: "bucket", Endpoint: server.URL, AccessKey: "fake", SecretKey: "fake"})
			if err != nil {
				t.Fatal(err)
			}
			// Exercise the publication guard exactly as the retirement does.
			err = client.stitchStageNoMultipart(context.Background(), key)
			if err == nil {
				err = client.requireStitchStageAbsent(context.Background(), key)
			}
			if (err == nil) != tc.want {
				t.Fatalf("publication = %v, want published %v", err, tc.want)
			}
			if tc.want && heads != 1 {
				t.Fatalf("a published absence did not read the exact key: %d heads", heads)
			}
			if !tc.want && heads != 0 {
				t.Fatalf("a withheld absence still read the key: %d heads", heads)
			}
		})
	}
}

// The stage path must never mint or send authority that has already outlived
// its wire bound. The pre-issuance gate is the same shape the RAW lanes proved
// in #572: a delayed context timer is not trusted, so the monotonic clock is
// compared against the bound itself before the attempt commit and again after
// an acknowledged one.
func TestStitchStagePreIssuanceRefusesAnExpiredBound(t *testing.T) {
	// A live bound is admitted, so an ordinary run never trips the gate.
	live, liveCancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer liveCancel()
	if err := stitchStagePreIssuanceExpired(live); err != nil {
		t.Fatalf("live pre-issuance bound was refused: %v", err)
	}
	// A bound that has passed is refused even though no timer has fired yet and
	// ctx.Err() is still nil: this is the delayed-timer window the gate closes.
	// A real deadline context may already have delivered its timer by the time
	// this runs, so the window is modelled exactly instead of raced for.
	if err := stitchStagePreIssuanceExpired(pastBound{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired pre-issuance bound was admitted: %v", err)
	}
	// An explicit cancellation is preserved as itself, never mistaken for a
	// timeout, so a caller can tell a real abort from a late commit.
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if err := stitchStagePreIssuanceExpired(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation was not preserved: %v", err)
	}
	// No deadline at all cannot be expired; the caller supplies the bound.
	if err := stitchStagePreIssuanceExpired(context.Background()); err != nil {
		t.Fatalf("unbounded context was refused: %v", err)
	}
}

// pastBound is a context whose deadline has already elapsed while its own
// Err() still reports nil, exactly the late-timer delivery the pre-issuance
// gate must not trust.
type pastBound struct{ context.Context }

func (pastBound) Deadline() (time.Time, bool) { return time.Now().Add(-time.Second), true }
func (pastBound) Err() error                  { return nil }
func (pastBound) Value(any) any               { return nil }
