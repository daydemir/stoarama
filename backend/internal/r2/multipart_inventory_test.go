package r2

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRequireNoMultipartUploadsUsesLiveBucketInventory(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		allowed    bool
	}{
		{"empty", `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`, 200, true},
		{"unfinished completion", `<ListMultipartUploadsResult><Upload><Key>managed/old.mp4</Key><UploadId>upload</UploadId></Upload><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`, 200, false},
		{"truncated", `<ListMultipartUploadsResult><IsTruncated>true</IsTruncated></ListMultipartUploadsResult>`, 200, false},
		{"denied", `<Error><Code>AccessDenied</Code></Error>`, 403, false},
		{"unsupported", `<Error><Code>NotImplemented</Code></Error>`, 501, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "GET" || r.URL.Path != "/bucket" || !r.URL.Query().Has("uploads") || r.URL.Query().Get("max-uploads") != "1" {
					t.Error("not ListMultipartUploads")
				}
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			c, err := New(context.Background(), Config{UngatedWriter: true, Bucket: "bucket", Endpoint: server.URL, AccessKey: "fake", SecretKey: "fake"})
			if err != nil {
				t.Fatal(err)
			}
			err = c.RequireNoMultipartUploads(context.Background())
			if (err == nil) != tc.allowed || calls != 1 {
				t.Fatal("inventory decision/calls", err, calls)
			}
		})
	}
}

func TestUnboundR2ClientCannotWrite(t *testing.T) {
	c, err := New(context.Background(), Config{Bucket: "bucket", Endpoint: "https://storage.example.test", AccessKey: "fake", SecretKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.PutBytes(context.Background(), "managed/x", "", []byte("x")); err == nil {
		t.Fatal("unbound PUT bypassed admission")
	}
	if _, err = c.PresignPut(context.Background(), "managed/x", "", time.Minute); err == nil {
		t.Fatal("unbound presign bypassed admission")
	}
}
