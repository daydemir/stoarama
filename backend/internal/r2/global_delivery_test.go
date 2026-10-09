package r2

import (
	"context"
	"mime"
	"strings"
	"testing"
	"time"
)

func TestPresignGlobalDeliveryDownloadRetainsIdentityAndUnicodeName(t *testing.T) {
	c, err := New(context.Background(), Config{AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: "https://storage.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	cap, err := c.PresignGetExactDownloadRequest(context.Background(), "original/a b.mp4", "etag", "version-7", "日本_Plaza.mp4", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u := mustURL(t, cap.URL)
	if u.Query().Get("versionId") != "version-7" || !strings.Contains(u.Query().Get("X-Amz-SignedHeaders"), "if-match") || strings.Contains(u.Query().Get("X-Amz-SignedHeaders"), "range") {
		t.Fatal("generation/resume binding lost")
	}
	kind, params, err := mime.ParseMediaType(u.Query().Get("response-content-disposition"))
	if err != nil || kind != "attachment" || params["filename"] != "日本_Plaza.mp4" {
		t.Fatalf("filename binding %v %v", params, err)
	}
	if cap.Headers.Get("If-Match") != `"etag"` || cap.Headers.Get("Authorization") != "" {
		t.Fatal("unexpected access headers")
	}
	for _, name := range []string{"../file.mp4", "a\\b.mp4", "x\n.mp4"} {
		if _, err := c.PresignGetExactDownloadRequest(context.Background(), "a", "etag", "", name, time.Minute); err == nil {
			t.Fatal("unsafe name accepted")
		}
	}
}
