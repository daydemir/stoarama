package main

import (
	"context"
	"errors"
	"github.com/daydemir/stoarama/backend/internal/collation"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestOutputCustody(t *testing.T) {
	root := t.TempDir()
	for _, key := range []string{"../x", "joined/best14-20261021/../../x", "joined/other/objects/x.mp4", "/joined/best14-20261021/x", "joined/best14-20261021//x"} {
		if _, err := safeDestination(root, key); err == nil {
			t.Fatalf("unsafe destination accepted: %s", key)
		}
	}
	if p, err := safeDestination(root, "joined/best14-20261021/objects/a.mp4"); err != nil || !strings.HasPrefix(p, root+string(filepath.Separator)) {
		t.Fatal("valid job output rejected")
	}
}
func TestSourceCapabilitiesDoNotLeakAndEnforceSize(t *testing.T) {
	secret := "NEVER_LOG_THIS"
	clip := collation.Clip{ClipID: 1, SizeBytes: 3}
	st := store{caps: map[string]capability{"1": {URL: "https://x.r2.cloudflarestorage.com/b/key?signature=" + secret}}, client: &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) { return nil, errors.New(secret) })}}
	_, _, err := st.Download(context.Background(), clip, filepath.Join(t.TempDir(), "x"))
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatal("network error leaked capability")
	}
	st.client = &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, ContentLength: 2, Body: io.NopCloser(strings.NewReader("ab"))}, nil
	})}
	if _, _, err := st.Download(context.Background(), clip, filepath.Join(t.TempDir(), "x")); err == nil {
		t.Fatal("short object accepted")
	}
}
