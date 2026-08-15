package apihttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPostJSONSetsAuthAndDecodes(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Fatalf("auth=%q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	c, err := New(srv.URL, "tok", srv.Client(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		OK bool `json:"ok"`
	}
	if err := c.PostJSON(context.Background(), "/x", map[string]any{}, &out); err != nil {
		t.Fatal(err)
	}
	if !out.OK {
		t.Fatalf("out=%+v", out)
	}
}

func TestPostRawReturnsConflictBody(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "cancel", http.StatusConflict)
	}))
	defer srv.Close()
	c, err := New(srv.URL, "tok", srv.Client(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	status, body, err := c.PostRaw(context.Background(), "/x", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusConflict || !strings.Contains(string(body), "cancel") {
		t.Fatalf("status=%d body=%q", status, body)
	}
}

func TestPutFileReportsErrorBody(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusBadGateway)
	}))
	defer srv.Close()
	tmp, err := os.CreateTemp(t.TempDir(), "upload-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.WriteString("data"); err != nil {
		t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatal(err)
	}
	c, err := New("https://api.example.test", "tok", srv.Client(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	err = c.PutFile(context.Background(), srv.URL, tmp.Name(), "text/plain")
	if err == nil || !strings.Contains(err.Error(), "status=502") || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("err=%v", err)
	}
}

func TestSetTokenIsAtomicForFutureRequests(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.Header.Get("Authorization")]++
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c, err := New(srv.URL, "old", srv.Client(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.PostJSON(context.Background(), "/before", map[string]any{}, nil); err != nil {
		t.Fatal(err)
	}
	if err = c.SetToken("successor"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for index := 0; index < 20; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if requestErr := c.PostJSON(context.Background(), "/after", map[string]any{}, nil); requestErr != nil {
				t.Errorf("request after rotation: %v", requestErr)
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if seen["Bearer old"] != 1 || seen["Bearer successor"] != 20 {
		t.Fatalf("seen=%v", seen)
	}
}
