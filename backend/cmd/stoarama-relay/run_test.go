package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

func TestExecutable(t *testing.T) {
	path := t.TempDir() + "/ffmpeg"
	if err := os.WriteFile(path, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	if executable(path) {
		t.Fatal("non-executable file accepted")
	}
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	if !executable(path) {
		t.Fatal("executable file rejected")
	}
}

func TestHeartbeatDoesNotWaitForExternalProbe(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	received := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/node/heartbeat" {
			var request struct {
				Capabilities map[string]any `json:"capabilities_json"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			received <- request.Capabilities
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	client, err := recordingapi.NewClient(recordingapi.ClientConfig{
		BaseURL:   server.URL,
		NodeToken: "test-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstSent := make(chan struct{})
	go relayHeartbeatLoop(ctx, client, newProbe("missing-yt-dlp"), &atomic.Int64{}, nil, time.Now().UTC(), firstSent)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()

	select {
	case capabilities := <-received:
		if _, ok := capabilities["youtube_ready"]; ok {
			t.Fatal("unprobed YouTube readiness was reported")
		}
		if _, ok := capabilities["youtube_error"]; ok {
			t.Fatal("unprobed YouTube error was reported")
		}
		if _, ok := capabilities["ytdlp_version"]; ok {
			t.Fatal("unread yt-dlp version was reported")
		}
		select {
		case <-firstSent:
		case <-deadline.C:
			t.Fatal("first heartbeat completion was not signaled")
		}
	case <-deadline.C:
		t.Fatal("first heartbeat waited for an external probe")
	}
}

func TestHeartbeatDiagnosticsReportsTypedRecoveryOnce(t *testing.T) {
	d := &heartbeatDiagnostics{}
	for i := 0; i < 3; i++ {
		d.Failed(errors.New("lookup api.stoarama.com on resolver: i/o timeout"))
	}
	events, ok := d.Snapshot()
	if !ok || len(events) != 1 {
		t.Fatalf("snapshot=(%v,%t) want one event", events, ok)
	}
	if events[0].ErrorClass != offlineDNS || events[0].FailureCount != 3 {
		t.Fatalf("event=%+v want dns failure count 3", events[0])
	}
	d.Sent()
	if _, ok := d.Snapshot(); ok {
		t.Fatal("unchanged diagnostics resent")
	}

	if err := d.Succeeded(); err != nil {
		t.Fatal(err)
	}
	events, ok = d.Snapshot()
	if !ok || len(events) != 1 || events[0].RecoveredAt == nil {
		t.Fatalf("recovery events=%+v ok=%t", events, ok)
	}
}

func TestHeartbeatDiagnosticsBoundsOutages(t *testing.T) {
	d := &heartbeatDiagnostics{}
	for i := 0; i < offlineDiagnosticLimit+2; i++ {
		if err := d.Failed(errors.New("request timeout")); err != nil {
			t.Fatal(err)
		}
		if err := d.Succeeded(); err != nil {
			t.Fatal(err)
		}
	}
	events, ok := d.Snapshot()
	if !ok || len(events) != offlineDiagnosticLimit {
		t.Fatalf("events=%d ok=%t want %d", len(events), ok, offlineDiagnosticLimit)
	}
}

func TestHeartbeatDiagnosticsPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offline-diagnostics.json")
	d := &heartbeatDiagnostics{path: path}
	if err := d.Failed(errors.New("lookup api.stoarama.com: i/o timeout")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%o want 600", info.Mode().Perm())
	}

	loaded, err := loadHeartbeatDiagnostics(path)
	if err != nil {
		t.Fatal(err)
	}
	events, ok := loaded.Snapshot()
	if !ok || len(events) != 1 || events[0].RecoveredAt != nil {
		t.Fatalf("loaded events=%+v ok=%t", events, ok)
	}
	if err := loaded.Succeeded(); err != nil {
		t.Fatal(err)
	}
	reloaded, err := loadHeartbeatDiagnostics(path)
	if err != nil {
		t.Fatal(err)
	}
	events, ok = reloaded.Snapshot()
	if !ok || len(events) != 1 || events[0].RecoveredAt == nil {
		t.Fatalf("reloaded events=%+v ok=%t", events, ok)
	}
}

func TestHeartbeatDiagnosticsRejectsOversizedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offline-diagnostics.json")
	if err := os.WriteFile(path, make([]byte, offlineDiagnosticMaxBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadHeartbeatDiagnostics(path); err == nil {
		t.Fatal("oversized diagnostics state accepted")
	}
}

func TestRelayHealthSnapshotUsesDataDirectory(t *testing.T) {
	health := relayHealthSnapshot(t.TempDir())
	if free, ok := health["disk_free_bytes"].(uint64); !ok || free == 0 {
		t.Fatalf("disk_free_bytes=%v want positive uint64", health["disk_free_bytes"])
	}
}

func TestAppendDiagnosticErrorsMergesSanitizesAndBounds(t *testing.T) {
	existing := []string{"capture old"}
	incoming := []string{
		"capture old",
		"fetch https://example.com/path?token=secret failed",
		"capture 2", "capture 3", "capture 4", "capture 5",
		"capture 6", "capture 7", "capture 8", "capture 9",
	}
	got := appendDiagnosticErrors(existing, incoming)
	if len(got) != 8 {
		t.Fatalf("len=%d want 8: %v", len(got), got)
	}
	for _, value := range got {
		if strings.Contains(value, "secret") {
			t.Fatalf("unsanitized diagnostic: %q", value)
		}
	}
	if got[len(got)-1] != "capture 9" {
		t.Fatalf("newest=%q want capture 9", got[len(got)-1])
	}
}

func TestMarkRelayExitPersistsSelfUpdate(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Cleanup(func() { plannedSelfUpdate.Store(false) })
	markRelayExit(relayExitSelfUpdate)
	state, err := loadRecoveryState(recoveryStatePath())
	if err != nil {
		t.Fatal(err)
	}
	if state.PreviousExit != relayExitSelfUpdate {
		t.Fatalf("previous_exit=%q want %q", state.PreviousExit, relayExitSelfUpdate)
	}
	if err := (&relayRecoveryState{PreviousExit: relayExitUncleanProcess}).persist(recoveryStatePath()); err != nil {
		t.Fatal(err)
	}
	state, err = loadRecoveryState(recoveryStatePath())
	if err != nil {
		t.Fatal(err)
	}
	if state.PreviousExit != relayExitSelfUpdate {
		t.Fatalf("stale write changed previous_exit to %q", state.PreviousExit)
	}
}

func TestRecoveryStateConcurrentWritesRemainAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay-recovery.json")
	var writes sync.WaitGroup
	for i := 0; i < 20; i++ {
		writes.Add(1)
		go func(exit string) {
			defer writes.Done()
			if err := (&relayRecoveryState{PreviousExit: exit}).persist(path); err != nil {
				t.Errorf("persist: %v", err)
			}
		}(fmt.Sprintf("exit-%d", i))
	}
	writes.Wait()
	state, err := loadRecoveryState(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(state.PreviousExit, "exit-") {
		t.Fatalf("previous_exit=%q", state.PreviousExit)
	}
	leftovers, err := filepath.Glob(path + ".new-*")
	if err != nil {
		t.Fatal(err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("temporary files remain: %v", leftovers)
	}
}

func TestHeartbeatDiagnosticsClampsBackwardClock(t *testing.T) {
	future := time.Now().UTC().Add(time.Hour)
	d := &heartbeatDiagnostics{current: &offlineDiagnostic{
		Kind:         heartbeatOutage,
		ErrorClass:   offlineDNS,
		StartedAt:    future,
		LastFailedAt: future,
		FailureCount: 1,
	}}
	if err := d.Failed(errors.New("request timeout")); err != nil {
		t.Fatal(err)
	}
	if d.current.LastFailedAt.Before(future) {
		t.Fatal("failure time moved backward")
	}
	if err := d.Succeeded(); err != nil {
		t.Fatal(err)
	}
	if d.recent[0].RecoveredAt.Before(future) {
		t.Fatal("recovery time moved backward")
	}
}

func TestClassifyOfflineError(t *testing.T) {
	tests := map[string]offlineErrorClass{
		"lookup api.stoarama.com: i/o timeout": offlineDNS,
		"context deadline exceeded":            offlineTimeout,
		"dial tcp: connection refused":         offlineConnection,
		"request status=503":                   offlineHTTP,
		"unexpected failure":                   offlineOther,
	}
	for message, want := range tests {
		if got := classifyOfflineError(errors.New(message)); got != want {
			t.Fatalf("classifyOfflineError(%q)=%q want %q", message, got, want)
		}
	}
}
