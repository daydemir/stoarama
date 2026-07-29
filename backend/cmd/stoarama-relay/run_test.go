package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

type failedLogWriter struct{}

func (failedLogWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

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
	go relayHeartbeatLoop(ctx, client, newProbe("missing-yt-dlp"), &atomic.Int64{}, nil, time.Now().UTC(), "", server.URL, firstSent)
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
	health := relayHealthSnapshot(t.TempDir(), "")
	if free, ok := health["disk_free_bytes"].(uint64); !ok || free == 0 {
		t.Fatalf("disk_free_bytes=%v want positive uint64", health["disk_free_bytes"])
	}
}

func TestRelayHealthSnapshotIncludesCaptureTempUsage(t *testing.T) {
	tempDir := t.TempDir()
	captureDir := filepath.Join(tempDir, "capture-continuous-test")
	if err := os.Mkdir(captureDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(captureDir, "seg.mp4"), []byte("clip"), 0o600); err != nil {
		t.Fatal(err)
	}
	health := relayHealthSnapshot(tempDir, tempDir)
	if got := health["capture_temp_bytes"]; got != uint64(4) {
		t.Fatalf("capture_temp_bytes=%v want 4", got)
	}
	if got := health["capture_temp_directories"]; got != 1 {
		t.Fatalf("capture_temp_directories=%v want 1", got)
	}
}

func TestParseNetworkCountersAggregatesNonLoopbackInterfaces(t *testing.T) {
	input := strings.NewReader(`
Inter-|   Receive                                                |  Transmit
 face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed
    lo: 100 2 3 4 0 0 0 0 100 2 3 4 0 0 0 0
  eth0: 1000 20 2 3 0 0 0 0 2000 30 4 5 0 0 0 0
 wlan0: 3000 40 6 7 0 0 0 0 4000 50 8 9 0 0 0 0
`)
	got, err := parseNetworkCounters(input)
	if err != nil {
		t.Fatal(err)
	}
	want := networkCounters{
		RXBytes: 4000, RXPackets: 60, RXErrors: 8, RXDrops: 10,
		TXBytes: 6000, TXPackets: 80, TXErrors: 12, TXDrops: 14,
	}
	if got != want {
		t.Fatalf("network counters=%+v want %+v", got, want)
	}
}

func TestParseNetworkCountersRejectsMissingInterfaces(t *testing.T) {
	if _, err := parseNetworkCounters(strings.NewReader("lo: 1 1 0 0 0 0 0 0 1 1 0 0 0 0 0 0")); err == nil {
		t.Fatal("loopback-only counters unexpectedly accepted")
	}
}

func TestParseDarwinNetworkCountersDeduplicatesInterfaceRows(t *testing.T) {
	input := strings.NewReader(`
Name Mtu Network Address Ipkts Ierrs Ibytes Opkts Oerrs Obytes Coll Drop
lo0 16384 <Link#1> 00:00:00:00:00:00 1 0 100 1 0 100 0 0
en0 1500 <Link#4> aa:bb:cc:dd:ee:ff 20 2 1000 30 4 2000 0 5
en0 1500 192.0.2 192.0.2.1 20 2 1000 30 4 2000 - -
en1 1500 <Link#5> 11:22:33:44:55:66 40 6 3000 50 8 4000 0 9
`)
	got, err := parseDarwinNetworkCounters(input)
	if err != nil {
		t.Fatal(err)
	}
	want := networkCounters{
		RXBytes: 4000, RXPackets: 60, RXErrors: 8,
		TXBytes: 6000, TXPackets: 80, TXErrors: 12, TXDrops: 14,
	}
	if got != want {
		t.Fatalf("network counters=%+v want %+v", got, want)
	}
}

func TestDNSProbeTelemetryReportsBoundedResultWithoutErrorDetails(t *testing.T) {
	started := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	got := newDNSProbeTelemetry(started, started.Add(125*time.Millisecond), errors.New("lookup stoarama.com: secret resolver detail"))
	if got.OK || got.Error != offlineDNS || got.LatencyMS != 125 || !got.CheckedAt.Equal(started.Add(125*time.Millisecond)) {
		t.Fatalf("dns telemetry=%+v", got)
	}
}

func TestDNSProbeHostUsesConfiguredAPIURL(t *testing.T) {
	tests := map[string]string{
		"https://stoarama.example/api/v1": "stoarama.example",
		"http://127.0.0.1:8080":           "127.0.0.1",
		"https://[2001:db8::1]:8443":      "2001:db8::1",
	}
	for raw, want := range tests {
		got, err := dnsProbeHost(raw)
		if err != nil {
			t.Fatalf("dnsProbeHost(%q): %v", raw, err)
		}
		if got != want {
			t.Fatalf("dnsProbeHost(%q)=%q want %q", raw, got, want)
		}
	}
	if _, err := dnsProbeHost("not-a-url"); err == nil {
		t.Fatal("hostless API URL accepted")
	}
}

func TestRelayHealthSnapshotBoundsCaptureTempScan(t *testing.T) {
	tempDir := t.TempDir()
	for i := 0; i < captureTempScanEntryLimit; i++ {
		if err := os.WriteFile(filepath.Join(tempDir, fmt.Sprintf("segment-%04d.mp4", i)), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	health := relayHealthSnapshot(tempDir, tempDir)
	if health["capture_temp_scan_truncated"] == true {
		t.Fatal("capture temp scan truncated at the exact entry limit")
	}
	if err := os.WriteFile(filepath.Join(tempDir, "segment-over-limit.mp4"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	health = relayHealthSnapshot(tempDir, tempDir)
	if health["capture_temp_scan_truncated"] != true {
		t.Fatalf("capture_temp_scan_truncated=%v want true", health["capture_temp_scan_truncated"])
	}
}

func TestDirectoryUsageOnlyIgnoresRacingDescendantRemoval(t *testing.T) {
	root := t.TempDir()
	if !removedDuringDirectoryScan(root, filepath.Join(root, "removed"), os.ErrNotExist) {
		t.Fatal("descendant removal was not tolerated")
	}
	if removedDuringDirectoryScan(root, root, os.ErrNotExist) {
		t.Fatal("missing scan root was suppressed")
	}
	if removedDuringDirectoryScan(root, filepath.Join(root, "denied"), os.ErrPermission) {
		t.Fatal("non-removal scan error was suppressed")
	}
	missing := filepath.Join(root, "missing")
	if _, _, _, err := directoryUsage(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing root error=%v", err)
	}
}

func TestCleanupRelayCaptureTempOnlyRemovesOwnedDirectories(t *testing.T) {
	root := t.TempDir()
	continuous := filepath.Join(root, "capture-continuous-first")
	anotherContinuous := filepath.Join(root, "capture-continuous-second")
	segment := filepath.Join(root, "capture-segment-first")
	unowned := filepath.Join(root, "other-directory")
	for _, path := range []string{continuous, anotherContinuous, segment, unowned} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := cleanupRelayCaptureTemp(root)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 3 {
		t.Fatalf("removed=%d want 3", removed)
	}
	if _, err := os.Stat(continuous); !os.IsNotExist(err) {
		t.Fatalf("relay capture directory remains: %v", err)
	}
	if _, err := os.Stat(segment); !os.IsNotExist(err) {
		t.Fatalf("relay segment directory remains: %v", err)
	}
	if _, err := os.Stat(anotherContinuous); !os.IsNotExist(err) {
		t.Fatalf("second relay capture directory remains: %v", err)
	}
	for _, path := range []string{unowned} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("preserved directory %s: %v", path, err)
		}
	}
}

func TestCleanupRelayCaptureTempContinuesAfterRemovalError(t *testing.T) {
	root := t.TempDir()
	failed := filepath.Join(root, "capture-continuous-first")
	removedPath := filepath.Join(root, "capture-segment-second")
	for _, path := range []string{failed, removedPath} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	denied := errors.New("remove denied")
	removed, err := cleanupRelayCaptureTempWith(root, func(path string) error {
		if path == failed {
			return denied
		}
		return os.RemoveAll(path)
	})
	if removed != 1 || !errors.Is(err, denied) {
		t.Fatalf("removed=%d err=%v", removed, err)
	}
	if _, statErr := os.Stat(removedPath); !os.IsNotExist(statErr) {
		t.Fatalf("later capture directory was not swept: %v", statErr)
	}
	if _, statErr := os.Stat(failed); statErr != nil {
		t.Fatalf("failed capture directory should remain for startup failure: %v", statErr)
	}
}

func TestBoundedRelayLogRetainsTailAtLimit(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	writer, err := openBoundedRelayLog()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if err := writer.file.Truncate(relayLogMaxBytes - 1); err != nil {
		t.Fatal(err)
	}
	previous := []byte("previous diagnostic")
	setup, err := os.OpenFile(writer.file.Name(), os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := setup.WriteAt(previous, relayLogMaxBytes-1-int64(len(previous))); err != nil {
		setup.Close()
		t.Fatal(err)
	}
	if err := setup.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	info, err := writer.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != relayLogTailBytes+3 {
		t.Fatalf("size=%d want %d", info.Size(), relayLogTailBytes+3)
	}
	content, err := os.ReadFile(writer.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(content), "new") {
		t.Fatalf("rolled log does not end with new entry")
	}
	if !strings.Contains(string(content), string(previous)) {
		t.Fatalf("rolled log discarded previous diagnostic tail")
	}
}

func TestDualRelayLogWriterAttemptsBothDestinations(t *testing.T) {
	var primary bytes.Buffer
	var recovery bytes.Buffer
	writer := dualRelayLogWriter{primary: &primary, recovery: &recovery}
	if _, err := writer.Write([]byte("entry")); err != nil {
		t.Fatal(err)
	}
	if primary.String() != "entry" || recovery.String() != "entry" {
		t.Fatalf("primary=%q recovery=%q", primary.String(), recovery.String())
	}
	recovery.Reset()
	writer = dualRelayLogWriter{primary: failedLogWriter{}, recovery: &recovery}
	if _, err := writer.Write([]byte("recovery")); err == nil {
		t.Fatal("primary failure was not reported")
	}
	if recovery.String() != "recovery" {
		t.Fatalf("primary failure blocked recovery log: %q", recovery.String())
	}
	primary.Reset()
	writer = dualRelayLogWriter{primary: &primary, recovery: failedLogWriter{}}
	if _, err := writer.Write([]byte("journal")); err == nil {
		t.Fatal("recovery failure was not reported")
	}
	if primary.String() != "journal" {
		t.Fatalf("recovery failure blocked primary log: %q", primary.String())
	}
}

func TestRelayLogOutputAvoidsTeeToSameFile(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	recovery, err := openBoundedRelayLog()
	if err != nil {
		t.Fatal(err)
	}
	defer recovery.Close()
	primary, err := os.OpenFile(recovery.file.Name(), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	output := relayLogOutput(primary, recovery)
	if output != io.Writer(recovery) {
		t.Fatalf("same-file log output=%T want bounded recovery writer", output)
	}
	if _, err := output.Write([]byte("single entry")); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(recovery.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "single entry" {
		t.Fatalf("same-file output duplicated: %q", content)
	}
}

func TestRelayLogOutputTeesDistinctFiles(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	recovery, err := openBoundedRelayLog()
	if err != nil {
		t.Fatal(err)
	}
	defer recovery.Close()
	primaryPath := filepath.Join(t.TempDir(), "journal.log")
	primary, err := os.OpenFile(primaryPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	output := relayLogOutput(primary, recovery)
	if _, ok := output.(dualRelayLogWriter); !ok {
		t.Fatalf("distinct-file log output=%T want dual writer", output)
	}
}

func TestRetryableNodeHeartbeatErrorOnlyRetriesTransientNetworkErrors(t *testing.T) {
	if !retryableNodeHeartbeatError(&net.DNSError{Err: "temporary", IsTemporary: true}) {
		t.Fatal("temporary DNS error was not retryable")
	}
	if retryableNodeHeartbeatError(&net.DNSError{Err: "not found", IsNotFound: true}) {
		t.Fatal("permanent DNS error was retryable")
	}
}

func TestCleanupLegacyCaptureTempRequiresAgeAndExactPrefix(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	stale := filepath.Join(root, "capture-continuous-stale")
	fresh := filepath.Join(root, "capture-segment-fresh")
	unownedName := filepath.Join(root, "other-stale")
	for _, path := range []string{stale, fresh, unownedName} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	old := now.Add(-time.Hour)
	for _, path := range []string{stale, unownedName} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := cleanupLegacyCaptureTemp(root, now, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed=%d want 1", removed)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale legacy capture remains: %v", err)
	}
	for _, path := range []string{fresh, unownedName} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("preserved directory %s: %v", path, err)
		}
	}
}

func TestCleanupLegacyCaptureTempBestEffortIgnoresUnavailableRoot(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if removed := cleanupLegacyCaptureTempBestEffort(missing, time.Now(), 15*time.Minute); removed != 0 {
		t.Fatalf("removed=%d want 0", removed)
	}
}

func TestAcquireRelayRunLockPreservesContentionCause(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	first, err := acquireRelayRunLock()
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := acquireRelayRunLock()
	if second != nil {
		second.Close()
		t.Fatal("second relay process lock unexpectedly succeeded")
	}
	if !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("lock error did not preserve contention cause: %v", err)
	}
}

func TestSystemdServiceUsesBoundedRestartRateWithoutLockout(t *testing.T) {
	template, err := templatesFS.ReadFile("templates/systemd.service.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	service := string(template)
	for _, setting := range []string{"Restart=always", "RestartSec=30s", "StartLimitIntervalSec=0", "ExecStopPost=", "record-exit", "SERVICE_RESULT", "EXIT_CODE", "EXIT_STATUS"} {
		if !strings.Contains(service, setting) {
			t.Fatalf("systemd service missing %q", setting)
		}
	}
}

func TestWriteSystemdUnitOnlyReportsContentChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), systemdUnit)
	current := []byte("current\n")
	if err := os.WriteFile(path, current, 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err := writeSystemdUnitIfChanged(path, current, append([]byte(nil), current...))
	if err != nil || changed {
		t.Fatalf("unchanged unit result=(%t,%v)", changed, err)
	}
	updated := []byte("updated\n")
	changed, err = writeSystemdUnitIfChanged(path, current, updated)
	if err != nil || !changed {
		t.Fatalf("updated unit result=(%t,%v)", changed, err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, updated) {
		t.Fatalf("unit=%q want %q", got, updated)
	}
}

func TestRelayDiskCapacityBlockIsVisibleAndRecovers(t *testing.T) {
	if got := relayCapacityBlock(relayMinLeaseFreeBytes - 1); got != "disk_pressure" {
		t.Fatalf("capacity block=%q", got)
	}
	if got := relayCapacityBlock(relayMinLeaseFreeBytes); got != "" {
		t.Fatalf("capacity remained blocked at safe free space: %q", got)
	}
}

func TestRelayLogTailIsBoundedAndSanitized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.log")
	lines := make([]string, 0, 13)
	for i := 0; i < 9; i++ {
		lines = append(lines, fmt.Sprintf("recording worker job=%d continuous window complete", i))
	}
	lines = append(lines,
		"recording worker job=10 continuous source dropped: https://example.com/live.m3u8?token=url-secret --password command-secret Authorization: Bearer bearer-secret api_key=field-secret",
		"arbitrary command --token unknown-secret",
	)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	tail := relayLogTail(path)
	if len(tail) != 8 {
		t.Fatalf("tail lines=%d want 8", len(tail))
	}
	joined := strings.Join(tail, "\n")
	for _, secret := range []string{"url-secret", "command-secret", "bearer-secret", "field-secret", "unknown-secret"} {
		if strings.Contains(joined, secret) {
			t.Fatalf("tail leaked %q: %q", secret, tail)
		}
	}
	if !strings.Contains(joined, "recording_worker.source_dropped") {
		t.Fatalf("tail omitted useful error category: %q", tail)
	}
}

func TestRelayLogTailUsesBoundedSuffix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.log")
	prefix := "relay heartbeat error\n" + strings.Repeat("x", 128<<10) + "\n"
	suffix := "recording worker job=10 continuous source dropped\n"
	if err := os.WriteFile(path, []byte(prefix+suffix), 0o600); err != nil {
		t.Fatal(err)
	}
	tail := relayLogTail(path)
	joined := strings.Join(tail, "\n")
	if strings.Contains(joined, "relay.heartbeat_error") {
		t.Fatalf("tail included category outside bounded suffix: %q", tail)
	}
	if !strings.Contains(joined, "recording_worker.source_dropped") {
		t.Fatalf("tail omitted suffix category: %q", tail)
	}
}

func TestSendNodeHeartbeatRetriesTransientFailure(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) < 3 {
			http.Error(w, "temporary", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := sendNodeHeartbeat(context.Background(), client, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts=%d want 3", got)
	}
}

func TestSendNodeHeartbeatDoesNotRetryAuthenticationFailure(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := sendNodeHeartbeat(context.Background(), client, map[string]any{}); err == nil {
		t.Fatal("authentication failure accepted")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts=%d want 1", got)
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

func TestClassifyPreviousRelayExit(t *testing.T) {
	started := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		previous    relayRecoveryState
		currentBoot string
		wantPending bool
		wantExit    string
	}{
		{name: "first run", previous: relayRecoveryState{}, wantPending: false},
		{name: "clean stop", previous: relayRecoveryState{StartedAt: started, PreviousExit: relayExitClean}, wantPending: false},
		{name: "self update", previous: relayRecoveryState{StartedAt: started, PreviousExit: relayExitSelfUpdate}, wantPending: false},
		{name: "legacy exit without start", previous: relayRecoveryState{PreviousExit: "systemd_signal"}, wantPending: true, wantExit: "unknown"},
		{name: "missing exit marker", previous: relayRecoveryState{StartedAt: started}, wantPending: true, wantExit: relayExitUncleanProcess},
		{name: "host reboot", previous: relayRecoveryState{StartedAt: started, PreviousExit: relayExitUncleanProcess, BootID: "old"}, currentBoot: "new", wantPending: true, wantExit: "unclean_reboot"},
		{name: "process restart", previous: relayRecoveryState{StartedAt: started, PreviousExit: relayExitUncleanProcess, BootID: "same"}, currentBoot: "same", wantPending: true, wantExit: relayExitUncleanProcess},
		{name: "systemd fact", previous: relayRecoveryState{StartedAt: started, PreviousExit: "systemd_signal_killed_KILL"}, wantPending: true, wantExit: "systemd_signal_killed_KILL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pending, exit := classifyPreviousRelayExit(tc.previous, tc.currentBoot)
			if pending != tc.wantPending || exit != tc.wantExit {
				t.Fatalf("classify=(%v,%q) want (%v,%q)", pending, exit, tc.wantPending, tc.wantExit)
			}
		})
	}
}

func TestRecordSystemdExitPersistsBoundedFactsAndPreservesSelfUpdate(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := recordSystemdExit([]string{
		"--service-result=signal",
		"--exit-code=killed",
		"--exit-status=KILL",
		"--ignored=secret",
	}); err != nil {
		t.Fatal(err)
	}
	state, err := loadRecoveryState(recoveryStatePath())
	if err != nil {
		t.Fatal(err)
	}
	if state.PreviousExit != "systemd_signal_killed_KILL" {
		t.Fatalf("previous_exit=%q", state.PreviousExit)
	}
	if err := recordSystemdExit([]string{"--service-result=success", "--exit-code=exited", "--exit-status=0"}); err != nil {
		t.Fatal(err)
	}
	state, err = loadRecoveryState(recoveryStatePath())
	if err != nil {
		t.Fatal(err)
	}
	if state.PreviousExit != relayExitClean {
		t.Fatalf("successful systemd stop persisted %q", state.PreviousExit)
	}
	state.PreviousExit = relayExitSelfUpdate
	if err := state.persist(recoveryStatePath()); err != nil {
		t.Fatal(err)
	}
	if err := recordSystemdExit([]string{"--service-result=success", "--exit-code=exited", "--exit-status=0"}); err != nil {
		t.Fatal(err)
	}
	state, err = loadRecoveryState(recoveryStatePath())
	if err != nil {
		t.Fatal(err)
	}
	if state.PreviousExit != relayExitSelfUpdate {
		t.Fatalf("systemd helper overwrote self-update exit with %q", state.PreviousExit)
	}
}

func TestRecordSystemdExitIgnoresUnsafeFacts(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := recordSystemdExit([]string{"--service-result=signal;token=secret"}); err != nil {
		t.Fatal(err)
	}
	state, err := loadRecoveryState(recoveryStatePath())
	if err != nil {
		t.Fatal(err)
	}
	if state.PreviousExit != "" {
		t.Fatalf("unsafe exit fact persisted: %q", state.PreviousExit)
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
