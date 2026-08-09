package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func fakeLaunchctl(t *testing.T, script string) string {
	t.Helper()
	home := t.TempDir()
	bin := t.TempDir()
	logPath := filepath.Join(bin, "calls")
	if err := os.WriteFile(filepath.Join(bin, "launchctl"), []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("LAUNCHCTL_TEST_LOG", logPath)
	return logPath
}

func TestRemoveLaunchdLabelWaitsForAsynchronousAbsence(t *testing.T) {
	logPath := fakeLaunchctl(t, `
echo "$*" >> "$LAUNCHCTL_TEST_LOG"
if [ "$1" = bootout ]; then exit 0; fi
if [ "$1" = print ] && [ ! -f "$HOME/first-print" ]; then touch "$HOME/first-print"; exit 0; fi
if [ "$1" = print ]; then exit 113; fi
exit 0
`)
	if err := removeLaunchdLabel("gui/501", launchdLabel); err != nil {
		t.Fatal(err)
	}
	calls, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(calls), "print gui/501/"+launchdLabel); got != 2 {
		t.Fatalf("print calls=%d want 2:\n%s", got, calls)
	}
}

func TestLaunchdProbeRejectsUnexpectedFailure(t *testing.T) {
	fakeLaunchctl(t, `if [ "$1" = print ]; then exit 77; fi; exit 0`)
	if _, err := loadedLaunchdDomains(501); err == nil {
		t.Fatal("unexpected launchctl failure was misclassified as service absence")
	}
}

func TestUninstallLaunchdKeepsPlistOnUnexpectedProbeFailure(t *testing.T) {
	fakeLaunchctl(t, `if [ "$1" = print ]; then exit 77; fi; exit 0`)
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	plistPath := filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist")
	if err := os.MkdirAll(filepath.Dir(plistPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plistPath, []byte("prior"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := uninstallLaunchd(501, home); err == nil || !strings.Contains(err.Error(), "probe launchd agent before uninstall") {
		t.Fatalf("uninstall error=%v", err)
	}
	if got, err := os.ReadFile(plistPath); err != nil || string(got) != "prior" {
		t.Fatalf("plist changed after probe failure: %q err=%v", got, err)
	}
}

func TestRemoveLaunchdLabelAcceptsAlreadyAbsentBootout(t *testing.T) {
	fakeLaunchctl(t, `if [ "$1" = bootout ]; then exit 113; fi; exit 1`)
	if err := removeLaunchdLabel("gui/501", launchdLabel); err != nil {
		t.Fatal(err)
	}
}

func TestRemovalTimeoutRestartsAndVerifiesPriorRelay(t *testing.T) {
	oldTimeout := launchdRemovalTimeout
	launchdRemovalTimeout = 100 * time.Millisecond
	t.Cleanup(func() { launchdRemovalTimeout = oldTimeout })
	fakeLaunchctl(t, `
if [ "$1" = bootout ]; then exit 0; fi
if [ "$1" = print ]; then echo 'state = running'; exit 0; fi
if [ "$1" = kickstart ]; then
  mkdir -p "$HOME/.stoarama"
  echo '{"service_instance_id":"prior","heartbeat_success_count":2,"started_at":"2099-01-01T00:00:00Z"}' > "$HOME/.stoarama/relay-recovery.json"
  exit 0
fi
exit 1
`)
	prior := []byte(`<key>STOARAMA_SERVICE_INSTANCE_ID</key><string>prior</string>`)
	err := removeLaunchdLabel("gui/501", launchdLabel)
	if err == nil {
		t.Fatal("persistent loaded job accepted as absent")
	}
	err = recoverPriorAfterRemovalFailure("gui/501", filepath.Join(t.TempDir(), "relay.plist"), prior, 0o644, err)
	if err == nil || !strings.Contains(err.Error(), "restarted and verified") {
		t.Fatalf("recovery error=%v", err)
	}
}

func TestRemovalRecoveryRequiresTwoHeartbeatsAfterKickstart(t *testing.T) {
	oldTimeout := launchdReadinessTimeout
	launchdReadinessTimeout = 100 * time.Millisecond
	t.Cleanup(func() { launchdReadinessTimeout = oldTimeout })
	fakeLaunchctl(t, `
if [ "$1" = print ]; then echo 'state = running'; exit 0; fi
if [ "$1" = kickstart ]; then
  echo '{"service_instance_id":"prior","heartbeat_success_count":2,"started_at":"2099-01-01T00:00:00Z"}' > "$HOME/.stoarama/relay-recovery.json"
  exit 0
fi
exit 1
`)
	if err := os.MkdirAll(filepath.Join(os.Getenv("HOME"), ".stoarama"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(recoveryStatePath(), []byte(`{"service_instance_id":"prior","heartbeat_success_count":1,"started_at":"2099-01-01T00:00:00Z"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	prior := []byte(`<key>STOARAMA_SERVICE_INSTANCE_ID</key><string>prior</string>`)
	err := recoverPriorAfterRemovalFailure("gui/501", filepath.Join(t.TempDir(), "relay.plist"), prior, 0o644, fmt.Errorf("removal unconfirmed"))
	if err == nil || !strings.Contains(err.Error(), "did not produce two verified heartbeats") {
		t.Fatalf("recovery error=%v", err)
	}
}

func TestRemovalRecoveryBootstrapsPriorWhenJobVanishesBeforeKickstart(t *testing.T) {
	fakeLaunchctl(t, `
if [ "$1" = print ]; then
  if [ -f "$HOME/bootstrap-complete" ]; then echo 'state = running'; exit 0; fi
  if [ -f "$HOME/kickstart-raced" ]; then exit 113; fi
  echo 'state = running'; exit 0
fi
if [ "$1" = kickstart ] && [ ! -f "$HOME/bootstrap-complete" ]; then touch "$HOME/kickstart-raced"; exit 7; fi
if [ "$1" = bootstrap ]; then touch "$HOME/bootstrap-complete"; exit 0; fi
if [ "$1" = kickstart ]; then
  mkdir -p "$HOME/.stoarama"
  echo '{"service_instance_id":"prior","heartbeat_success_count":2,"started_at":"2099-01-01T00:00:00Z"}' > "$HOME/.stoarama/relay-recovery.json"
  exit 0
fi
exit 1
`)
	path := filepath.Join(t.TempDir(), "relay.plist")
	prior := []byte(`<key>STOARAMA_SERVICE_INSTANCE_ID</key><string>prior</string>`)
	err := recoverPriorAfterRemovalFailure("gui/501", path, prior, 0o644, fmt.Errorf("removal unconfirmed"))
	if err == nil || !strings.Contains(err.Error(), "prior service was restored and verified") {
		t.Fatalf("recovery error=%v", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || string(got) != string(prior) {
		t.Fatalf("prior plist=%q err=%v", got, readErr)
	}
}

func TestInstallLaunchdReplacesOwnedLoadedService(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("launchd install is macOS-only")
	}
	logPath := fakeLaunchctl(t, `
echo "$*" >> "$LAUNCHCTL_TEST_LOG"
marker="$HOME/loaded"
if [ "$1" = print ]; then case "$2" in gui/*) [ -f "$marker" ] && echo 'state = running' && exit 0;; esac; exit 113; fi
if [ "$1" = bootout ]; then rm -f "$marker"; exit 0; fi
if [ "$1" = bootstrap ]; then touch "$marker"; exit 0; fi
if [ "$1" = kickstart ]; then
  id=$(sed -n 's/.*STOARAMA_SERVICE_INSTANCE_ID<\/key><string>\([^<]*\).*/\1/p' "$HOME/Library/LaunchAgents/com.stoarama.relay.plist")
  mkdir -p "$HOME/.stoarama"
  echo "{\"service_instance_id\":\"$id\",\"heartbeat_success_count\":2,\"started_at\":\"2099-01-01T00:00:00Z\",\"last_heartbeat_at\":\"2099-01-01T00:01:00Z\"}" > "$HOME/.stoarama/relay-recovery.json"
  exit 0
fi
exit 0
`)
	_, prior := writeTestLaunchdPlist(t, "")
	_ = prior
	home, _ := os.UserHomeDir()
	if err := os.WriteFile(filepath.Join(home, "loaded"), []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := installLaunchd(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist"))
	if err != nil {
		t.Fatal(err)
	}
	if serviceInstanceIDFromPlist(got) == "" {
		t.Fatal("replacement did not install identity contract")
	}
	calls, _ := os.ReadFile(logPath)
	if !strings.Contains(string(calls), "bootout gui/") || !strings.Contains(string(calls), "bootstrap gui/") {
		t.Fatalf("replacement handoff missing:\n%s", calls)
	}
}

func TestInstallLaunchdRestartsPriorServiceAfterCandidateFailure(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("launchd install is macOS-only")
	}
	fakeLaunchctl(t, `
marker="$HOME/loaded"
if [ "$1" = print ]; then case "$2" in gui/*) [ -f "$marker" ] && echo 'state = running' && exit 0;; esac; exit 113; fi
if [ "$1" = bootout ]; then rm -f "$marker"; exit 0; fi
if [ "$1" = bootstrap ]; then touch "$marker"; exit 0; fi
if [ "$1" = kickstart ]; then
  plist="$HOME/Library/LaunchAgents/com.stoarama.relay.plist"
  if grep -q STOARAMA_SERVICE_INSTANCE_ID "$plist"; then exit 9; fi
  mkdir -p "$HOME/.stoarama"
  echo '{"heartbeat_success_count":2,"started_at":"2099-01-01T00:00:00Z","last_heartbeat_at":"2099-01-01T00:01:00Z"}' > "$HOME/.stoarama/relay-recovery.json"
  exit 0
fi
exit 0
`)
	path, prior := writeTestLaunchdPlist(t, "")
	home, _ := os.UserHomeDir()
	if err := os.WriteFile(filepath.Join(home, "loaded"), []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := installLaunchd()
	if err == nil || !strings.Contains(err.Error(), "prior service was restored and verified") {
		t.Fatalf("error=%v, want verified rollback", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || string(got) != string(prior) {
		t.Fatalf("prior plist not restored: %v", readErr)
	}
	if _, statErr := os.Stat(filepath.Join(home, "loaded")); statErr != nil {
		t.Fatalf("prior job not reloaded: %v", statErr)
	}
}

func writeTestLaunchdPlist(t *testing.T, instanceID string) (string, []byte) {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	bd, err := binDir()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	b, err := executeTemplate("templates/launchd.plist.tmpl", launchdTemplateData(launchdLabel, filepath.Join(bd, "stoarama-relay"), filepath.Join(home, ".stoarama", "logs", "relay.log"), instanceID))
	if err != nil {
		t.Fatal(err)
	}
	if instanceID == "" {
		start := strings.Index(string(b), "  <key>EnvironmentVariables</key>")
		end := strings.Index(string(b), "  <key>RunAtLoad</key>")
		if start < 0 || end < 0 || start >= end {
			t.Fatal("launchd template environment block not found")
		}
		b = append(append([]byte{}, b[:start]...), b[end:]...)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return path, b
}

func TestEnsureNoLoadedRelayChecksBothDomains(t *testing.T) {
	logPath := fakeLaunchctl(t, `
echo "$*" >> "$LAUNCHCTL_TEST_LOG"
if [ "$1" = print ]; then exit 0; fi
exit 1
`)
	err := ensureNoLoadedRelay(501)
	if err == nil || !strings.Contains(err.Error(), "gui/501 and user/501") {
		t.Fatalf("error=%v, want both loaded domains", err)
	}
	calls, _ := os.ReadFile(logPath)
	if !strings.Contains(string(calls), "print gui/501/com.stoarama.relay") ||
		!strings.Contains(string(calls), "print user/501/com.stoarama.relay") {
		t.Fatalf("both domains were not checked:\n%s", calls)
	}
}

func TestBootstrapLaunchdRefusesHeadlessFallback(t *testing.T) {
	logPath := fakeLaunchctl(t, `
echo "$*" >> "$LAUNCHCTL_TEST_LOG"
if [ "$1" = bootstrap ]; then echo no-gui >&2; exit 125; fi
exit 0
`)
	err := bootstrapLaunchd("gui/501", "/tmp/relay.plist", "candidate", 0)
	if err == nil || !strings.Contains(err.Error(), "requires an active GUI login session") {
		t.Fatalf("error=%v, want explicit non-admin durability refusal", err)
	}
	calls, _ := os.ReadFile(logPath)
	if strings.Contains(string(calls), "user/501") {
		t.Fatalf("unsafe user-domain fallback attempted:\n%s", calls)
	}
}

func TestBootstrapLaunchdReportsCleanupFailure(t *testing.T) {
	fakeLaunchctl(t, `
if [ "$1" = bootstrap ]; then exit 0; fi
if [ "$1" = kickstart ]; then exit 5; fi
if [ "$1" = bootout ]; then exit 6; fi
exit 1
`)
	err := bootstrapLaunchd("gui/501", "/tmp/relay.plist", "candidate", 0)
	if err == nil || !strings.Contains(err.Error(), "candidate cleanup failed") {
		t.Fatalf("error=%v, want cleanup failure", err)
	}
}

func TestWaitLaunchdReadyRequiresCandidateAndSecondHeartbeat(t *testing.T) {
	fakeLaunchctl(t, `
if [ "$1" = print ]; then echo 'state = running'; exit 0; fi
exit 0
`)
	started := time.Now().UTC()
	state := &relayRecoveryState{
		ServiceInstanceID:     "old",
		StartedAt:             started.Add(time.Second),
		LastHeartbeatAt:       started.Add(time.Minute),
		HeartbeatSuccessCount: 12,
	}
	if err := state.persist(recoveryStatePath()); err != nil {
		t.Fatal(err)
	}
	if waitLaunchdReady("gui/501", "candidate", started, 10, 150*time.Millisecond) {
		t.Fatal("heartbeat from another instance accepted")
	}
	state.ServiceInstanceID = "candidate"
	state.HeartbeatSuccessCount = 11
	if err := state.persist(recoveryStatePath()); err != nil {
		t.Fatal(err)
	}
	if waitLaunchdReady("gui/501", "candidate", started, 10, 150*time.Millisecond) {
		t.Fatal("first heartbeat accepted without stability interval")
	}
	state.HeartbeatSuccessCount = 12
	if err := state.persist(recoveryStatePath()); err != nil {
		t.Fatal(err)
	}
	if !waitLaunchdReady("gui/501", "candidate", started, 10, 150*time.Millisecond) {
		t.Fatal("stable candidate heartbeat rejected")
	}
}

func TestServiceOperationLockRejectsConcurrentInstaller(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	first, err := acquireServiceOperationLock()
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := acquireServiceOperationLock()
	if second != nil {
		second.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "another relay service operation") {
		t.Fatalf("error=%v, want contention", err)
	}
}

func TestCleanupStaleHandoffFilesOnlyRemovesOwnedArtifacts(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"handoff-candidate-old", "handoff-prior-old", "unrelated"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := cleanupStaleHandoffFiles(dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"handoff-candidate-old", "handoff-prior-old"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("owned stale artifact %s remains", name)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "unrelated")); err != nil {
		t.Fatalf("unrelated file removed: %v", err)
	}
}

func TestLegacyHandoffPlistEscapesSpecialCharacterPaths(t *testing.T) {
	fakeLaunchctl(t, `if [ "$1" = print ]; then exit 113; fi; exit 0`)
	root := filepath.Join(t.TempDir(), "home&special")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", root)
	path, prior := writeTestLaunchdPlist(t, "")
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	if err := scheduleLegacyLaunchdHandoff(domain, path, prior); err != nil {
		t.Fatal(err)
	}
	helper, err := os.ReadFile(filepath.Join(root, ".stoarama", "service-install", "handoff-job"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(helper), "home&special") || !strings.Contains(string(helper), "home&amp;special") {
		t.Fatalf("helper plist path was not XML escaped: %s", helper)
	}
	if strings.Count(string(helper), "<key>StandardOutPath</key>") != 1 || strings.Count(string(helper), "<key>StandardErrorPath</key>") != 1 {
		t.Fatal("helper diagnostics are not routed to the advertised log")
	}
	candidates, err := filepath.Glob(filepath.Join(root, ".stoarama", "service-install", "handoff-candidate-*"))
	if err != nil || len(candidates) != 1 {
		t.Fatalf("candidate files=%v err=%v", candidates, err)
	}
	candidate, err := os.ReadFile(candidates[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(candidate), "home&special") || !strings.Contains(string(candidate), "home&amp;special") {
		t.Fatalf("candidate plist path was not XML escaped: %s", candidate)
	}
}

func TestValidateLaunchdHandoffRejectsForeignTargets(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	home, _ := os.UserHomeDir()
	stage := filepath.Join(home, ".stoarama", "service-install")
	canonical := filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist")
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	if err := validateLaunchdHandoffPaths(domain, canonical, filepath.Join(stage, "handoff-candidate-x"), filepath.Join(stage, "handoff-prior-x"), launchdLabel+".handoff", filepath.Join(stage, "handoff-job")); err != nil {
		t.Fatal(err)
	}
	if err := validateLaunchdHandoffPaths("user/0", canonical, filepath.Join(stage, "handoff-candidate-x"), filepath.Join(stage, "handoff-prior-x"), launchdLabel+".handoff", filepath.Join(stage, "handoff-job")); err == nil {
		t.Fatal("foreign domain accepted")
	}
	if err := validateLaunchdHandoffPaths(domain, "/tmp/relay.plist", filepath.Join(stage, "handoff-candidate-x"), filepath.Join(stage, "handoff-prior-x"), launchdLabel+".handoff", filepath.Join(stage, "handoff-job")); err == nil {
		t.Fatal("foreign plist accepted")
	}
}

func TestRestorePriorFileIsTransactional(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "relay.plist")
	if err := os.WriteFile(path, []byte("candidate"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := restorePriorFile(path, []byte("prior"), 0o600, true); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "prior" {
		t.Fatalf("restored=%q err=%v", got, err)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%o", info.Mode().Perm())
	}
	if err := restorePriorFile(path, nil, 0, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("candidate not removed: %v", err)
	}
}

func TestRestartServiceUsesOnlyLoadedUserDomain(t *testing.T) {
	uid := 501
	logPath := fakeLaunchctl(t, `
echo "$*" >> "$LAUNCHCTL_TEST_LOG"
if [ "$1" = print ] && [ "$2" = "user/`+fmt.Sprint(uid)+`/com.stoarama.relay" ]; then exit 0; fi
if [ "$1" = print ]; then exit 113; fi
exit 0
`)
	writeTestLaunchdPlist(t, "existing")
	if err := restartLaunchd(uid); err != nil {
		t.Fatal(err)
	}
	calls, _ := os.ReadFile(logPath)
	if !strings.Contains(string(calls), "user/"+fmt.Sprint(uid)+"/com.stoarama.relay") {
		t.Fatalf("existing user-domain service was not restartable:\n%s", calls)
	}
}

func TestRestartSchedulesLegacyHandoffWithoutMutatingCachedJob(t *testing.T) {
	logPath := fakeLaunchctl(t, `
echo "$*" >> "$LAUNCHCTL_TEST_LOG"
if [ "$1" = print ] && [ "$2" = "gui/501/com.stoarama.relay" ]; then exit 0; fi
if [ "$1" = print ]; then exit 113; fi
exit 0
`)
	path, prior := writeTestLaunchdPlist(t, "")
	if err := restartLaunchd(501); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(prior) {
		t.Fatal("canonical plist changed before cached job was booted out")
	}
	calls, _ := os.ReadFile(logPath)
	if strings.Contains(string(calls), "bootout gui/501/com.stoarama.relay") || !strings.Contains(string(calls), "handoff-job") {
		t.Fatalf("migration was not delegated to an independent launchd job:\n%s", calls)
	}
}

func TestRestartLeavesLegacyPlistWhenHandoffCannotStart(t *testing.T) {
	fakeLaunchctl(t, `
if [ "$1" = print ] && [ "$2" = "gui/501/com.stoarama.relay" ]; then exit 0; fi
if [ "$1" = print ]; then exit 113; fi
if [ "$1" = kickstart ]; then exit 7; fi
exit 0
`)
	path, prior := writeTestLaunchdPlist(t, "")
	if err := restartLaunchd(501); err == nil {
		t.Fatal("kickstart failure accepted")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(prior) {
		t.Fatal("legacy plist was not restored")
	}
}

func TestHandoffBootsOutCachedJobBeforeApplyingCandidate(t *testing.T) {
	logPath := fakeLaunchctl(t, `
echo "$*" >> "$LAUNCHCTL_TEST_LOG"
marker="$HOME/loaded"
if [ "$1" = print ]; then [ -f "$marker" ] && echo 'state = running' && exit 0; exit 113; fi
if [ "$1" = bootout ]; then rm -f "$marker"; exit 0; fi
if [ "$1" = bootstrap ]; then
  touch "$marker"
  id=$(sed -n 's/.*STOARAMA_SERVICE_INSTANCE_ID<\/key><string>\([^<]*\).*/\1/p' "$3")
  echo "{\"service_instance_id\":\"$id\",\"heartbeat_success_count\":2,\"started_at\":\"2099-01-01T00:00:00Z\"}" > "$HOME/.stoarama/relay-recovery.json"
  exit 0
fi
# kickstart intentionally does not reload config: only bootstrap above applies it.
if [ "$1" = kickstart ]; then exit 0; fi
exit 0
`)
	path, prior := writeTestLaunchdPlist(t, "")
	home, _ := os.UserHomeDir()
	if err := os.MkdirAll(filepath.Join(home, ".stoarama"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "loaded"), []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}
	candidateBytes, err := executeTemplate("templates/launchd.plist.tmpl", map[string]string{
		"Label": launchdLabel, "ExePath": filepath.Join(home, ".stoarama", "bin", "stoarama-relay"),
		"LogPath": filepath.Join(home, ".stoarama", "logs", "relay.log"), "InstanceID": "candidate",
	})
	if err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(home, ".stoarama", "service-install")
	if err := os.MkdirAll(stage, 0o700); err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Join(stage, "handoff-candidate-candidate")
	rollback := filepath.Join(stage, "handoff-prior-candidate")
	helper := filepath.Join(stage, "handoff-job")
	if err := os.WriteFile(candidate, candidateBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rollback, prior, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(helper, []byte("helper"), 0o600); err != nil {
		t.Fatal(err)
	}
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	if err := completeLaunchdHandoff([]string{domain, path, candidate, rollback, "candidate", launchdLabel + ".handoff", helper}); err != nil {
		t.Fatal(err)
	}
	calls, _ := os.ReadFile(logPath)
	text := string(calls)
	bootoutAt := strings.Index(text, "bootout "+domain+"/com.stoarama.relay")
	bootstrapAt := strings.Index(text, "bootstrap "+domain+" "+path)
	if bootoutAt < 0 || bootstrapAt < 0 || bootoutAt > bootstrapAt {
		t.Fatalf("candidate was bootstrapped before cached job bootout:\n%s", text)
	}
	got, _ := os.ReadFile(path)
	if serviceInstanceIDFromPlist(got) != "candidate" {
		t.Fatal("candidate config was not applied by bootstrap")
	}
}

func TestRestartServiceRefusesDuplicateDomains(t *testing.T) {
	fakeLaunchctl(t, `
if [ "$1" = print ]; then exit 0; fi
exit 0
`)
	err := restartLaunchd(501)
	if err == nil || !strings.Contains(err.Error(), "duplicate relay jobs") {
		t.Fatalf("error=%v, want duplicate refusal", err)
	}
}
