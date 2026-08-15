package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"embed"
	"errors"
	"fmt"
	"html"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"text/template"
	"time"
)

//go:embed templates/launchd.plist.tmpl templates/systemd.service.tmpl
var templatesFS embed.FS

const (
	launchdLabel = "com.stoarama.relay"
	systemdUnit  = "stoarama-relay.service"
)

var (
	launchdReadinessTimeout = 2*heartbeatInterval + 15*time.Second
	launchdRemovalTimeout   = 10 * time.Second
)

const launchctlCommandTimeout = 5 * time.Second

// installLaunchd writes the login-durable GUI-domain LaunchAgent. The explicit
// user-domain mode supports cookieless relays on headless accounts that already
// have a launchd user domain; it is intentionally never selected automatically.
func installLaunchd() error {
	return installLaunchdInDomain(false)
}

func installLaunchdInDomain(userDomain bool) error {
	if runtime.GOOS != "darwin" {
		return fmt.Errorf("install-launchd is only supported on macOS")
	}
	if userDomain && experimentalCookieMode() {
		return fmt.Errorf("--user-domain supports cookieless relay mode only")
	}
	installLock, err := acquireServiceOperationLock()
	if err != nil {
		return err
	}
	defer installLock.Close()
	var runLock *os.File
	defer func() {
		if runLock != nil {
			_ = runLock.Close()
		}
	}()
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home dir: %w", err)
	}
	bd, err := binDir()
	if err != nil {
		return err
	}
	logPath := filepath.Join(home, ".stoarama", "logs", "relay.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return fmt.Errorf("create log dir: %w", err)
	}
	agentsDir := filepath.Join(home, "Library", "LaunchAgents")
	if err := os.MkdirAll(agentsDir, 0o755); err != nil {
		return fmt.Errorf("create LaunchAgents dir: %w", err)
	}
	plistPath := filepath.Join(agentsDir, launchdLabel+".plist")
	instanceID := newServiceInstanceID()
	rendered, err := executeTemplate("templates/launchd.plist.tmpl", launchdTemplateData(launchdLabel, filepath.Join(bd, "stoarama-relay"), logPath, instanceID))
	if err != nil {
		return err
	}
	stageDir := filepath.Join(home, ".stoarama", "service-install")
	if err := os.MkdirAll(stageDir, 0o700); err != nil {
		return fmt.Errorf("create service staging dir: %w", err)
	}
	staged, err := os.CreateTemp(stageDir, "launchd-candidate-")
	if err != nil {
		return fmt.Errorf("stage launchd plist: %w", err)
	}
	stagedPath := staged.Name()
	defer os.Remove(stagedPath)
	if err := staged.Chmod(0o644); err != nil {
		_ = staged.Close()
		return err
	}
	if _, err := staged.Write(rendered); err != nil {
		_ = staged.Close()
		return err
	}
	if err := staged.Sync(); err != nil {
		_ = staged.Close()
		return err
	}
	if err := staged.Close(); err != nil {
		return err
	}

	prior, priorMode, hadPrior, err := readPriorFile(plistPath)
	if err != nil {
		return err
	}
	uid := os.Getuid()
	loaded, err := loadedLaunchdDomains(uid)
	if err != nil {
		return err
	}
	if len(loaded) > 1 {
		return fmt.Errorf("duplicate relay jobs loaded in %s; refusing replacement", strings.Join(loaded, " and "))
	}
	priorDomain := ""
	domain := fmt.Sprintf("gui/%d", uid)
	if userDomain {
		domain = fmt.Sprintf("user/%d", uid)
		if _, err := runLaunchctlBounded("print", domain); err != nil {
			return fmt.Errorf("--user-domain requires an existing %s launchd domain: %w", domain, err)
		}
	}
	if len(loaded) == 1 {
		if loaded[0] != domain {
			return fmt.Errorf("relay is already loaded in %s; refusing cross-domain replacement with %s", loaded[0], domain)
		}
		if !hadPrior || !ownedRelayPlist(prior, filepath.Join(bd, "stoarama-relay")) {
			return fmt.Errorf("relay job is loaded in %s but its canonical plist is not owned by this installer; refusing replacement", loaded[0])
		}
		priorDomain = loaded[0]
		if err := removeLaunchdJob(priorDomain); err != nil {
			return recoverPriorAfterRemovalFailure(priorDomain, plistPath, prior, priorMode, fmt.Errorf("stop prior relay for replacement: %w", err))
		}
		runLock, err = waitForRelayRunLock(5 * time.Second)
	} else {
		runLock, err = acquireRelayRunLock()
	}
	if err != nil {
		cause := fmt.Errorf("relay process is still running; wait for active recordings to finish, then rerun install-launchd (or run uninstall only after confirming it is safe to stop recordings): %w", err)
		if priorDomain != "" {
			return restorePriorLaunchd(plistPath, prior, priorMode, priorDomain, heartbeatSuccessCount(), cause)
		}
		return cause
	}
	baseline := heartbeatSuccessCount()
	if err := os.Rename(stagedPath, plistPath); err != nil {
		cause := fmt.Errorf("install launchd plist: %w", err)
		if priorDomain != "" {
			return restorePriorLaunchd(plistPath, prior, priorMode, priorDomain, heartbeatSuccessCount(), cause)
		}
		return cause
	}
	// The candidate must acquire the process lock when launchd starts it.
	_ = runLock.Close()
	runLock = nil
	if err := bootstrapLaunchd(domain, plistPath, instanceID, baseline); err != nil {
		if absenceErr := ensureNoLoadedRelay(uid); absenceErr != nil {
			return fmt.Errorf("%w; rollback could not confirm candidate absence, so its matching plist was retained: %v", err, absenceErr)
		}
		restoreErr := restorePriorFile(plistPath, prior, priorMode, hadPrior)
		if restoreErr != nil {
			return fmt.Errorf("%w; prior plist restoration failed: %v", err, restoreErr)
		}
		if hadPrior && priorDomain != "" {
			if rollbackErr := bootstrapLaunchd(priorDomain, plistPath, serviceInstanceIDFromPlist(prior), heartbeatSuccessCount()); rollbackErr != nil {
				return fmt.Errorf("%w; prior service rollback failed: %v", err, rollbackErr)
			}
			return fmt.Errorf("%w; prior service was restored and verified", err)
		}
		return err
	}
	fmt.Printf("Wrote %s\n", plistPath)
	fmt.Println("Loaded launchd user agent com.stoarama.relay (starts at login; a non-admin agent cannot run before login after reboot)")
	return nil
}

func restorePriorLaunchd(plistPath string, prior []byte, priorMode os.FileMode, priorDomain string, baseline uint64, cause error) error {
	if err := restorePriorFile(plistPath, prior, priorMode, true); err != nil {
		return fmt.Errorf("%w; prior plist restoration failed: %v", cause, err)
	}
	if err := bootstrapLaunchd(priorDomain, plistPath, serviceInstanceIDFromPlist(prior), baseline); err != nil {
		return fmt.Errorf("%w; prior service rollback failed: %v", cause, err)
	}
	return fmt.Errorf("%w; prior service was restored and verified", cause)
}

func bootstrapLaunchd(domain, plistPath, instanceID string, baselineSuccesses uint64) error {
	startedAt := time.Now().UTC()
	out, err := runLaunchctlBounded("bootstrap", domain, plistPath)
	if err != nil {
		hint := "the selected launchd domain is unavailable"
		if strings.HasPrefix(domain, "gui/") {
			hint = "this non-admin service requires an active GUI login session"
		}
		cause := fmt.Errorf("launchctl bootstrap %s: %w (%s); %s", domain, err, strings.TrimSpace(string(out)), hint)
		loaded, probeErr := launchdJobLoadedBounded(domain + "/" + launchdLabel)
		if probeErr != nil {
			return fmt.Errorf("%w; candidate state probe failed: %v", cause, probeErr)
		}
		if loaded {
			return cleanupLaunchdFailure(domain, cause)
		}
		return cause
	}
	// The canonical plist is RunAtLoad, so bootstrap normally starts the
	// candidate. A non-destructive kickstart also covers a delayed start without
	// killing that fresh process or racing launchd's registration transition.
	if out, err := runLaunchctlBounded("kickstart", domain+"/"+launchdLabel); err != nil {
		return cleanupLaunchdFailure(domain, fmt.Errorf("launchctl kickstart %s: %w (%s)", domain, err, strings.TrimSpace(string(out))))
	}
	if !waitLaunchdReady(domain, instanceID, startedAt, baselineSuccesses, launchdReadinessTimeout) {
		return cleanupLaunchdFailure(domain, fmt.Errorf("health check %s: candidate did not remain running through a second successful heartbeat", domain))
	}
	return nil
}

func cleanupLaunchdFailure(domain string, cause error) error {
	if cleanupErr := removeLaunchdJob(domain); cleanupErr != nil {
		return fmt.Errorf("%w; candidate cleanup failed: %v", cause, cleanupErr)
	}
	return cause
}

func waitLaunchdReady(domain, instanceID string, startedAt time.Time, baselineSuccesses uint64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		out, err := runLaunchctlBounded("print", domain+"/"+launchdLabel)
		if err == nil && strings.Contains(string(out), "state = running") {
			if state, stateErr := loadRecoveryState(recoveryStatePath()); stateErr == nil &&
				state.ServiceInstanceID == instanceID && !state.StartedAt.Before(startedAt) &&
				state.HeartbeatSuccessCount >= baselineSuccesses+2 {
				return true
			}
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func removeLaunchdJob(domain string) error {
	return removeLaunchdLabel(domain, launchdLabel)
}

func recoverPriorAfterRemovalFailure(domain, plistPath string, prior []byte, priorMode os.FileMode, cause error) error {
	target := domain + "/" + launchdLabel
	loaded, probeErr := launchdJobLoadedBounded(target)
	if probeErr != nil {
		return fmt.Errorf("%w; prior relay recovery probe failed: %v", cause, probeErr)
	}
	if loaded {
		baseline := heartbeatSuccessCount()
		startedAt := time.Now().UTC()
		out, err := runLaunchctlBounded("kickstart", "-k", target)
		if err != nil {
			// bootout may have completed between the successful print and kickstart.
			// Re-probe and bootstrap the still-canonical prior definition if it vanished.
			stillLoaded, recheckErr := launchdJobLoadedBounded(target)
			if recheckErr != nil {
				return fmt.Errorf("%w; prior relay recovery kickstart failed: %v (%s); recheck failed: %v", cause, err, strings.TrimSpace(string(out)), recheckErr)
			}
			if stillLoaded {
				return fmt.Errorf("%w; prior relay recovery kickstart failed while job remains loaded: %v (%s)", cause, err, strings.TrimSpace(string(out)))
			}
			return restorePriorLaunchd(plistPath, prior, priorMode, domain, heartbeatSuccessCount(), cause)
		}
		if !waitLaunchdReady(domain, serviceInstanceIDFromPlist(prior), startedAt, baseline, launchdReadinessTimeout) {
			return fmt.Errorf("%w; prior relay recovery did not produce two verified heartbeats", cause)
		}
		return fmt.Errorf("%w; prior relay remained loaded and was restarted and verified", cause)
	}
	return restorePriorLaunchd(plistPath, prior, priorMode, domain, heartbeatSuccessCount(), cause)
}

func loadedLaunchdDomains(uid int) ([]string, error) {
	var loaded []string
	for _, domain := range []string{fmt.Sprintf("gui/%d", uid), fmt.Sprintf("user/%d", uid)} {
		isLoaded, err := launchdJobLoadedBounded(domain + "/" + launchdLabel)
		if err != nil {
			return nil, fmt.Errorf("probe launchd service in %s: %w", domain, err)
		}
		if isLoaded {
			loaded = append(loaded, domain)
		}
	}
	return loaded, nil
}

func ensureNoLoadedRelay(uid int) error {
	loaded, err := loadedLaunchdDomains(uid)
	if err != nil {
		return err
	}
	if len(loaded) > 0 {
		return fmt.Errorf("relay service remains loaded in %s; wait for active recordings to finish, then retry (or run uninstall only after confirming it is safe to stop recordings)", strings.Join(loaded, " and "))
	}
	return nil
}

func heartbeatSuccessCount() uint64 {
	state, err := loadRecoveryState(recoveryStatePath())
	if err != nil {
		return 0
	}
	return state.HeartbeatSuccessCount
}

func ownedRelayPlist(content []byte, executable string) bool {
	s := string(content)
	return strings.Contains(s, "<string>"+launchdLabel+"</string>") && strings.Contains(s, "<string>"+html.EscapeString(executable)+"</string>")
}

func serviceInstanceIDFromPlist(content []byte) string {
	const key = "<key>STOARAMA_SERVICE_INSTANCE_ID</key><string>"
	start := strings.Index(string(content), key)
	if start < 0 {
		return ""
	}
	value := string(content)[start+len(key):]
	end := strings.Index(value, "</string>")
	if end < 0 {
		return ""
	}
	return value[:end]
}

func acquireServiceOperationLock() (*os.File, error) {
	home, err := stoaramaHome()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(home, "service-install.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another relay service operation is in progress: %w", err)
	}
	return f, nil
}

func newServiceInstanceID() string {
	return rand.Text()
}

func launchdTemplateData(label, exePath, logPath, instanceID string) map[string]string {
	escape := html.EscapeString
	return map[string]string{"Label": escape(label), "ExePath": escape(exePath), "LogPath": escape(logPath), "InstanceID": escape(instanceID)}
}

func readPriorFile(path string) ([]byte, os.FileMode, bool, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, err
	}
	b, err := os.ReadFile(path)
	return b, info.Mode().Perm(), true, err
}

func restorePriorFile(path string, content []byte, mode os.FileMode, existed bool) error {
	if !existed {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".stoarama-relay-restore-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(mode); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(content); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// installSystemd writes the systemd USER unit (so the login user's keyring is
// reachable for Chrome cookie decryption) and enables + starts it.
func installSystemd() error {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("install-systemd is only supported on Linux")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home dir: %w", err)
	}
	bd, err := binDir()
	if err != nil {
		return err
	}
	unitDir := filepath.Join(home, ".config", "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o755); err != nil {
		return fmt.Errorf("create systemd user dir: %w", err)
	}
	unitPath := filepath.Join(unitDir, systemdUnit)
	if err := renderTemplate("templates/systemd.service.tmpl", unitPath, 0o644, map[string]string{
		"ExePath": filepath.Join(bd, "stoarama-relay"),
	}); err != nil {
		return err
	}
	fmt.Printf("Wrote %s\n", unitPath)

	if out, err := exec.Command("systemctl", "--user", "daemon-reload").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl daemon-reload: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	if out, err := exec.Command("systemctl", "--user", "enable", "--now", systemdUnit).CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl enable --now: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	fmt.Println("Enabled systemd user unit stoarama-relay.service")
	return nil
}

func refreshSystemdUnit() error {
	if runtime.GOOS != "linux" {
		return nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	unitPath := filepath.Join(home, ".config", "systemd", "user", systemdUnit)
	current, err := os.ReadFile(unitPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	bd, err := binDir()
	if err != nil {
		return err
	}
	updated, err := executeTemplate("templates/systemd.service.tmpl", map[string]string{"ExePath": filepath.Join(bd, "stoarama-relay")})
	if err != nil {
		return err
	}
	changed, err := writeSystemdUnitIfChanged(unitPath, current, updated)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "systemctl", "--user", "daemon-reload").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl daemon-reload: %w (%s)", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func writeSystemdUnitIfChanged(unitPath string, current, updated []byte) (bool, error) {
	if bytes.Equal(current, updated) {
		return false, nil
	}
	temp, err := os.CreateTemp(filepath.Dir(unitPath), systemdUnit+".new-")
	if err != nil {
		return false, err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o644); err != nil {
		_ = temp.Close()
		return false, err
	}
	if _, err := temp.Write(updated); err != nil {
		_ = temp.Close()
		return false, err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return false, err
	}
	if err := temp.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(tempPath, unitPath); err != nil {
		return false, err
	}
	return true, nil
}

// uninstall stops the running service and removes the unit file for the current OS.
func uninstall() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home dir: %w", err)
	}
	switch runtime.GOOS {
	case "darwin":
		operationLock, lockErr := acquireServiceOperationLock()
		if lockErr != nil {
			return lockErr
		}
		defer operationLock.Close()
		if err := uninstallLaunchd(os.Getuid(), home); err != nil {
			return err
		}
	case "linux":
		_ = exec.Command("systemctl", "--user", "disable", "--now", systemdUnit).Run()
		unitPath := filepath.Join(home, ".config", "systemd", "user", systemdUnit)
		_ = os.Remove(unitPath)
		_ = exec.Command("systemctl", "--user", "daemon-reload").Run()
		fmt.Printf("Stopped systemd unit and removed %s\n", unitPath)
	default:
		return fmt.Errorf("uninstall is only supported on macOS and Linux")
	}
	return nil
}

func uninstallLaunchd(uid int, home string) error {
	for _, domain := range []string{fmt.Sprintf("gui/%d", uid), fmt.Sprintf("user/%d", uid)} {
		loaded, probeErr := launchdJobLoadedBounded(domain + "/" + launchdLabel)
		if probeErr != nil {
			return fmt.Errorf("probe launchd agent before uninstall: %w", probeErr)
		}
		if loaded {
			if err := removeLaunchdJob(domain); err != nil {
				return fmt.Errorf("stop launchd agent before uninstall: %w", err)
			}
		}
	}
	if err := ensureNoLoadedRelay(uid); err != nil {
		return fmt.Errorf("confirm relay absence before uninstall: %w", err)
	}
	runLock, runLockErr := waitForRelayRunLock(5 * time.Second)
	if runLockErr != nil {
		return fmt.Errorf("relay process remains live outside launchd; refusing to remove its service definition: %w", runLockErr)
	}
	defer runLock.Close()
	plistPath := filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist")
	if err := os.Remove(plistPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove launchd plist: %w", err)
	}
	fmt.Printf("Stopped launchd agent and removed %s\n", plistPath)
	return nil
}

// restartService kicks the platform service so a self-updated binary is picked up.
func restartService() error {
	switch runtime.GOOS {
	case "darwin":
		return restartLaunchd(os.Getuid())
	case "linux":
		if err := exec.Command("systemctl", "--user", "restart", systemdUnit).Run(); err != nil {
			return fmt.Errorf("restart systemd service: %w", err)
		}
	default:
		return fmt.Errorf("restart is only supported on macOS and Linux")
	}
	return nil
}

func restartLaunchd(uid int) error {
	operationLock, err := acquireServiceOperationLock()
	if err != nil {
		return err
	}
	defer operationLock.Close()
	loaded, err := loadedLaunchdDomains(uid)
	if err != nil {
		return err
	}
	if len(loaded) == 0 {
		return fmt.Errorf("restart launchd service: relay is not loaded")
	}
	if len(loaded) > 1 {
		return fmt.Errorf("restart launchd service: duplicate relay jobs loaded in %s; refusing to leave two workers active", strings.Join(loaded, " and "))
	}
	target := loaded[0] + "/" + launchdLabel
	prior, plistPath, err := currentLaunchdPlist()
	if err != nil {
		return err
	}
	if serviceInstanceIDFromPlist(prior) == "" {
		return scheduleLegacyLaunchdHandoff(loaded[0], plistPath, prior)
	}
	out, err := runLaunchctlBounded("kickstart", "-k", target)
	if err != nil {
		return fmt.Errorf("restart launchd service %s: %w (%s)", target, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func currentLaunchdPlist() ([]byte, string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, "", err
	}
	plistPath := filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist")
	prior, _, exists, err := readPriorFile(plistPath)
	if err != nil {
		return nil, plistPath, err
	}
	if !exists {
		return nil, plistPath, fmt.Errorf("loaded relay has no canonical plist; refusing restart")
	}
	return prior, plistPath, nil
}

func scheduleLegacyLaunchdHandoff(domain, plistPath string, prior []byte) error {
	if strings.HasPrefix(domain, "user/") {
		return fmt.Errorf("legacy user-domain relay cannot be made login-durable in place; run install-launchd from an active GUI login")
	}
	home, _ := os.UserHomeDir()
	bd, err := binDir()
	if err != nil {
		return err
	}
	exe := filepath.Join(bd, "stoarama-relay")
	if !ownedRelayPlist(prior, exe) {
		return fmt.Errorf("loaded relay canonical plist is not installer-owned; refusing migration")
	}
	instanceID := newServiceInstanceID()
	logPath := filepath.Join(home, ".stoarama", "logs", "relay.log")
	updated, err := executeTemplate("templates/launchd.plist.tmpl", launchdTemplateData(launchdLabel, exe, logPath, instanceID))
	if err != nil {
		return err
	}
	stageDir := filepath.Join(home, ".stoarama", "service-install")
	if err := os.MkdirAll(stageDir, 0o700); err != nil {
		return err
	}
	candidate := filepath.Join(stageDir, "handoff-candidate-"+instanceID)
	rollback := filepath.Join(stageDir, "handoff-prior-"+instanceID)
	label := launchdLabel + ".handoff"
	helper := filepath.Join(stageDir, "handoff-job")
	helperLoaded, probeErr := launchdJobLoadedBounded(domain + "/" + label)
	if probeErr != nil {
		return fmt.Errorf("probe stale migration helper: %w", probeErr)
	}
	if helperLoaded {
		if err := removeLaunchdLabel(domain, label); err != nil {
			return fmt.Errorf("remove stale migration helper: %w", err)
		}
	}
	if err := cleanupStaleHandoffFiles(stageDir); err != nil {
		return err
	}
	if err := os.WriteFile(candidate, updated, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(rollback, prior, 0o600); err != nil {
		_ = os.Remove(candidate)
		return err
	}
	escape := html.EscapeString
	helperXML := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd"><plist version="1.0"><dict><key>Label</key><string>%s</string><key>ProgramArguments</key><array><string>%s</string><string>launchd-handoff</string><string>%s</string><string>%s</string><string>%s</string><string>%s</string><string>%s</string><string>%s</string><string>%s</string></array><key>ProcessType</key><string>Background</string><key>StandardOutPath</key><string>%s</string><key>StandardErrorPath</key><string>%s</string></dict></plist>`, escape(label), escape(exe), escape(domain), escape(plistPath), escape(candidate), escape(rollback), escape(instanceID), escape(label), escape(helper), escape(logPath), escape(logPath))
	if err := os.WriteFile(helper, []byte(helperXML), 0o600); err != nil {
		if cleanupErr := cleanupLaunchdHandoff(domain, label, candidate, rollback, helper); cleanupErr != nil {
			return fmt.Errorf("write migration helper: %w; cleanup failed: %v", err, cleanupErr)
		}
		return err
	}
	if out, err := runLaunchctlBounded("bootstrap", domain, helper); err != nil {
		cause := fmt.Errorf("bootstrap migration helper: %w (%s)", err, strings.TrimSpace(string(out)))
		if cleanupErr := cleanupLaunchdHandoff(domain, label, candidate, rollback, helper); cleanupErr != nil {
			return fmt.Errorf("%w; cleanup failed: %v", cause, cleanupErr)
		}
		return cause
	}
	if out, err := runLaunchctlBounded("kickstart", domain+"/"+label); err != nil {
		cause := fmt.Errorf("start migration helper: %w (%s)", err, strings.TrimSpace(string(out)))
		if cleanupErr := cleanupLaunchdHandoff(domain, label, candidate, rollback, helper); cleanupErr != nil {
			return fmt.Errorf("%w; cleanup failed: %v", cause, cleanupErr)
		}
		return cause
	}
	fmt.Printf("Scheduled legacy relay migration with helper %s; progress is logged to %s\n", label, logPath)
	return nil
}

func cleanupStaleHandoffFiles(stageDir string) error {
	var failures []string
	for _, pattern := range []string{"handoff-candidate-*", "handoff-prior-*"} {
		matches, err := filepath.Glob(filepath.Join(stageDir, pattern))
		if err != nil {
			return err
		}
		for _, path := range matches {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				failures = append(failures, path+": "+err.Error())
			}
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("clean stale launchd handoff files: %s", strings.Join(failures, "; "))
	}
	return nil
}

func completeLaunchdHandoff(args []string) error {
	if len(args) != 7 {
		return fmt.Errorf("invalid launchd handoff")
	}
	domain, plistPath, candidate, rollback, instanceID, helperLabel, helperPath := args[0], args[1], args[2], args[3], args[4], args[5], args[6]
	if err := validateLaunchdHandoffPaths(domain, plistPath, candidate, rollback, helperLabel, helperPath); err != nil {
		return err
	}
	completed := false
	defer func() {
		if !completed {
			_ = cleanupLaunchdHandoff(domain, helperLabel, candidate, rollback, helperPath)
		}
	}()
	lock, err := waitForServiceOperationLock(10 * time.Second)
	if err != nil {
		return err
	}
	defer lock.Close()
	prior, err := os.ReadFile(rollback)
	if err != nil {
		return err
	}
	defer os.Remove(candidate)
	defer os.Remove(rollback)
	defer os.Remove(helperPath)
	if err := removeLaunchdJob(domain); err != nil {
		return recoverPriorAfterRemovalFailure(domain, plistPath, prior, 0o644, err)
	}
	runLock, err := waitForRelayRunLock(5 * time.Second)
	if err != nil {
		return restorePriorLaunchd(plistPath, prior, 0o644, domain, heartbeatSuccessCount(), err)
	}
	if err := os.Rename(candidate, plistPath); err != nil {
		_ = runLock.Close()
		return restorePriorLaunchd(plistPath, prior, 0o644, domain, heartbeatSuccessCount(), err)
	}
	baseline := heartbeatSuccessCount()
	_ = runLock.Close()
	if err := bootstrapLaunchd(domain, plistPath, instanceID, baseline); err != nil {
		if absenceErr := ensureNoLoadedRelay(os.Getuid()); absenceErr != nil {
			return fmt.Errorf("%w; cannot safely rollback: %v", err, absenceErr)
		}
		return restorePriorLaunchd(plistPath, prior, 0o644, domain, heartbeatSuccessCount(), err)
	}
	// Everything the helper owns must be durable or removed before it synchronously
	// asks launchd to terminate this one-shot job; no goroutine can outlive it.
	_ = lock.Close()
	if err := cleanupLaunchdHandoff(domain, helperLabel, candidate, rollback, helperPath); err != nil {
		return err
	}
	completed = true
	return nil
}

func validateLaunchdHandoffPaths(domain, plistPath, candidate, rollback, helperLabel, helperPath string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	stageDir := filepath.Join(home, ".stoarama", "service-install")
	if domain != fmt.Sprintf("gui/%d", os.Getuid()) ||
		filepath.Clean(plistPath) != filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist") ||
		helperLabel != launchdLabel+".handoff" || filepath.Clean(helperPath) != filepath.Join(stageDir, "handoff-job") {
		return fmt.Errorf("invalid launchd handoff target")
	}
	for path, prefix := range map[string]string{candidate: "handoff-candidate-", rollback: "handoff-prior-"} {
		clean := filepath.Clean(path)
		if filepath.Dir(clean) != stageDir || !strings.HasPrefix(filepath.Base(clean), prefix) {
			return fmt.Errorf("invalid launchd handoff stage path")
		}
	}
	return nil
}

func cleanupLaunchdHandoff(domain, label string, paths ...string) error {
	var failures []string
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			failures = append(failures, "remove "+path+": "+err.Error())
		}
	}
	loaded, probeErr := launchdJobLoadedBounded(domain + "/" + label)
	if probeErr != nil {
		failures = append(failures, "probe launchd handoff: "+probeErr.Error())
	} else if loaded {
		if err := removeLaunchdLabel(domain, label); err != nil {
			failures = append(failures, err.Error())
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%s", strings.Join(failures, "; "))
	}
	return nil
}

func removeLaunchdLabel(domain, label string) error {
	target := domain + "/" + label
	out, err := runLaunchctlBounded("bootout", target)
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 113 {
			return nil
		}
		return fmt.Errorf("bootout %s: %w (%s)", target, err, strings.TrimSpace(string(out)))
	}
	// launchctl can return from bootout before the job disappears from print. That
	// transition is observable on real Macs and is not a failed removal. Wait for a
	// bounded absence instead of falsely failing a transactional replacement after
	// we have already stopped the prior service.
	deadline := time.Now().Add(launchdRemovalTimeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("bootout %s returned success but job remains loaded", target)
		}
		if remaining > time.Second {
			remaining = time.Second
		}
		loaded, err := launchdJobLoadedWithin(target, remaining)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			return fmt.Errorf("check bootout %s: %w", target, err)
		}
		if !loaded {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("bootout %s returned success but job remains loaded", target)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func runLaunchctlBounded(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), launchctlCommandTimeout)
	defer cancel()
	return exec.CommandContext(ctx, "launchctl", args...).CombinedOutput()
}

func launchdJobLoadedBounded(target string) (bool, error) {
	return launchdJobLoadedWithin(target, launchctlCommandTimeout)
}

func launchdJobLoadedWithin(target string, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := exec.CommandContext(ctx, "launchctl", "print", target).Run()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && (exitErr.ExitCode() == 113 || exitErr.ExitCode() == 125) {
		return false, nil
	}
	return false, err
}

func waitForServiceOperationLock(timeout time.Duration) (*os.File, error) {
	deadline := time.Now().Add(timeout)
	for {
		lock, err := acquireServiceOperationLock()
		if err == nil {
			return lock, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForRelayRunLock(timeout time.Duration) (*os.File, error) {
	deadline := time.Now().Add(timeout)
	for {
		lock, err := acquireRelayRunLock()
		if err == nil {
			return lock, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func renderTemplate(name, dest string, mode os.FileMode, data any) error {
	rendered, err := executeTemplate(name, data)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("create %s: %w", dest, err)
	}
	defer f.Close()
	if _, err := f.Write(rendered); err != nil {
		return fmt.Errorf("write %s: %w", dest, err)
	}
	return nil
}

func executeTemplate(name string, data any) ([]byte, error) {
	tmpl, err := template.ParseFS(templatesFS, name)
	if err != nil {
		return nil, fmt.Errorf("parse template %s: %w", name, err)
	}
	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, data); err != nil {
		return nil, fmt.Errorf("render template %s: %w", name, err)
	}
	return rendered.Bytes(), nil
}
