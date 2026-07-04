package main

import (
	"embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"
)

//go:embed templates/launchd.plist.tmpl templates/systemd.service.tmpl
var templatesFS embed.FS

const (
	launchdLabel = "com.stoarama.relay"
	systemdUnit  = "stoarama-relay.service"
)

// installLaunchd writes the launchd USER agent (so the login user's Keychain is
// reachable for Chrome cookie decryption, which a system LaunchDaemon could not do)
// and bootstraps it into the per-user GUI domain.
func installLaunchd() error {
	if runtime.GOOS != "darwin" {
		return fmt.Errorf("install-launchd is only supported on macOS")
	}
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
	if err := renderTemplate("templates/launchd.plist.tmpl", plistPath, 0o644, map[string]string{
		"Label":   launchdLabel,
		"ExePath": filepath.Join(bd, "stoarama-relay"),
		"LogPath": logPath,
	}); err != nil {
		return err
	}
	fmt.Printf("Wrote %s\n", plistPath)

	uid := os.Getuid()
	domain := fmt.Sprintf("gui/%d", uid)
	// Replace any prior instance, then bootstrap + start.
	_ = exec.Command("launchctl", "bootout", domain+"/"+launchdLabel).Run()
	if out, err := exec.Command("launchctl", "bootstrap", domain, plistPath).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl bootstrap: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	_ = exec.Command("launchctl", "kickstart", "-k", domain+"/"+launchdLabel).Run()
	fmt.Println("Loaded launchd user agent com.stoarama.relay")
	return nil
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

// uninstall stops the running service and removes the unit file for the current OS.
func uninstall() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home dir: %w", err)
	}
	switch runtime.GOOS {
	case "darwin":
		domain := fmt.Sprintf("gui/%d", os.Getuid())
		_ = exec.Command("launchctl", "bootout", domain+"/"+launchdLabel).Run()
		plistPath := filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist")
		_ = os.Remove(plistPath)
		fmt.Printf("Stopped launchd agent and removed %s\n", plistPath)
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

// restartService kicks the platform service so a self-updated binary is picked up.
func restartService() {
	switch runtime.GOOS {
	case "darwin":
		domain := fmt.Sprintf("gui/%d", os.Getuid())
		_ = exec.Command("launchctl", "kickstart", "-k", domain+"/"+launchdLabel).Run()
	case "linux":
		_ = exec.Command("systemctl", "--user", "restart", systemdUnit).Run()
	}
}

func renderTemplate(name, dest string, mode os.FileMode, data any) error {
	tmpl, err := template.ParseFS(templatesFS, name)
	if err != nil {
		return fmt.Errorf("parse template %s: %w", name, err)
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("create %s: %w", dest, err)
	}
	defer f.Close()
	if err := tmpl.Execute(f, data); err != nil {
		return fmt.Errorf("render %s: %w", dest, err)
	}
	return nil
}
