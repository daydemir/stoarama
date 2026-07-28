package main

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"
	"time"
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
	if !bytes.Equal(current, updated) {
		temp, err := os.CreateTemp(filepath.Dir(unitPath), systemdUnit+".new-")
		if err != nil {
			return err
		}
		tempPath := temp.Name()
		defer os.Remove(tempPath)
		if err := temp.Chmod(0o644); err != nil {
			_ = temp.Close()
			return err
		}
		if _, err := temp.Write(updated); err != nil {
			_ = temp.Close()
			return err
		}
		if err := temp.Sync(); err != nil {
			_ = temp.Close()
			return err
		}
		if err := temp.Close(); err != nil {
			return err
		}
		if err := os.Rename(tempPath, unitPath); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "systemctl", "--user", "daemon-reload").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl daemon-reload: %w (%s)", err, strings.TrimSpace(string(output)))
	}
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
func restartService() error {
	switch runtime.GOOS {
	case "darwin":
		domain := fmt.Sprintf("gui/%d", os.Getuid())
		if err := exec.Command("launchctl", "kickstart", "-k", domain+"/"+launchdLabel).Run(); err != nil {
			return fmt.Errorf("restart launchd service: %w", err)
		}
	case "linux":
		if err := exec.Command("systemctl", "--user", "restart", systemdUnit).Run(); err != nil {
			return fmt.Errorf("restart systemd service: %w", err)
		}
	default:
		return fmt.Errorf("restart is only supported on macOS and Linux")
	}
	return nil
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
