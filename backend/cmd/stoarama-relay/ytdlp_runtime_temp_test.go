package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

func TestServiceTemplatesKeepPrivateTempDefaultOffAndPersistOptIn(t *testing.T) {
	for _, template := range []struct {
		name string
		path string
		data func() any
	}{
		{
			name: "launchd",
			path: "templates/launchd.plist.tmpl",
			data: func() any { return launchdTemplateData("label", "/relay", "/log", "instance", false) },
		},
		{
			name: "systemd",
			path: "templates/systemd.service.tmpl",
			data: func() any { return systemdTemplateData("/relay") },
		},
	} {
		t.Run(template.name, func(t *testing.T) {
			t.Setenv(capture.YTDLPPrivateTempEnv, "")
			off, err := executeTemplate(template.path, template.data())
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(off), capture.YTDLPPrivateTempEnv) {
				t.Fatalf("default-off template persisted opt-in:\n%s", off)
			}

			t.Setenv(capture.YTDLPPrivateTempEnv, "1")
			on, err := executeTemplate(template.path, template.data())
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(on), capture.YTDLPPrivateTempEnv) {
				t.Fatalf("opt-in template omitted gate:\n%s", on)
			}
		})
	}
}

func TestPrepareYTDLPPrivateTempIsDefaultOff(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv(capture.YTDLPPrivateTempEnv, "")
	t.Setenv(capture.YTDLPRuntimeTempRootEnv, "parent-value")
	if err := prepareYTDLPPrivateTemp("run"); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv(capture.YTDLPRuntimeTempRootEnv); got != "parent-value" {
		t.Fatalf("default-off root env changed to %q", got)
	}
	if _, err := os.Stat(filepath.Join(home, ".stoarama")); !os.IsNotExist(err) {
		t.Fatalf("default-off mode changed filesystem: %v", err)
	}
}

func TestPrepareYTDLPPrivateTempCreatesPrivateRelayRoot(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv(capture.YTDLPPrivateTempEnv, "1")
	t.Setenv(capture.YTDLPRuntimeTempRootEnv, "")
	if err := prepareYTDLPPrivateTemp("run"); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(home, ".stoarama", "tmp", "yt-dlp-runtime")
	if got := os.Getenv(capture.YTDLPRuntimeTempRootEnv); got != root {
		t.Fatalf("root env=%q want %q", got, root)
	}
	info, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("root mode=%v", info.Mode())
	}
}
