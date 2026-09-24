package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

// The private temp root is unconditional, so service templates must not carry
// the retired STOARAMA_YTDLP_PRIVATE_TMP opt-in gate.
func TestServiceTemplatesDoNotCarryRetiredPrivateTempGate(t *testing.T) {
	for _, template := range []struct {
		name string
		path string
		data any
	}{
		{name: "launchd", path: "templates/launchd.plist.tmpl", data: launchdTemplateData("label", "/relay", "/log", "instance", false)},
		{name: "systemd", path: "templates/systemd.service.tmpl", data: systemdTemplateData("/relay")},
	} {
		t.Run(template.name, func(t *testing.T) {
			t.Setenv("STOARAMA_YTDLP_PRIVATE_TMP", "1")
			rendered, err := executeTemplate(template.path, template.data)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(rendered), "STOARAMA_YTDLP_PRIVATE_TMP") {
				t.Fatalf("template persisted retired gate:\n%s", rendered)
			}
		})
	}
}

func TestPrepareYTDLPPrivateTempSkipsNonServiceCommands(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv(capture.YTDLPRuntimeTempRootEnv, "parent-value")
	if err := prepareYTDLPPrivateTemp("version"); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv(capture.YTDLPRuntimeTempRootEnv); got != "parent-value" {
		t.Fatalf("non-service command changed root env to %q", got)
	}
	if _, err := os.Stat(filepath.Join(home, ".stoarama")); !os.IsNotExist(err) {
		t.Fatalf("non-service command changed filesystem: %v", err)
	}
}

func TestPrepareYTDLPPrivateTempCreatesPrivateRelayRootByDefault(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("STOARAMA_YTDLP_PRIVATE_TMP", "")
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
