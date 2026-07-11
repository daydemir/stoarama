package main

import (
	"os"
	"testing"
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
