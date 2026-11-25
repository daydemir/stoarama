package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/clockjoin"
)

func TestClockParentGeneration(t *testing.T) {
	dir := t.TempDir()
	write := func(key, body string) string {
		p := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(body))
		return hex.EncodeToString(sum[:])
	}
	gen := strings.Repeat("a", 32)
	registry := func(growthSHA string) {
		write("gen.json", `{"generation":"`+gen+`","assets":{"/public/growth.json":{"key":"assets/g.json","sha256":"`+growthSHA+`"}},"files":{
		"11":{"key":"joined/b/objects/1.mp4","path":"p/x_hour_01_part_01.mp4","sha256":"s1","size_bytes":5,"recording_id":435},
		"12":{"key":"joined/b/objects/2.mp4","path":"p/x_hour_01_part_02.mp4","sha256":"s2","size_bytes":6,"recording_id":435},
		"sha256:ff":{"key":"experimental/a.mp4","path":"p/x_hour_02.mp4","sha256":"ff","size_bytes":7,"recording_id":435}}}`)
	}
	registry(write("assets/g.json", `{"generation":"`+gen+`","repairs":[{"sha256":"ff","input_ids":[21,22]},{"sha256":"ee","input_ids":[12]}]}`))
	src := clockjoin.DirSource{Root: dir}
	env, err := clockParentGeneration(context.Background(), src, "gen.json")
	if err != nil {
		t.Fatal(err)
	}
	if d := env.Delivered["joined/b/objects/2.mp4"]; d.ID != 12 || d.Path != "p/x_hour_01_part_02.mp4" || d.SizeBytes != 6 || len(env.Delivered) != 2 {
		t.Fatalf("delivered %+v", env.Delivered)
	}
	// Only a live assembly's inputs are taken; a repair no longer listed is not.
	if !env.Assembled[21] || !env.Assembled[22] || env.Assembled[12] || !env.Paths["p/x_hour_02.mp4"] {
		t.Fatalf("assembled %v paths %v", env.Assembled, env.Paths)
	}
	registry(strings.Repeat("0", 64))
	if _, err := clockParentGeneration(context.Background(), src, "gen.json"); err == nil {
		t.Fatal("growth bytes other than the registry's accepted")
	}
	registry(write("assets/g.json", `{"generation":"`+strings.Repeat("b", 32)+`","repairs":[]}`))
	if _, err := clockParentGeneration(context.Background(), src, "gen.json"); err == nil {
		t.Fatal("growth of another generation accepted")
	}
}

func TestReadManifestKeys(t *testing.T) {
	dir := t.TempDir()
	good := "joined/recovery-x/coverage/hours/recovery-x__recording-435__date-2026-09-19__hour-03__generation-2.json"
	p := filepath.Join(dir, "hours.txt")
	if err := os.WriteFile(p, []byte(good+"\n\n"+good+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if keys, err := readManifestKeys(p); err != nil || len(keys) != 1 {
		t.Fatalf("keys %v %v", keys, err)
	}
	for _, bad := range []string{"joined/recovery-x/objects/abc.mp4", "joined/a/coverage/hours/b__recording-1__date-2026-09-19__hour-03__generation-2.json"} {
		if err := os.WriteFile(p, []byte(bad+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readManifestKeys(p); err == nil {
			t.Fatalf("%s accepted", bad)
		}
	}
}
