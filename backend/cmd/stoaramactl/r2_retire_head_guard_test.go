package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

func writeGuardProofs(t *testing.T, opts *r2RetireApplyOptions, h r2RetireHeader) {
	t.Helper()
	now := time.Now().UTC().Add(-time.Second)
	raw := []byte(`{"bucket":"` + h.Bucket + `","endpoint_sha256":"` + r2tSHA + `","created_at":"` + now.Format(time.RFC3339Nano) + `","conditional_delete_unsupported":true,"wrong_etag_delete_succeeded":true,"wrong_etag_delete_confirmed_absent":true,"probe_key":"r2-retire-probes/test.bin"}`)
	opts.DeleteProofPath = filepath.Join(t.TempDir(), "provider.json")
	opts.DeleteProofSHA256 = r2SHA(raw)
	if err := os.WriteFile(opts.DeleteProofPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestR2RetireHeadGuardProofMode(t *testing.T) {
	h := r2RetireHeader{Bucket: "stoarama"}
	opts := r2RetireApplyOptions{ExpectedSHA: r2tSHA}
	writeGuardProofs(t, &opts, h)
	raw, _ := os.ReadFile(opts.DeleteProofPath)
	check := func() error {
		return checkR2RetireDeleteProofMode(opts.DeleteProofPath, opts.DeleteProofSHA256, h.Bucket, r2tSHA, true)
	}
	if err := check(); err != nil {
		t.Fatal(err)
	}
	if err := checkR2RetireDeleteProof(opts.DeleteProofPath, opts.DeleteProofSHA256, h.Bucket, r2tSHA); err == nil {
		t.Fatal("unsupported proof authorized conditional mode")
	}
	for _, change := range []func(map[string]any){
		func(p map[string]any) {
			p["wrong_etag_rejected"] = true
			p["object_preserved"] = true
			p["matching_delete_confirmed"] = true
			p["conditional_delete_unsupported"] = false
		},
		func(p map[string]any) { delete(p, "wrong_etag_delete_confirmed_absent") },
		func(p map[string]any) { p["created_at"] = time.Now().Add(-25 * time.Hour) },
		func(p map[string]any) { p["created_at"] = time.Now().Add(time.Hour) },
		func(p map[string]any) { p["bucket"] = "other" },
		func(p map[string]any) { p["endpoint_sha256"] = strings.Repeat("b", 64) },
		func(p map[string]any) { p["probe_key"] = "managed/footage" },
	} {
		var p map[string]any
		json.Unmarshal(raw, &p)
		change(p)
		changed, _ := json.Marshal(p)
		os.WriteFile(opts.DeleteProofPath, changed, 0600)
		opts.DeleteProofSHA256 = r2SHA(changed)
		if check() == nil {
			t.Fatal("invalid provider evidence accepted")
		}
	}
}

func TestR2RetireHeadGuardUngatedWritersVeto(t *testing.T) {
	for _, key := range []string{"repaired/a", "managed/acct-47/a.mp4", "joined/a", "raw/a", "exports/a", "r2-retire-probes/a"} {
		if err := r2RetireCheckUngatedPrefix(key); err == nil {
			t.Fatalf("ungated credential authority accepted for %s", key)
		}
	}
}

func TestR2RetireHeadGuardExactIdentityAndAge(t *testing.T) {
	now := time.Now()
	age := 7 * 24 * time.Hour
	it := r2RetireItem{r2RetireCandidate: r2RetireCandidate{ETag: "e", SizeBytes: 100, SHA256: r2tSHA}}
	good := r2.ObjectHead{ETag: `"e"`, SizeBytes: 100, SHA256Metadata: r2tSHA, LastModified: now.Add(-age - time.Second)}
	if err := r2RetireValidateGuardHead(good, it, "e", age, now); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*r2.ObjectHead){
		func(h *r2.ObjectHead) { h.ETag = "" }, func(h *r2.ObjectHead) { h.ETag = "x" }, func(h *r2.ObjectHead) { h.SizeBytes++ },
		func(h *r2.ObjectHead) { h.SHA256Metadata = strings.Repeat("b", 64) }, func(h *r2.ObjectHead) { h.LastModified = time.Time{} },
		func(h *r2.ObjectHead) { h.LastModified = now.Add(time.Hour) }, func(h *r2.ObjectHead) { h.LastModified = now.Add(-age) },
	} {
		h := good
		mutate(&h)
		if r2RetireValidateGuardHead(h, it, "e", age, now) == nil {
			t.Fatalf("unsafe HEAD accepted %+v", h)
		}
	}
	for _, catalog := range []string{"", "replacement"} {
		if r2RetireValidateGuardHead(good, it, catalog, age, now) == nil {
			t.Fatal("missing/drifted catalog ETag accepted")
		}
	}
	if r2RetireValidateGuardHead(good, it, "e", time.Hour, now) == nil {
		t.Fatal("minimum age weakened")
	}
	good.SHA256Metadata = ""
	if err := r2RetireValidateGuardHead(good, it, "e", age, now); err != nil {
		t.Fatal("optional absent SHA metadata rejected", err)
	}
}
