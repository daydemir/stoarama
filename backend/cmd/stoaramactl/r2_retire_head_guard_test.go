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
	proof := r2RetireWriterProof{Bucket: h.Bucket, EndpointSHA256: r2tSHA, ManifestSHA256: opts.ExpectedSHA, CreatedAt: now, ExpiresAt: now.Add(time.Hour), KeepStoppedThroughExpiry: true, ActiveWriterPrefixes: []string{}, Writers: map[string]r2RetireWriterDrain{}}
	for _, name := range r2RetireWriterInventory {
		proof.Writers[name] = r2RetireWriterDrain{StoppedAt: now.Add(-2 * time.Hour), DrainedAt: now.Add(-time.Hour), CapabilitiesExpiredAt: now.Add(-time.Hour), MultipartUploadsDrained: true, DeployedRevision: "test-revision", Evidence: "test fixture; all jobs stopped and capabilities expired"}
	}
	raw, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	opts.WriterProofPath = filepath.Join(t.TempDir(), "writers.json")
	opts.WriterProofSHA256 = r2SHA(raw)
	if err := os.WriteFile(opts.WriterProofPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	raw = []byte(`{"bucket":"` + h.Bucket + `","endpoint_sha256":"` + r2tSHA + `","created_at":"` + now.Format(time.RFC3339Nano) + `","conditional_delete_unsupported":true,"wrong_etag_delete_succeeded":true,"wrong_etag_delete_confirmed_absent":true,"probe_key":"r2-retire-probes/test.bin"}`)
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

func TestR2RetireHeadGuardWriterEvidence(t *testing.T) {
	h := r2RetireHeader{Bucket: "stoarama"}
	opts := r2RetireApplyOptions{ExpectedSHA: r2tSHA}
	writeGuardProofs(t, &opts, h)
	raw, _ := os.ReadFile(opts.WriterProofPath)
	check := func() error { return checkR2RetireWriterProof(opts, h, r2tSHA, r2tSHA, "managed/acct-47/a.mp4") }
	if err := check(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*r2RetireWriterProof){
		func(p *r2RetireWriterProof) { p.Bucket = "other" },
		func(p *r2RetireWriterProof) { p.EndpointSHA256 = strings.Repeat("b", 64) },
		func(p *r2RetireWriterProof) { p.ManifestSHA256 = strings.Repeat("b", 64) },
		func(p *r2RetireWriterProof) { p.CreatedAt = time.Now().Add(time.Minute) },
		func(p *r2RetireWriterProof) { p.CreatedAt = time.Now().Add(-25 * time.Hour) },
		func(p *r2RetireWriterProof) { p.ExpiresAt = time.Now().Add(-time.Second) },
		func(p *r2RetireWriterProof) { p.ExpiresAt = p.CreatedAt.Add(25 * time.Hour) },
		func(p *r2RetireWriterProof) { p.KeepStoppedThroughExpiry = false },
		func(p *r2RetireWriterProof) { delete(p.Writers, "repair") },
		func(p *r2RetireWriterProof) { p.Writers["unknown"] = p.Writers["repair"] },
		func(p *r2RetireWriterProof) {
			w := p.Writers["recorder"]
			w.MultipartUploadsDrained = false
			p.Writers["recorder"] = w
		},
		func(p *r2RetireWriterProof) {
			w := p.Writers["recorder"]
			w.CapabilitiesExpiredAt = time.Now().Add(time.Hour)
			p.Writers["recorder"] = w
		},
		func(p *r2RetireWriterProof) { p.ActiveWriterPrefixes = nil },
		func(p *r2RetireWriterProof) { p.ActiveWriterPrefixes = []string{"managed/acct-47/"} },
		func(p *r2RetireWriterProof) { p.ActiveWriterPrefixes = []string{"managed/acct"} },
		func(p *r2RetireWriterProof) { p.ActiveWriterPrefixes = []string{""} },
	} {
		var p r2RetireWriterProof
		json.Unmarshal(raw, &p)
		mutate(&p)
		changed, _ := json.Marshal(p)
		os.WriteFile(opts.WriterProofPath, changed, 0600)
		opts.WriterProofSHA256 = r2SHA(changed)
		if check() == nil {
			t.Fatal("unsafe writer evidence accepted")
		}
	}
	os.WriteFile(opts.WriterProofPath, []byte(`{}`), 0600)
	opts.WriterProofSHA256 = r2SHA(raw)
	if check() == nil {
		t.Fatal("changed evidence accepted")
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
