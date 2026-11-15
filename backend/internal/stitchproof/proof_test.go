package stitchproof

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestR2StitchStrictFrozenFields(t *testing.T) {
	valid := []byte(`{"schema":"stoarama.nas-stitch-attestation.v1","synthetic":false,"payload":{"file":"payload.json","sha256":"a","size_bytes":1},"independent_review":{"file":"review.json","sha256":"b","size_bytes":1}}`)
	var out Attestation
	if err := decode(valid, &out); err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{
		"duplicate":           bytes.Replace(valid, []byte(`"synthetic":false`), []byte(`"synthetic":true,"synthetic":false`), 1),
		"escaped duplicate":   bytes.Replace(valid, []byte(`"synthetic":false`), []byte(`"synthetic":true,"\u0073ynthetic":false`), 1),
		"missing false field": bytes.Replace(valid, []byte(`"synthetic":false,`), nil, 1),
		"case alias":          bytes.Replace(valid, []byte(`"schema"`), []byte(`"Schema"`), 1),
		"unknown nested":      bytes.Replace(valid, []byte(`"size_bytes":1`), []byte(`"size_bytes":1,"other":true`), 1),
		"null":                bytes.Replace(valid, []byte(`"synthetic":false`), []byte(`"synthetic":null`), 1),
		"wrong type":          bytes.Replace(valid, []byte(`"synthetic":false`), []byte(`"synthetic":0`), 1),
		"fractional integer":  bytes.Replace(valid, []byte(`"size_bytes":1`), []byte(`"size_bytes":1.5`), 1),
		"integer overflow":    bytes.Replace(valid, []byte(`"size_bytes":1`), []byte(`"size_bytes":9223372036854775808`), 1),
		"extra object":        append(bytes.Clone(valid), []byte(` {}`)...),
		"nonfinite":           bytes.Replace(valid, []byte(`"size_bytes":1`), []byte(`"size_bytes":NaN`), 1),
		"invalid utf8":        append(bytes.Clone(valid), 0xff),
	} {
		t.Run(name, func(t *testing.T) {
			if err := decode(raw, &out); err == nil {
				t.Fatal("invalid evidence accepted")
			}
		})
	}
}

func canonicalTemp(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestR2StitchSnapshotsRejectLinksDriftAndDuplicateReferences(t *testing.T) {
	dir := canonicalTemp(t)
	raw := []byte("immutable byte copy")
	if err := os.WriteFile(filepath.Join(dir, "evidence"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	ref := File{"evidence", digest(raw), int64(len(raw))}
	s := snapshot{dir: dir, seen: map[string]bool{}}
	copy, err := s.read(ref, smallLimit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.read(ref, smallLimit); err == nil {
		t.Fatal("duplicate reference accepted")
	}
	if err := os.WriteFile(filepath.Join(dir, "evidence"), []byte("replacement bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if string(copy) != string(raw) {
		t.Fatal("owned evidence copy changed")
	}
	s = snapshot{dir: dir, seen: map[string]bool{}}
	if _, err := s.read(ref, smallLimit); err == nil {
		t.Fatal("changed bytes accepted")
	}
	if err := os.Symlink("evidence", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if f, err := openRegular(dir, "link"); err == nil {
		f.Close()
		t.Fatal("symlink accepted")
	}
	if err := os.Symlink(dir, filepath.Join(dir, "alias")); err != nil {
		t.Fatal(err)
	}
	if f, err := openRegular(filepath.Join(dir, "alias"), "evidence"); err == nil {
		f.Close()
		t.Fatal("symlink ancestor accepted")
	}
	for _, name := range []string{"../evidence", "a/../evidence", "a//evidence", "/evidence", ".", "a\\evidence"} {
		if f, err := openRegular(dir, name); err == nil {
			f.Close()
			t.Fatalf("noncanonical %q accepted", name)
		}
	}
}

func TestR2StitchSyntheticOrUnmappedArtifactNeverAuthorizes(t *testing.T) {
	dir := canonicalTemp(t)
	for _, synthetic := range []bool{true, false} {
		raw, _ := json.Marshal(Attestation{Schema: "stoarama.nas-stitch-attestation.v1", Synthetic: synthetic, Payload: File{"missing", strings.Repeat("a", 64), 1}, IndependentReview: File{"review", strings.Repeat("b", 64), 1}})
		name := filepath.Join(dir, "attestation.json")
		if err := os.WriteFile(name, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if a, err := Verify(context.Background(), name, digest(raw), time.Now().UTC(), time.Now()); err == nil || a != nil {
			t.Fatal("synthetic/unmapped artifact authorized")
		}
	}
}

func TestR2StitchMonotonicExpiryAndManifestBinding(t *testing.T) {
	if new(Authority).Check() == nil {
		t.Fatal("zero authority accepted")
	}
	// Private test state only; no artifact constructor or wire-delete caller can
	// obtain this value. Exercise expiry arithmetic without pretending a NAS run.
	now := time.Now()
	a := Authority{utc: now.UTC(), observed: now.Add(-2 * time.Hour), provenance: Provenance{OuterSHA: strings.Repeat("a", 64), ExpiresAt: now.UTC().Add(time.Hour)}}
	if a.Check() == nil {
		t.Fatal("monotonic elapsed did not expire authority")
	}
	a.observed = now
	if a.RequireWire("stoarama", strings.Repeat("b", 64)) == nil {
		t.Fatal("inspection-only authority used for retirement")
	}
	b, err := a.BindRetirement(strings.Repeat("c", 64), now.Add(-23*time.Hour), MaxAge, "stoarama", strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	if b.provenance.ExpiresAt.After(now.Add(time.Hour)) || !a.provenance.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatal("binding extended proof")
	}
	if _, err := a.BindRetirement(strings.Repeat("c", 64), now.Add(time.Hour), MaxAge, "stoarama", strings.Repeat("b", 64)); err == nil {
		t.Fatal("future manifest accepted")
	}
	if _, err := a.BindRetirement(strings.Repeat("c", 64), now, MaxAge+time.Second, "stoarama", strings.Repeat("b", 64)); err == nil {
		t.Fatal("long age bound accepted")
	}
}

func TestR2StitchGzipLimitsAndExactRegistrationShape(t *testing.T) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte(`{"result":{"outputs":[]}}`))
	w.Close()
	raw, err := inflate(b.Bytes())
	if err != nil || string(raw) != `{"result":{"outputs":[]}}` {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{append(bytes.Clone(b.Bytes()), 'x'), append(bytes.Clone(b.Bytes()), b.Bytes()...), b.Bytes()[:b.Len()-1]} {
		if _, err := inflate(bad); err == nil {
			t.Fatal("trailing/concatenated/truncated gzip accepted")
		}
	}
	b.Reset()
	w = gzip.NewWriter(&b)
	w.Write(bytes.Repeat([]byte("x"), 16<<20+1))
	w.Close()
	if _, err := inflate(b.Bytes()); err == nil {
		t.Fatal("oversized native receipt accepted")
	}
}

func TestR2StitchExecutableSubstitutionFailsBeforeReplay(t *testing.T) {
	if _, err := replay(context.Background(), []byte("raise Exception('substituted')"), nil, nil, nil, strings.Repeat("a", 64), strings.Repeat("b", 64)); err == nil {
		t.Fatal("unpinned verifier accepted")
	}
}

func TestR2StitchFrozenProducerContracts(t *testing.T) {
	for file, want := range map[string]string{
		"attestation.schema.json":    "03d9a5fa930b67249036a6b522dcffa59f6bba9744f79abc0747f85d9ee63abc",
		"native-receipt.schema.json": "18fffd8f94334210eb12ccee8c0856a824cd9ae9c3dc97b6fe6124e99a0906d4",
	} {
		raw, err := os.ReadFile("../../../clients/nas-pull/stitch-proof/" + file)
		if err != nil || digest(raw) != want {
			t.Fatal("frozen contract changed without reviewed integration", file, err)
		}
	}
	raw, err := os.ReadFile("../../../clients/nas-pull/verify_stitch_proof.py")
	if err != nil || digest(raw) != VerifierSHA {
		t.Fatal("executing verifier source differs from reviewed consumer pin", err)
	}
	for _, media := range []string{"joined/서울 카메라/October/08/proof.mp4", "a/with space.mp4"} {
		if !canonicalMedia(media) {
			t.Fatal("valid NAS identity rejected", media)
		}
		if canonicalFile(media) {
			t.Fatal("NAS media identity mistaken for artifact reference")
		}
	}
}

// These are cross-file join fixtures, not native replay evidence. They call only
// the private binding validator and can never create an Authority or send a DELETE.
func TestR2StitchCrossFileIdentityBindings(t *testing.T) {
	fixture := func() (Payload, Validation, []byte, []byte, Replay) {
		sha := strings.Repeat("a", 64)
		p := Payload{
			Scope:    PayloadScope{ProofPlanSHA256: sha},
			Pipeline: Pipeline{ClientVersion: "v85r-nas-proof-v1"},
			Run:      Run{ID: "79dc929e-087f-4fbf-9317-8e0e0994d132", ClientBootID: "bc24648c-f613-421c-8989-6d43c245417e", Host: RunHost{ConnectionID: 13, System: "Linux", Machine: "x86_64", CpuModel: "test-only", CpuCount: 4}, StartedAt: "2026-11-15T00:00:00Z", CompletedAt: "2026-11-15T00:01:00Z", MediaSeconds: 120, BuildWallSeconds: 60},
			Inputs:   []Input{{373218, "input/a.mp4", sha, 10}, {373262, "input/b.mp4", sha, 20}},
			Outputs:  []Output{{"joined/test.mp4", sha, 30, []int64{373218, 373262}, sha}},
			Evidence: PayloadEvidence{NativeBundle: File{SHA256: strings.Repeat("b", 64)}},
		}
		result, _ := json.Marshal(map[string]any{"outputs": p.Outputs})
		p.Verification.ResultSHA256 = digest(result)
		raw, _ := json.Marshal(map[string]any{"run": p.Run, "client_version": p.Pipeline.ClientVersion, "client_artifact_sha256": ClientSHA, "plan_sha256": sha, "source_plan_sha256": sourcePlanSHA, "result": json.RawMessage(result), "build_wall_seconds": 60, "verified_at": "2026-11-15T00:01:01Z"})
		clips := make([]map[string]any, len(p.Inputs))
		for i, in := range p.Inputs {
			clips[i] = map[string]any{"source_id": in.ClipID, "relative_path": in.RelativePath, "sha256": in.SHA256, "size_bytes": in.SizeBytes}
		}
		plan, _ := json.Marshal(map[string]any{"plan_sha256": sha, "plan": map[string]any{"clips": clips}})
		v := Validation{Schema: "stoarama.nas-stitch-validation.v1", RunID: p.Run.ID, ProofPlanSHA256: sha, ClientArtifactSHA256: ClientSHA, ServerBinding: ValidationServerBinding{true, "nas-only-v85r-proof-v1", sha, sha, true, false}, NasReadback: ValidationNasReadback{VerifiedAt: "2026-11-15T00:01:02Z", ClientBootID: p.Run.ClientBootID, RunID: p.Run.ID, ReceiptSHA256: p.Evidence.NativeBundle.SHA256, Outputs: []Media{{"joined/test.mp4", sha, 30}}}}
		replayed := Replay{"stoarama.nas-stitch-native-replay.v1", "passed", 13, p.Run.ID, p.Evidence.NativeBundle.SHA256, sha, ClientSHA, sha, 30, 120, 60, 2}
		return p, v, raw, plan, replayed
	}
	p, v, raw, plan, replayed := fixture()
	if err := bindings(p, v, Review{}, raw, plan, replayed); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Payload, *Validation, *Replay){
		"run":                    func(p *Payload, _ *Validation, _ *Replay) { p.Run.ID = "another-run" },
		"original boot":          func(p *Payload, _ *Validation, _ *Replay) { p.Run.ClientBootID = "another-boot" },
		"input identity":         func(p *Payload, _ *Validation, _ *Replay) { p.Inputs[0].ClipID++ },
		"input size":             func(p *Payload, _ *Validation, _ *Replay) { p.Inputs[1].SizeBytes++ },
		"input order":            func(p *Payload, _ *Validation, _ *Replay) { p.Inputs[0], p.Inputs[1] = p.Inputs[1], p.Inputs[0] },
		"output path":            func(p *Payload, _ *Validation, _ *Replay) { p.Outputs[0].RelativePath = "other.mp4" },
		"output readback":        func(p *Payload, _ *Validation, _ *Replay) { p.Outputs[0].ReadbackSHA256 = strings.Repeat("c", 64) },
		"result digest":          func(p *Payload, _ *Validation, _ *Replay) { p.Verification.ResultSHA256 = strings.Repeat("c", 64) },
		"wall measurement":       func(p *Payload, _ *Validation, _ *Replay) { p.Run.BuildWallSeconds++ },
		"readback before native": func(_ *Payload, v *Validation, _ *Replay) { v.NasReadback.VerifiedAt = "2026-11-15T00:00:00Z" },
		"readback run":           func(_ *Payload, v *Validation, _ *Replay) { v.NasReadback.RunID = "another-run" },
		"readback output":        func(_ *Payload, v *Validation, _ *Replay) { v.NasReadback.Outputs[0].SizeBytes++ },
		"readback boot":          func(_ *Payload, v *Validation, _ *Replay) { v.NasReadback.ClientBootID = "not-a-uuid" },
		"server grant":           func(_ *Payload, v *Validation, _ *Replay) { v.ServerBinding.GrantsRetirement = true },
		"pipeline drift":         func(_ *Payload, v *Validation, _ *Replay) { v.ClientArtifactSHA256 = strings.Repeat("c", 64) },
		"policy drift":           func(_ *Payload, v *Validation, _ *Replay) { v.Policy.Revision = "changed" },
		"tool drift":             func(_ *Payload, v *Validation, _ *Replay) { v.Tools.FFmpeg.SizeBytes++ },
		"replay rate":            func(_ *Payload, _ *Validation, r *Replay) { r.RecordingHoursPerWallHour++ },
		"replay output":          func(_ *Payload, _ *Validation, r *Replay) { r.OutputSizeBytes++ },
	} {
		t.Run(name, func(t *testing.T) {
			p, v, raw, plan, replayed := fixture()
			mutate(&p, &v, &replayed)
			if err := bindings(p, v, Review{}, raw, plan, replayed); err == nil {
				t.Fatal("conflicting cross-file evidence accepted")
			}
		})
	}
}
