package api

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchproof"
	"github.com/jackc/pgx/v5/pgtype"
)

func syntheticNASStitchReadback(t *testing.T) (nasStitchReadbackReceipt, nasStitchReadbackFacts, time.Time) {
	t.Helper()
	policy, err := nasStitchReadbackPolicy()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	const id = "11111111-1111-4111-8111-111111111111"
	receipt := nasStitchReadbackReceipt{
		Schema: nasStitchReadbackSchema, NativeRunID: id, ID: id,
		NativeBundleSHA: strings.Repeat("a", 64), NativePlanSHA: strings.Repeat("b", 64),
		ProducerProfileSHA: strings.Repeat("c", 64), ReaderProfileSHA: strings.Repeat("d", 64),
		PairSHA: strings.Repeat("e", 64), NativeRegistration: strings.Repeat("f", 64),
		StartedAt: now.Add(-10 * time.Second).Format(time.RFC3339Nano), VerifiedAt: now.Format(time.RFC3339Nano),
		Reader: nasStitchReaderExecution{ConnectionID: 13, AccountID: 47, BootID: id,
			Version: nasCleanupR2CopyControlClientVersion, ArtifactSHA: strings.Repeat("a", 64), ArtifactSize: 2,
			Host: stitchproof.RunHost{ConnectionID: 13, System: "Linux", Machine: "x86_64", CpuModel: "SYNTHETIC", CpuCount: 1}},
		Outputs: []stitchproof.Media{{RelativePath: "SYNTHETIC/proof.mp4", SHA256: strings.Repeat("b", 64), SizeBytes: 1}},
		Policy:  policy, Tools: nasStitchReadbackTools(),
	}
	observed := pgtype.Timestamptz{Time: now, Valid: true}
	facts := nasStitchReadbackFacts{HeartbeatAt: observed, HostAt: observed, StatusAt: observed,
		InventoryMismatches: pgtype.Int8{Int64: 0, Valid: true},
		Host: nasHostFacts{System: "Linux", Machine: "x86_64", CPUModel: "SYNTHETIC", CPUCount: 1,
			Release: &nasReleaseHealth{Stable: true, Candidate: false}},
	}
	return receipt, facts, now
}

func TestNASStitchReadbackProtocolStrictWireAndPipeline(t *testing.T) {
	receipt, _, _ := syntheticNASStitchReadback(t)
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := decodeNASStitchReadback(raw)
	if err != nil || parsed.Reader != receipt.Reader {
		t.Fatalf("explicit synthetic wire did not reach strict decoder: %v", err)
	}
	for _, mutate := range []func(map[string]any){
		func(v map[string]any) { delete(v, "native_registration_sha256") },
		func(v map[string]any) { v["reader_version_mismatch_count"] = 0 },
		func(v map[string]any) { v["passed"] = true },
		func(v map[string]any) { v["schema"] = "unknown" },
		func(v map[string]any) { v["native_run_id"] = "00000000-0000-0000-0000-000000000000" },
		func(v map[string]any) { v["started_at"] = "2026-11-15T00:00:01Z" },
		func(v map[string]any) { v["verified_at"] = "2026-02-30T00:00:00Z" },
		func(v map[string]any) { v["verified_at"] = "2026-11-15T00:00:00+00:00" },
		func(v map[string]any) { v["outputs"] = []any{} },
	} {
		var value map[string]any
		if json.Unmarshal(raw, &value) != nil {
			t.Fatal("synthetic fixture unreadable")
		}
		mutate(value)
		changed, _ := json.Marshal(value)
		if _, err := decodeNASStitchReadback(changed); err == nil {
			t.Fatal("mutated wire accepted")
		}
	}
	for _, changed := range []string{
		strings.Replace(string(raw), `"client_version":`, `"client_version":"development","client_version":`, 1),
		strings.Replace(string(raw), `"client_artifact_size_bytes":2`, `"client_artifact_size_bytes":2.0`, 1),
		strings.Replace(string(raw), `"cpu_count":1`, `"cpu_count":true`, 1),
		strings.Replace(string(raw), `SYNTHETIC/proof.mp4`, `../proof.mp4`, 1),
		strings.Replace(string(raw), `SYNTHETIC/proof.mp4`, `SYNTHETIC//proof.mp4`, 1),
		strings.Replace(string(raw), `"package_manifest_sha256":"`+nasStitchManifestSHA+`"`, `"package_manifest_sha256":"`+strings.Repeat("0", 64)+`"`, 1),
	} {
		if changed == string(raw) {
			t.Fatal("negative test did not change the intended field")
		}
		if _, err := decodeNASStitchReadback([]byte(changed)); err == nil {
			t.Fatal("duplicate, untyped, traversal or package substitution accepted")
		}
	}
}

func TestNASStitchReadbackReaderFactsUseActualInventoryAndIndependentIdentity(t *testing.T) {
	receipt, facts, now := syntheticNASStitchReadback(t)
	expected := receipt.Reader
	if err := validateNASStitchReadbackReader(receipt, expected, facts, now); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*nasStitchReadbackFacts){
		func(f *nasStitchReadbackFacts) { f.InventoryMismatches.Valid = false },
		func(f *nasStitchReadbackFacts) { f.InventoryMismatches.Int64 = 1 },
		func(f *nasStitchReadbackFacts) { f.InventoryMismatches.Int64 = -1 },
		func(f *nasStitchReadbackFacts) { f.Host.Release = nil },
		func(f *nasStitchReadbackFacts) { f.Host.Release = &nasReleaseHealth{Stable: false} },
		func(f *nasStitchReadbackFacts) { f.Host.Release = &nasReleaseHealth{Stable: true, Candidate: true} },
		func(f *nasStitchReadbackFacts) { f.Host.CPUModel = "different" },
		func(f *nasStitchReadbackFacts) { f.StatusAt.Valid = false },
		func(f *nasStitchReadbackFacts) { f.HeartbeatAt.Time = now.Add(-120*time.Second - time.Nanosecond) },
		func(f *nasStitchReadbackFacts) { f.HostAt.Time = now.Add(-120*time.Second - time.Nanosecond) },
		func(f *nasStitchReadbackFacts) { f.StatusAt.Time = now.Add(5*time.Minute + time.Nanosecond) },
	} {
		changed := facts
		mutate(&changed)
		if err := validateNASStitchReadbackReader(receipt, expected, changed, now); err == nil {
			t.Fatal("missing/nonzero actual inventory, release, host or freshness accepted")
		}
		if err := validateNASStitchReadbackReader(receipt, expected, facts, now); err != nil {
			t.Fatal("negative fixture contaminated the positive case")
		}
	}
	for _, mutate := range []func(*nasStitchReaderExecution){
		func(r *nasStitchReaderExecution) { r.Version = "development" },
		func(r *nasStitchReaderExecution) { r.ArtifactSHA = strings.Repeat("0", 64) },
		func(r *nasStitchReaderExecution) { r.ArtifactSize++ },
		func(r *nasStitchReaderExecution) { r.BootID = "22222222-2222-4222-8222-222222222222" },
	} {
		changed := expected
		mutate(&changed)
		if facts.InventoryMismatches.Int64 != 0 || !facts.InventoryMismatches.Valid {
			t.Fatal("source counter unexpectedly changed")
		}
		if err := validateNASStitchReadbackReader(receipt, changed, facts, now); err == nil {
			t.Fatal("inventory zero substituted for exact current version/artifact/boot")
		}
	}
}

func TestNASStitchReadbackFreshnessBoundariesUseSuppliedDBTime(t *testing.T) {
	receipt, facts, now := syntheticNASStitchReadback(t)
	facts.HeartbeatAt.Time = now.Add(-120 * time.Second)
	if err := validateNASStitchReadbackReader(receipt, receipt.Reader, facts, now); err != nil {
		t.Fatal(err)
	}
	receipt.VerifiedAt = now.Add(-24 * time.Hour).Format(time.RFC3339Nano)
	receipt.StartedAt = now.Add(-24*time.Hour - 10*time.Second).Format(time.RFC3339Nano)
	if err := validateNASStitchReadbackReader(receipt, receipt.Reader, facts, now); err != nil {
		t.Fatal(err)
	}
	receipt.VerifiedAt = now.Add(-24*time.Hour - time.Nanosecond).Format(time.RFC3339Nano)
	if err := validateNASStitchReadbackReader(receipt, receipt.Reader, facts, now); err == nil {
		t.Fatal("expired readback accepted")
	}
	receipt.VerifiedAt = now.Add(5 * time.Minute).Format(time.RFC3339Nano)
	receipt.StartedAt = now.Add(5*time.Minute - 10*time.Second).Format(time.RFC3339Nano)
	if err := validateNASStitchReadbackReader(receipt, receipt.Reader, facts, now); err != nil {
		t.Fatal(err)
	}
	receipt.VerifiedAt = now.Add(5*time.Minute + time.Nanosecond).Format(time.RFC3339Nano)
	if err := validateNASStitchReadbackReader(receipt, receipt.Reader, facts, now); err == nil {
		t.Fatal("future readback accepted")
	}
	if err := validateNASStitchReadbackReader(receipt, receipt.Reader, facts, time.Time{}); err == nil {
		t.Fatal("missing DB time defaulted to process time")
	}
}
