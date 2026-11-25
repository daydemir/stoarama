package api

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

// TestGlobalDeliveryBuiltRegistry applies the serving API's public-registry
// rules to a registry written by backend/scripts/gss/build_generation.py. Its
// Python tests set the path; plain `go test ./...` skips it.
func TestGlobalDeliveryBuiltRegistry(t *testing.T) {
	path := os.Getenv("STOARAMA_GSS_BUILT_REGISTRY")
	if path == "" {
		t.Skip("STOARAMA_GSS_BUILT_REGISTRY not set; run by backend/scripts/gss tests")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGlobalDeliveryRegistry(raw)
	if err != nil {
		t.Fatal(err)
	}
	var registry globalDeliveryRegistry
	if err := json.Unmarshal(decoded, &registry); err != nil {
		t.Fatal(err)
	}
	if err := validateGlobalDeliveryRegistry(&registry); err != nil {
		t.Fatal(err)
	}
	if generation, err := hex.DecodeString(registry.Generation); err != nil || len(generation) != 16 {
		t.Fatalf("generation %q is not 16 hex bytes", registry.Generation)
	}
	if !registry.PublicDelivery || registry.SelectionScope != "available" {
		t.Fatal("registry is not a public available generation")
	}
	keep := DeliveryKeepSnapshot{Objects: map[string]DeliveryKeepIdentity{}}
	for name, o := range registry.Assets {
		if !publicGlobalDeliveryAsset(name, registry.RecordingIDs) {
			t.Fatalf("private asset %q", name)
		}
		if err := keep.add(o, "asset"); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range registry.Files {
		if err := keep.add(o, "live_file"); err != nil {
			t.Fatal(err)
		}
	}
}
