package joinedrecording

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestCleanupInactiveLeaseScratchRemovesOnlyAPIProvenLease(t *testing.T) {
	root := privateScratchRoot(t)
	inactive := strings.Repeat("I", 43)
	active := strings.Repeat("A", 43)
	unknown := "operator-notes"
	for _, name := range []string{inactive, active, unknown} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := CleanupInactiveLeaseScratch(context.Background(), root, func(_ context.Context, got []string) (map[string]bool, error) {
		want := []string{active, inactive}
		if !slices.Equal(got, want) {
			t.Fatalf("lease proof request=%v want %v", got, want)
		}
		return map[string]bool{inactive: true, active: false}, nil
	})
	if err != nil || !slices.Equal(removed, []string{inactive}) {
		t.Fatalf("cleanup removed=%v err=%v", removed, err)
	}
	for _, name := range []string{active, unknown} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("retained path %s: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, inactive)); !os.IsNotExist(err) {
		t.Fatalf("inactive lease still exists: %v", err)
	}
}

func TestCleanupInactiveLeaseScratchProofFailureMakesNoChanges(t *testing.T) {
	root := privateScratchRoot(t)
	leaseID := strings.Repeat("L", 43)
	dir := filepath.Join(root, leaseID)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, proof := range []ScratchLeaseProof{
		func(context.Context, []string) (map[string]bool, error) { return nil, errors.New("api unavailable") },
		func(context.Context, []string) (map[string]bool, error) { return map[string]bool{}, nil },
	} {
		if _, err := CleanupInactiveLeaseScratch(context.Background(), root, proof); err == nil {
			t.Fatal("incomplete lease proof was accepted")
		}
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("proof failure changed scratch: %v", err)
		}
	}
}

func TestCleanupInactiveLeaseScratchNeverFollowsSymlink(t *testing.T) {
	root := privateScratchRoot(t)
	leaseID := strings.Repeat("S", 43)
	external := t.TempDir()
	marker := filepath.Join(external, "keep")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(root, leaseID)); err != nil {
		t.Fatal(err)
	}
	if _, err := CleanupInactiveLeaseScratch(context.Background(), root, func(context.Context, []string) (map[string]bool, error) {
		return map[string]bool{leaseID: true}, nil
	}); err == nil {
		t.Fatal("symlink lease scratch was accepted")
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "keep" {
		t.Fatalf("cleanup followed symlink: data=%q err=%v", data, err)
	}
}

func TestCleanupInactiveLeaseScratchResumesFencedDirectory(t *testing.T) {
	root := privateScratchRoot(t)
	leaseID := strings.Repeat("R", 43)
	if err := os.Mkdir(filepath.Join(root, scratchReapPrefix+leaseID), 0o700); err != nil {
		t.Fatal(err)
	}
	removed, err := CleanupInactiveLeaseScratch(context.Background(), root, func(context.Context, []string) (map[string]bool, error) {
		return map[string]bool{leaseID: true}, nil
	})
	if err != nil || !slices.Equal(removed, []string{leaseID}) {
		t.Fatalf("resume cleanup removed=%v err=%v", removed, err)
	}
	if _, err := os.Stat(filepath.Join(root, scratchReapPrefix+leaseID)); !os.IsNotExist(err) {
		t.Fatalf("fenced directory remains: %v", err)
	}
}

func TestCleanupInactiveLeaseScratchChunksProofBeforeMutation(t *testing.T) {
	root := privateScratchRoot(t)
	for i := 0; i < scratchLeaseProofLimit+1; i++ {
		leaseID := strings.Repeat("A", 39) + string(rune('0'+i/100)) + string(rune('0'+i/10%10)) + string(rune('0'+i%10)) + "Z"
		if err := os.Mkdir(filepath.Join(root, leaseID), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	calls := 0
	removed, err := CleanupInactiveLeaseScratch(context.Background(), root, func(_ context.Context, ids []string) (map[string]bool, error) {
		calls++
		if len(ids) > scratchLeaseProofLimit {
			t.Fatalf("proof batch=%d", len(ids))
		}
		out := make(map[string]bool, len(ids))
		for _, id := range ids {
			out[id] = true
		}
		return out, nil
	})
	if err != nil || calls != 2 || len(removed) != scratchLeaseProofLimit+1 {
		t.Fatalf("chunked cleanup calls=%d removed=%d err=%v", calls, len(removed), err)
	}
}

func privateScratchRoot(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "scratch")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}
