package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
)

// A fresh partition must record the picks digest it actually classified with,
// not the approved one: without the post-read check, a picks file swapped in
// after the listing would be reused as if the owner had approved it.
func TestR2InverseFreshPartitionRequiresTheApprovedPicks(t *testing.T) {
	_, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	w := windows[3]
	picks := r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "none", KeepStartUTC: w.Start.Format("2006-01-02T15:04:05Z07:00"), KeepEndUTC: w.End.Format("2006-01-02T15:04:05Z07:00")}}}
	dir := t.TempDir()
	path := filepath.Join(dir, "picks.json")
	data := []byte(`{"choices":{"3":{"choice":"none"}}}`)
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	read, err := loadR2RetirePicks(path)
	if err != nil {
		t.Fatal(err)
	}
	picks = read
	approved, err := inverseFileSHA(path)
	if err != nil || approved != picks.SHA256 {
		t.Fatal(approved, picks.SHA256, err)
	}
	protection := inverseProtection{Identities: map[string][]inverseIdentity{}, Held: map[string]bool{}, Delivery: map[string]bool{}, KEFolders: map[string]bool{}, Windows: windows, Picks: picks}
	listing := inverseTestListing{}
	out := filepath.Join(dir, "partition")
	sum, err := inverseFreshPartition(context.Background(), listing, protection, "policy", approved, out)
	if err != nil {
		t.Fatal(err)
	}
	if sum.PicksSHA256 != approved {
		t.Fatalf("fresh partition recorded picks %q, want the approved %q", sum.PicksSHA256, approved)
	}
	// A partition whose listing used other picks is refused, not sealed.
	swapped := picks
	swapped.SHA256 = strings.Repeat("f", 64)
	other := inverseProtection{Identities: map[string][]inverseIdentity{}, Held: map[string]bool{}, Delivery: map[string]bool{}, KEFolders: map[string]bool{}, Windows: windows, Picks: swapped}
	if _, err = inverseFreshPartition(context.Background(), listing, other, "policy", approved, filepath.Join(dir, "swapped")); err == nil || !strings.Contains(err.Error(), "approved digest") {
		t.Fatalf("fresh partition used other picks: %v", err)
	}
}

// The CLI requires the picks file on disk to be the approved digest before it
// reads anything: a replacement file must not ride along on the old digest.
func TestR2InverseApprovedPicksDigestIsRequired(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "picks.json")
	if err := os.WriteFile(path, []byte(`{"choices":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	approved, err := inverseFileSHA(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = approvedInversePicks(path, approved); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte(`{"choices":{"3":{"choice":"all"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err = approvedInversePicks(path, approved); err == nil || !strings.Contains(err.Error(), "approved digest") {
		t.Fatalf("swapped picks file accepted: %v", err)
	}
	if err = approvedInversePicks(filepath.Join(dir, "absent.json"), approved); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing picks file: %v", err)
	}
}

// The job CLI refuses a swapped picks file for both a fresh partition and a
// reused one, before it opens a pool, reads a partition or restores one.
func TestR2InverseJobCLIRequiresThePicksDigest(t *testing.T) {
	if args := os.Getenv("STOARAMA_TEST_R2_INVERSE_JOB_ARGS"); args != "" {
		runR2Inverse(context.Background(), config.Config{DatabaseURL: os.Getenv("STOARAMA_TEST_DATABASE_URL")}, strings.Split(args, "\n"))
		t.Fatal("r2-inverse returned without stopping")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "picks.json")
	if err := os.WriteFile(path, []byte(`{"choices":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	approved, err := inverseFileSHA(path)
	if err != nil {
		t.Fatal(err)
	}
	// The approved digest is of the file the owner approved; the file on disk
	// has since changed.
	if err = os.WriteFile(path, []byte(`{"choices":{"3":{"choice":"all"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"job", "job-keep28"} {
		reuse := []string{}
		if command == "job" {
			reuse = []string{"--reuse-partition", strings.Repeat("a", 64)}
		} else {
			reuse = []string{"--max-bytes", "10000", "--budget-24h-bytes", "0"}
		}
		work := filepath.Join(dir, "work-"+command)
		args := append([]string{command, "--picks", path, "--expected-picks-sha256", approved, "--owner-decision", "owner.md", "--work", work}, reuse...)
		cmd := exec.Command(os.Args[0], "-test.run=^TestR2InverseJobCLIRequiresThePicksDigest$")
		cmd.Env = append(os.Environ(), "STOARAMA_TEST_R2_INVERSE_JOB_ARGS="+strings.Join(args, "\n"))
		out, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(out), "owner picks differ from the approved digest") {
			t.Fatalf("%s accepted other picks: %v %s", command, err, out)
		}
		if _, statErr := os.Stat(work); !os.IsNotExist(statErr) {
			t.Fatalf("%s created its work directory before refusing: %s", command, out)
		}
	}
}
