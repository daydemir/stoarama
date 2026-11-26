package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The repair ledger export: done rows need a repaired/ key, size and both
// sha256s; every row names one clip once.
func TestReadRepairLedger(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "ledger.tsv")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	const header = "clip_id\tstatus\torig_sha256\trepaired_key\trepaired_size\trepaired_sha256\n"
	a, b := strings.Repeat("a", 64), strings.Repeat("b", 64)
	got, err := readRepairLedger(write(header + "7\tdone\t" + a + "\trepaired/managed/x.mp4\t10\t" + b + "\n8\tfailed\t\t\t\t\n9\tskipped_signature\t\t\t\t\n"))
	if err != nil || len(got) != 3 || got[7] != (clipRepair{status: repairDone, originalSHA: a, key: "repaired/managed/x.mp4", size: 10, sha: b}) || got[8].status != "failed" {
		t.Fatalf("ledger %+v %v", got, err)
	}
	if none, err := readRepairLedger(""); err != nil || len(none) != 0 {
		t.Fatalf("no ledger: %v %v", none, err)
	}
	for name, body := range map[string]string{
		"no header":         "7\tdone\t" + a + "\trepaired/x\t10\t" + b + "\n",
		"twice":             header + "8\tfailed\t\t\t\t\n8\tfailed\t\t\t\t\n",
		"done outside":      header + "7\tdone\t" + a + "\tmanaged/x.mp4\t10\t" + b + "\n",
		"done without size": header + "7\tdone\t" + a + "\trepaired/x.mp4\t\t" + b + "\n",
		"done without sha":  header + "7\tdone\t" + a + "\trepaired/x.mp4\t10\t\n",
		"short row":         header + "7\tdone\n",
	} {
		if _, err := readRepairLedger(write(body)); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}
