package api

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
)

func cleanupPolicyFixture(t *testing.T, name, sha string) nasCleanupPolicy {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if cleanupSHA(raw) != sha {
		t.Fatal("policy fixture lost immutable source binding")
	}
	z, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	plain, err := io.ReadAll(z)
	if err != nil {
		t.Fatal(err)
	}
	var policy nasCleanupPolicy
	if err = json.Unmarshal(plain, &policy); err != nil {
		t.Fatal(err)
	}
	return policy
}
func TestNASCleanupRefreshedPolicyNeverShrinksHistoricalProtection(t *testing.T) {
	old := cleanupPolicyFixture(t, "nas_cleanup_policy_20261101.json.gz", "428620008349a12c468bd3663cf678a1047344c019ef9d53caa8adf6c6ad75a7")
	fresh := cleanupPolicyFixture(t, "nas_cleanup_policy_20261102.json.gz", cleanupPolicySHA)
	key := func(w cleanupWindow) string {
		return fmt.Sprint(w.StreamID, "/", w.Start.UnixNano(), "/", w.End.UnixNano())
	}
	windows := map[string]bool{}
	for _, w := range fresh.Windows {
		windows[key(w)] = true
	}
	for _, w := range old.Windows {
		if !windows[key(w)] {
			t.Fatalf("historical KEEP interval lost: %v", w)
		}
	}
	whole := map[int64]bool{}
	for _, id := range fresh.Whole {
		whole[id] = true
	}
	for _, id := range old.Whole {
		if !whole[id] {
			t.Fatalf("whole-stream KEEP narrowed: %d", id)
		}
	}
	if len(fresh.Windows) != 223 || len(fresh.Streams) != 109 || len(fresh.Whole) != 31 {
		t.Fatal("incomplete refreshed generation")
	}
	raw, err := os.ReadFile("testdata/nas_cleanup_periods_20261102.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	if cleanupSHA(raw) != "a084bfb8997123f144bff86882a4da2d09919b19cbc410e23576f57cc4b76f95" {
		t.Fatal("snapshot SHA")
	}
	z, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	var snapshot struct {
		Streams []struct {
			ID        int64    `json:"stream_id"`
			Timezones []string `json:"timezones"`
			Data      struct {
				Rankings map[string]string `json:"rankings"`
			} `json:"data"`
		} `json:"streams"`
	}
	if err = json.NewDecoder(z).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Streams) != 109 {
		t.Fatal("missing refreshed stream")
	}
	expected := map[int64]map[string]string{}
	for _, r := range snapshot.Streams {
		expected[r.ID] = r.Data.Rankings
	}
	for _, r := range fresh.Streams {
		a, _ := json.Marshal(r.Rankings)
		b, _ := json.Marshal(expected[r.ID])
		if !nasSpaceAuditJSONEqual(a, b) {
			t.Fatalf("winner differs from frozen snapshot: %d", r.ID)
		}
	}
}
