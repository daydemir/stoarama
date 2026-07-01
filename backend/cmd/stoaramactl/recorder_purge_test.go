package main

import (
	"strings"
	"testing"
)

// TestReleaseAccountClipsSQLShape pins the retention-release UPDATE: it must set
// released_at, be scoped to a single account's MANAGED clips, and skip clips that
// are already purged or released (idempotent). It must NEVER delete an R2 object or
// set purged_at, since released clips keep their bytes (DENIZ policy).
func TestReleaseAccountClipsSQLShape(t *testing.T) {
	for _, want := range []string{
		"UPDATE recording_clips",
		"SET released_at = now()",
		"r.account_id = $1",
		"sd.managed",
		"c.purged_at IS NULL",
		"c.released_at IS NULL",
	} {
		if !strings.Contains(releaseAccountClipsSQL, want) {
			t.Fatalf("release account clips SQL missing %q", want)
		}
	}
	for _, forbidden := range []string{"DELETE", "purged_at = now()", "purged_at=now()"} {
		if strings.Contains(releaseAccountClipsSQL, forbidden) {
			t.Fatalf("release account clips SQL must not contain %q (release keeps the R2 object + never purges)", forbidden)
		}
	}
}

// TestEligibleReleaseAccountsSQLShape pins the eligibility scan: only managed,
// still-org-visible clips of non-paying accounts past the grace count, so an
// active payer or an already-detached clip never triggers a release.
func TestEligibleReleaseAccountsSQLShape(t *testing.T) {
	for _, want := range []string{
		"sd.managed",
		"c.purged_at IS NULL",
		"c.released_at IS NULL",
		"b.has_payment_method = false",
		"make_interval(days => $1)",
	} {
		if !strings.Contains(eligibleReleaseAccountsSQL, want) {
			t.Fatalf("eligible release accounts SQL missing %q", want)
		}
	}
	if strings.Contains(eligibleReleaseAccountsSQL, "DELETE") {
		t.Fatalf("eligible release accounts SQL must not delete anything")
	}
}

// TestSnapshotManagedStorageExcludesReleased pins that the nightly billing snapshot
// excludes BOTH purged AND released clips, so an org stops being billed for a clip
// the instant it is released (NAS-pulled / delivered / retention-released).
func TestSnapshotManagedStorageExcludesReleased(t *testing.T) {
	for _, want := range []string{
		"sd.managed",
		"c.purged_at IS NULL",
		"c.released_at IS NULL",
	} {
		if !strings.Contains(snapshotManagedStorageSQL, want) {
			t.Fatalf("managed-storage snapshot SQL missing %q", want)
		}
	}
}
