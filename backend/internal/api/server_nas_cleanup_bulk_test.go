package api

import (
	"fmt"
	"strings"
	"testing"
)

func TestNASCleanupBulkDurabilitySessionGates(t *testing.T) {
	for _, v := range []string{nasCleanupBulkDurabilityClientVersion, nasCleanupR2CopyClientVersion, nasCleanupItemTruthClientVersion} {
		t.Run(v, func(t *testing.T) { testNASCleanupReviewedReleaseGates(t, v) })
	}
}

func TestNASCleanupBulkDurabilityRequiresCompleteFreshLease(t *testing.T) {
	for _, v := range []string{nasCleanupBulkDurabilityClientVersion, nasCleanupR2CopyClientVersion, nasCleanupItemTruthClientVersion} {
		t.Run(v, func(t *testing.T) { testNASCleanupBulkDurabilityRequiresCompleteFreshLease(t, v) })
	}
}
func testNASCleanupBulkDurabilityRequiresCompleteFreshLease(t *testing.T, version string) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("bulk-durability", syntheticFiles(100, 20, 20))
	f.dryRun()
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version=$1 WHERE id=13`, version); err != nil {
		t.Fatal(err)
	}
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable %d %s", code, body)
	}
	tasks := groupLease(t, f, strings.Repeat("a", 32), version)
	if len(tasks) != 8 {
		t.Fatalf("lease size %d", len(tasks))
	}
	generation := 0
	observe := func(part []cleanupTask) (int, string) {
		generation++
		obs := []map[string]any{}
		for _, task := range part {
			obs = append(obs, map[string]any{"item_id": task.ItemID, "attempt": task.Attempt, "plan_sha256": sha, "outcome": "match", "proof": fixtureProof(task.Item), "error": "", "client_version": version, "reconciliation_protocol": 1, "request_id": fmt.Sprintf("%016x%016x", generation, task.ItemID)})
		}
		w := cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": version, "execution_batch": true, "observations": obs}, "fixture-boot "+version)
		return w.Code, w.Body.String()
	}
	// An omitted member remains part of this lease even after it ceases to be busy.
	for _, state := range []string{"leased", "pending", "unverified"} {
		if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state=$1 WHERE id=$2`, state, tasks[7].ItemID); err != nil {
			t.Fatal(err)
		}
		if code, body := observe(tasks[:7]); code != 409 {
			t.Fatalf("omitted %s member: %d %s", state, code, body)
		}
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='leased' WHERE id=$1`, tasks[7].ItemID); err != nil {
		t.Fatal(err)
	}
	// The full roster must also be one exact session and lease request.
	for _, column := range []string{"lease_request_id", "lease_client_boot_id", "lease_client_version"} {
		var original string
		if err := f.pool.QueryRow(f.ctx, "SELECT "+column+" FROM nas_cleanup_items WHERE id=$1", tasks[7].ItemID).Scan(&original); err != nil {
			t.Fatal(err)
		}
		value := "foreign"
		if column == "lease_request_id" {
			value = strings.Repeat("b", 32)
		}
		if _, err := f.pool.Exec(f.ctx, "UPDATE nas_cleanup_items SET "+column+"=$1 WHERE id=$2", value, tasks[7].ItemID); err != nil {
			t.Fatal(err)
		}
		if code, body := observe(tasks); code != 409 {
			t.Fatalf("foreign %s: %d %s", column, code, body)
		}
		if _, err := f.pool.Exec(f.ctx, "UPDATE nas_cleanup_items SET "+column+"=$1 WHERE id=$2", original, tasks[7].ItemID); err != nil {
			t.Fatal(err)
		}
	}
	var issued int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND delete_ever_issued`, id).Scan(&issued); err != nil || issued != 0 {
		t.Fatalf("invalid roster issued %d %v", issued, err)
	}
	if code, body := observe(tasks); code != 200 || strings.Count(body, `"delete":true`) != 8 || strings.Count(body, `"delivery_keep_checked":true`) != 8 {
		t.Fatalf("bulk permit: %d %s", code, body)
	}
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND delete_ever_issued`, id).Scan(&issued); err != nil || issued != 8 {
		t.Fatalf("bulk issued %d %v", issued, err)
	}
	// Already-issued authority must never be refreshed by another bulk request.
	if code, body := observe(tasks); code != 409 {
		t.Fatalf("repeat bulk authority %d %s", code, body)
	}
	if more := groupLease(t, f, strings.Repeat("c", 32), version); len(more) != 0 {
		t.Fatal("unacknowledged bulk group lost execution slot")
	}
}
