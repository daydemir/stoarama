package api

import (
	"context"
	"os"
	"testing"
)

// The cleanup release is one exact immutable literal: it inherits reviewed
// bulk/group execution and the raw-copy raw-only lease, and nothing else.
func TestNASCleanupLowMemoryCleanupReleaseCapabilityIsExact(t *testing.T) {
	for _, v := range []string{nasLowMemoryCleanupClientVersion, nasKeep20CleanupClientVersion} {
		testNASCleanupLowMemoryCleanupReleaseCapability(t, v)
	}
}

func testNASCleanupLowMemoryCleanupReleaseCapability(t *testing.T, v string) {
	if !nasCleanupSupportsBulk(v) || !nasCleanupSupportsGroupedExecution(v) || nasCleanupSupportsV6(v) {
		t.Fatal("cleanup literal lost bulk/group admission or gained legacy V6 semantics")
	}
	if !nasCopySupportsVersion(v) || !SupportsNASStorageBudget(v) {
		t.Fatal("cleanup literal lost raw-copy or storage-budget admission")
	}
	if !nasLowMemoryRawCopyOnly(v) || !nasLowMemoryRawCopyOnly(nasLowMemoryCopyClientVersion) {
		t.Fatal("low-memory literals must share the raw-copy-only lease bound")
	}
	if nasClientSupportsStitchProof(v) || stitchProofSelector(1, 13, v) != 0 || stitchProofSelector(2, 13, v) != 0 {
		t.Fatal("cleanup literal gained native proof or readback activation")
	}
	if nasR2StitchCopyEnabledForVersion(v) {
		t.Fatal("cleanup literal gained certified-stitch copy authority")
	}
	// The already-deployed copy-only release keeps its cleanup refusal exactly.
	if nasCleanupSupportsBulk(nasLowMemoryCopyClientVersion) || nasCleanupSupportsGroupedExecution(nasLowMemoryCopyClientVersion) || nasCleanupSupportsExecution(nasLowMemoryCopyClientVersion) {
		t.Fatal("deployed v8 copy release gained cleanup admission")
	}
	for _, unknown := range []string{v + "-extra", "r2-copy-v8", "r2-copy-v8-low-memory-", "r2-copy-v8-low-memory-cleanup2", "R2-copy-v8-low-memory-cleanup", "r2-copy-v8-keep20", "r2-copy-v8-keep20-cleanup2"} {
		if nasCleanupSupportsBulk(unknown) || nasCleanupSupportsGroupedExecution(unknown) || nasCopySupportsVersion(unknown) || SupportsNASStorageBudget(unknown) {
			t.Fatalf("prefix/unknown release admitted: %q", unknown)
		}
	}
}

// Mirror of the handler gate so the test keeps the certified-stitch binding
// referenced even without a live server instance.
func nasR2StitchCopyEnabledForVersion(version string) bool {
	return version == nasCleanupCertifiedStitchVersion
}

// Master OFF must leave the new release dry-run only: no permits, no execution
// lease, no batch approval, and no deletion authority for the items.
func TestNASCleanupLowMemoryCleanupReleaseMasterOffIsDryOnly(t *testing.T) {
	f := newCleanupFixture(t, 24)
	plan, sha, _ := f.admit("low-memory-cleanup-dry", syntheticFiles(100, 8, 4))
	f.useClientVersion(nasLowMemoryCleanupClientVersion)
	if f.s.cfg.NASCleanupDeletionEnabled {
		t.Fatal("fixture started with the master switch on")
	}
	f.dryRun()
	tasks := f.leaseV2(64, 8, 1<<40, "0123456789abcdef0123456789abcdef")
	if len(tasks) != 0 {
		t.Fatalf("master OFF leased execution work: %+v", tasks)
	}
	var issued, wouldDelete int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FILTER (WHERE delete_ever_issued), count(*) FILTER (WHERE state='would_delete') FROM nas_cleanup_items WHERE plan_id=$1`, plan).Scan(&issued, &wouldDelete); err != nil {
		t.Fatal(err)
	}
	if issued != 0 || wouldDelete != 8 {
		t.Fatalf("master OFF changed dry evidence: issued=%d would_delete=%d", issued, wouldDelete)
	}
	if mode, _ := f.planMode(plan); mode != "dry_run" {
		t.Fatalf("master OFF changed the plan mode to %q", mode)
	}
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 409 {
		t.Fatalf("master OFF enabled a batch: %d %s", code, body)
	}
}

// The deployed 0235 check keeps every earlier value; 0237 only adds the one
// exact cleanup literal and still refuses unknown or suffixed values.
func TestNASStorageBindingV8CleanupMigrationPreservesEvidence(t *testing.T) {
	s, pool := storageHeartbeatFixture(t)
	ctx := context.Background()
	for _, name := range []string{"0235_nas_storage_v8_binding.sql"} {
		body, err := os.ReadFile("../../../infra/sql/migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, string(body)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version='r2-copy-v8-low-memory',nas_storage_budget_at=now()-interval '1 minute' WHERE id=13;
 INSERT INTO connections(id,account_id,kind,api_key_id,nas_storage_client_version,nas_storage_budget_at) VALUES(16,47,'nas_pull',124,'r2-copy-v7-certified-stitch',now()-interval '2 minutes'),(17,47,'nas_pull',125,'r2-copy-v6',now()-interval '3 minutes'),(18,47,'nas_pull',126,'',NULL)`); err != nil {
		t.Fatal(err)
	}
	snapshot := `SELECT jsonb_agg(jsonb_build_array(id,nas_storage_client_version,nas_storage_budget_at) ORDER BY id)::text FROM connections WHERE id IN(13,16,17,18)`
	var before, after string
	if err := pool.QueryRow(ctx, snapshot).Scan(&before); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile("../../../infra/sql/migrations/0237_nas_storage_v8_cleanup_binding.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(body)); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, snapshot).Scan(&after); err != nil || before != after {
		t.Fatalf("0237 changed storage evidence or destroyed prior literals: %v", err)
	}
	for _, bad := range []string{nasLowMemoryCleanupClientVersion + "-extra", "r2-copy-v8", "r2-copy-v8-low-memory-cleanup2"} {
		if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version=$1,nas_storage_budget_at=now() WHERE id=13`, bad); err == nil {
			t.Fatalf("unknown literal %q passed the 0237 check", bad)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version=$1,nas_storage_budget_at=NULL WHERE id=13`, nasLowMemoryCleanupClientVersion); err == nil {
		t.Fatal("0237 accepted a version without its budget pair")
	}
	// One live nonce-bound storage budget on the new literal.
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version='',nas_storage_budget_at=NULL,client_version='be17fd73' WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	nonce := storageHeartbeat(t, s, nasLowMemoryCleanupClientVersion, storageSample(""))
	issued := storageObservation(t, pool).issued
	storageHeartbeat(t, s, nasLowMemoryCleanupClientVersion, storageSample(nonce))
	observation := storageObservation(t, pool)
	if issued == nil || observation.version != nasLowMemoryCleanupClientVersion || observation.budget == nil || !observation.budget.Equal(*issued) {
		t.Fatalf("cleanup literal storage proof did not bind the preceding challenge: %+v", observation)
	}
}

// 0238 only adds the keep20 cleanup literal on top of 0237: every earlier
// value stays valid, suffixed values are refused, and the literal binds a
// nonce-bound storage budget like the release it mirrors.
func TestNASStorageBindingV8Keep20CleanupMigration(t *testing.T) {
	s, pool := storageHeartbeatFixture(t)
	ctx := context.Background()
	for _, name := range []string{"0237_nas_storage_v8_cleanup_binding.sql", "0238_nas_storage_v8_keep20_cleanup_binding.sql"} {
		body, err := os.ReadFile("../../../infra/sql/migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, string(body)); err != nil {
			t.Fatal(err)
		}
	}
	for _, kept := range []string{"r2-copy-v6", "r2-copy-v7-certified-stitch", "r2-copy-v8-low-memory", nasLowMemoryCleanupClientVersion, nasKeep20CleanupClientVersion} {
		if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version=$1,nas_storage_budget_at=now() WHERE id=13`, kept); err != nil {
			t.Fatalf("0238 refused %q: %v", kept, err)
		}
	}
	for _, bad := range []string{nasKeep20CleanupClientVersion + "-extra", "r2-copy-v8-keep20", "r2-copy-v8-keep20-cleanup2"} {
		if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version=$1,nas_storage_budget_at=now() WHERE id=13`, bad); err == nil {
			t.Fatalf("unknown literal %q passed the 0238 check", bad)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version='',nas_storage_budget_at=NULL,client_version='be17fd73' WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	nonce := storageHeartbeat(t, s, nasKeep20CleanupClientVersion, storageSample(""))
	issued := storageObservation(t, pool).issued
	storageHeartbeat(t, s, nasKeep20CleanupClientVersion, storageSample(nonce))
	observation := storageObservation(t, pool)
	if issued == nil || observation.version != nasKeep20CleanupClientVersion || observation.budget == nil || !observation.budget.Equal(*issued) {
		t.Fatalf("keep20 literal storage proof did not bind the preceding challenge: %+v", observation)
	}
}
