package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNASCleanupExecutionPollingDoesNotQueueDryWork(t *testing.T) {
	f := newCleanupFixture(t, 12)
	f.admit("dry-supply", syntheticFiles(100, 12, 10))
	version := nasCleanupBulkDurabilityClientVersion
	lease := func(only bool, request string) (int, []cleanupTask) {
		w := cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{
			"client_version": version, "protocol": 2, "reconciliation_protocol": 1, "limit": 8,
			"execution_files": 8, "execution_bytes": 1 << 30, "request_id": request,
			"execution_batch": true, "execution_only": only, "dry_run_limit": 2,
		}, "fixture-boot "+version)
		var body struct {
			Tasks []cleanupTask `json:"tasks"`
		}
		if w.Code == 200 && json.Unmarshal(w.Body.Bytes(), &body) != nil {
			t.Fatal("invalid response")
		}
		return w.Code, body.Tasks
	}
	if code, tasks := lease(true, strings.Repeat("a", 32)); code != 200 || len(tasks) != 0 {
		t.Fatalf("execution-only leased dry tasks: %d %+v", code, tasks)
	}
	if code, tasks := lease(false, strings.Repeat("b", 32)); code != 200 || len(tasks) != 2 {
		t.Fatalf("dry limit ignored: %d %+v", code, tasks)
	}
	if code, _ := lease(true, strings.Repeat("b", 32)); code != 409 {
		t.Fatalf("dry replay changed roles: %d", code)
	}
	var leased int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE state='leased'`).Scan(&leased); err != nil || leased != 2 {
		t.Fatalf("poll changed dry leases: %d %v", leased, err)
	}
}

func TestNASCleanupDryLimitDoesNotShrinkExecutionGroup(t *testing.T) {
	f := newCleanupFixture(t, 12)
	id, sha, _ := f.admit("execute", syntheticFiles(100, 8, 10))
	f.dryRun()
	version := nasCleanupBulkDurabilityClientVersion
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version=$1 WHERE id=13`, version); err != nil {
		t.Fatal(err)
	}
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable %d %s", code, body)
	}
	w := cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{
		"client_version": version, "protocol": 2, "reconciliation_protocol": 1, "limit": 8,
		"execution_files": 8, "execution_bytes": 1 << 30, "request_id": strings.Repeat("c", 32),
		"execution_batch": true, "execution_only": true, "dry_run_limit": 2,
	}, "fixture-boot "+version)
	var body struct {
		Tasks []cleanupTask `json:"tasks"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &body) != nil || len(body.Tasks) != 8 {
		t.Fatalf("execution group shrunk: %d %s", w.Code, w.Body.String())
	}
	for _, task := range body.Tasks {
		if task.ApprovedProof == nil || !task.ExecutionBatch {
			t.Fatal("incorrect execution role")
		}
	}
}
