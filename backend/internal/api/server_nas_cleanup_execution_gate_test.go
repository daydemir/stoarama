package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestNASCleanupExecutionCapabilityAndConcurrentLeaseLimit(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("execution-gate", syntheticFiles(100, 20, 10))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	request := func(protocol, capability int, requestID string) []cleanupTask {
		t.Helper()
		body := map[string]any{"client_version": "old-protocol-2", "protocol": protocol, "limit": 8, "reconciliation_protocol": capability}
		if protocol == 2 {
			body["limit"], body["execution_files"], body["execution_bytes"] = 64, 64, int64(1<<40)
			body["request_id"] = requestID
		}
		w := cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", body)
		var result struct {
			Tasks []cleanupTask `json:"tasks"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil {
			t.Errorf("lease: %d %s", w.Code, w.Body.String())
		}
		return result.Tasks
	}
	setHeartbeat := func(version string) {
		t.Helper()
		if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version=$1 WHERE id=13`, version); err != nil {
			t.Fatal(err)
		}
	}
	setHeartbeat("old-protocol-2")
	for _, protocol := range []int{1, 2} {
		for _, capability := range []int{0, 1, 2} {
			if tasks := request(protocol, capability, ""); len(tasks) != 0 {
				t.Fatalf("legacy/unknown capability leased execution: %+v", tasks)
			}
		}
	}
	setHeartbeat(nasCleanupExecutionClientVersion)
	// All sixteen clients ask for 64 execution items at once. The persisted
	// outstanding count must be one, including across different request IDs.
	start := make(chan struct{})
	results := make(chan []cleanupTask, 16)
	for n := 0; n < 16; n++ {
		go func(n int) {
			<-start
			results <- request(2, nasCleanupReconciliationProtocol, fmt.Sprintf("%032x", n+1))
		}(n)
	}
	close(start)
	var leased []cleanupTask
	for n := 0; n < 16; n++ {
		leased = append(leased, (<-results)...)
	}
	if len(leased) != 1 {
		t.Fatalf("concurrent execution leases: %d", len(leased))
	}
	var requestID string
	if err := f.pool.QueryRow(f.ctx, `SELECT lease_request_id FROM nas_cleanup_items WHERE id=$1`, leased[0].ItemID).Scan(&requestID); err != nil {
		t.Fatal(err)
	}
	setHeartbeat("old-protocol-2")
	if tasks := request(2, 0, requestID); len(tasks) != 0 {
		t.Fatal("old client replay received execution lease")
	}
	setHeartbeat(nasCleanupExecutionClientVersion)
	if tasks := request(2, nasCleanupReconciliationProtocol, requestID); len(tasks) != 1 || tasks[0].Attempt != leased[0].Attempt {
		t.Fatal("capable lease replay changed identity")
	}
	permit := f.observe(leased, nil)
	if !permit[0].Delete {
		t.Fatalf("permit: %+v", permit)
	}
	if tasks := request(2, nasCleanupReconciliationProtocol, strings.Repeat("f", 32)); len(tasks) != 0 {
		t.Fatal("lease overtook outstanding permit")
	}
	f.receipts(leased, permit, "deleted")
	if tasks := request(2, nasCleanupReconciliationProtocol, ""); len(tasks) != 1 {
		t.Fatal("receipt did not unblock next execution lease")
	}
	// Legacy clients remain useful for dry runs while execution is occupied.
	f.admit("legacy-dry-run", syntheticFiles(200, 3, 10))
	dry := request(2, 0, "")
	if len(dry) != 3 {
		t.Fatalf("legacy dry-run tasks: %d", len(dry))
	}
	for _, task := range dry {
		if len(task.ApprovedProof) != 0 {
			t.Fatal("old client received execution mixed with dry run")
		}
	}
}

func TestNASCleanupOldProtocolReplayCannotMintFirstPermit(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("old-replay", syntheticFiles(100, 1, 1))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	task := f.leaseV2(64, 64, 1<<40, "")[0]
	// Model a pre-upgrade lease whose original observation never committed.
	// Old clients retry /observe (also via observe-batch) instead of /reconcile.
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version='old',client_boot_id='rollback-boot' WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"reconciliation_protocol": nasCleanupReconciliationProtocol, "attempt": task.Attempt, "plan_sha256": sha, "outcome": "match", "proof": fixtureProof(task.Item), "client_version": "old", "request_id": strings.Repeat("a", 32)}
	for n := 0; n < 2; n++ {
		w := cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(task.ItemID), body)
		if w.Code != 200 || strings.Contains(w.Body.String(), `"delete":true`) {
			t.Fatalf("old replay minted permit: %d %s", w.Code, w.Body.String())
		}
		body["item_id"] = task.ItemID
		w = cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": "old", "observations": []any{body}})
		delete(body, "item_id")
		var response struct {
			Results []batchResult `json:"results"`
		}
		if json.Unmarshal(w.Body.Bytes(), &response) != nil || len(response.Results) != 1 || response.Results[0].Status != 200 || response.Results[0].Delete {
			t.Fatalf("old batch replay: %d %s", w.Code, w.Body.String())
		}
	}
	var unchanged bool
	if err := f.pool.QueryRow(f.ctx, `SELECT state='leased' AND NOT delete_ever_issued AND permit_expires_at IS NULL FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&unchanged); err != nil || !unchanged {
		t.Fatalf("old replay changed authority: %t %v", unchanged, err)
	}
}

func TestNASCleanupRolledBackCheckpointNeverMintsFirstPermit(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("rollback-checkpoint", syntheticFiles(100, 1, 1))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	task := f.leaseV2(1, 1, 1<<30, "")[0]
	req := cleanupObserve{ReconciliationProtocol: nasCleanupReconciliationProtocol, Attempt: task.Attempt, PlanSHA: sha, Outcome: "match", Proof: fixtureProof(task.Item), ClientVersion: nasCleanupExecutionClientVersion, RequestID: strings.Repeat("b", 32)}
	fresh, err := f.s.cleanupRegisterObservation(f.ctx, req.RequestID, task.ItemID)
	if err != nil || !fresh {
		t.Fatalf("register: %t %v", fresh, err)
	}
	prefetched := f.s.cleanupPrefetchRecovery(f.ctx, 13, map[int64]string{task.ItemID: ""})
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	result := f.s.cleanupObserveOne(f.ctx, tx, 13, task.ItemID, req, &cleanupObserveEnv{session: cleanupClientSession{boot: "fixture-boot", version: nasCleanupExecutionClientVersion}, freshRequests: map[int64]bool{task.ItemID: true}, prefetched: prefetched})
	if result.status != 200 || result.body["delete"] != true {
		t.Fatalf("original observation: %+v", result)
	}
	if err := tx.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	// The actual old process replays the new process's exact checkpoint,
	// including its capability, version and request_id. Neither that rollback
	// identity nor the original identity can turn this replay into a first permit.
	for _, version := range []string{"old-protocol-2", nasCleanupExecutionClientVersion} {
		if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version=$1 WHERE id=13`, version); err != nil {
			t.Fatal(err)
		}
		w := cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(task.ItemID), req)
		if w.Code != 200 && w.Code != 409 || strings.Contains(w.Body.String(), `"delete":true`) {
			t.Fatalf("replay minted permit: %d %s", w.Code, w.Body.String())
		}
		o := cleanupBatchObservation{ItemID: task.ItemID, cleanupObserve: req}
		w = cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": nasCleanupExecutionClientVersion, "observations": []any{o}})
		if w.Code != 200 || strings.Contains(w.Body.String(), `"delete":true`) {
			t.Fatalf("batch replay minted permit: %d %s", w.Code, w.Body.String())
		}
	}
	var unchanged bool
	if err := f.pool.QueryRow(f.ctx, `SELECT state='leased' AND NOT delete_ever_issued AND permit_expires_at IS NULL FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&unchanged); err != nil || !unchanged {
		t.Fatalf("authority changed: %t %v", unchanged, err)
	}
	duplicate := req
	duplicate.RequestID = strings.Repeat("f", 32)
	batch := cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": nasCleanupExecutionClientVersion, "observations": []cleanupBatchObservation{{ItemID: task.ItemID, cleanupObserve: req}, {ItemID: task.ItemID, cleanupObserve: duplicate}}})
	if batch.Code != 200 || !strings.Contains(batch.Body.String(), `"status":409`) {
		t.Fatalf("conflicting duplicate: %d %s", batch.Code, batch.Body.String())
	}
	w := cleanupCall(t, f.s.handleAccountNASCleanupReconcile, 47, 5, "itemId", fmt.Sprint(task.ItemID), req)
	if w.Code != 200 {
		t.Fatalf("reconcile: %d %s", w.Code, w.Body.String())
	}
	next := f.leaseV2(1, 1, 1<<30, "")[0]
	req.Attempt = next.Attempt
	for _, requestID := range []string{req.RequestID, duplicate.RequestID} {
		req.RequestID = requestID
		w = cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(task.ItemID), req)
		if w.Code != 409 || strings.Contains(w.Body.String(), `"delete":true`) {
			t.Fatalf("reused ID minted permit on new attempt: %d %s", w.Code, w.Body.String())
		}
	}
	// A genuinely new request can issue once; only that identical request can
	// retrieve the identical permit, with no new expiry or authority.
	req.RequestID = strings.Repeat("e", 32)
	first := cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(task.ItemID), req)
	replay := cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(task.ItemID), req)
	var a, b map[string]any
	json.Unmarshal(first.Body.Bytes(), &a)
	json.Unmarshal(replay.Body.Bytes(), &b)
	if first.Code != 200 || replay.Code != 200 || a["delete"] != true || b["delete"] != true || b["replayed"] != true || a["expires_at"] != b["expires_at"] {
		t.Fatalf("permit replay: %s / %s", first.Body.String(), replay.Body.String())
	}
}

func TestNASCleanupBootChangeMidLeaseCannotMintPermit(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("boot-change", syntheticFiles(100, 1, 1))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	task := f.leaseV2(1, 1, 1<<30, strings.Repeat("c", 32))[0]
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_boot_id='replacement-boot' WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	if tasks := f.leaseV2(1, 1, 1<<30, strings.Repeat("c", 32)); len(tasks) != 0 {
		t.Fatal("new boot replayed old lease")
	}
	body := cleanupObserve{ReconciliationProtocol: nasCleanupReconciliationProtocol, Attempt: task.Attempt, PlanSHA: sha, Outcome: "match", Proof: fixtureProof(task.Item), ClientVersion: nasCleanupExecutionClientVersion, RequestID: strings.Repeat("d", 32)}
	w := cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(task.ItemID), body)
	if w.Code != 200 || strings.Contains(w.Body.String(), `"delete":true`) {
		t.Fatalf("new boot minted permit: %d %s", w.Code, w.Body.String())
	}
}

// The saved body describes the new client, but the transport describes the
// actual sender. In particular, no old heartbeat is needed to deny a rollback.
func TestNASCleanupLiveSessionRequiredBeforeRollbackHeartbeat(t *testing.T) {
	for _, tc := range []struct{ name, header, heartbeat string }{
		{"old client missing header", "", ""},
		{"rollback version before heartbeat", "rollback-boot old-protocol-2", ""},
		{"other boot before heartbeat", "other-boot " + nasCleanupExecutionClientVersion, ""},
		{"stale heartbeat", "fixture-boot " + nasCleanupExecutionClientVersion, "last_seen_at=now()-interval '6 minutes'"},
		{"missing heartbeat", "fixture-boot " + nasCleanupExecutionClientVersion, "last_seen_at=NULL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCleanupFixture(t, 24)
			id, sha, _ := f.admit("live-session", syntheticFiles(100, 2, 1))
			f.dryRun()
			f.s.cfg.NASCleanupDeletionEnabled = true
			if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
				t.Fatalf("enable: %d %s", code, body)
			}
			if tc.heartbeat != "" {
				if _, err := f.pool.Exec(f.ctx, "UPDATE connections SET "+tc.heartbeat+" WHERE id=13"); err != nil {
					t.Fatal(err)
				}
			}
			// Enabled work is pending, so occupancy cannot hide an unsafe lease.
			f.admit("old-client-dry-run", syntheticFiles(200, 1, 1))
			leaseBody := map[string]any{"protocol": 2, "limit": 64, "execution_files": 64, "execution_bytes": 1 << 30, "reconciliation_protocol": nasCleanupReconciliationProtocol, "client_version": nasCleanupExecutionClientVersion}
			for _, protocol := range []int{1, 2} {
				body := any(leaseBody)
				if protocol == 1 {
					body = map[string]any{"protocol": 1, "reconciliation_protocol": nasCleanupReconciliationProtocol, "client_version": nasCleanupExecutionClientVersion}
				}
				w := cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", body, tc.header)
				var response struct {
					Tasks []cleanupTask `json:"tasks"`
				}
				if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil {
					t.Fatalf("lease: %d %s", w.Code, w.Body.String())
				}
				for _, task := range response.Tasks {
					if len(task.ApprovedProof) != 0 {
						t.Fatalf("unsafe execution lease: %+v", task)
					}
				}
				if protocol == 1 && len(response.Tasks) != 1 {
					t.Fatalf("old client lost dry run: %s", w.Body.String())
				}
			}
			if _, err := f.pool.Exec(f.ctx, "UPDATE connections SET last_seen_at=now() WHERE id=13"); err != nil {
				t.Fatal(err)
			}
			task := f.leaseV2(1, 1, 1<<30, strings.Repeat("c", 32))[0]
			if tc.heartbeat != "" {
				if _, err := f.pool.Exec(f.ctx, "UPDATE connections SET "+tc.heartbeat+" WHERE id=13"); err != nil {
					t.Fatal(err)
				}
			}
			// Crash before send: this ID has never reached the server. Every body
			// field, including its version and capability, is from the newer process.
			req := cleanupObserve{ReconciliationProtocol: nasCleanupReconciliationProtocol, Attempt: task.Attempt, PlanSHA: sha, Outcome: "match", Proof: fixtureProof(task.Item), ClientVersion: nasCleanupExecutionClientVersion, RequestID: strings.Repeat("a", 32)}
			for _, batch := range []bool{false, true} {
				req.RequestID = strings.Repeat("a", 32)
				handler, param, value, body := f.s.handleAccountNASCleanupObserve, "itemId", fmt.Sprint(task.ItemID), any(req)
				if batch {
					req.RequestID = strings.Repeat("b", 32)
					handler, param, value = f.s.handleAccountNASCleanupObserveBatch, "", ""
					body = map[string]any{"client_version": nasCleanupExecutionClientVersion, "observations": []cleanupBatchObservation{{ItemID: task.ItemID, cleanupObserve: req}}}
				}
				w := cleanupCall(t, handler, 47, 5, param, value, body, tc.header)
				if w.Code != 200 || !strings.Contains(w.Body.String(), `"action":"reconcile"`) || strings.Contains(w.Body.String(), `"delete":true`) || strings.Contains(w.Body.String(), `"expires_at"`) {
					t.Fatalf("rollback replay minted first authority: %d %s", w.Code, w.Body.String())
				}
			}
			var unchanged bool
			if err := f.pool.QueryRow(f.ctx, `SELECT state='leased' AND NOT delete_ever_issued AND permit_expires_at IS NULL FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&unchanged); err != nil || !unchanged {
				t.Fatalf("authority mutated: %t %v", unchanged, err)
			}
			// After rollback sends its own heartbeat, the version allowlist still denies it.
			if _, err := f.pool.Exec(f.ctx, "UPDATE connections SET client_version='old-protocol-2',client_boot_id='rollback-boot',last_seen_at=now() WHERE id=13"); err != nil {
				t.Fatal(err)
			}
			w := cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", leaseBody, "rollback-boot old-protocol-2")
			if w.Code != 200 || strings.Contains(w.Body.String(), `"approved_proof"`) {
				t.Fatalf("old version execution: %s", w.Body.String())
			}
		})
	}
}

func TestNASCleanupMatchingHeartbeatCannotReplaceLeaseSession(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("lease-session", syntheticFiles(100, 1, 1))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	task := f.leaseV2(1, 1, 1<<30, "")[0]
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_boot_id='replacement-boot',last_seen_at=now() WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	req := cleanupObserve{ReconciliationProtocol: nasCleanupReconciliationProtocol, Attempt: task.Attempt, PlanSHA: sha, Outcome: "match", Proof: fixtureProof(task.Item), RequestID: strings.Repeat("a", 32)}
	w := cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(task.ItemID), req, "replacement-boot "+nasCleanupExecutionClientVersion)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"action":"reconcile"`) || strings.Contains(w.Body.String(), `"delete":true`) {
		t.Fatalf("new heartbeat borrowed lease: %d %s", w.Code, w.Body.String())
	}
}

func TestNASCleanupObservationRequestRetention(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, _, _ := f.admit("request-retention", syntheticFiles(100, 3, 1))
	_, err := f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_observation_requests(request_id,item_id,created_at) SELECT repeat(CASE ordinal WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END,32),id,now()-CASE WHEN ordinal=2 THEN interval '29 days' ELSE interval '31 days' END FROM nas_cleanup_items WHERE plan_id=$1`, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET delete_ever_issued=true,state='unverified' WHERE plan_id=$1 AND ordinal=3`, id); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`UPDATE nas_cleanup_observation_requests SET created_at=now()`,
		`DELETE FROM nas_cleanup_observation_requests WHERE request_id=repeat('b',32)`,
		`DELETE FROM nas_cleanup_observation_requests WHERE request_id=repeat('c',32)`,
	} {
		if _, err := f.pool.Exec(f.ctx, query); err == nil {
			t.Fatalf("request history lost its guard: %s", query)
		}
	}
	f.s.cleanupHousekeeping(f.ctx, 13)
	var retained []string
	rows, err := f.pool.Query(f.ctx, `SELECT request_id FROM nas_cleanup_observation_requests ORDER BY request_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var requestID string
		if err := rows.Scan(&requestID); err != nil {
			t.Fatal(err)
		}
		retained = append(retained, requestID)
	}
	if rows.Err() != nil || len(retained) != 2 || retained[0] != strings.Repeat("b", 32) || retained[1] != strings.Repeat("c", 32) {
		t.Fatalf("retention: %v %v", retained, rows.Err())
	}
}
