package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestNASCleanupPriorityAndReadOnlyResume(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	_, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode,approved_until,approval_reason,priority) VALUES
 (1,47,13,'bulk',repeat('a',64),'[{"kind":"raw","source_id":1},{"kind":"raw","source_id":2}]',repeat('b',64),'dry_run',NULL,'',0),
 (2,47,13,'pilot',repeat('a',64),'[{"kind":"raw","source_id":3}]',repeat('b',64),'dry_run',NULL,'',0),
 (3,47,13,'paused',repeat('a',64),'[{"kind":"raw","source_id":4}]',repeat('b',64),'paused',NULL,'',100),
 (4,47,13,'master-fenced',repeat('a',64),'[{"kind":"raw","source_id":5}]',repeat('b',64),'enabled',now()+interval '1 hour','fixture',100),
 (5,48,14,'foreign',repeat('a',64),'[{"kind":"raw","source_id":6}]',repeat('b',64),'dry_run',NULL,'',100);
 INSERT INTO nas_cleanup_items(id,plan_id,ordinal) VALUES(1,1,1),(2,1,2),(100,2,1),(200,3,1),(300,4,1),(400,5,1);`)
	if err != nil {
		t.Fatal(err)
	}
	control := func(id int64, body map[string]any) int {
		return cleanupCall(t, s.handleAdminNASCleanupControl, 47, 5, "id", fmt.Sprint(id), body).Code
	}
	body := func(mode string, priority any) map[string]any {
		b := map[string]any{"plan_sha256": strings.Repeat("b", 64), "mode": mode, "reason": "read-only queue fixture", "allow_nas_only": false}
		if priority != nil {
			b["priority"] = priority
		}
		return b
	}
	if got := control(2, body("dry_run", 100)); got != 200 {
		t.Fatalf("priority set: %d", got)
	}
	var mode string
	var priority int
	var allow bool
	var approved bool
	if err = pool.QueryRow(ctx, `SELECT mode,priority,allow_nas_only,approved_until IS NOT NULL FROM nas_cleanup_plans WHERE id=2`).Scan(&mode, &priority, &allow, &approved); err != nil {
		t.Fatal(err)
	}
	if mode != "dry_run" || priority != 100 || allow || approved {
		t.Fatal("read-only scheduling armed deletion")
	}
	w := cleanupCall(t, s.handleAdminNASCleanupStatus, 47, 5, "id", "2", nil)
	var status map[string]any
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || status["priority"] != float64(100) || status["master_deletion_enabled"] != false {
		t.Fatal("status omitted scheduling/master gates")
	}
	lease := func() cleanupTask {
		w := cleanupCall(t, s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{"client_version": "test", "protocol": 1})
		var r struct {
			Tasks []cleanupTask `json:"tasks"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &r) != nil || len(r.Tasks) != 1 {
			t.Fatalf("lease: %s", w.Body.String())
		}
		if len(r.Tasks[0].ApprovedProof) != 0 {
			t.Fatal("dry priority supplied a destructive proof")
		}
		return r.Tasks[0]
	}
	if got := lease().ItemID; got != 100 {
		t.Fatalf("pilot waited behind bulk: %d", got)
	}
	if got := lease().ItemID; got != 1 {
		t.Fatalf("eligible bulk did not resume after pilot: %d", got)
	}
	if got := lease().ItemID; got != 2 {
		t.Fatalf("within-plan order changed: %d", got)
	}
	// Master OFF, paused and foreign plans stay excluded even at high priority.
	w = cleanupCall(t, s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{"client_version": "test", "protocol": 1})
	var empty struct {
		Tasks []cleanupTask `json:"tasks"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &empty) != nil || len(empty.Tasks) != 0 {
		t.Fatal("priority bypassed master/pause/tenant gate")
	}
	if got := control(2, body("enabled", nil)); got != http.StatusConflict {
		t.Fatal("priority bypassed master OFF")
	}
	if got := control(2, body("dry_run", 101)); got != 400 {
		t.Fatal("unbounded priority accepted")
	}
	if got := control(2, body("paused", 10)); got != 400 {
		t.Fatal("priority changed outside a read-only operation")
	}
	invalid := body("dry_run", nil)
	invalid["allow_nas_only"] = true
	if got := control(2, invalid); got != 400 {
		t.Fatal("read-only scheduling accepted NAS-only deletion permission")
	}
	if got := control(4, body("dry_run", nil)); got != 409 {
		t.Fatal("active approval silently resumed as dry run")
	}
	if got := control(5, body("dry_run", 100)); got != 409 {
		t.Fatal("foreign plan was rescheduled")
	}
	// Resume preserves terminal observations and lease attempts rather than
	// laundering conflicts or issued-deletion evidence into new pending work.
	if _, err = pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='keep',attempt=2,proof='{}' WHERE id=200`); err != nil {
		t.Fatal(err)
	}
	if got := control(3, body("dry_run", -1)); got != 200 {
		t.Fatal("nonexecuting paused plan could not resume")
	}
	var state string
	var attempt int
	if err = pool.QueryRow(ctx, `SELECT state,attempt FROM nas_cleanup_items WHERE id=200`).Scan(&state, &attempt); err != nil || state != "keep" || attempt != 2 {
		t.Fatal("resume rewrote item evidence")
	}
	if got := control(3, body("paused", nil)); got != 200 {
		t.Fatal("pause failed")
	}
	if _, err = pool.Exec(ctx, `UPDATE nas_cleanup_items SET delete_ever_issued=true,state='unverified' WHERE id=200`); err != nil {
		t.Fatal(err)
	}
	if got := control(3, body("dry_run", 100)); got != 409 {
		t.Fatal("issued-deletion evidence was resumed without reconciliation")
	}
}

func TestNASCleanupPriorityProbeSkipsLockedPlan(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	_, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,priority) VALUES
 (1,47,13,'old',repeat('a',64),'[{"kind":"raw","source_id":1}]',repeat('b',64),0),
 (2,47,13,'new',repeat('a',64),'[{"kind":"raw","source_id":2}]',repeat('b',64),100);
 INSERT INTO nas_cleanup_items(id,plan_id,ordinal) VALUES(1,1,1),(2,2,1);`)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT id FROM nas_cleanup_items WHERE id=2 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	w := cleanupCall(t, s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{"client_version": "test", "protocol": 1})
	var result struct {
		Tasks []cleanupTask `json:"tasks"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result.Tasks) != 1 || result.Tasks[0].ItemID != 1 {
		t.Fatalf("locked pilot stalled other eligible work: %s", w.Body.String())
	}
}
