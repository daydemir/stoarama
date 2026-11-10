package api

import (
	"context"
	"encoding/json"
	"testing"
)

func TestNASCleanupBatchLeaseBoundAndExecutionIsolation(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	// Synthetic enabled lease-selection fixture; real activation is tested separately.
	if _, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_frozen_streams(plan_id,stream_id,validated) VALUES(2,3,true)`); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256)
 SELECT 1,47,13,'batch','060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811',jsonb_agg(jsonb_build_object('kind','raw','source_id',n)),repeat('b',64) FROM generate_series(1,10)n;
 INSERT INTO nas_cleanup_items(id,plan_id,ordinal)SELECT n,1,n FROM generate_series(1,10)n;
 INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode,approved_until,approval_reason)
 VALUES(2,47,13,'execute','060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811','[{"kind":"raw","source_id":11,"stream_id":3},{"kind":"raw","source_id":12,"stream_id":3}]',repeat('b',64),'enabled',now()+interval '1 hour','fixture'),
 (3,48,14,'foreign','060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811','[{"kind":"raw","source_id":13}]',repeat('b',64),'dry_run',NULL,'');
 INSERT INTO nas_cleanup_items(id,plan_id,ordinal,proof)VALUES(11,2,1,'{}'),(12,2,2,'{}'),(13,3,1,NULL);`)
	if err != nil {
		t.Fatal(err)
	}
	lease := func(limit any) []cleanupTask {
		body := map[string]any{"client_version": "test", "protocol": 1, "reconciliation_protocol": nasCleanupReconciliationProtocol}
		if limit != nil {
			body["limit"] = limit
		}
		w := cleanupCall(t, s.handleAccountNASCleanupLease, 47, 5, "", "", body)
		var x struct {
			Tasks []cleanupTask `json:"tasks"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &x) != nil {
			t.Fatalf("lease %v: %s", limit, w.Body.String())
		}
		for _, task := range x.Tasks {
			if task.Attempt != 1 {
				t.Fatal("task was leased twice")
			}
		}
		return x.Tasks
	}
	if tasks := lease(nil); len(tasks) != 1 || tasks[0].ItemID != 1 {
		t.Fatal("legacy client default changed")
	}
	tasks := lease(8)
	if len(tasks) != 8 {
		t.Fatal("bounded batch missing", len(tasks))
	}
	seen := map[int64]bool{}
	for _, task := range tasks {
		if seen[task.ItemID] || len(task.ApprovedProof) != 0 || task.ItemID < 2 || task.ItemID > 9 {
			t.Fatal("invalid batch", task)
		}
		seen[task.ItemID] = true
	}
	s.cfg.NASCleanupDeletionEnabled = true
	if tasks := lease(8); len(tasks) != 1 || tasks[0].ItemID != 10 || len(tasks[0].ApprovedProof) != 0 {
		t.Fatal("mixed execution and preflight lease")
	}
	for _, id := range []int64{11, 12} {
		tasks := lease(8)
		if len(tasks) != 1 || tasks[0].ItemID != id || len(tasks[0].ApprovedProof) == 0 {
			t.Fatal("execution batch was not serial")
		}
		if got := lease(8); len(got) != 0 {
			t.Fatal("leased beside outstanding execution")
		}
		// Simulate a completed lifecycle; this fixture tests lease selection only.
		if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='keep' WHERE id=$1`, id); err != nil {
			t.Fatal(err)
		}
	}
	if tasks := lease(8); len(tasks) != 0 {
		t.Fatal("foreign task leaked")
	}
	for _, limit := range []any{-1, 9, 1.5} {
		w := cleanupCall(t, s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{"client_version": "test", "protocol": 1, "limit": limit})
		if w.Code != 400 {
			t.Fatal("invalid batch bound accepted", limit, w.Code)
		}
	}
}
