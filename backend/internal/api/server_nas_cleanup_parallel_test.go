package api

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestNASCleanupConcurrentObservationStillFencesControl(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES(1,47,13,'parallel','060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811','[]',repeat('b',64));INSERT INTO nas_cleanup_items(id,plan_id,ordinal)VALUES(1,1,1),(2,1,2)`)
	if err != nil {
		t.Fatal(err)
	}
	one, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Rollback(ctx)
	two, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer two.Rollback(ctx)
	if _, err = one.Exec(ctx, cleanupObservationLoadSQL, 1, 13); err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err = two.Exec(bounded, cleanupObservationLoadSQL, 2, 13); err != nil {
		t.Fatal("independent preflight was serialized on parent: ", err)
	}
	blocked := func() {
		control, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer control.Rollback(ctx)
		if _, err = control.Exec(ctx, `SET LOCAL lock_timeout='150ms'`); err != nil {
			t.Fatal(err)
		}
		_, err = control.Exec(ctx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=1`)
		if err == nil || !strings.Contains(err.Error(), "lock timeout") {
			t.Fatal("control overtook active observation: ", err)
		}
	}
	blocked()
	if err = one.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	blocked()
	if err = two.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=1`); err != nil {
		t.Fatal("control did not proceed after readers drained: ", err)
	}
}
