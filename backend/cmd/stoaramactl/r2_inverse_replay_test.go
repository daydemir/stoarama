package main

import (
	"strings"
	"testing"
)

func TestR2InverseRecorderReplayVeto(t *testing.T) {
	f := newKeep28Fixture(t)
	inverseUseOwnerFenceEvidence(t, &f.options)
	for _, tc := range []struct {
		name, setup string
	}{
		{"active recording", `UPDATE recordings SET status='active',paused_at=NULL`},
		{"pending recording job", `UPDATE recordings SET status='paused',paused_at=now(); INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key) VALUES(30,now(),now(),60,'pending','owner-recorder-replay')`},
		{"leased recording job", `UPDATE recording_jobs SET status='leased'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Only the isolated fixture bypasses admission, to model a recorder
			// already runnable before the real durable fence was activated.
			tx, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(f.ctx)
			if _, err = tx.Exec(f.ctx, `SET LOCAL session_replication_role=replica; `+tc.setup); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(f.ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = applyInverse(f.ctx, f.pool, f.sdk, f.options); err == nil || !strings.Contains(err.Error(), "recorder replay") || f.deletes() != 0 {
				t.Fatal("recorder replay reached wire", err)
			}
		})
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE recording_jobs SET status='done',completed_at=now()`); err != nil {
		t.Fatal(err)
	}
	result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
	if err != nil || result.Deleted != 1 || f.deletes() != 1 {
		t.Fatalf("inactive recorder blocked: %+v %v", result, err)
	}
}
