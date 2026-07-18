package api

import (
	"context"
	"net/http/httptest"
	"testing"
)

func TestUniqueRecordingIDs(t *testing.T) {
	ids, err := uniqueRecordingIDs([]int64{3, 1, 2})
	if err != nil || len(ids) != 3 || ids[0] != 3 {
		t.Fatalf("unexpected result: %v, %v", ids, err)
	}
	for _, invalid := range [][]int64{nil, {0}, {1, 1}, make([]int64, 51)} {
		if _, err := uniqueRecordingIDs(invalid); err == nil {
			t.Fatalf("expected error for %v", invalid)
		}
	}
}

func TestCancelRecordingsIsAtomic(t *testing.T) {
	pool, cleanup := testRecordingPatchPool(t)
	defer cleanup()
	ctx := context.Background()
	var destinationID int64
	if err := pool.QueryRow(ctx, `INSERT INTO storage_destinations (account_id,name) VALUES (1,'test') RETURNING id`).Scan(&destinationID); err != nil {
		t.Fatal(err)
	}
	insertRecording := func(accountID int64, name string) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO recordings (account_id,storage_destination_id,name,stream_url)
			VALUES ($1,$2,$3,'https://example.com/live.m3u8') RETURNING id
		`, accountID, destinationID, name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	ownedID, otherID := insertRecording(1, "owned"), insertRecording(2, "other")
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_jobs (recording_id,fire_at,scheduled_for,clip_duration_sec,status,lease_owner,lease_expires_at,idempotency_key)
		VALUES ($1,now(),now(),60,'leased','worker',now()+interval '1 minute','cancel-test')
	`, ownedID); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	req := httptest.NewRequest("POST", "/", nil)
	if s.cancelRecordings(httptest.NewRecorder(), req, 1, []int64{ownedID, otherID}) {
		t.Fatal("mixed-account batch unexpectedly succeeded")
	}
	var status string
	var owner *string
	if err := pool.QueryRow(ctx, `SELECT status FROM recordings WHERE id=$1`, ownedID).Scan(&status); err != nil || status != "active" {
		t.Fatalf("partial cancellation was not rolled back: status=%q err=%v", status, err)
	}
	if err := pool.QueryRow(ctx, `SELECT status,lease_owner FROM recording_jobs WHERE recording_id=$1`, ownedID).Scan(&status, &owner); err != nil || status != "leased" || owner == nil || *owner != "worker" {
		t.Fatalf("job cancellation was not rolled back: status=%q owner=%v err=%v", status, owner, err)
	}
	if !s.cancelRecordings(httptest.NewRecorder(), req, 1, []int64{ownedID}) {
		t.Fatal("owned cancellation failed")
	}
	if err := pool.QueryRow(ctx, `SELECT status,lease_owner FROM recording_jobs WHERE recording_id=$1`, ownedID).Scan(&status, &owner); err != nil || status != "canceled" || owner != nil {
		t.Fatalf("job not canceled cleanly: status=%q owner=%v err=%v", status, owner, err)
	}
}
