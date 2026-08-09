package api

import (
	"context"
	"testing"
)

func TestPropagateStreamSourceUpdatesOnlyActiveRelayRecordings(t *testing.T) {
	pool, cleanup := testRecordingPatchPool(t)
	defer cleanup()

	ctx := context.Background()
	destID := insertPatchDestination(t, pool, 42)
	for _, row := range []struct {
		streamID   int64
		status     string
		capture    string
		sourceURL  string
		sourceKind string
	}{
		{7, "active", "relay", "https://old.example/live", "hls_live"},
		{7, "canceled", "relay", "https://old.example/canceled", "auto"},
		{7, "active", "cloud", "https://old.example/cloud.m3u8", "hls_live"},
		{8, "active", "relay", "https://old.example/other", "auto"},
	} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO recordings
			(account_id,storage_destination_id,name,stream_url,stream_id,source_kind,status,capture_via)
			VALUES (42,$1,'source propagation',$2,$3,$4,$5,$6)
		`, destID, row.sourceURL, row.streamID, row.sourceKind, row.status, row.capture); err != nil {
			t.Fatal(err)
		}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := propagateStreamSourceToActiveRelayRecordingsTx(ctx, tx, 7, " https://new.example/watch ")
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if updated != 1 {
		_ = tx.Rollback(ctx)
		t.Fatalf("updated=%d want 1", updated)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	rows, err := pool.Query(ctx, `SELECT stream_id,status,capture_via,stream_url,source_kind FROM recordings ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	wantURLs := []string{
		"https://new.example/watch",
		"https://old.example/canceled",
		"https://old.example/cloud.m3u8",
		"https://old.example/other",
	}
	wantKinds := []string{"auto", "auto", "hls_live", "auto"}
	index := 0
	for rows.Next() {
		var streamID int64
		var status, capture, sourceURL, sourceKind string
		if err := rows.Scan(&streamID, &status, &capture, &sourceURL, &sourceKind); err != nil {
			t.Fatal(err)
		}
		if sourceURL != wantURLs[index] || sourceKind != wantKinds[index] {
			t.Fatalf("row %d source=(%q,%q) want=(%q,%q)", index, sourceURL, sourceKind, wantURLs[index], wantKinds[index])
		}
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if index != len(wantURLs) {
		t.Fatalf("rows=%d want %d", index, len(wantURLs))
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	updated, err = propagateStreamSourceToActiveRelayRecordingsTx(ctx, tx, 7, "   ")
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if updated != 0 {
		_ = tx.Rollback(ctx)
		t.Fatalf("blank source updated=%d want 0", updated)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
