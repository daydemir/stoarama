package main

import (
	"context"
	"errors"
	"testing"
)

func TestFinishConsumesActualA1Status(t *testing.T) {
	pool, safety, copy := finishFixture(t, 3)
	ctx := context.Background()
	finishRun(t, pool, safety, copy)
	_, err := pool.Exec(ctx, `UPDATE nas_r2_copy_items SET state='verified',verified_at=now(),completion='verified_in_place',committed_bytes=100 WHERE source_id=1;
 UPDATE nas_r2_copy_items SET state='verified',verified_at=now(),completion='downloaded',downloaded_bytes=40,committed_bytes=100 WHERE source_id=2;
 UPDATE nas_r2_copy_items SET state='blocked',blocked_reason='checksum_mismatch' WHERE source_id=3`)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := loadNASCopyStatus(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	finishTestJSON(t, copy, producer)
	_, consumer, err := finishReadEvidence(safety, copy)
	if err != nil {
		t.Fatal(err)
	}
	if consumer.VerifiedInPlaceItemsPerMinute != 1 || consumer.DownloadedItemsPerMinute != 1 || consumer.VerifiedInPlace.Objects != 1 || consumer.Downloaded.Objects != 1 || consumer.DownloadedBytes != 40 || consumer.CommittedBytes != 200 || consumer.Blocked != 1 || consumer.BlockedByReason["checksum_mismatch"].Objects != 1 || consumer.MismatchLatched != 1 {
		t.Fatalf("A1 status lost: %+v", consumer)
	}
	status, err := finishTick(ctx, pool, safety, copy)
	if !errors.Is(err, finishMismatch) || status.Stop != finishMismatch || status.ItemsPerMinute != 2 {
		t.Fatalf("A1 latch ignored: %v %+v", err, status)
	}
}
