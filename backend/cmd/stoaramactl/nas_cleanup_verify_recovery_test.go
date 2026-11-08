package main

import (
	"context"
	"testing"
)

func TestNASCleanupVerifyRecoveryHeadsEveryIssuedItem(t *testing.T) {
	store := &fakeNASPurgeStore{objects: map[string][]byte{"ok": []byte("12345"), "short": []byte("123")}}
	item := func(key, bucket string, size int64) map[string]any {
		return map[string]any{"kind": "raw", "source_id": 1, "bucket": bucket, "object_key": key, "size_bytes": size}
	}
	result := func(id int64, ordinal int, issued bool) map[string]any {
		return map[string]any{"item_id": id, "ordinal": ordinal, "state": "deleted", "delete_ever_issued": issued}
	}
	report := map[string]any{"id": 7, "plan_sha256": "b",
		"items":   []any{item("ok", "stoarama", 5), item("gone", "stoarama", 5), item("short", "stoarama", 5), item("ok", "other", 5), item("never-issued", "stoarama", 5)},
		"results": []any{result(1, 1, true), result(2, 2, true), result(3, 3, true), result(4, 4, true), result(5, 5, false)},
	}
	sum, err := verifyNASCleanupRecovery(context.Background(), report, store)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Issued != 4 || sum.Verified != 1 || sum.Problems != 3 || sum.ByStatus["missing"] != 1 || sum.ByStatus["size_mismatch"] != 1 || sum.ByStatus["bucket_mismatch"] != 1 {
		t.Fatalf("summary %+v", sum)
	}
	if store.heads != 3 || len(store.deleted) != 0 {
		t.Fatalf("expected exactly 3 read-only HEADs, got heads=%d deleted=%v", store.heads, store.deleted)
	}
	if _, err := verifyNASCleanupRecovery(context.Background(), map[string]any{"id": 7, "items": []any{item("ok", "stoarama", 5)}}, store); err == nil {
		t.Fatal("incomplete report accepted")
	}
}
