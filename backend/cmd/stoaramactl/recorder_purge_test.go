package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// fakeDeleter records the batches of keys passed to DeleteObjects and can be made
// to fail on a chosen batch index, so the purge batching + mark-purged ordering is
// exercised without R2.
type fakeDeleter struct {
	batches  [][]string
	failOn   int // 1-based batch index to fail on; 0 = never fail
	failErr  error
	callSeen int
}

func (f *fakeDeleter) DeleteObjects(_ context.Context, keys []string) error {
	f.callSeen++
	if f.failOn != 0 && f.callSeen == f.failOn {
		if f.failErr == nil {
			f.failErr = errors.New("delete failed")
		}
		return f.failErr
	}
	cp := make([]string, len(keys))
	copy(cp, keys)
	f.batches = append(f.batches, cp)
	return nil
}

func makeClips(n int) []clipObject {
	clips := make([]clipObject, n)
	for i := 0; i < n; i++ {
		clips[i] = clipObject{id: int64(i + 1), objectKey: fmt.Sprintf("managed/acct-7/recordings/%d.mp4", i+1)}
	}
	return clips
}

func TestPurgeClipsBatchesAndMarks(t *testing.T) {
	ctx := context.Background()
	// 2300 clips => 3 DeleteObjects batches (1000, 1000, 300), each marked purged
	// only after its delete succeeds, and every clip id marked exactly once.
	clips := makeClips(2300)
	del := &fakeDeleter{}
	var marked []int64
	mark := func(_ context.Context, ids []int64) error {
		marked = append(marked, ids...)
		return nil
	}

	if err := purgeClips(ctx, del, clips, mark); err != nil {
		t.Fatalf("purgeClips: %v", err)
	}
	if got := []int{len(del.batches[0]), len(del.batches[1]), len(del.batches[2])}; len(del.batches) != 3 ||
		got[0] != r2DeleteBatchSize || got[1] != r2DeleteBatchSize || got[2] != 300 {
		t.Fatalf("batch sizes = %v (count %d), want [1000 1000 300]", got, len(del.batches))
	}
	if len(marked) != 2300 {
		t.Fatalf("marked %d ids, want 2300", len(marked))
	}
	for i, id := range marked {
		if id != int64(i+1) {
			t.Fatalf("marked[%d]=%d, want %d (each clip purged exactly once, in order)", i, id, i+1)
		}
	}
}

func TestPurgeClipsStopsOnDeleteError(t *testing.T) {
	ctx := context.Background()
	// Fail the SECOND batch: the first batch's rows are marked purged, the second's
	// (and everything after) are left un-purged for the next pass (idempotent retry).
	clips := makeClips(2300)
	del := &fakeDeleter{failOn: 2}
	var marked []int64
	mark := func(_ context.Context, ids []int64) error {
		marked = append(marked, ids...)
		return nil
	}

	err := purgeClips(ctx, del, clips, mark)
	if err == nil {
		t.Fatalf("purgeClips: want error from failed batch, got nil")
	}
	if len(marked) != r2DeleteBatchSize {
		t.Fatalf("marked %d ids after batch-2 failure, want 1000 (only the first successful batch)", len(marked))
	}
	for i, id := range marked {
		if id != int64(i+1) {
			t.Fatalf("marked[%d]=%d, want %d", i, id, i+1)
		}
	}
}

func TestPurgeClipsMarkErrorPropagates(t *testing.T) {
	ctx := context.Background()
	// A mark-purged DB error must propagate (the object is gone but the row is not
	// flagged; the next pass re-fetches it as purged_at IS NULL and re-deletes,
	// which is a harmless no-op on an already-deleted key).
	clips := makeClips(10)
	del := &fakeDeleter{}
	mark := func(_ context.Context, _ []int64) error { return errors.New("db down") }
	if err := purgeClips(ctx, del, clips, mark); err == nil {
		t.Fatalf("purgeClips: want mark-purged error to propagate, got nil")
	}
}

func TestPurgeClipsEmpty(t *testing.T) {
	ctx := context.Background()
	del := &fakeDeleter{}
	called := false
	mark := func(_ context.Context, _ []int64) error { called = true; return nil }
	if err := purgeClips(ctx, del, nil, mark); err != nil {
		t.Fatalf("purgeClips(empty): %v", err)
	}
	if del.callSeen != 0 || called {
		t.Fatalf("empty clip set must not call DeleteObjects (%d) or markPurged (%v)", del.callSeen, called)
	}
}
