package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Scope and payload must share one snapshot. Make the handler pause after its
// scope read, then change owner/holds and add a mature clip before its page read.
func TestAccountClipsCursorScopeAndPageSnapshot(t *testing.T) {
	pool, w := feedFenceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,created_at) VALUES(1,1,1000,$1,$2,now()-interval '2 hours')`, w.Start, w.Start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	activateFeedFence(t, pool)
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, `LOCK TABLE recording_clips IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/account/clips?limit=500", nil)
	req = req.WithContext(context.WithValue(ctx, accountPrincipalContextKey, accountPrincipal{AccountID: 47}))
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { rec := httptest.NewRecorder(); (&Server{pool: pool}).handleAccountClips(rec, req); done <- rec }()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var waiting bool
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='recording_clips'::regclass AND mode='AccessShareLock' AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case rec := <-done:
			t.Fatalf("handler escaped blocker: %d", rec.Code)
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if _, err = blocker.Exec(ctx, `UPDATE recordings SET account_id=99 WHERE id=1;
 INSERT INTO clip_storage_billing_contracts(clip_id,mode,authoritative) VALUES(1,'nas_collated_hold',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err = blocker.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,created_at) VALUES(2,1,1000,$1,$2,now()-interval '2 hours')`, w.Start, w.Start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case rec := <-done:
		if rec.Code != http.StatusOK {
			t.Fatalf("handler: %d %s", rec.Code, rec.Body.String())
		}
		var page accountClipsPage
		if err = json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		if !equalInt64(clipIDs(page.Clips), []int64{1}) {
			t.Fatalf("mixed scope/page snapshots: %v", clipIDs(page.Clips))
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if page := getAccountClips(t, pool, 47, 0, 500); len(page.Clips) != 0 {
		t.Fatal("new request retained the old owner scope")
	}
}
