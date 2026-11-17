package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func blockCopyHEAD(f *cleanupFixture) (<-chan string, func()) {
	started, release := make(chan string, 16), make(chan struct{})
	f.bucket.mu.Lock()
	f.bucket.headStarted, f.bucket.headRelease = started, release
	f.bucket.mu.Unlock()
	var once sync.Once
	return started, func() { once.Do(func() { close(release) }) }
}

func waitCopyHEAD(t *testing.T, started <-chan string) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("HEAD did not enter barrier")
	}
}

func copyLockWaiters(t *testing.T, f *cleanupFixture, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>pg_backend_pid()`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("request did not reach lock barrier")
}

func TestNASR2CopySlowHEADDoesNotDelayControls(t *testing.T) {
	f, i := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	queueCopySources(t, f, i)
	task := copyLease(t, f)
	started, release := blockCopyHEAD(f)
	defer release()
	lease := make(chan *httptest.ResponseRecorder, 1)
	go func() { lease <- copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9}) }()
	waitCopyHEAD(t, started)

	// Neither the preparation row lock nor1192 may survive across HEAD.
	probe, err := BeginNASR2CopyTx(f.ctx, f.pool)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Rollback(f.ctx)
	var held bool
	if err = probe.QueryRow(f.ctx, `SELECT pg_try_advisory_xact_lock($1)`, nasR2CopyLock).Scan(&held); err != nil || !held {
		t.Fatalf("HEAD holds admission lock: held=%t err=%v", held, err)
	}
	if _, err = probe.Exec(f.ctx, `SELECT id FROM nas_r2_copy_items WHERE state='pending' ORDER BY id LIMIT 1 FOR UPDATE NOWAIT`); err != nil {
		t.Fatalf("HEAD holds preparation row lock: %v", err)
	}
	if err = probe.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}

	controls := make(chan error, 1)
	go func() {
		req := nasR2CopyControl{Token: task.Token, FreeBytes: NASR2CopyFloor + 1e9}
		if out := copyCall(f, "checkpoint", task.ID, req); out.Code != 200 {
			controls <- fmt.Errorf("checkpoint %d %s", out.Code, out.Body)
			return
		}
		req.Length = i.SizeBytes
		out := copyCall(f, "range", task.ID, req)
		if out.Code != 200 {
			controls <- fmt.Errorf("range %d %s", out.Code, out.Body)
			return
		}
		var grant struct {
			RangeID string `json:"range_id"`
		}
		if err := json.Unmarshal(out.Body.Bytes(), &grant); err != nil {
			controls <- err
			return
		}
		req.RangeID = grant.RangeID
		if out = copyCall(f, "checkpoint", task.ID, req); out.Code != 200 {
			controls <- fmt.Errorf("completion %d %s", out.Code, out.Body)
			return
		}
		controls <- nil
	}()
	select {
	case err := <-controls:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked HEAD delayed active controls")
	}
	release()
	select {
	case out := <-lease:
		if out.Code != 200 {
			t.Fatalf("lease %d %s", out.Code, out.Body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("lease failed to finish")
	}
}

func TestNASR2CopyHEADInFlightMaintenanceRefusesGrant(t *testing.T) {
	f, _ := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	started, release := blockCopyHEAD(f)
	defer release()
	lease := make(chan *httptest.ResponseRecorder, 1)
	go func() { lease <- copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9}) }()
	waitCopyHEAD(t, started)
	// Reproduce the controller's exact lock order and zero-ever-issued check.
	ctx, cancel := context.WithTimeout(f.ctx, time.Second)
	defer cancel()
	fence, err := BeginNASR2CopyTx(ctx, f.pool)
	if err != nil {
		t.Fatal(err)
	}
	defer fence.Rollback(f.ctx)
	if _, err = fence.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasR2CopyLock); err != nil {
		t.Fatalf("HEAD blocked deploy fence: %v", err)
	}
	if _, err = fence.Exec(ctx, `SELECT singleton FROM nas_cleanup_maintenance WHERE singleton FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	var issued int
	if err = fence.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE attempts<>0 OR lease_token IS NOT NULL OR range_id IS NOT NULL OR range_until IS NOT NULL`).Scan(&issued); err != nil || issued != 0 {
		t.Fatalf("preparation issued authority: %d %v", issued, err)
	}
	if _, err = fence.Exec(ctx, `UPDATE nas_cleanup_maintenance SET active=true,reason='test COPY deploy fence',started_at=clock_timestamp() WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	if err = fence.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case out := <-lease:
		if out.Code != 409 {
			t.Fatalf("late HEAD crossed fence: %d %s", out.Code, out.Body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("late preparation did not refuse")
	}
	var state string
	if err = f.pool.QueryRow(f.ctx, `SELECT state,attempts FROM nas_r2_copy_items`).Scan(&state, &issued); err != nil || state != "pending" || issued != 0 {
		t.Fatalf("late grant persisted: state=%s attempts=%d err=%v", state, issued, err)
	}
}

func TestNASR2CopyMaintenanceFenceDrainsActiveControl(t *testing.T) {
	f, _ := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	task := copyLease(t, f)
	blocker, err := BeginNASR2CopyTx(f.ctx, f.pool)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(f.ctx)
	if _, err = blocker.Exec(f.ctx, `SELECT id FROM nas_r2_copy_items WHERE id=$1 FOR UPDATE`, task.ID); err != nil {
		t.Fatal(err)
	}
	checkpoint := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		checkpoint <- copyCall(f, "checkpoint", task.ID, nasR2CopyControl{Token: task.Token, FreeBytes: NASR2CopyFloor + 1e9})
	}()
	copyLockWaiters(t, f, 1)
	fenced := make(chan error, 1)
	go func() {
		tx, err := BeginNASR2CopyTx(f.ctx, f.pool)
		if err != nil {
			fenced <- err
			return
		}
		defer tx.Rollback(f.ctx)
		if _, err = tx.Exec(f.ctx, `SELECT pg_advisory_xact_lock($1)`, nasR2CopyLock); err == nil {
			_, err = tx.Exec(f.ctx, `SELECT singleton FROM nas_cleanup_maintenance WHERE singleton FOR UPDATE`)
		}
		if err == nil {
			_, err = tx.Exec(f.ctx, `UPDATE nas_cleanup_maintenance SET active=true,reason='test drain',started_at=clock_timestamp() WHERE singleton`)
		}
		if err == nil {
			err = tx.Commit(f.ctx)
		}
		fenced <- err
	}()
	copyLockWaiters(t, f, 2)
	select {
	case err := <-fenced:
		t.Fatalf("fence crossed uncommitted control: %v", err)
	default:
	}
	if err = blocker.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case out := <-checkpoint:
		if out.Code != 200 {
			t.Fatalf("control %d %s", out.Code, out.Body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("control did not drain")
	}
	select {
	case err := <-fenced:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fence failed to commit")
	}
	if out := copyCall(f, "checkpoint", task.ID, nasR2CopyControl{Token: task.Token, FreeBytes: NASR2CopyFloor + 1e9}); out.Code != 409 {
		t.Fatal("control crossed committed maintenance", out.Body)
	}
}

func TestNASR2CopyConcurrentLeaseControlLoad(t *testing.T) {
	f, i := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	queueCopySources(t, f, i)
	task := copyLease(t, f)
	f.bucket.mu.Lock()
	f.bucket.headDelay = 25 * time.Millisecond
	f.bucket.mu.Unlock()
	failures := make(chan error, 5)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 8; n++ {
				out := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9})
				if out.Code != 200 && out.Code != 409 {
					failures <- fmt.Errorf("lease %d %s", out.Code, out.Body)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; n < 32; n++ {
			out := copyCall(f, "checkpoint", task.ID, nasR2CopyControl{Token: task.Token, FreeBytes: NASR2CopyFloor + 1e9})
			if out.Code != 200 {
				failures <- fmt.Errorf("control %d %s", out.Code, out.Body)
				return
			}
		}
	}()
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	var live, attempts int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FILTER(WHERE state='leased' AND lease_expires_at>clock_timestamp()),sum(attempts) FROM nas_r2_copy_items`).Scan(&live, &attempts); err != nil {
		t.Fatal(err)
	}
	if live != NASR2CopyWorkers || attempts != NASR2CopyWorkers {
		t.Fatalf("lease cap/load: live=%d attempts=%d", live, attempts)
	}
}
