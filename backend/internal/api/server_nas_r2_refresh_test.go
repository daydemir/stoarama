package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNASRefreshDBExcludesLegacyDownload(t *testing.T) {
	f, _ := copyTestFixture(t)
	var err error
	if _, err = f.pool.Exec(f.ctx, `UPDATE nas_r2_copy_items SET state='verified',verified_at=now(),completion='downloaded',committed_bytes=100; UPDATE nas_r2_copy_items SET work_mode='verify_existing',state='refresh_pending'`); err != nil {
		t.Fatal(err)
	}
	var oldReady int
	if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE state='pending' OR (state='leased' AND lease_expires_at<clock_timestamp())`).Scan(&oldReady); err != nil || oldReady != 0 {
		t.Fatalf("legacy ready=%d %v", oldReady, err)
	}
	for _, sql := range []string{`UPDATE nas_r2_copy_items SET state='leased',lease_token='legacy',lease_expires_at=now()+interval '30 seconds'`, `UPDATE nas_r2_copy_items SET state='pending'`, `UPDATE nas_r2_copy_items SET range_until=now()+interval '10 seconds',range_id='grant',range_length=1`, `UPDATE nas_r2_copy_items SET work_mode='copy'`, `UPDATE nas_r2_copy_items SET committed_bytes=0`} {
		if _, err = f.pool.Exec(f.ctx, sql); err == nil {
			t.Fatalf("legacy mutation accepted: %s", sql)
		}
	}
}

func TestNASRefreshProtocolNeverRangesAndPreservesCompletion(t *testing.T) {
	f, i := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_r2_copy_items SET state='verified',verified_at=now(),completion='downloaded',downloaded_bytes=$1,committed_bytes=$1`, i.SizeBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE r2_keep28_refresh_control SET enabled=true; UPDATE nas_r2_copy_items SET work_mode='verify_existing',state='refresh_pending'`); err != nil {
		t.Fatal(err)
	}
	legacy := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9})
	if legacy.Code != 200 || !strings.Contains(legacy.Body.String(), `"task":null`) {
		t.Fatalf("legacy client took refresh %d %s", legacy.Code, legacy.Body)
	}
	for turn := 1; turn <= 2; turn++ {
		out := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9, VerifyExisting: true})
		var response struct {
			Task nasR2CopyTask `json:"task"`
		}
		if out.Code != 200 || json.Unmarshal(out.Body.Bytes(), &response) != nil || response.Task.ID == 0 || response.Task.WorkMode != NASR2VerifyExisting {
			t.Fatalf("refresh lease %d %s", out.Code, out.Body)
		}
		req := nasR2CopyControl{Token: response.Task.Token, FreeBytes: NASR2CopyFloor + 1e9, Length: i.SizeBytes}
		if out = copyCall(f, "range", response.Task.ID, req); out.Code != 409 {
			t.Fatalf("refresh RANGE %d %s", out.Code, out.Body)
		}
		if turn == 1 {
			if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_inventory_files VALUES(13,$1,$2,$3,$4,'present',now(),now(),$2||'.stoarama.json',100,repeat('b',64))`, i.SourceID, i.RelativePath, i.SHA256, i.SizeBytes); err != nil {
				t.Fatal(err)
			}
		}
		req.Length = 0
		req.Completion = NASR2CopyVerifiedInPlace
		if out = copyCall(f, "result", response.Task.ID, req); out.Code != 200 {
			t.Fatalf("refresh result %d %s", out.Code, out.Body)
		}
		var completion string
		var downloaded, committed, count int64
		if err := f.pool.QueryRow(f.ctx, `SELECT completion,downloaded_bytes,committed_bytes,refresh_count FROM nas_r2_copy_items`).Scan(&completion, &downloaded, &committed, &count); err != nil || completion != "downloaded" || downloaded != i.SizeBytes || committed != i.SizeBytes || count != int64(turn) {
			t.Fatalf("history %s %d %d count%d %v", completion, downloaded, committed, count, err)
		}
		if turn == 1 {
			if _, err := f.pool.Exec(f.ctx, `UPDATE nas_r2_copy_items SET state='refresh_pending',lease_token=NULL,lease_expires_at=NULL`); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestNASRefreshPauseAndExpiredLeaseRemainRetryableWithoutDownload(t *testing.T) {
	f, i := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_r2_copy_items SET state='verified',verified_at=now()-interval '25 hours',completion='verified_in_place'; UPDATE nas_r2_copy_items SET state='refresh_pending',work_mode='verify_existing'`); err != nil {
		t.Fatal(err)
	}
	lease := func() nasR2CopyTask {
		t.Helper()
		out := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9, VerifyExisting: true})
		var response struct {
			Task nasR2CopyTask `json:"task"`
		}
		if out.Code != 200 || json.Unmarshal(out.Body.Bytes(), &response) != nil {
			t.Fatalf("lease %d %s", out.Code, out.Body)
		}
		return response.Task
	}
	if task := lease(); task.ID != 0 {
		t.Fatal("paused pending work leased")
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE r2_keep28_refresh_control SET enabled=true`); err != nil {
		t.Fatal(err)
	}
	first := lease()
	if first.ID == 0 {
		t.Fatal("enabled work not leased")
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE r2_keep28_refresh_control SET enabled=false; UPDATE nas_r2_copy_items SET lease_expires_at=now()-interval '25 hours'`); err != nil {
		t.Fatal(err)
	}
	if task := lease(); task.ID != 0 {
		t.Fatal("paused expired work leased")
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE r2_keep28_refresh_control SET enabled=true`); err != nil {
		t.Fatal(err)
	}
	second := lease()
	if second.ID == 0 || second.Token == first.Token {
		t.Fatal("expired lease did not get a new token")
	}
	if out := copyCall(f, "checkpoint", first.ID, nasR2CopyControl{Token: first.Token, FreeBytes: NASR2CopyFloor + 1e9}); out.Code != 409 {
		t.Fatal("prior lease revived")
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_inventory_files VALUES(13,$1,$2,$3,$4,'present',now()-interval '25 hours',now(),$2||'.stoarama.json',100,repeat('b',64))`, i.SourceID, i.RelativePath, i.SHA256, i.SizeBytes); err != nil {
		t.Fatal(err)
	}
	req := nasR2CopyControl{Token: second.Token, FreeBytes: NASR2CopyFloor + 1e9, Completion: NASR2CopyVerifiedInPlace}
	if out := copyCall(f, "result", second.ID, req); out.Code != 409 {
		t.Fatal("replayed expired verification counted as fresh")
	}
}

func TestNASRefreshMixedLeaseCapAndSharedRenewals(t *testing.T) {
	f, i := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	for n := int64(902); n <= 906; n++ {
		item := i
		item.SourceID = n
		item.ObjectKey = fmt.Sprintf("managed/acct-47/%d.mp4", n)
		item.RelativePath = fmt.Sprintf("p/%d.mp4", n)
		f.bucket.mu.Lock()
		body := append([]byte(nil), f.bucket.objects[i.ObjectKey]...)
		f.bucket.mu.Unlock()
		f.bucket.put(item.ObjectKey, body)
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket) VALUES($1,2,$2,$3,$4,$5,$6,$7,'bucket')`, n, item.ObjectKey, item.SHA256, item.SizeBytes, item.RelativePath, item.StartAt, item.EndAt); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(item)
		policy, _, _ := Keep28Policy()
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item) VALUES(13,'raw',$1,repeat('a',64),$2,$3)`, n, policy, data); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE r2_keep28_refresh_control SET enabled=true; UPDATE nas_r2_copy_items SET state='verified',verified_at=now(),completion='verified_in_place' WHERE source_id=901; UPDATE nas_r2_copy_items SET state='refresh_pending',work_mode='verify_existing' WHERE source_id=901`); err != nil {
		t.Fatal(err)
	}
	tasks := []nasR2CopyTask{}
	for range 4 {
		out := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9, VerifyExisting: true})
		var response struct {
			Task nasR2CopyTask `json:"task"`
		}
		if out.Code != 200 || json.Unmarshal(out.Body.Bytes(), &response) != nil || response.Task.ID == 0 {
			t.Fatalf("lease %d %s", out.Code, out.Body)
		}
		tasks = append(tasks, response.Task)
	}
	var oldCount int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE state='leased' AND lease_expires_at>clock_timestamp()`).Scan(&oldCount); err != nil || oldCount != 3 {
		t.Fatalf("legacy sees%d %v", oldCount, err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_r2_copy_items SET state='leased',lease_token='legacy-fifth',lease_expires_at=now()+interval '30 seconds' WHERE id=(SELECT min(id) FROM nas_r2_copy_items WHERE state='pending')`); err == nil {
		t.Fatal("legacy exceeded combined cap")
	}
	var wg sync.WaitGroup
	failures := make(chan string, 4)
	for _, task := range tasks {
		wg.Add(1)
		go func(task nasR2CopyTask) {
			defer wg.Done()
			out := copyCall(f, "checkpoint", task.ID, nasR2CopyControl{Token: task.Token, FreeBytes: NASR2CopyFloor + 1e9})
			if out.Code != 200 {
				failures <- out.Body.String()
			}
		}(task)
	}
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Fatal(failure)
	}
	out := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9, VerifyExisting: true})
	if out.Code != 200 || !strings.Contains(out.Body.String(), `"task":null`) {
		t.Fatalf("cap %d %s", out.Code, out.Body)
	}
}

func TestNASRefreshInventoryReplayKeepsVerificationUpperBound(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	var cid int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES(42,'nas_pull',99) RETURNING id`).Scan(&cid); err != nil {
		t.Fatal(err)
	}
	server := &Server{pool: pool}
	for n, age := range []time.Duration{-25 * time.Hour, -time.Hour, time.Minute} {
		verified := time.Now().UTC().Add(age).Truncate(time.Microsecond)
		updated := time.Now().UTC().Truncate(time.Microsecond)
		payload := nasInventorySyncRequest{Generation: "live", DatasetSchema: "raw-dataset-v2", RootExclusions: []string{"#recycle"}, Files: []nasInventoryFileReport{{ClipID: int64(700 + n), RecordingID: 20, RelativePath: fmt.Sprintf("clips/replay-%d.mp4", n), SizeBytes: 3, SHA256: strings.Repeat("a", 64), State: "present", VerifiedAt: &verified, ClientUpdatedAt: updated}}}
		call := func() {
			t.Helper()
			body, _ := json.Marshal(payload)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/inventory", bytes.NewReader(body))
			req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 42, APIKeyID: ptrInt64(99)}))
			out := httptest.NewRecorder()
			server.handleAccountConnectionInventorySync(out, req)
			if out.Code != 200 {
				t.Fatalf("inventory %d %s", out.Code, out.Body)
			}
		}
		call()
		if _, err := pool.Exec(ctx, `UPDATE nas_inventory_files SET server_received_at=clock_timestamp()-interval '2 hours' WHERE connection_id=$1 AND clip_id=$2`, cid, 700+n); err != nil {
			t.Fatal(err)
		}
		call()
		var actual, receipt, expiry, now time.Time
		if err := pool.QueryRow(ctx, `SELECT verified_at,server_received_at,LEAST(verified_at,server_received_at)+interval '24 hours',clock_timestamp() FROM nas_inventory_files WHERE connection_id=$1 AND clip_id=$2`, cid, 700+n).Scan(&actual, &receipt, &expiry, &now); err != nil {
			t.Fatal(err)
		}
		if !actual.Equal(verified) || expiry.After(verified.Add(24*time.Hour)) {
			t.Fatalf("replay invented verification age%d actual%s expiry%s", n, actual, expiry)
		}
		if age < 0 && !receipt.After(actual) {
			t.Fatal("expected receipt after verification")
		}
		if age > 0 && !receipt.Before(actual) {
			t.Fatal("expected allowed future-skew verification after receipt")
		}
		if age == -25*time.Hour && expiry.After(now) {
			t.Fatal("expired verification revived by replay")
		}
	}
}
