package api

import (
	"encoding/json"
	"strings"
	"testing"
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
