package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
)

func TestFinishRefreshFlagAndRequeueKeepsOriginalWork(t *testing.T) {
	f := newKeep28Fixture(t)
	_, windows, _ := api.Keep28Policy()
	w := windows[3]
	item := api.NASR2CopyItem{Kind: api.NASR2CopyRaw, SourceID: 9000001, RecordingID: 30, StreamID: 3, Bucket: "stoarama", ObjectKey: f.key, RelativePath: "keep28/1.mp4", SizeBytes: 100, SHA256: f.entrySHA(), StartAt: w.Start.Add(time.Hour), EndAt: w.Start.Add(2 * time.Hour)}
	data, _ := json.Marshal(item)
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,verified_at,completion,downloaded_bytes,committed_bytes) VALUES(13,'raw',9000001,repeat('a',64),$1,$2,'verified',now()-interval '25 hours','downloaded',100,100)`, f.policy(t), data); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_inventory_files SET verified_at=now()-interval '25 hours',server_received_at=now()-interval '25 hours'; UPDATE connections SET nas_storage_total_bytes=20000000000000,nas_storage_free_bytes=10000000000000,nas_storage_reported_at=now(),nas_storage_client_version='r2-copy-v5',nas_storage_budget_at=now()`); err != nil {
		t.Fatal(err)
	}
	status := finishStatus{}
	opts := finishRefreshOptions{Picks: f.options.Picks, PicksSHA: f.picksSHA(t)}
	n, err := feedKeep28Refresh(f.ctx, f.pool, opts, 1, &status)
	if err != nil || n != 0 {
		t.Fatalf("disabled %d %v", n, err)
	}
	if err = setKeep28Refresh(f.ctx, f.pool, true); err != nil {
		t.Fatal(err)
	}
	n, err = feedKeep28Refresh(f.ctx, f.pool, opts, 1, &status)
	if err != nil || n != 1 {
		t.Fatalf("enabled %d %v %+v", n, err, status)
	}
	var state, mode, completion string
	var downloaded, committed int64
	if err = f.pool.QueryRow(f.ctx, `SELECT state,work_mode,completion,downloaded_bytes,committed_bytes FROM nas_r2_copy_items`).Scan(&state, &mode, &completion, &downloaded, &committed); err != nil || state != "refresh_pending" || mode != "verify_existing" || completion != "downloaded" || downloaded != 100 || committed != 100 {
		t.Fatalf("history %s %s %s %d %d %v", state, mode, completion, downloaded, committed, err)
	}
	copyStatus, err := loadNASCopyStatus(f.ctx, f.pool)
	if err != nil || copyStatus.Downloaded.Objects != 1 || copyStatus.DownloadedBytes != 100 || copyStatus.CommittedBytes != 100 || copyStatus.RefreshQueueDepth != 1 {
		t.Fatalf("status erased original work: %+v %v", copyStatus, err)
	}

}
func (f keep28Fixture) entrySHA() string {
	return "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
}
func (f keep28Fixture) policy(t *testing.T) string {
	t.Helper()
	p, _, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func (f keep28Fixture) picksSHA(t *testing.T) string {
	t.Helper()
	p, err := loadR2RetirePicks(f.options.Picks)
	if err != nil {
		t.Fatal(err)
	}
	return p.SHA256
}
func TestRefreshDeficitShare(t *testing.T) {
	for _, test := range []struct{ depth, refresh, want int }{{10000, 0, 0}, {9998, 0, 1}, {9990, 0, 5}, {0, 0, 5000}, {0, 5000, 0}, {9999, 0, 0}} {
		if got := finishRefreshShare(test.depth, test.refresh); got != test.want {
			t.Fatalf("depth%d refresh%d got%d want%d", test.depth, test.refresh, got, test.want)
		}
	}
}

func TestFinishRefreshGetsShareOfContinuouslyFullOrdinaryQueue(t *testing.T) {
	f := newKeep28FixtureCount(t, 4)
	_, windows, _ := api.Keep28Policy()
	w := windows[3]
	item := api.NASR2CopyItem{Kind: api.NASR2CopyRaw, SourceID: 9000001, RecordingID: 30, StreamID: 3, Bucket: "stoarama", ObjectKey: f.key, RelativePath: "keep28/1.mp4", SizeBytes: 100, SHA256: f.entrySHA(), StartAt: w.Start.Add(time.Hour), EndAt: w.Start.Add(2 * time.Hour)}
	data, _ := json.Marshal(item)
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,verified_at,completion) VALUES(13,'raw',9000001,repeat('a',64),$1,$2,'verified',now()-interval '25 hours','verified_in_place')`, f.policy(t), data); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item)
 SELECT 13,'raw',10000000+n,repeat('a',64),$1,jsonb_build_object('bucket','stoarama','object_key','managed/dummy/'||n,'size_bytes',100) FROM generate_series(1,10000)n`, f.policy(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_inventory_files SET verified_at=now()-interval '25 hours',server_received_at=now()-interval '25 hours'; UPDATE r2_keep28_refresh_control SET enabled=true; UPDATE connections SET nas_storage_total_bytes=20000000000000,nas_storage_free_bytes=10000000000000,nas_storage_reported_at=now(),nas_storage_client_version='r2-copy-v5',nas_storage_budget_at=now()`); err != nil {
		t.Fatal(err)
	}
	var planID int64
	if err := f.pool.QueryRow(f.ctx, `INSERT INTO nas_cleanup_plans(account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode) VALUES(47,13,'refresh-fixture',repeat('d',64),'[]',repeat('d',64),'retired') RETURNING id`).Scan(&planID); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	safety := filepath.Join(dir, "safety")
	copyPath := filepath.Join(dir, "copy")
	finishTestJSON(t, safety, finishSafety{UpdatedAt: time.Now().UTC(), RecoveryCheckedAt: time.Now().UTC(), RecoveryPlanID: planID, RecoveryPlanSHA: strings.Repeat("d", 64), LocalCheckedAt: time.Now().UTC(), MasterCheckedAt: time.Now().UTC(), KnownPlans: []int64{}, KnownBatches: []int64{}, Holds: map[string]int64{}})
	finishTestJSON(t, copyPath, finishCopy{Schema: "stoarama.nas-r2-copy-status.v1", UpdatedAt: time.Now().UTC(), BlockedByReason: map[string]nasCopyStat{}})
	options := finishRefreshOptions{Picks: f.options.Picks, PicksSHA: f.picksSHA(t)}
	status, err := finishTickWithRefresh(f.ctx, f.pool, safety, copyPath, options)
	if err != nil || status.ImportedItems != 0 {
		t.Fatalf("full queue: %+v %v", status, err)
	}
	for turn := 0; turn < 2; turn++ {
		if _, err = f.pool.Exec(f.ctx, `UPDATE nas_r2_copy_items SET state='verified',verified_at=now(),completion='verified_in_place' WHERE source_id IN($1,$2)`, 10000001+2*turn, 10000002+2*turn); err != nil {
			t.Fatal(err)
		}
		status, err = finishTickWithRefresh(f.ctx, f.pool, safety, copyPath, options)
		if err != nil || status.RefreshAdmittedItems != 1 || status.ImportedHistoricalInventory != 1+turn || status.ImportedItems != 2+turn || status.QueueDepth != 10000 {
			t.Fatalf("fair tick%d: %+v %v", turn, status, err)
		}
		// Simulate the completed hash while keeping expired proof: an enabled B2
		// consumer may deliberately refresh it again; this bounded waste is allowed.
		if _, err = f.pool.Exec(f.ctx, `UPDATE nas_r2_copy_items SET state='verified',custody_refreshed_at=now(),refresh_count=refresh_count+1 WHERE source_id=9000001`); err != nil {
			t.Fatal(err)
		}
	}
}

func seedRefreshCandidates(t *testing.T, f keep28Fixture, sizes []int64) finishRefreshOptions {
	t.Helper()
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(f.ctx, `SET LOCAL session_replication_role=replica`); err != nil {
		t.Fatal(err)
	}
	_, windows, _ := api.Keep28Policy()
	w := windows[3]
	for n, size := range sizes {
		id := int64(9000001 + n)
		path := fmt.Sprintf("keep28/%d.mp4", n+1)
		key := fmt.Sprintf("managed/acct-47/keep28/%d.mp4", n+1)
		if _, err = tx.Exec(f.ctx, `UPDATE recording_clips SET size_bytes=$2 WHERE id=$1`, id, size); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(f.ctx, `UPDATE nas_inventory_files SET size_bytes=$2,verified_at=now()-interval '23 hours 30 minutes',server_received_at=now()-interval '23 hours 30 minutes' WHERE clip_id=$1`, id, size); err != nil {
			t.Fatal(err)
		}
		i := api.NASR2CopyItem{Kind: api.NASR2CopyRaw, SourceID: id, RecordingID: 30, StreamID: 3, Bucket: "stoarama", ObjectKey: key, RelativePath: path, SizeBytes: size, SHA256: f.entrySHA(), StartAt: w.Start.Add(time.Hour), EndAt: w.Start.Add(2 * time.Hour)}
		data, _ := json.Marshal(i)
		if _, err = tx.Exec(f.ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,verified_at,completion,downloaded_bytes,committed_bytes) VALUES(13,'raw',$1,repeat('a',64),$2,$3,'verified',now()-interval '24 hours','downloaded',$4,$4)`, id, f.policy(t), data, size); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.Exec(f.ctx, `UPDATE r2_keep28_refresh_control SET enabled=true; UPDATE connections SET nas_storage_total_bytes=20000000000000,nas_storage_free_bytes=10000000000000,nas_storage_reported_at=now(),nas_storage_client_version='r2-copy-v5',nas_storage_budget_at=now()`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	return finishRefreshOptions{Picks: f.options.Picks, PicksSHA: f.picksSHA(t)}
}
func TestFinishRefreshAdmitsMaximumFileAndSkipsOnlyRemainingByteCap(t *testing.T) {
	f := newKeep28FixtureCount(t, 3)
	opts := seedRefreshCandidates(t, f, []int64{api.NASR2CopyMaxFile, api.NASR2CopyMaxFile, 100})
	status := finishStatus{}
	n, err := feedKeep28Refresh(f.ctx, f.pool, opts, 3, &status)
	if err != nil || n != 2 {
		t.Fatalf("max-file batch %d %v %+v", n, err, status)
	}
	var bytes int64
	if err = f.pool.QueryRow(f.ctx, `SELECT sum((item->>'size_bytes')::bigint) FROM nas_r2_copy_items WHERE state='refresh_pending'`).Scan(&bytes); err != nil || bytes != 68719476836 {
		t.Fatalf("queued bytes %d %v", bytes, err)
	}
	n, err = feedKeep28Refresh(f.ctx, f.pool, opts, 3, &status)
	if err != nil || n != 1 {
		t.Fatalf("next max-file %d %v", n, err)
	}
}
func TestFinishRefreshAndB2ReservationRaceBothOrders(t *testing.T) {
	for _, refreshFirst := range []bool{false, true} {
		t.Run(fmt.Sprint("refresh-first-", refreshFirst), func(t *testing.T) {
			f := newKeep28Fixture(t)
			opts := seedRefreshCandidates(t, f, []int64{100})
			a := f.applier(t)
			entry := f.entry()
			gate, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer gate.Rollback(f.ctx)
			if _, err = gate.Exec(f.ctx, `SELECT lock_nas_r2_copy_key('stoarama',$1)`, f.key); err != nil {
				t.Fatal(err)
			}
			reserveResult := make(chan error, 1)
			refreshResult := make(chan struct {
				n   int
				err error
			}, 1)
			reserve := func() { _, err := a.reserve(f.ctx, entry); reserveResult <- err }
			refresh := func() {
				status := finishStatus{}
				n, err := feedKeep28Refresh(f.ctx, f.pool, opts, 1, &status)
				refreshResult <- struct {
					n   int
					err error
				}{n, err}
			}
			if refreshFirst {
				go refresh()
			} else {
				go reserve()
			}
			deadline := time.Now().Add(time.Second)
			for {
				var waiting bool
				if err = f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%lock_nas_r2_copy_key%')`).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("first admission did not reach exact key")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if refreshFirst {
				go reserve()
			} else {
				go refresh()
			}
			if err = gate.Commit(f.ctx); err != nil {
				t.Fatal(err)
			}
			reserveErr := <-reserveResult
			result := <-refreshResult
			if result.err != nil {
				t.Fatal(result.err)
			}
			if refreshFirst {
				if reserveErr == nil || result.n != 1 {
					t.Fatalf("refresh did not protect key: reserved=%v refresh=%d", reserveErr, result.n)
				}
			} else {
				if reserveErr != nil || result.n != 0 {
					t.Fatalf("durable intent did not veto refresh: reserved=%v refresh=%d", reserveErr, result.n)
				}
			}
		})
	}
}
