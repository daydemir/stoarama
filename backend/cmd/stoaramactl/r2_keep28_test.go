package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeep28LedgerRejectsOtherLaneWithoutAppend(t *testing.T) {
	for _, b2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "B1-refuses-B2", true: "B2-refuses-B1"}[b2], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ledger")
			r := inverseLedgerRecord{Bucket: "stoarama", EndpointSHA256: strings.Repeat("a", 64), Event: inverseIntent, Key: "managed/test", ManifestSHA256: strings.Repeat("b", 64), At: time.Now().UTC(), SizeBytes: 100}
			var l *inverseLedger
			var err error
			if b2 {
				l, err = openInverseLedger(path)
			} else {
				l, err = openKeep28Ledger(path)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = l.append(r); err != nil {
				t.Fatal(err)
			}
			l.close()
			before, _ := os.ReadFile(path)
			if b2 {
				l, err = openKeep28Ledger(path)
			} else {
				l, err = openInverseLedger(path)
			}
			if err == nil {
				l.close()
				t.Fatal("wrong-lane ledger accepted")
			}
			after, _ := os.ReadFile(path)
			if string(before) != string(after) {
				t.Fatal("wrong-lane open modified ledger")
			}
		})
	}
}

func TestKeep28ExecutorFreshCustodyAndNoReplay(t *testing.T) {
	f := newKeep28Fixture(t)
	result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
	if err != nil || result.Deleted != 1 || f.deletes() != 1 {
		t.Fatalf("fresh custody: %+v %v provider=%d", result, err, f.deletes())
	}
	result, err = applyInverse(f.ctx, f.pool, f.sdk, f.options)
	if err != nil || result.Deleted != 0 || f.deletes() != 1 {
		t.Fatalf("replay: %+v %v provider=%d", result, err, f.deletes())
	}
	var b1, b2, events int
	f.pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM r2_inverse_intents),(SELECT count(*) FROM r2_keep28_intents),(SELECT count(*) FROM r2_keep28_events)`).Scan(&b1, &b2, &events)
	if b1 != 0 || b2 != 1 || events != 1 {
		t.Fatalf("history mixed: %d %d %d", b1, b2, events)
	}
}

func TestKeep28UnlimitedBudgetRetainsAccounting(t *testing.T) {
	f := newKeep28Fixture(t)
	inverseUseOwnerFenceEvidence(t, &f.options)
	const charged = 6_000_000_000_000
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_inverse_budget_minutes(bucket,minute,charged_bytes) VALUES('stoarama',date_trunc('minute',clock_timestamp()),$1)`, int64(charged)); err != nil {
		t.Fatal(err)
	}
	f.options.Budget24h = 5_000_000_000_000
	if _, err := applyInverse(f.ctx, f.pool, f.sdk, f.options); err == nil || f.deletes() != 0 {
		t.Fatal("positive exhausted budget reached wire", err)
	}
	f.options.Budget24h = 0
	result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
	if err != nil || result.Deleted != 1 || f.deletes() != 1 {
		t.Fatalf("unlimited above 5 TB: %+v %v provider=%d", result, err, f.deletes())
	}
	var got int64
	if err = f.pool.QueryRow(f.ctx, `SELECT sum(charged_bytes) FROM r2_inverse_budget_minutes WHERE bucket='stoarama'`).Scan(&got); err != nil || got != charged+100 {
		t.Fatal("unlimited budget lost accounting", got, err)
	}
}

func TestKeep28LastModifiedCutoffReportsHeldBytes(t *testing.T) {
	for _, modified := range []time.Time{{}, inverseRetirementCutoff(), inverseRetirementCutoff().Add(time.Second), time.Now().Add(time.Hour)} {
		t.Run(modified.Format(time.RFC3339Nano), func(t *testing.T) {
			f := newKeep28Fixture(t)
			e := f.entry()
			e.LastModified = modified
			data, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(f.options.Manifest, data, 0600); err != nil {
				t.Fatal(err)
			}
			f.options.ManifestSHA, _ = inverseFileSHA(f.options.Manifest)
			inverseUseOwnerFenceEvidence(t, &f.options)
			result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
			if err != nil || result.Deleted != 0 || result.LastModifiedHeld.Objects != 1 || result.LastModifiedHeld.Bytes != 100 || f.deletes() != 0 {
				t.Fatalf("cutoff hold: %+v err=%v provider=%d", result, err, f.deletes())
			}
		})
	}
}

func TestKeep28ChangedHeadReportsHeldBytesAndStops(t *testing.T) {
	f := newKeep28Fixture(t)
	inverseUseOwnerFenceEvidence(t, &f.options)
	f.store.mu.Lock()
	o := f.store.objects[f.key]
	o.LastModified = inverseRetirementCutoff()
	f.store.objects[f.key] = o
	f.store.mu.Unlock()
	result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
	if err == nil || result.Deleted != 0 || result.LastModifiedHeld.Objects != 1 || result.LastModifiedHeld.Bytes != 100 || f.deletes() != 0 {
		t.Fatalf("changed HEAD: %+v err=%v provider=%d", result, err, f.deletes())
	}
}

func TestKeep28OwnerFenceEvidenceBinding(t *testing.T) {
	f := newKeep28Fixture(t)
	inverseUseOwnerFenceEvidence(t, &f.options)
	a := f.applier(t)
	var original inverseDrainEvidence
	if err := inverseReadPinned(a.opts.DrainEvidence, a.opts.DrainSHA, &original); err != nil {
		t.Fatal(err)
	}
	if err := a.checkEvidence(f.ctx, f.pool); err != nil {
		t.Fatal("valid owner evidence", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*inverseDrainEvidence)
	}{
		{"bucket", func(e *inverseDrainEvidence) { e.Bucket = "other" }},
		{"endpoint", func(e *inverseDrainEvidence) { e.EndpointSHA256 = r2tSHA }},
		{"activation", func(e *inverseDrainEvidence) { e.FenceActivatedAt = e.FenceActivatedAt.Add(time.Microsecond) }},
		{"deadline", func(e *inverseDrainEvidence) { e.DrainNotBefore = e.DrainNotBefore.Add(time.Second) }},
		{"observation before deadline", func(e *inverseDrainEvidence) { e.ObservedAt = e.DrainNotBefore }},
		{"future observation", func(e *inverseDrainEvidence) { e.ObservedAt = time.Now().Add(time.Hour) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := original
			tc.mutate(&e)
			data, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(a.opts.DrainEvidence, data, 0600); err != nil {
				t.Fatal(err)
			}
			a.opts.DrainSHA, _ = inverseFileSHA(a.opts.DrainEvidence)
			if err = a.checkEvidence(f.ctx, f.pool); err == nil || f.deletes() != 0 {
				t.Fatal("unbound owner evidence accepted", err)
			}
		})
	}
}

type keep28Fixture struct {
	ctx     context.Context
	pool    *pgxpool.Pool
	sdk     *inverseSDKStore
	store   *inverseFakeStore
	deletes func() int64
	options inverseApplyOptions
	key     string
}

func newKeep28Fixture(t *testing.T) keep28Fixture { return newKeep28FixtureCount(t, 1) }
func newKeep28FixtureCount(t *testing.T, count int) keep28Fixture {
	t.Helper()
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "keep28")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	f := keep28Fixture{ctx: ctx, pool: pool, key: "managed/acct-47/keep28/1.mp4"}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO accounts(id,email) VALUES(47,'keep28@test.invalid');
 INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(1,47,'keep28','fixture');
 INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',1);
 INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps) VALUES(3,'test','3','keep28','keep28','https://test.invalid','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
 INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc) VALUES(1,47,'test','https://test.invalid','auto','stoarama','fixture',decode('00','hex'));
 INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at) VALUES(30,47,1,'keep28','https://test.invalid','* * * * *','Asia/Tokyo','paused',3,now());`)
	policy, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	w := windows[3]
	exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path) VALUES(9000001,30,1,'https://test.invalid','stoarama',$1,100,'etag',repeat('b',64),$2,$2,$3,'keep28/1.mp4')`, f.key, w.Start.Add(time.Hour), w.Start.Add(2*time.Hour))
	exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at,server_received_at,file_ctime_ns,file_inode,file_device,sidecar_relative_path,sidecar_size_bytes,sidecar_sha256) VALUES(13,9000001,30,'keep28/1.mp4',100,repeat('b',64),'present',clock_timestamp()-interval '1 hour',clock_timestamp(),clock_timestamp()-interval '2 hours',1,2,3,'keep28/1.mp4.stoarama.json',50,repeat('c',64))`)
	object := r2.ObjectInfo{Key: f.key, ETag: "etag", SizeBytes: 100, LastModified: inverseRetirementCutoff().Add(-time.Second)}
	f.store = &inverseFakeStore{objects: map[string]r2.ObjectInfo{f.key: object}}
	for n := 2; n <= count; n++ {
		key := fmt.Sprintf("managed/acct-47/keep28/%d.mp4", n)
		path := fmt.Sprintf("keep28/%d.mp4", n)
		exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path)
 SELECT $1,recording_id,storage_destination_id,endpoint,bucket,$2,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,$3 FROM recording_clips WHERE id=9000001`, 9000000+n, key, path)
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at,server_received_at,file_ctime_ns,file_inode,file_device,sidecar_relative_path,sidecar_size_bytes,sidecar_sha256)
 SELECT connection_id,$1,recording_id,$2,size_bytes,sha256,state,clock_timestamp()-interval '3 hours',client_updated_at,server_received_at,file_ctime_ns,file_inode,file_device,$2||'.stoarama.json',sidecar_size_bytes,sidecar_sha256 FROM nas_inventory_files WHERE clip_id=9000001`, 9000000+n, path)
		second := object
		second.Key = key
		f.store.objects[key] = second
	}

	f.sdk, f.deletes = inverseSDKTestStore(t, f.store)
	f.store.endpointSHA = f.sdk.EndpointSHA256()
	exec(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, f.sdk.EndpointSHA256())
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `SET LOCAL session_replication_role=replica; UPDATE r2_retirement_writer_fences SET activated_at='2026-11-12T14:28:47.037031Z',drain_not_before=clock_timestamp()-interval '1 hour'`)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var activated, deadline time.Time
	if err = pool.QueryRow(ctx, `SELECT activated_at,drain_not_before FROM r2_retirement_writer_fences`).Scan(&activated, &deadline); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	write := func(name string, v any) string {
		t.Helper()
		path := filepath.Join(dir, name)
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, append(b, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	picksPath := write("picks.json", map[string]any{"choices": map[string]any{"3": r2RetirePick{Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}})
	picks, err := loadR2RetirePicks(picksPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest := write("partition.jsonl", inverseEntry{ObjectInfo: object, Class: inverseKeep28})
	manifestSHA, _ := inverseFileSHA(manifest)
	summary := write("summary.json", inverseSummary{Version: 1, Bucket: "stoarama", EndpointSHA256: f.sdk.EndpointSHA256(), CreatedAt: time.Now().UTC(), PolicySHA256: policy, PicksSHA256: picks.SHA256, ReadOnly: true})
	summarySHA, _ := inverseFileSHA(summary)
	drain := write("drain.json", inverseDrainEvidence{Proof: inversePUTCompletionProof, Bucket: "stoarama", EndpointSHA256: f.sdk.EndpointSHA256(), FenceActivatedAt: activated, DrainNotBefore: deadline, ObservedAt: time.Now().UTC(), DeployedWriterCoverage: "synthetic fixture has no writer", AcceptedPUTCompletion: "synthetic provider accepted no PUT"})
	drainSHA, _ := inverseFileSHA(drain)
	f.options = inverseApplyOptions{Lane: inverseNASKeep28, Manifest: manifest, ManifestSHA: manifestSHA, Summary: summary, SummarySHA: summarySHA, Picks: picksPath, Ledger: filepath.Join(dir, "ledger"), DrainEvidence: drain, DrainSHA: drainSHA, MaxItems: 100, MaxBytes: 10000, Budget24h: 10000, Workers: 1}
	return f
}

// A stopped process can leave only its committed intent. Restart must never
// adopt a parent recording's changed stream as the intended source identity.
func TestKeep28CrashIdentityFreezeAndExactReconciliation(t *testing.T) {
	f := newKeep28FixtureCount(t, 2)
	broken := &keep28ReadStore{inverseSDKStore: f.sdk, headErr: errors.New("process stopped before final HEAD")}
	if _, err := applyInverse(f.ctx, f.pool, broken, f.options); err == nil {
		t.Fatal("expected stopped attempt")
	}
	if f.deletes() != 0 {
		t.Fatal("wire ran before HEAD")
	}
	for _, sql := range []string{`UPDATE recording_clips SET sha256=repeat('a',64) WHERE id=9000001`, `UPDATE recording_clips SET display_path='other.mp4' WHERE id=9000001`, `DELETE FROM recording_clips WHERE id=9000001`, `UPDATE recording_clips SET object_key='managed/acct-47/keep28/1.mp4' WHERE id=9000002`} {
		if _, err := f.pool.Exec(f.ctx, sql); err == nil {
			t.Fatalf("pending identity mutated: %s", sql)
		}
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE recordings SET stream_id=NULL WHERE id=30`); err != nil {
		t.Fatal(err)
	}
	f.store.mu.Lock()
	delete(f.store.objects, f.key)
	f.store.mu.Unlock()
	if _, err := applyInverse(f.ctx, f.pool, f.sdk, f.options); err == nil {
		t.Fatal("reconciliation adopted changed parent stream")
	}
}

type keep28ReadStore struct {
	*inverseSDKStore
	headErr error
}

func (s *keep28ReadStore) Head(ctx context.Context, key string) (r2.ObjectHead, error) {
	if s.headErr != nil {
		return r2.ObjectHead{}, s.headErr
	}
	return s.inverseSDKStore.Head(ctx, key)
}

func TestKeep28CustodyVetoesBeforeIntent(t *testing.T) {
	for name, change := range map[string]string{
		"old-inventory": `verified_at=clock_timestamp()-interval '25 hours'`,
		"old-receipt":   `server_received_at=clock_timestamp()-interval '25 hours'`,
		"missing":       `state='missing'`, "wrong-sha": `sha256=repeat('a',64)`,
		"wrong-size": `size_bytes=99`, "wrong-path": `relative_path='other.mp4'`,
		"wrong-sidecar": `sidecar_relative_path='other.mp4.stoarama.json'`,
	} {
		t.Run(name, func(t *testing.T) {
			f := newKeep28Fixture(t)
			if _, err := f.pool.Exec(f.ctx, `UPDATE nas_inventory_files SET `+change); err != nil {
				t.Fatal(err)
			}
			result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
			if err != nil || result.Deleted != 0 || f.deletes() != 0 {
				t.Fatalf("custody veto: %+v %v", result, err)
			}
			var intents int
			f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_keep28_intents`).Scan(&intents)
			if intents != 0 {
				t.Fatal("stale proof consumed permanent intent")
			}
		})
	}
}
func (f keep28Fixture) applier(t *testing.T) *inverseApplier {
	t.Helper()
	var sum inverseSummary
	if err := inverseReadPinned(f.options.Summary, f.options.SummarySHA, &sum); err != nil {
		t.Fatal(err)
	}
	picks, err := loadR2RetirePicks(f.options.Picks)
	if err != nil {
		t.Fatal(err)
	}
	_, windows, _ := api.Keep28Policy()
	p, err := loadInverseApplyProtection(f.ctx, f.pool, "stoarama", picks, windows)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := openKeep28Ledger(f.options.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ledger.close() })
	var epoch int64
	if err = f.pool.QueryRow(f.ctx, `SELECT epoch FROM r2_inverse_ke_epoch`).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	return &inverseApplier{pool: f.pool, store: f.sdk, opts: f.options, summary: sum, protection: p, ledger: ledger, keEpoch: epoch}
}
func (f keep28Fixture) entry() inverseEntry {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	return inverseEntry{ObjectInfo: f.store.objects[f.key], Class: inverseKeep28}
}

func TestKeep28RejectsChangedProofWithSameExpiry(t *testing.T) {
	for name, change := range map[string]string{"sidecar": `sidecar_sha256=repeat('d',64)`, "newer-timestamp": `verified_at=clock_timestamp()`} {
		t.Run(name, func(t *testing.T) {
			f := newKeep28Fixture(t)
			a := f.applier(t)
			e := f.entry()
			if _, err := a.reserve(f.ctx, e); err != nil {
				t.Fatal(err)
			}
			if err := a.record(e, inverseIntent, a.opts.ManifestSHA); err != nil {
				t.Fatal(err)
			}
			if _, err := f.pool.Exec(f.ctx, `UPDATE nas_inventory_files SET `+change); err != nil {
				t.Fatal(err)
			}
			tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(f.ctx)
			auth, err := a.authorizeKeep28(f.ctx, tx, e)
			if err == nil {
				auth.cancel()
				t.Fatal("changed proof accepted")
			}
			if !strings.Contains(err.Error(), "proof changed") {
				t.Fatal(err)
			}
			if f.deletes() != 0 {
				t.Fatal("changed proof reached provider")
			}
		})
	}
}
func TestKeep28LostResponseReconcilesWithoutReplay(t *testing.T) {
	f := newKeep28Fixture(t)
	f.store.setLost(true)
	if _, err := applyInverse(f.ctx, f.pool, f.sdk, f.options); err == nil {
		t.Fatal("lost response reported success")
	}
	f.store.setLost(false)
	result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
	if err != nil || result.Reconciled != 1 || result.Deleted != 0 || f.deletes() != 1 {
		t.Fatalf("reconcile %+v %v deletes=%d", result, err, f.deletes())
	}
}
func TestKeep28ExistingPresentIntentNeverReissues(t *testing.T) {
	f := newKeep28Fixture(t)
	broken := &keep28ReadStore{inverseSDKStore: f.sdk, headErr: errors.New("process stopped")}
	if _, err := applyInverse(f.ctx, f.pool, broken, f.options); err == nil {
		t.Fatal("expected stop")
	}
	result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
	if err != nil || result.AmbiguousHeld != 1 || result.Deleted != 0 || f.deletes() != 0 {
		t.Fatalf("present intent replay: %+v %v", result, err)
	}
}
func TestKeep28CustodyClockConsumesHandoffTime(t *testing.T) {
	// DB UTC can differ arbitrarily from local wall time. Only DB-relative
	// lifetime and monotonic elapsed duration may bound the wire.
	c := keep28DBClock{utc: time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC), observed: time.Now().Add(-2 * time.Second)}
	if rem := c.remaining(c.utc.Add(time.Second)); rem > 0 {
		t.Fatalf("expired handoff gained time: %v", rem)
	}
	if rem := c.remaining(c.utc.Add(5 * time.Second)); rem <= 0 || rem > 3*time.Second {
		t.Fatalf("local wall clock used: %v", rem)
	}
}

func TestKeep28AuthorityCopiesCannotReplay(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	e := f.entry()
	if _, err := a.reserve(f.ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := a.record(e, inverseIntent, a.opts.ManifestSHA); err != nil {
		t.Fatal(err)
	}
	tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	auth, err := a.authorizeKeep28(f.ctx, tx, e)
	if err != nil {
		t.Fatal(err)
	}
	defer auth.cancel()
	copy := *auth
	if err = f.sdk.deleteKeep28Once(auth.ctx, auth); err != nil {
		t.Fatal(err)
	}
	if err = f.sdk.deleteKeep28Once(copy.ctx, &copy); err == nil {
		t.Fatal("copied authority replayed")
	}
	if f.deletes() != 1 {
		t.Fatalf("provider calls %d", f.deletes())
	}
}

type keep28RoundTripFunc func(*http.Request) (*http.Response, error)

func (f keep28RoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestKeep28WireDeadlineAndCustodyLock(t *testing.T) {
	f := newKeep28Fixture(t)
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_inventory_files SET verified_at=clock_timestamp()-interval '23 hours 59 minutes 55 seconds',server_received_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	var observed atomic.Bool
	f.sdk.transport = keep28RoundTripFunc(func(req *http.Request) (*http.Response, error) {
		deadline, ok := req.Context().Deadline()
		if !ok || time.Until(deadline) > 5*time.Second {
			t.Error("wire lacks custody deadline")
		}
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		if _, err = tx.Exec(f.ctx, `SET LOCAL lock_timeout='100ms'; UPDATE nas_inventory_files SET state='missing' WHERE clip_id=9000001`); err == nil {
			t.Error("custody row unlocked at wire")
		}
		observed.Store(true)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	if _, err := applyInverse(f.ctx, f.pool, f.sdk, f.options); err == nil {
		t.Fatal("expected custody expiry")
	}
	if !observed.Load() || f.deletes() != 0 {
		t.Fatalf("wire deadline observation=%v deletes=%d", observed.Load(), f.deletes())
	}
	result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
	if err != nil || result.AmbiguousHeld != 1 || f.deletes() != 0 {
		t.Fatalf("expiry replay %+v %v", result, err)
	}
}
func TestKeep28HoldsAndClassVetoes(t *testing.T) {
	for name, sql := range map[string]string{
		"delivery":       `INSERT INTO delivery_nas_source_holds(clip_id) VALUES(9000001)`,
		"A1-blocked":     `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,blocked_reason) VALUES(13,'raw',9000001,repeat('a',64),repeat('b',64),'{"bucket":"stoarama","object_key":"managed/acct-47/keep28/1.mp4"}','blocked','checksum_mismatch')`,
		"outside-window": `UPDATE recording_clips SET clip_start_at='2026-06-01',clip_end_at='2026-06-01 00:01'`,
		"unknown":        `UPDATE recordings SET stream_id=NULL WHERE id=30`,
	} {
		t.Run(name, func(t *testing.T) {
			f := newKeep28Fixture(t)
			if _, err := f.pool.Exec(f.ctx, sql); err != nil {
				t.Fatal(err)
			}
			result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
			if err == nil || result.Deleted != 0 || f.deletes() != 0 {
				t.Fatalf("veto %+v %v", result, err)
			}
		})
	}
}

func TestKeep28SelectionTrailsOldestFreshCustody(t *testing.T) {
	f := newKeep28FixtureCount(t, 2)
	key := "managed/acct-47/keep28/2.mp4"
	f.store.mu.Lock()
	object := f.store.objects[f.key]
	object.Key = key
	f.store.objects[key] = object
	f.store.mu.Unlock()
	file, err := os.OpenFile(f.options.Manifest, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.NewEncoder(file).Encode(inverseEntry{ObjectInfo: object, Class: inverseKeep28}); err != nil {
		t.Fatal(err)
	}
	file.Close()
	f.options.ManifestSHA, _ = inverseFileSHA(f.options.Manifest)
	f.options.MaxItems = 1
	result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
	if err != nil || result.Deleted != 1 {
		t.Fatalf("%+v %v", result, err)
	}
	f.store.mu.Lock()
	_, newer := f.store.objects[f.key]
	_, older := f.store.objects[key]
	f.store.mu.Unlock()
	if !newer || older {
		t.Fatal("manifest order overrode verified_at order")
	}
	for _, sql := range []string{`UPDATE r2_keep28_intents SET size_bytes=1`, `DELETE FROM r2_keep28_intents`, `TRUNCATE r2_keep28_intents CASCADE`, `UPDATE r2_keep28_events SET event='absent'`, `DELETE FROM r2_keep28_events`, `TRUNCATE r2_keep28_events`, `UPDATE r2_keep28_attempts SET issued_at=now()`, `DELETE FROM r2_keep28_attempts`, `TRUNCATE r2_keep28_attempts`} {
		if _, err = f.pool.Exec(f.ctx, sql); err == nil {
			t.Fatalf("B2 history mutable: %s", sql)
		}
	}
}
