package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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
			e := f.entry(t)
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

// entry is the fixture key as selection would queue it, with the sidecar it
// observes now.
func (f keep28Fixture) entry(t *testing.T) inverseEntry {
	t.Helper()
	f.store.mu.Lock()
	e := inverseEntry{ObjectInfo: f.store.objects[f.key], Class: inverseKeep28}
	f.store.mu.Unlock()
	id := &e.Selected
	if err := f.pool.QueryRow(f.ctx, `SELECT c.id,c.size_bytes,c.display_path,lower(c.sha256),n.sidecar_size_bytes,n.sidecar_sha256 FROM recording_clips c JOIN nas_inventory_files n ON n.clip_id=c.id WHERE c.object_key=$1`, f.key).Scan(&id.SourceID, &id.SizeBytes, &id.Path, &id.SHA, &id.SidecarSize, &id.SidecarSHA); err != nil {
		t.Fatal(err)
	}
	return e
}

func TestKeep28RejectsChangedProofWithSameExpiry(t *testing.T) {
	for name, change := range map[string]string{"sidecar": `sidecar_sha256=repeat('d',64)`, "newer-timestamp": `verified_at=clock_timestamp()`} {
		t.Run(name, func(t *testing.T) {
			f := newKeep28Fixture(t)
			a := f.applier(t)
			e := f.entry(t)
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
			auth, err := a.authorize(f.ctx, tx, e, &inverseSendGate{})
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
func TestKeep28CustodyDeadlineIsFixedAtSampling(t *testing.T) {
	// DB UTC can differ arbitrarily from local wall time. Only DB-relative
	// lifetime and the monotonic sample time may place custody end, and a pause
	// between the sample and the timer made from it must not move it.
	c := keep28DBClock{utc: time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC), observed: time.Now()}
	want := c.observed.Add(time.Second) // 3s of custody less the 2s margin
	time.Sleep(200 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), c.deadline(c.utc.Add(3*time.Second)))
	defer cancel()
	if got, _ := ctx.Deadline(); !got.Equal(want) {
		t.Fatalf("custody end moved with the pause: %v, want %v", got, want)
	}
	stale := keep28DBClock{utc: c.utc, observed: time.Now().Add(-2 * time.Second)}
	if end := stale.deadline(c.utc.Add(time.Second + keep28CustodyMargin)); end.After(time.Now()) {
		t.Fatalf("expired handoff gained time: %v", time.Until(end))
	}
}

// deadlineSpyConn records the socket write deadline the wire sets.
type deadlineSpyConn struct {
	net.Conn
	set *atomic.Value
}

func (c deadlineSpyConn) SetWriteDeadline(d time.Time) error {
	c.set.Store(d)
	return c.Conn.SetWriteDeadline(d)
}

// Neither the authority's deadline nor the wire's first-byte and socket
// deadline lies past custody expiry, measured against the database clock
// reaching that expiry. The authority's timer is the earlier of the network
// timeout and custody end. A pause separates its clock sample from the
// transport's, which must land exactly on the earliest custody end.
func TestKeep28DeadlinesNeverOutliveCustody(t *testing.T) {
	f := newKeep28Fixture(t)
	_, auth := f.keep28Authorized(t, f.applier(t), &inverseSendGate{})
	// Custody expiry as a local instant, never earlier than the true one: the
	// local reading follows the database's. Deadlines keep a 2s margin from it.
	expiry := func() time.Time {
		t.Helper()
		var now time.Time
		if err := f.pool.QueryRow(f.ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			t.Fatal(err)
		}
		return time.Now().Add(auth.custody.Expiry.Sub(now))
	}
	authorized, _ := auth.ctx.Deadline()
	if authorized.After(auth.custodyEnd) || time.Until(authorized) > inverseNetworkTimeout || authorized.After(expiry().Add(-2*time.Second)) {
		t.Fatalf("authority outlives custody: deadline %v, custody end %v", authorized, auth.custodyEnd)
	}
	time.Sleep(300 * time.Millisecond)
	var wire atomic.Value
	template := newInverseDeleteHTTPTransport()
	dial := template.DialContext
	template.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return deadlineSpyConn{Conn: conn, set: &wire}, nil
	}
	original := f.sdk.transport
	f.sdk.transport = template
	defer func() { f.sdk.transport = original }()
	// An earlier clock sample: custody end now precedes the transport's fresh
	// sample, so the wire deadline must equal it exactly.
	auth.custodyEnd = time.Now().Add(3 * time.Second)
	if err := f.sdk.deleteOnce(f.ctx, auth); err != nil {
		t.Fatal(err)
	}
	deadline, ok := wire.Load().(time.Time)
	if !ok || !deadline.Equal(auth.custodyEnd) || deadline.After(expiry().Add(-2*time.Second)) {
		t.Fatalf("wire outlives custody: deadline %v, custody end %v", deadline, auth.custodyEnd)
	}
}

func TestKeep28AuthorityCopiesCannotReplay(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	e := f.entry(t)
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
	auth, err := a.authorize(f.ctx, tx, e, &inverseSendGate{})
	if err != nil {
		t.Fatal(err)
	}
	defer auth.cancel()
	copy := *auth
	if err = f.sdk.deleteOnce(auth.ctx, auth); err != nil {
		t.Fatal(err)
	}
	if err = f.sdk.deleteOnce(copy.ctx, &copy); err == nil {
		t.Fatal("copied authority replayed")
	}
	if f.deletes() != 1 {
		t.Fatalf("provider calls %d", f.deletes())
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

// setWire installs loopback server answers and a server-side DELETE hook.
func (f keep28Fixture) setWire(modes map[string]string, hook func(string)) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	f.store.wire, f.store.onDelete = modes, hook
}

// The keep-28 DELETE crosses the same first-wire transport as RAW: only a
// request its connection accepted in full with no response byte is a lost
// response, held for good; any response byte stops the job. Restart never
// resends either one.
func TestKeep28WireOutcomes(t *testing.T) {
	for _, mode := range []string{"eof", "503", "503-truncated", "503-headers-cut"} {
		t.Run(mode, func(t *testing.T) {
			f := newKeep28Fixture(t)
			f.setWire(map[string]string{f.key: mode}, nil)
			result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
			lost := mode == "eof"
			if err == nil || errors.Is(err, errInverseWireAmbiguous) != lost || slices.Equal(result.WireAmbiguous, []string{f.key}) != lost || result.Deleted != 0 || f.deletes() != 1 {
				t.Fatalf("%s: %+v %v deletes=%d", mode, result, err, f.deletes())
			}
			// Only a lost response may tell the operator the result is ambiguous.
			if text := err.Error(); strings.Contains(text, "ambiguous") != lost || strings.Contains(text, "single DELETE failed") == lost {
				t.Fatalf("%s: operator text %q", mode, text)
			}
			f.setWire(nil, nil)
			result, err = applyInverse(f.ctx, f.pool, f.sdk, f.options)
			if err != nil || result.AmbiguousHeld != 1 || slices.Equal(result.HeldAbsent, []string{f.key}) != lost || result.Deleted != 0 || f.deletes() != 1 {
				t.Fatalf("%s restart: %+v %v deletes=%d", mode, result, err, f.deletes())
			}
		})
	}
}

// A slow answer is a lost response, cut at custody end rather than the
// 30-second cap, while the custody row stays locked on the wire. Reserve admits
// only a key with a network timeout of custody to spare, so an earlier clock
// sample leaves the authority 3s here.
func TestKeep28WireDeadlineAndCustodyLock(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	tx, auth := f.keep28Authorized(t, a, &inverseSendGate{})
	var unlocked atomic.Value
	f.setWire(map[string]string{f.key: "slow"}, func(string) {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			unlocked.Store(err.Error())
			return
		}
		defer tx.Rollback(f.ctx)
		if _, err = tx.Exec(f.ctx, `SET LOCAL lock_timeout='100ms'; UPDATE nas_inventory_files SET state='missing' WHERE clip_id=9000001`); err == nil {
			unlocked.Store("custody row unlocked at wire")
		}
	})
	auth.custodyEnd = time.Now().Add(3 * time.Second)
	started := time.Now()
	err := f.sdk.deleteOnce(auth.ctx, auth)
	f.setWire(nil, nil)
	if !errors.Is(err, errInverseWireAmbiguous) || time.Since(started) > 15*time.Second || f.deletes() != 1 || unlocked.Load() != nil {
		t.Fatalf("slow answer: %v after %v deletes=%d lock=%v", err, time.Since(started), f.deletes(), unlocked.Load())
	}
	tx.Rollback(f.ctx)
	a.ledger.close()
	result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
	if err != nil || result.AmbiguousHeld != 1 || !slices.Equal(result.HeldAbsent, []string{f.key}) || f.deletes() != 1 {
		t.Fatalf("expiry replay %+v %v", result, err)
	}
}

// writeManifest makes the partition list keys 1..n.
func (f *keep28Fixture) writeManifest(t *testing.T, n int) {
	t.Helper()
	file, err := os.Create(f.options.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for i := 1; i <= n; i++ {
		f.store.mu.Lock()
		object := f.store.objects[fmt.Sprintf("managed/acct-47/keep28/%d.mp4", i)]
		f.store.mu.Unlock()
		if err = json.NewEncoder(file).Encode(inverseEntry{ObjectInfo: object, Class: inverseKeep28}); err != nil {
			t.Fatal(err)
		}
	}
	f.options.ManifestSHA, _ = inverseFileSHA(f.options.Manifest)
}

// durableWrites counts every row a reservation, intent, attempt or budget
// charge would leave.
func (f keep28Fixture) durableWrites(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM r2_retirement_reservations)+(SELECT count(*) FROM r2_keep28_intents)+(SELECT count(*) FROM r2_keep28_attempts)+(SELECT count(*) FROM r2_inverse_budget_minutes)`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A key whose custody could end within one network timeout plus the margin is
// never selected, so it takes no allowance, and reserve skips it under lock
// before any reservation, intent, budget charge or ledger record. Once custody
// is fresh, the next run deletes it.
func TestKeep28ShortCustodySkipsBeforeAnyWrite(t *testing.T) {
	for _, left := range []string{"1.75 seconds", "31.5 seconds"} {
		t.Run(left, func(t *testing.T) {
			f := newKeep28Fixture(t)
			if _, err := f.pool.Exec(f.ctx, `UPDATE nas_inventory_files SET verified_at=clock_timestamp()-interval '24 hours'+$1::interval,server_received_at=clock_timestamp()`, left); err != nil {
				t.Fatal(err)
			}
			result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
			if err != nil || result.Selected != 0 || result.Deleted != 0 || f.durableWrites(t) != 0 || f.deletes() != 0 {
				t.Fatalf("short custody selected: %+v %v", result, err)
			}
			a := f.applier(t)
			if _, err = a.reserve(f.ctx, f.entry(t)); !errors.Is(err, errKeep28CustodyShort) || f.durableWrites(t) != 0 || len(a.ledger.seen) != 0 {
				t.Fatalf("short custody reserved: %v, %d durable writes", err, f.durableWrites(t))
			}
			a.ledger.close()
			if _, err = f.pool.Exec(f.ctx, `UPDATE nas_inventory_files SET verified_at=clock_timestamp()-interval '1 hour'`); err != nil {
				t.Fatal(err)
			}
			result, err = applyInverse(f.ctx, f.pool, f.sdk, f.options)
			if err != nil || result.Deleted != 1 || result.CustodyShort.Objects != 0 || f.deletes() != 1 {
				t.Fatalf("fresh custody: %+v %v deletes %d", result, err, f.deletes())
			}
		})
	}
}

// Exact custody that ages out between selection and reserve is a skip with no
// durable write, not "no rows"; a changed sidecar is still a hard veto.
func TestKeep28AgedCustodyIsSkippedChangedIdentityIsNot(t *testing.T) {
	f := newKeep28Fixture(t)
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_inventory_files SET verified_at=clock_timestamp()-interval '24 hours'+interval '1 second',server_received_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	a := f.applier(t)
	if _, err := a.reserve(f.ctx, f.entry(t)); !errors.Is(err, errKeep28CustodyShort) || f.durableWrites(t) != 0 {
		t.Fatalf("aged custody: %v, %d durable writes", err, f.durableWrites(t))
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_inventory_files SET verified_at=clock_timestamp()-interval '1 hour',sidecar_relative_path='other.mp4.stoarama.json'`); err != nil {
		t.Fatal(err)
	}
	if _, err := a.reserve(f.ctx, f.entry(t)); err == nil || errors.Is(err, errKeep28CustodyShort) || f.durableWrites(t) != 0 {
		t.Fatalf("changed sidecar: %v, %d durable writes", err, f.durableWrites(t))
	}
}

// While one key's DELETE is on the wire, a queued key's custody ages out: that
// key is skipped and the batch finishes. If its source or custody identity
// changes instead (another clip under the key, a new media SHA, a new sidecar
// path or digest), even with aged custody, the batch halts before that key's
// DELETE and nothing counts as custody_short.
func TestKeep28QueuedKeyCustodyOutcome(t *testing.T) {
	for change, sql := range map[string]string{
		"aged":     `UPDATE nas_inventory_files SET verified_at=clock_timestamp()-interval '24 hours 1 second' WHERE clip_id=9000001`,
		"sidecar":  `UPDATE nas_inventory_files SET sidecar_relative_path='other.mp4.stoarama.json' WHERE clip_id=9000001`,
		"identity": `UPDATE nas_inventory_files SET sha256=repeat('a',64) WHERE clip_id=9000001`,
		// Another valid digest: the sidecar selection observed was replaced.
		"sidecar_digest":      `UPDATE nas_inventory_files SET sidecar_sha256=repeat('d',64) WHERE clip_id=9000001`,
		"sidecar_digest_aged": `UPDATE nas_inventory_files SET sidecar_sha256=repeat('d',64),verified_at=clock_timestamp()-interval '24 hours 1 second' WHERE clip_id=9000001`,
		// The key now names clip 3: same size, ETag and digests, its own valid custody.
		"reassigned_clip": `UPDATE recording_clips SET object_key='managed/acct-47/keep28/moved.mp4' WHERE id=9000001; UPDATE recording_clips SET object_key='managed/acct-47/keep28/1.mp4' WHERE id=9000003`,
		// The same clip's media SHA changes in catalog and inventory alike.
		"media_sha": `UPDATE recording_clips SET sha256=repeat('e',64) WHERE id=9000001; UPDATE nas_inventory_files SET sha256=repeat('e',64) WHERE clip_id=9000001`,
	} {
		t.Run(change, func(t *testing.T) {
			// Selection order: the older key 2 first, then key 1 waits queued.
			// Clip 3 is outside the partition. HEAD carries no SHA metadata, so
			// only the identity selection observed can catch a changed source.
			f := newKeep28FixtureCount(t, 3)
			f.writeManifest(t, 2)
			f.store.headWithoutSHA = true
			// The change waits for the epoch row behind key 2's network
			// transaction, so it commits after key 2 and before key 1 reserves:
			// protection metadata changes cannot commit while a DELETE holds it.
			changed := make(chan error, 1)
			f.setWire(nil, func(key string) {
				if key == f.key {
					return
				}
				pid := make(chan int, 1)
				go func() {
					tx, err := f.pool.Begin(f.ctx)
					if err != nil {
						pid <- 0
						changed <- err
						return
					}
					defer tx.Rollback(f.ctx)
					var id int
					if err = tx.QueryRow(f.ctx, `SELECT pg_backend_pid()`).Scan(&id); err != nil {
						pid <- 0
						changed <- err
						return
					}
					pid <- id
					if _, err = tx.Exec(f.ctx, `SELECT epoch FROM r2_inverse_ke_epoch FOR UPDATE`); err == nil {
						if _, err = tx.Exec(f.ctx, sql); err == nil {
							err = tx.Commit(f.ctx)
						}
					}
					changed <- err
				}()
				id := <-pid
				for deadline := time.Now().Add(10 * time.Second); id != 0 && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
					var waiting bool
					if err := f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')`, id).Scan(&waiting); err != nil || waiting {
						return
					}
				}
			})
			result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
			f.setWire(nil, nil)
			select {
			case changeErr := <-changed:
				if changeErr != nil {
					t.Fatal(changeErr)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the queued key's change never committed")
			}
			aged := change == "aged"
			if (err == nil) != aged || result.Selected != 2 || result.Deleted != 1 || (result.CustodyShort.Objects == 1) != aged || f.deletes() != 1 {
				t.Fatalf("%s: %+v %v deletes %d", change, result, err, f.deletes())
			}
		})
	}
}

// A legacy inventory row without sidecar evidence is ineligible, not a batch
// failure: the eligible key beside it is selected and deleted.
func TestKeep28SelectionSkipsIncompleteEvidence(t *testing.T) {
	f := newKeep28FixtureCount(t, 2)
	f.writeManifest(t, 2)
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_inventory_files SET sidecar_relative_path=NULL,sidecar_size_bytes=NULL,sidecar_sha256=NULL WHERE clip_id=9000002`); err != nil {
		t.Fatal(err)
	}
	result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
	f.store.mu.Lock()
	_, eligibleLeft := f.store.objects[f.key]
	f.store.mu.Unlock()
	if err != nil || result.Selected != 1 || result.Deleted != 1 || eligibleLeft || f.deletes() != 1 {
		t.Fatalf("legacy row: %+v %v deletes %d", result, err, f.deletes())
	}
}

// A job batch never spends its allowance on a key that custody would skip:
// with one slot, the older short-custody key is passed over and the fresh key
// deleted, and the job is done only when nothing eligible is left.
func TestKeep28JobSelectionSkipsShortCustody(t *testing.T) {
	f := newKeep28FixtureCount(t, 2)
	f.writeManifest(t, 2)
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_inventory_files SET verified_at=clock_timestamp()-interval '24 hours'+interval '28 seconds',server_received_at=clock_timestamp() WHERE clip_id=9000002`); err != nil {
		t.Fatal(err)
	}
	f.options.MaxItems = 1
	owner := filepath.Join("..", "..", "..", "docs", "records", "r2-owner-fence-policy-20261118.md")
	dir := t.TempDir()
	done, first, err := runInverseJobBatch(f.ctx, f.pool, f.sdk, f.options, owner, filepath.Join(dir, "evidence-1.json"))
	f.store.mu.Lock()
	_, freshLeft := f.store.objects[f.key]
	f.store.mu.Unlock()
	if err != nil || done || first.Selected != 1 || first.Deleted != 1 || freshLeft || f.deletes() != 1 {
		t.Fatalf("first batch: done %v %+v %v", done, first, err)
	}
	done, second, err := runInverseJobBatch(f.ctx, f.pool, f.sdk, f.options, owner, filepath.Join(dir, "evidence-2.json"))
	if err != nil || !done || second.Selected != 0 || f.deletes() != 1 {
		t.Fatalf("second batch: done %v %+v %v", done, second, err)
	}
}

// slowSampleTracer delays the nth plain DB clock read on its pool after its
// local observation, as a slow network or a database clock step would.
type slowSampleTracer struct {
	nth   int64
	delay time.Duration
	seen  atomic.Int64
}

func (s *slowSampleTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if data.SQL == `SELECT clock_timestamp()` && s.seen.Add(1) == s.nth {
		time.Sleep(s.delay)
	}
	return ctx
}
func (*slowSampleTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// Through the production delete path, with its 30-second parent deadline, the
// socket deadline is the earlier of the parent deadline and custody end less
// the margin. Custody wins when the authorization clock sample is delayed 4s,
// as a database clock step would leave it; otherwise the parent wins.
func TestKeep28DeletePathComposesParentAndCustodyDeadlines(t *testing.T) {
	const delay = 4 * time.Second
	for _, custodyWins := range []bool{true, false} {
		t.Run(map[bool]string{true: "custody_wins", false: "parent_wins"}[custodyWins], func(t *testing.T) {
			f := newKeep28Fixture(t)
			if _, err := f.pool.Exec(f.ctx, `UPDATE nas_inventory_files SET verified_at=clock_timestamp()-interval '24 hours'+interval '34 seconds',server_received_at=clock_timestamp()`); err != nil {
				t.Fatal(err)
			}
			tracer := &slowSampleTracer{}
			if custodyWins {
				// Clock read 1 is reserve's admission sample; 2 is authorization's.
				tracer.nth, tracer.delay = 2, delay
			}
			config := f.pool.Config()
			config.ConnConfig.Tracer = tracer
			pool, err := pgxpool.NewWithConfig(f.ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			a := f.applier(t)
			a.pool = pool
			var wire atomic.Value
			template := newInverseDeleteHTTPTransport()
			dial := template.DialContext
			template.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := dial(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return deadlineSpyConn{Conn: conn, set: &wire}, nil
			}
			original := f.sdk.transport
			f.sdk.transport = template
			defer func() { f.sdk.transport = original }()
			called := time.Now()
			if ok, err := a.delete(f.ctx, f.ctx, &inverseSendGate{}, f.entry(t)); err != nil || !ok {
				t.Fatalf("delete: %v %v", ok, err)
			}
			deadline, _ := wire.Load().(time.Time)
			// Custody expiry less the margin as a local instant, bracketed by the
			// local readings around one database clock read.
			var dbNow time.Time
			before := time.Now()
			if err = f.pool.QueryRow(f.ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
				t.Fatal(err)
			}
			after := time.Now()
			var expiry time.Time
			if err = f.pool.QueryRow(f.ctx, `SELECT LEAST(verified_at,server_received_at)+interval '24 hours' FROM nas_inventory_files WHERE clip_id=9000001`).Scan(&expiry); err != nil {
				t.Fatal(err)
			}
			custodyLow, custodyHigh := before.Add(expiry.Sub(dbNow)-2*time.Second), after.Add(expiry.Sub(dbNow)-2*time.Second)
			parentLow, parentHigh := called.Add(inverseNetworkTimeout), time.Now().Add(inverseNetworkTimeout)
			if custodyWins {
				// The delayed sample places custody end up to the delay early.
				if deadline.After(custodyHigh.Add(-delay)) || deadline.Before(custodyLow.Add(-delay-time.Second)) || !deadline.Before(parentLow) {
					t.Fatalf("custody should bound the wire: %v, custody end %v..%v less %v, parent from %v", deadline, custodyLow, custodyHigh, delay, parentLow)
				}
				return
			}
			if deadline.Before(parentLow) || deadline.After(parentHigh) || !deadline.Before(custodyLow) {
				t.Fatalf("parent should bound the wire: %v, parent %v..%v, custody end from %v", deadline, parentLow, parentHigh, custodyLow)
			}
		})
	}
}

// slowClockTx delays one armed DB clock read after its local observation, as a
// slow network or scheduler would, which makes that sample the tightest.
type slowClockTx struct {
	pgx.Tx
	armed *atomic.Bool
}

func (t slowClockTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if sql == `SELECT clock_timestamp()` && t.armed.CompareAndSwap(true, false) {
		time.Sleep(600 * time.Millisecond)
	}
	return t.Tx.QueryRow(ctx, sql, args...)
}

// A delayed intermediate clock read gives the tightest custody end. Every later
// handoff keeps it: the wire's socket deadline and the confirming bound.
func TestKeep28HandoffsKeepTheTightestSample(t *testing.T) {
	f := newKeep28Fixture(t)
	tx, auth := f.keep28Authorized(t, f.applier(t), &inverseSendGate{})
	armed := &atomic.Bool{}
	auth.guard.tx = slowClockTx{Tx: tx, armed: armed}
	var wire atomic.Value
	template := newInverseDeleteHTTPTransport()
	dial := template.DialContext
	template.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return deadlineSpyConn{Conn: conn, set: &wire}, nil
	}
	original := f.sdk.transport
	f.sdk.transport = template
	defer func() { f.sdk.transport = original }()
	armed.Store(true)
	if err := f.sdk.deleteOnce(auth.ctx, auth); err != nil || armed.Load() {
		t.Fatalf("delete %v, delayed clock read pending %v", err, armed.Load())
	}
	// An undelayed sample places custody end about 600 ms after the delayed one.
	clock, err := keep28Clock(f.ctx, f.pool)
	if err != nil {
		t.Fatal(err)
	}
	tightest := clock.deadline(auth.custody.Expiry).Add(-500 * time.Millisecond)
	deadline, ok := wire.Load().(time.Time)
	if !ok || deadline.After(tightest) {
		t.Fatalf("socket deadline %v later than the tightest sample %v", deadline, tightest)
	}
	confirming, cancel, err := auth.bound(auth.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if got, _ := confirming.Deadline(); got.After(tightest) {
		t.Fatalf("confirming deadline %v later than the tightest sample %v", got, tightest)
	}
}

// keep28Authorized reserves the fixture key and issues its authority over a
// fresh network transaction.
func (f keep28Fixture) keep28Authorized(t *testing.T, a *inverseApplier, gate *inverseSendGate) (pgx.Tx, *inverseDeleteAuthority) {
	t.Helper()
	e := f.entry(t)
	if _, err := a.reserve(f.ctx, e); err != nil {
		t.Fatal(err)
	}
	tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback(f.ctx) })
	auth, err := a.authorize(f.ctx, tx, e, gate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(auth.cancel)
	return tx, auth
}

// The gate closes while the DELETE is past every check and dialing: its first
// request byte is refused, nothing reaches the provider, and the key stays held.
func TestKeep28SendGateClosedDuringDialSendsNothing(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	gate := &inverseSendGate{}
	tx, auth := f.keep28Authorized(t, a, gate)
	dialing, release := make(chan struct{}), make(chan struct{})
	template := newInverseDeleteHTTPTransport()
	dial := template.DialContext
	template.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		close(dialing)
		<-release
		return dial(ctx, network, addr)
	}
	original := f.sdk.transport
	f.sdk.transport = template
	done := make(chan error, 1)
	go func() { done <- f.sdk.deleteOnce(auth.ctx, auth) }()
	waitFor(t, dialing, "the DELETE dial")
	gate.close()
	close(release)
	err := <-done
	f.sdk.transport = original
	if !errors.Is(err, errInverseAdmissionClosed) || errors.Is(err, errInverseWireAmbiguous) || f.deletes() != 0 {
		t.Fatalf("closed gate: %v deletes=%d", err, f.deletes())
	}
	tx.Rollback(f.ctx)
	a.ledger.close()
	result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
	if err != nil || result.AmbiguousHeld != 1 || len(result.HeldAbsent) != 0 || result.Deleted != 0 || f.deletes() != 0 {
		t.Fatalf("restart: %+v %v deletes=%d", result, err, f.deletes())
	}
}

// A keep-28 authority is spoken over HTTP/1.1 on a fresh connection, even to an
// HTTP/2-capable TLS server.
func TestKeep28DeleteNeverSpeaksHTTP2(t *testing.T) {
	f := newKeep28Fixture(t)
	_, auth := f.keep28Authorized(t, f.applier(t), &inverseSendGate{})
	var protos []string
	var protosMu sync.Mutex
	var connections atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		protosMu.Lock()
		protos = append(protos, req.Proto)
		protosMu.Unlock()
		w.WriteHeader(204)
	}))
	server.EnableHTTP2 = true
	server.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			connections.Add(1)
		}
	}
	server.StartTLS()
	defer server.Close()
	tr := newInverseDeleteHTTPTransport()
	tr.TLSClientConfig.RootCAs = x509.NewCertPool()
	tr.TLSClientConfig.RootCAs.AddCert(server.Certificate())
	wire := &inverseWire{gate: auth.gate}
	client := &http.Client{Transport: &inverseDeleteTransport{base: wire.transport(tr), authority: auth, bucket: f.sdk.Bucket(), endpoint: f.sdk.EndpointSHA256(), wire: wire}}
	req, _ := http.NewRequestWithContext(auth.ctx, http.MethodDelete, server.URL+"/stoarama/"+f.key+"?x-id=DeleteObject", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	protosMu.Lock()
	defer protosMu.Unlock()
	if resp.ProtoMajor != 1 || resp.StatusCode != 204 || !slices.Equal(protos, []string{"HTTP/1.1"}) || connections.Load() != 1 || wire.lost() {
		t.Fatalf("DELETE over %s (%d), server saw %v over %d connections", resp.Proto, resp.StatusCode, protos, connections.Load())
	}
}

// Startup covers the keep-28 tables: an intent that never issued a DELETE but
// whose object is gone stops the run before any new work.
func TestKeep28UnissuedAbsentIntentStopsStartup(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	if _, err := a.reserve(f.ctx, f.entry(t)); err != nil {
		t.Fatal(err)
	}
	a.ledger.close()
	f.store.mu.Lock()
	delete(f.store.objects, f.key)
	f.store.mu.Unlock()
	if _, err := applyInverse(f.ctx, f.pool, f.sdk, f.options); err == nil || !strings.Contains(err.Error(), "never issued a DELETE") || f.deletes() != 0 {
		t.Fatalf("unissued absent intent: %v deletes=%d", err, f.deletes())
	}
}

// A keep-28 job batch with one lost response deletes the rest, and its readback
// of the keep-28 tables proves exactly that key unresolved. The next batch
// reports it issued and absent, never resends it, and finds nothing else.
func TestKeep28JobBatchHoldsOnlyTheLostKey(t *testing.T) {
	f := newKeep28FixtureCount(t, 4)
	f.writeManifest(t, 4)
	// One worker in verified_at order: keys 2-4 first, the fresher key 1 last.
	f.setWire(map[string]string{f.key: "eof"}, nil)
	owner := filepath.Join("..", "..", "..", "docs", "records", "r2-owner-fence-policy-20261118.md")
	dir := t.TempDir()
	done, first, err := runInverseJobBatch(f.ctx, f.pool, f.sdk, f.options, owner, filepath.Join(dir, "evidence-1.json"))
	f.setWire(nil, nil)
	if err != nil || done || first.Deleted != 3 || !slices.Equal(first.WireAmbiguous, []string{f.key}) || f.deletes() != 4 {
		t.Fatalf("lost-response batch: done %v %+v %v deletes=%d", done, first, err, f.deletes())
	}
	var unresolved []string
	var attempts, events, raw int
	if err = f.pool.QueryRow(f.ctx, `SELECT (SELECT coalesce(array_agg(i.object_key ORDER BY i.object_key),'{}') FROM r2_keep28_intents i WHERE NOT EXISTS(SELECT 1 FROM r2_keep28_events v WHERE v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent')),
 (SELECT count(*) FROM r2_keep28_attempts),(SELECT count(*) FROM r2_keep28_events),(SELECT count(*) FROM r2_inverse_intents)`).Scan(&unresolved, &attempts, &events, &raw); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(unresolved, []string{f.key}) || attempts != 4 || events != 3 || raw != 0 {
		t.Fatalf("keep-28 tables: unresolved %v attempts %d events %d RAW intents %d", unresolved, attempts, events, raw)
	}
	done, second, err := runInverseJobBatch(f.ctx, f.pool, f.sdk, f.options, owner, filepath.Join(dir, "evidence-2.json"))
	if err != nil || !done || second.Deleted != 0 || second.AmbiguousHeld != 1 || !slices.Equal(second.HeldAbsent, []string{f.key}) || f.deletes() != 4 {
		t.Fatalf("next batch: done %v %+v %v deletes=%d", done, second, err, f.deletes())
	}
}

// stallConn holds a DELETE's connection where a slow writer could resume after
// custody expiry. It counts only the bytes its socket accepted, and its Close
// waits for a write under way so HTTP cancellation cannot win that race.
type stallConn struct {
	net.Conn
	stall   func()
	sent    *atomic.Int64
	writing sync.Mutex
}

func (c *stallConn) Write(p []byte) (int, error) {
	c.writing.Lock()
	defer c.writing.Unlock()
	c.stall()
	n, err := c.Conn.Write(p)
	c.sent.Add(int64(n))
	return n, err
}
func (c *stallConn) Close() error {
	c.writing.Lock()
	defer c.writing.Unlock()
	return c.Conn.Close()
}

// A writer that resumes after custody expiry sends no request byte: neither
// one paused after acquiring its connection nor one paused inside its write.
func TestKeep28FirstByteNeverLeavesAfterCustody(t *testing.T) {
	for _, pause := range []string{"after_connection", "inside_write"} {
		t.Run(pause, func(t *testing.T) {
			f := newKeep28Fixture(t)
			_, auth := f.keep28Authorized(t, f.applier(t), &inverseSendGate{})
			// An earlier clock sample leaves 2s of custody; the writer resumes
			// only once that custody end has passed.
			end := time.Now().Add(2 * time.Second)
			auth.custodyEnd = end
			expired := func() { time.Sleep(time.Until(end) + 50*time.Millisecond) }
			var sent atomic.Int64
			var once sync.Once
			template := newInverseDeleteHTTPTransport()
			dial := template.DialContext
			template.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := dial(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				stall := func() {}
				if pause == "inside_write" {
					stall = func() { once.Do(expired) }
				}
				return &stallConn{Conn: conn, stall: stall, sent: &sent}, nil
			}
			original := f.sdk.transport
			f.sdk.transport = template
			defer func() { f.sdk.transport = original }()
			ctx := auth.ctx
			if pause == "after_connection" {
				ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { expired() }})
			}
			err := f.sdk.deleteOnce(ctx, auth)
			if err == nil || errors.Is(err, errInverseWireAmbiguous) || sent.Load() != 0 || f.deletes() != 0 {
				t.Fatalf("%s: %v, %d request bytes accepted, deletes=%d", pause, err, sent.Load(), f.deletes())
			}
		})
	}
}

// readRacesCancelConn cancels the DELETE as its first response bytes arrive
// and returns them only after the round trip has given up on cancellation.
type readRacesCancelConn struct {
	net.Conn
	cancel context.CancelFunc
	once   sync.Once
}

func (c *readRacesCancelConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.once.Do(func() { c.cancel(); time.Sleep(200 * time.Millisecond) })
	}
	return n, err
}

// A 503 whose bytes the socket delivered as cancellation ended the round trip
// is a hard failure, never a hold-only lost response.
func TestKeep28ResponseRacingCancellationIsAHardFailure(t *testing.T) {
	f := newKeep28Fixture(t)
	f.setWire(map[string]string{f.key: "503"}, nil)
	_, auth := f.keep28Authorized(t, f.applier(t), &inverseSendGate{})
	ctx, cancel := context.WithCancel(auth.ctx)
	defer cancel()
	template := newInverseDeleteHTTPTransport()
	dial := template.DialContext
	template.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &readRacesCancelConn{Conn: conn, cancel: cancel}, nil
	}
	original := f.sdk.transport
	f.sdk.transport = template
	defer func() { f.sdk.transport = original }()
	err := f.sdk.deleteOnce(ctx, auth)
	if err == nil || errors.Is(err, errInverseWireAmbiguous) || f.deletes() != 1 {
		t.Fatalf("answered DELETE: %v deletes=%d", err, f.deletes())
	}
}

// Absence publication resolves the exact intended source: once the clip's
// parent recording names another stream, it commits no receipt, no absent
// event and no purge; the unchanged source publishes.
func TestKeep28PublishAbsentRefusesChangedParentStream(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	e := f.entry(t)
	if _, err := a.reserve(f.ctx, e); err != nil {
		t.Fatal(err)
	}
	outcome := func() (receipts, events int, purged bool) {
		t.Helper()
		if err := f.pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM r2_object_retirements WHERE object_key=$1),(SELECT count(*) FROM r2_keep28_events WHERE object_key=$1),(SELECT purged_at IS NOT NULL FROM recording_clips WHERE object_key=$1)`, f.key).Scan(&receipts, &events, &purged); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 SELECT 4,provider,'4','other','other',source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps FROM streams WHERE id=3`); err != nil {
		t.Fatal(err)
	}
	for _, stream := range []string{"4", "NULL"} {
		if _, err := f.pool.Exec(f.ctx, `UPDATE recordings SET stream_id=`+stream+` WHERE id=30`); err != nil {
			t.Fatal(err)
		}
		err := a.publishAbsent(f.ctx, e, a.opts.ManifestSHA)
		if receipts, events, purged := outcome(); err == nil || !strings.Contains(err.Error(), "exact source") || receipts != 0 || events != 0 || purged {
			t.Fatalf("stream %s published: %v receipts %d events %d purged %v", stream, err, receipts, events, purged)
		}
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE recordings SET stream_id=3 WHERE id=30`); err != nil {
		t.Fatal(err)
	}
	err := a.publishAbsent(f.ctx, e, a.opts.ManifestSHA)
	if receipts, events, purged := outcome(); err != nil || receipts != 1 || events != 1 || !purged {
		t.Fatalf("exact source: %v receipts %d events %d purged %v", err, receipts, events, purged)
	}
}

// job-keep28 dispatches to the keep-28 limits: a 100-object default batch and
// no run without explicit per-batch and rolling 24h byte limits. An oversized
// worker pool stops before any work directory, partition or evidence exists.
func TestKeep28JobCLIDispatchAndLimits(t *testing.T) {
	if args := os.Getenv("STOARAMA_TEST_R2_INVERSE_ARGS"); args != "" {
		runR2Inverse(context.Background(), config.Config{DatabaseURL: os.Getenv("STOARAMA_TEST_DATABASE_URL")}, strings.Split(args, "\n"))
		t.Fatal("r2-inverse returned without stopping")
	}
	raw, err := parseInverseJobFlags([]string{"--picks", "p", "--expected-picks-sha256", strings.Repeat("a", 64), "--owner-decision", "o", "--work", "w"}, inverseOutsideKeep28)
	if err != nil || raw.items != 5000 || raw.maxBytes != 1<<40 || raw.budget24h != 0 {
		t.Fatalf("RAW job defaults changed: %+v %v", raw, err)
	}
	for reuse, ok := range map[string]bool{strings.Repeat("b", 64): true, "B" + strings.Repeat("b", 63): false, "bb": false} {
		f, err := parseInverseJobFlags([]string{"--picks", "p", "--expected-picks-sha256", strings.Repeat("a", 64), "--owner-decision", "o", "--work", "w", "--reuse-partition", reuse}, inverseOutsideKeep28)
		if (err == nil) != ok || ok && f.reuse != reuse {
			t.Fatalf("--reuse-partition %q: %+v %v", reuse, f, err)
		}
	}
	dir := t.TempDir()
	picks := filepath.Join(dir, "picks.json")
	if err = os.WriteFile(picks, []byte(`{"choices":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	digest, _ := inverseFileSHA(picks)
	work := filepath.Join(dir, "work")
	base := []string{"--picks", picks, "--expected-picks-sha256", digest, "--owner-decision", "owner.md", "--work", work}
	keep28, err := parseInverseJobFlags(append(base, "--max-bytes", "10000", "--budget-24h-bytes", "0"), inverseNASKeep28)
	if err != nil || keep28.items != 100 || keep28.maxBytes != 10000 || keep28.budget24h != 0 {
		t.Fatalf("keep-28 job defaults: %+v %v", keep28, err)
	}
	invoke := func(args ...string) (string, error) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestKeep28JobCLIDispatchAndLimits$")
		cmd.Env = append(os.Environ(), "STOARAMA_TEST_R2_INVERSE_ARGS="+strings.Join(args, "\n"))
		output, err := cmd.CombinedOutput()
		return string(output), err
	}
	for command, want := range map[string]string{"job": "(default 5000)", "job-keep28": "(default 100)"} {
		if out, err := invoke(command, "--help"); err != nil || !strings.Contains(out, "Usage of r2-inverse "+command+":") || !strings.Contains(out, "-max-items") || !strings.Contains(out, want) {
			t.Fatalf("%s --help: %v %s", command, err, out)
		}
	}
	run := func(args ...string) string {
		t.Helper()
		output, err := invoke(args...)
		if err == nil {
			t.Fatalf("%v ran: %s", args, output)
		}
		if _, statErr := os.Stat(work); !os.IsNotExist(statErr) {
			t.Fatalf("%v created its work directory before refusing: %s", args, output)
		}
		return string(output)
	}
	for _, missing := range [][]string{{}, {"--max-bytes", "10000"}, {"--budget-24h-bytes", "0"}} {
		if out := run(append(append([]string{"job-keep28"}, base...), missing...)...); !strings.Contains(out, "job-keep28 requires --max-bytes and --budget-24h-bytes") {
			t.Fatalf("job-keep28 %v: %s", missing, out)
		}
	}
	if os.Getenv("STOARAMA_TEST_DATABASE_URL") == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL for the worker capacity check")
	}
	// The same arguments pass RAW parsing and reach the capacity check:
	// dispatch, not the flags, chose the keep-28 limits.
	if out := run(append(append([]string{"job"}, base...), "--workers", "19")...); !strings.Contains(out, "workers exceed database capacity") {
		t.Fatalf("RAW job: %s", out)
	}
	if out := run(append(append([]string{"job-keep28"}, base...), "--max-bytes", "10000", "--budget-24h-bytes", "0", "--workers", "19")...); !strings.Contains(out, "workers exceed database capacity") {
		t.Fatalf("oversized workers: %s", out)
	}
}
