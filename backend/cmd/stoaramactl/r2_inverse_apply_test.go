package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

type inverseFakeStore struct {
	endpointSHA  string
	mu           sync.Mutex
	objects      map[string]r2.ObjectInfo
	calls        int
	lost         bool
	delay        time.Duration
	beforeHead   func(string) error
	beforeDelete func(string) error
}

func (s *inverseFakeStore) Bucket() string         { return "stoarama" }
func (s *inverseFakeStore) EndpointSHA256() string { return s.endpointSHA }
func (s *inverseFakeStore) ListPrefix(context.Context, string, int, func(r2.ObjectInfo) error) error {
	return errors.New("apply must not relist")
}
func (s *inverseFakeStore) RequireNoMultipartUploads(context.Context) error { return nil }
func (s *inverseFakeStore) Head(ctx context.Context, key string) (r2.ObjectHead, error) {
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return r2.ObjectHead{}, ctx.Err()
		}
	}
	if s.beforeHead != nil {
		if err := s.beforeHead(key); err != nil {
			return r2.ObjectHead{}, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[key]
	if !ok {
		return r2.ObjectHead{}, &smithy.GenericAPIError{Code: "NotFound"}
	}
	return r2.ObjectHead{ETag: o.ETag, SizeBytes: o.SizeBytes, LastModified: o.LastModified}, nil
}
func (s *inverseFakeStore) DeleteNonKeep28Once(ctx context.Context, authority *r2.NonKeep28DeleteAuthority) error {
	key := authority.Binding().Key
	if err := authority.RequireWire(ctx, s.Bucket(), s.EndpointSHA256()); err != nil {
		return err
	}
	if s.beforeDelete != nil {
		if err := s.beforeDelete(key); err != nil {
			return err
		}
	}
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	delete(s.objects, key)
	if s.lost {
		return errors.New("lost response")
	}
	return nil
}
func TestR2InverseExecutorLifecycle(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "inverse_apply")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	policy, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	w := windows[3]
	root := t.TempDir()
	picksPath := filepath.Join(root, "picks.json")
	picksData, _ := json.Marshal(map[string]any{"choices": map[string]any{"3": r2RetirePick{Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}})
	if err = os.WriteFile(picksPath, picksData, 0600); err != nil {
		t.Fatal(err)
	}
	picks, err := loadR2RetirePicks(picksPath)
	if err != nil {
		t.Fatal(err)
	}
	exec(`INSERT INTO accounts(id,email) VALUES(47,'inverse@test.invalid');
 INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps) VALUES(3,'test','3','inverse','inverse','https://test.invalid','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
 INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc) VALUES(1,47,'test','https://test.invalid','auto','stoarama','fixture',decode('00','hex'));
 INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at) VALUES(30,47,1,'inverse','https://test.invalid','* * * * *','Asia/Tokyo','paused',3,now());`)
	const count = 1200
	store := &inverseFakeStore{objects: map[string]r2.ObjectInfo{}}
	sdk, providerDeletes := inverseSDKTestStore(t, store)
	store.endpointSHA = sdk.EndpointSHA256()
	for n := range count {
		key := fmt.Sprintf("managed/acct-47/ordinary/%04d.mp4", n)
		etag := fmt.Sprintf("etag-%04d", n)
		store.objects[key] = r2.ObjectInfo{Key: key, ETag: etag, SizeBytes: 100, LastModified: time.Now().UTC().Add(-8 * 24 * time.Hour).Truncate(time.Second)}
		exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path) VALUES($1,30,1,'https://test.invalid','stoarama',$2,100,$3,repeat('b',64),'2026-06-01','2026-06-01','2026-06-01 00:01:00','ordinary/'||$4)`, 9000000+n, key, etag, fmt.Sprintf("%04d.mp4", n))
	}
	exec(`INSERT INTO recording_collation_hours(id,hour_id,batch_id,generation,recording_id,local_date,delivery_hour,status,manifest_object_key,manifest_sha256,manifest,source_clip_count,included_clip_count,excluded_clip_count,join_count,split_count)
 VALUES(1,'inverse__recording-30__date-2026-06-01__hour-01__generation-2','inverse',2,30,'2026-06-01',1,'collated','joined/inverse/manifest.json',repeat('a',64),'{}',1,1,0,0,0);
 INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 VALUES(1,1,1,1,'joined/inverse/objects/'||repeat('a',64)||'.mp4','joined/ordinary/test-output.mp4',100,repeat('a',64),'2026-06-01','2026-06-01 00:01',ARRAY[9000000]::bigint[],now());`)
	exec(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, store.EndpointSHA256())
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `SET LOCAL session_replication_role=replica; UPDATE r2_retirement_writer_fences SET activated_at=clock_timestamp()-interval '8 days',drain_not_before=clock_timestamp()-interval '23 hours 58 minutes'`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var activated, deadline time.Time
	if err = pool.QueryRow(ctx, `SELECT activated_at,drain_not_before FROM r2_retirement_writer_fences`).Scan(&activated, &deadline); err != nil {
		t.Fatal(err)
	}
	drain := inverseDrainEvidence{Bucket: "stoarama", EndpointSHA256: store.EndpointSHA256(), FenceActivatedAt: activated, DrainNotBefore: deadline, ObservedAt: time.Now().UTC(), DeployedWriterCoverage: "test fixture all admissions checked", AcceptedPUTCompletion: "test fixture no provider PUTs accepted"}
	drainPath := filepath.Join(root, "drain.json")
	data, _ := json.Marshal(drain)
	if err = os.WriteFile(drainPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	drainSHA, _ := inverseFileSHA(drainPath)
	next := 0
	build := func(n int) (*inverseApplier, []inverseEntry) {
		t.Helper()
		dir := filepath.Join(root, fmt.Sprintf("run%d", next))
		if err = os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		var entries []inverseEntry
		for i := next; i < next+n; i++ {
			key := fmt.Sprintf("managed/acct-47/ordinary/%04d.mp4", i)
			entries = append(entries, inverseEntry{ObjectInfo: store.objects[key], Class: inverseDeletable})
		}
		next += n
		manifest := filepath.Join(dir, "partition.jsonl")
		f, err := os.Create(manifest)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if err = json.NewEncoder(f).Encode(e); err != nil {
				t.Fatal(err)
			}
		}
		f.Close()
		sha, _ := inverseFileSHA(manifest)
		sum := inverseSummary{Version: 1, Bucket: "stoarama", EndpointSHA256: store.EndpointSHA256(), CreatedAt: time.Now().UTC(), PolicySHA256: policy, PicksSHA256: picks.SHA256, ReadOnly: true}
		summary := filepath.Join(dir, "summary.json")
		data, _ := json.Marshal(sum)
		os.WriteFile(summary, data, 0600)
		summarySHA, _ := inverseFileSHA(summary)
		o := inverseApplyOptions{Manifest: manifest, ManifestSHA: sha, Summary: summary, SummarySHA: summarySHA, Picks: picksPath, Ledger: filepath.Join(dir, "ledger.jsonl"), DrainEvidence: drainPath, DrainSHA: drainSHA, MaxItems: n, MaxBytes: int64(n) * 100, Budget24h: 200000, Workers: 8}
		p, err := loadInverseProtection(ctx, pool, "stoarama", picks, windows)
		if err != nil {
			t.Fatal(err)
		}
		ledger, err := openInverseLedger(o.Ledger)
		if err != nil {
			t.Fatal(err)
		}
		var epoch int64
		if err = pool.QueryRow(ctx, `SELECT epoch FROM r2_inverse_ke_epoch`).Scan(&epoch); err != nil {
			t.Fatal(err)
		}
		return &inverseApplier{pool: pool, store: store, opts: o, summary: sum, protection: p, ledger: ledger, keEpoch: epoch}, entries
	}
	t.Run("success", func(t *testing.T) {
		a, _ := build(2)
		a.ledger.close()
		s, err := applyInverse(ctx, pool, store, a.opts)
		if err != nil || s.Deleted != 2 {
			t.Fatalf("%+v %v", s, err)
		}
		var receipts int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM r2_inverse_events`).Scan(&receipts); err != nil || receipts != 2 {
			t.Fatal(receipts, err)
		}
	})
	t.Run("durable_history_cannot_be_erased_or_rolled_back", func(t *testing.T) {
		for _, query := range []string{`DELETE FROM r2_inverse_intents`, `UPDATE r2_inverse_intents SET size_bytes=1`, `TRUNCATE r2_inverse_intents CASCADE`, `DELETE FROM r2_inverse_events`, `TRUNCATE r2_inverse_events`, `UPDATE r2_inverse_budget_minutes SET charged_bytes=1`, `DELETE FROM r2_inverse_budget_minutes`, `TRUNCATE r2_inverse_budget_minutes`, `UPDATE r2_inverse_ke_epoch SET epoch=0`, `DELETE FROM r2_inverse_ke_epoch`, `TRUNCATE r2_inverse_ke_epoch`} {
			if _, err := pool.Exec(ctx, query); err == nil {
				t.Fatal("durable authority could be erased", query)
			}
		}
		var events int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM r2_inverse_events`).Scan(&events); err != nil || events != 2 {
			t.Fatal("ledger history changed", events, err)
		}
	})
	t.Run("crash_after_intent_and_restart", func(t *testing.T) {
		a, entries := build(1)
		if _, err := a.reserve(ctx, entries[0]); err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{`UPDATE recording_clips SET display_path='changed' WHERE object_key=$1`, `UPDATE recording_clips SET id=id+100000 WHERE object_key=$1`} {
			if _, err := pool.Exec(ctx, query, entries[0].Key); err == nil {
				t.Fatal("pending source identity changed", query)
			}
		}
		a.ledger.close()
		before := store.calls
		s, err := applyInverse(ctx, pool, store, a.opts)
		if err != nil || s.AmbiguousHeld != 1 || store.calls != before {
			t.Fatalf("%+v %v calls%d", s, err, store.calls)
		}
	})
	t.Run("lost_response_reconciles_without_retransmit", func(t *testing.T) {
		a, _ := build(1)
		a.ledger.close()
		store.lost = true
		_, err := applyInverse(ctx, pool, store, a.opts)
		store.lost = false
		if err == nil {
			t.Fatal("lost response accepted")
		}
		before := store.calls
		s, err := applyInverse(ctx, pool, store, a.opts)
		if err != nil || s.Reconciled != 1 || store.calls != before {
			t.Fatalf("%+v %v calls%d", s, err, store.calls)
		}
	})
	t.Run("protection_changed_during_HEAD", func(t *testing.T) {
		a, entries := build(1)
		a.ledger.close()
		target := entries[0].Key
		var once sync.Once
		store.beforeHead = func(key string) error {
			if key == target {
				once.Do(func() {
					if err := os.WriteFile(picksPath, append(picksData, ' '), 0600); err != nil {
						t.Fatal(err)
					}
				})
			}
			return nil
		}
		before := store.calls
		_, err := applyInverse(ctx, pool, store, a.opts)
		store.beforeHead = nil
		if restoreErr := os.WriteFile(picksPath, picksData, 0600); restoreErr != nil {
			t.Fatal(restoreErr)
		}
		if err == nil || store.calls != before {
			t.Fatal("changed picks reached DELETE", err)
		}
	})
	t.Run("delivery_admitted_before_intent_wins", func(t *testing.T) {
		for _, kind := range []string{"key", "source"} {
			t.Run(kind, func(t *testing.T) {
				a, entries := build(1)
				defer a.ledger.close()
				if kind == "key" {
					exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',100,'metadata')`, entries[0].Key)
				} else {
					exec(`INSERT INTO delivery_nas_source_holds SELECT id FROM recording_clips WHERE object_key=$1`, entries[0].Key)
				}
				if _, err := a.reserve(ctx, entries[0]); err == nil {
					t.Fatal("delivery admission lost to intent")
				}
			})
		}
	})
	t.Run("late_admission_blocked_through_wire_then_intent_vetoes", func(t *testing.T) {
		for _, kind := range []string{"delivery_key", "delivery_source", "delivery_output", "delivery_path_enrichment", "collated_key"} {
			t.Run(kind, func(t *testing.T) {
				a, entries := build(1)
				defer a.ledger.close()
				e := entries[0]
				var id int64
				if err := pool.QueryRow(ctx, `SELECT id FROM recording_clips WHERE object_key=$1`, e.Key).Scan(&id); err != nil {
					t.Fatal(err)
				}
				exec(`UPDATE recording_collation_outputs SET source_clip_ids=ARRAY[$1]::bigint[] WHERE id=1`, id)
				query := `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',100,'metadata')`
				var arg any = e.Key
				switch kind {
				case "delivery_source":
					query = `INSERT INTO delivery_nas_source_holds(clip_id) VALUES($1)`
					arg = id
				case "delivery_output":
					query = `INSERT INTO delivery_keep_output_dependencies(output_id,recording_id) VALUES(1,$1)`
					arg = int64(30)
				case "delivery_path_enrichment":
					placeholder := "datasets/test/" + strconv.FormatInt(id, 10)
					exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',100,'metadata')`, placeholder)
					query = `UPDATE delivery_keep_holds SET relative_path=(SELECT display_path FROM recording_clips WHERE id=$1) WHERE object_key='` + placeholder + `'`
					arg = id
				case "collated_key":
					query = `UPDATE recording_collation_outputs SET object_key=$1 WHERE id=1`
				}
				store.beforeDelete = func(key string) error {
					if key != e.Key {
						return errors.New("unexpected wire key")
					}
					// This runs after the final live guard, while the network transaction
					// owns shared protection locks. Direct SQL must be unable to commit.
					lateCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
					defer cancel()
					_, err := pool.Exec(lateCtx, query, arg)
					if err == nil || lateCtx.Err() == nil {
						return fmt.Errorf("late admission was not fenced: %v", err)
					}
					return nil
				}
				before := store.calls
				ok, err := a.delete(ctx, e)
				store.beforeDelete = nil
				if err != nil || !ok || store.calls != before+1 {
					t.Fatalf("wire result %v %v", ok, err)
				}
				if _, err = pool.Exec(ctx, query, arg); err == nil || !strings.Contains(err.Error(), "reservation") {
					t.Fatal("committed intent failed to forbid later admission", err)
				}
			})
		}
	})
	t.Run("missing_source_cannot_publish_completion", func(t *testing.T) {
		a, entries := build(1)
		defer a.ledger.close()
		e := entries[0]
		target, err := a.reserve(ctx, e)
		if err != nil {
			t.Fatal(err)
		}
		// Deliberately bypass triggers in this isolated test database to prove the
		// publication invariant independently from the identity freeze trigger.
		move := func(from, to int64) {
			t.Helper()
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err = tx.Exec(ctx, `SET LOCAL session_replication_role=replica`); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec(ctx, `UPDATE recording_clips SET id=$2 WHERE id=$1`, from, to); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		}
		move(target.ID, target.ID+100000)
		defer move(target.ID+100000, target.ID)
		if err = a.publishAbsent(ctx, e, a.opts.ManifestSHA, true); err == nil {
			t.Fatal("missing source published completion")
		}
		var events int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM r2_inverse_events WHERE object_key=$1`, e.Key).Scan(&events); err != nil || events != 0 {
			t.Fatal(events, err)
		}
	})
	t.Run("installed_purge_veto_precedes_intent_and_wire", func(t *testing.T) {
		a, entries := build(1)
		a.ledger.close()
		exec(`CREATE FUNCTION test_inverse_purge_veto() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test installed purge veto'; END $$;
 CREATE TRIGGER zz_test_inverse_purge_veto BEFORE UPDATE OF purged_at ON recording_clips FOR EACH ROW EXECUTE FUNCTION test_inverse_purge_veto();`)
		before := store.calls
		_, err := applyInverse(ctx, pool, store, a.opts)
		exec(`DROP TRIGGER zz_test_inverse_purge_veto ON recording_clips; DROP FUNCTION test_inverse_purge_veto()`)
		if err == nil || !strings.Contains(err.Error(), "pre-delete purge veto") || store.calls != before {
			t.Fatal("purge veto reached wire", err)
		}
		var count int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM r2_inverse_intents WHERE object_key=$1`, entries[0].Key).Scan(&count); err != nil || count != 0 {
			t.Fatal("veto published intent", count, err)
		}
	})
	t.Run("budget", func(t *testing.T) {
		a, _ := build(2)
		a.ledger.close()
		a.opts.Budget24h = 1
		before := store.calls
		_, err := applyInverse(ctx, pool, store, a.opts)
		if err == nil || store.calls != before {
			t.Fatal("budget exceeded", err)
		}
	})
	t.Run("real_SDK_canary_contract", func(t *testing.T) {
		a, entries := build(100)
		a.ledger.close()
		before := providerDeletes()
		result, err := applyInverse(ctx, pool, sdk, a.opts)
		if err != nil || result.Deleted != 100 || providerDeletes()-before != 100 {
			t.Fatalf("SDK canary %+v deletes%d err%v", result, providerDeletes()-before, err)
		}
		var count int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM r2_inverse_events e JOIN r2_inverse_intents i USING(bucket,object_key) WHERE i.manifest_sha256=$1`, a.opts.ManifestSHA).Scan(&count); err != nil || count != 100 {
			t.Fatal(count, err)
		}
		ledger, err := openInverseLedger(a.opts.Ledger)
		if err != nil {
			t.Fatal(err)
		}
		defer ledger.close()
		count = 0
		for _, record := range ledger.seen {
			if record.Event == inverseAbsent && record.ManifestSHA256 == a.opts.ManifestSHA {
				count++
			}
		}
		if count != 100 {
			t.Fatal("ledger/provider mismatch", count)
		}
		for _, e := range entries {
			if _, err = sdk.Head(ctx, e.Key); !r2.IsNotFound(err) {
				t.Fatal("SDK confirming HEAD", e.Key, err)
			}
		}
	})
	t.Run("worker_pool_rate_with_durable_DB_and_ledger", func(t *testing.T) {
		a, _ := build(1024)
		a.ledger.close()
		store.delay = 100 * time.Millisecond
		defer func() { store.delay = 0 }()
		s, err := applyInverse(ctx, pool, store, a.opts)
		if err != nil || s.Deleted != 1024 {
			t.Fatalf("%+v %v", s, err)
		}
		t.Logf("objects/s %.2f (full schema, real locks/intent commits/fsync,100ms each HEAD/DELETE)", s.ObjectsPerSecond)
		if s.ObjectsPerSecond < 8 {
			t.Fatalf("rate %.2f below8", s.ObjectsPerSecond)
		}
	})
}
func TestR2InverseLedgerRestartAndPartialTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger")
	l, err := openInverseLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	r := inverseLedgerRecord{Event: inverseIntent, Bucket: "stoarama", EndpointSHA256: strings.Repeat("a", 64), Key: "managed/x", ManifestSHA256: strings.Repeat("a", 64), At: time.Now().UTC(), SizeBytes: 1}
	if err = l.append(r); err != nil {
		t.Fatal(err)
	}
	l.close()
	l, err = openInverseLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = l.append(r); err != nil {
		t.Fatal(err)
	}
	l.close()
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	io.WriteString(f, "{")
	f.Close()
	if _, err = openInverseLedger(path); err == nil {
		t.Fatal("partial ledger tail accepted")
	}
}

// Exercises the actual SDK, single-attempt transport and live DB authority in
// one end-to-end path. The endpoint is an isolated loopback server, never R2.
func inverseSDKTestStore(t *testing.T, store *inverseFakeStore) (*r2.Client, func() int64) {
	t.Helper()
	var deleted atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet && req.URL.Path == "/stoarama" && req.URL.Query().Has("uploads") {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`)
			return
		}
		key := strings.TrimPrefix(req.URL.Path, "/stoarama/")
		store.mu.Lock()
		defer store.mu.Unlock()
		o, ok := store.objects[key]
		switch req.Method {
		case http.MethodHead:
			if !ok {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("ETag", `"`+o.ETag+`"`)
			w.Header().Set("Content-Length", strconv.FormatInt(o.SizeBytes, 10))
			w.Header().Set("Last-Modified", o.LastModified.Format(http.TimeFormat))
			w.Header().Set("x-amz-meta-sha256", strings.Repeat("b", 64))
			w.WriteHeader(200)
		case http.MethodDelete:
			deleted.Add(1)
			delete(store.objects, key)
			w.WriteHeader(204)
		default:
			w.WriteHeader(400)
		}
	}))
	t.Cleanup(server.Close)
	client, err := r2.New(context.Background(), r2.Config{Bucket: "stoarama", Endpoint: server.URL, AccessKey: "test", SecretKey: "test", Region: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	return client, deleted.Load
}
