package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

type inverseFakeStore struct {
	mu         sync.Mutex
	objects    map[string]r2.ObjectInfo
	calls      int
	lost       bool
	delay      time.Duration
	beforeHead func(string) error
}

func (s *inverseFakeStore) Bucket() string         { return "stoarama" }
func (s *inverseFakeStore) EndpointSHA256() string { return strings.Repeat("a", 64) }
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
func (s *inverseFakeStore) DeleteNonKeep28Once(ctx context.Context, key string, g r2.NonKeep28DeleteGuard) error {
	if err := g.CheckNonKeep28Delete(ctx, s.Bucket(), s.EndpointSHA256(), key); err != nil {
		return err
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
	const count = 80
	store := &inverseFakeStore{objects: map[string]r2.ObjectInfo{}}
	for n := range count {
		key := fmt.Sprintf("managed/acct-47/ordinary/%04d.mp4", n)
		etag := fmt.Sprintf("etag-%04d", n)
		store.objects[key] = r2.ObjectInfo{Key: key, ETag: etag, SizeBytes: 100, LastModified: time.Now().UTC().Add(-8 * 24 * time.Hour).Truncate(time.Microsecond)}
		exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path) VALUES($1,30,1,'https://test.invalid','stoarama',$2,100,$3,repeat('b',64),'2026-06-01','2026-06-01','2026-06-01 00:01:00','ordinary/'||$4)`, 9000000+n, key, etag, fmt.Sprintf("%04d.mp4", n))
	}
	exec(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',repeat('a',64))`)
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
	drain := inverseDrainEvidence{Bucket: "stoarama", EndpointSHA256: store.EndpointSHA256(), FenceActivatedAt: activated, DrainNotBefore: deadline, ObservedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour), DeployedWriterCoverage: "test fixture all admissions checked", AcceptedPUTCompletion: "test fixture no provider PUTs accepted"}
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
		sum := inverseSummary{Version: 1, Bucket: "stoarama", CreatedAt: time.Now().UTC(), PolicySHA256: policy, PicksSHA256: picks.SHA256, ReadOnly: true}
		summary := filepath.Join(dir, "summary.json")
		data, _ := json.Marshal(sum)
		os.WriteFile(summary, data, 0600)
		summarySHA, _ := inverseFileSHA(summary)
		o := inverseApplyOptions{Manifest: manifest, ManifestSHA: sha, Summary: summary, SummarySHA: summarySHA, Picks: picksPath, Ledger: filepath.Join(dir, "ledger.jsonl"), DrainEvidence: drainPath, DrainSHA: drainSHA, MaxItems: n, MaxBytes: int64(n) * 100, Budget24h: 100000, Workers: 8}
		p, err := loadInverseProtection(ctx, pool, "stoarama", picks, windows)
		if err != nil {
			t.Fatal(err)
		}
		ledger, err := openInverseLedger(o.Ledger)
		if err != nil {
			t.Fatal(err)
		}
		return &inverseApplier{pool, store, o, sum, p, ledger}, entries
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
	t.Run("crash_after_intent_and_restart", func(t *testing.T) {
		a, entries := build(1)
		if _, err := a.reserve(ctx, entries[0]); err != nil {
			t.Fatal(err)
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
					exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',100,'metadata')`, key)
				})
			}
			return nil
		}
		before := store.calls
		_, err := applyInverse(ctx, pool, store, a.opts)
		store.beforeHead = nil
		if err == nil || store.calls != before {
			t.Fatal("changed hold reached DELETE", err)
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
	t.Run("worker_pool_rate_with_durable_DB_and_ledger", func(t *testing.T) {
		a, _ := build(32)
		a.ledger.close()
		store.delay = 10 * time.Millisecond
		defer func() { store.delay = 0 }()
		s, err := applyInverse(ctx, pool, store, a.opts)
		if err != nil || s.Deleted != 32 {
			t.Fatalf("%+v %v", s, err)
		}
		t.Logf("objects/s %.2f (full schema, real locks/intent commits/fsync,10ms each HEAD/DELETE)", s.ObjectsPerSecond)
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
	r := inverseLedgerRecord{Event: inverseIntent, Bucket: "stoarama", Key: "managed/x", ManifestSHA256: strings.Repeat("a", 64), At: time.Now().UTC(), SizeBytes: 1}
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
