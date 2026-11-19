package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type inverseFakeStore struct {
	endpointSHA string
	mu          sync.Mutex
	objects     map[string]r2.ObjectInfo
	calls       int
	lost        bool
	delay       time.Duration
	// wire sets how the loopback SDK server answers a DELETE of a key: "eof"
	// closes the connection after reading it, "403"/"503" answer completely,
	// "503-truncated" cuts the error body, "slow" never answers in time.
	wire map[string]string
	// onDelete runs on the server for every DELETE before it answers, and
	// onAnswered after the answer (or the dropped connection) is sent.
	onDelete, onAnswered func(string)
	beforeHead           func(context.Context, string) error
	beforeDelete         func(string) error
	// headWithoutSHA omits the optional SHA-256 metadata from loopback HEADs.
	headWithoutSHA bool
}

func (s *inverseFakeStore) setLost(lost bool)      { s.mu.Lock(); defer s.mu.Unlock(); s.lost = lost }
func (s *inverseFakeStore) Bucket() string         { return "stoarama" }
func (s *inverseFakeStore) EndpointSHA256() string { return s.endpointSHA }
func (s *inverseFakeStore) ListPrefix(context.Context, string, int, func(r2.ObjectInfo) error) error {
	return errors.New("apply must not relist")
}
func (s *inverseFakeStore) RequireNoMultipartOutsideStitchStaging(
	context.Context, func(context.Context, string, string) (bool, error),
) error {
	return nil
}
func (s *inverseFakeStore) Head(ctx context.Context, key string) (r2.ObjectHead, error) {
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return r2.ObjectHead{}, ctx.Err()
		}
	}
	if s.beforeHead != nil {
		if err := s.beforeHead(ctx, key); err != nil {
			return r2.ObjectHead{}, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[key]
	if !ok {
		return r2.ObjectHead{}, &smithy.GenericAPIError{Code: "NotFound"}
	}
	// Like R2, HEAD reports Last-Modified as an HTTP date in whole seconds.
	return r2.ObjectHead{ETag: o.ETag, SizeBytes: o.SizeBytes, LastModified: o.LastModified.Truncate(time.Second)}, nil
}
func (s *inverseFakeStore) deleteOnce(ctx context.Context, authority *inverseDeleteAuthority) error {
	key := authority.binding.Key
	if err := authority.requireWire(ctx, s.Bucket(), s.EndpointSHA256()); err != nil {
		return err
	}
	if authority.gate.isClosed() {
		return errInverseAdmissionClosed
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
		return fmt.Errorf("%w: lost response", errInverseWireAmbiguous)
	}
	return nil
}

// waitFor bounds a test barrier: a schedule that never reaches it fails the
// test instead of hanging it.
func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Errorf("timed out waiting for %s", what)
	}
}

func TestR2InverseExecutorLifecycle(t *testing.T) {
	ctx := context.Background()
	schemaPool := joinedPurgeTestPool(t, "inverse_apply")
	workerConfig := schemaPool.Config()
	workerConfig.MaxConns = 20
	workerConfig.MinConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, workerConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
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
 INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(1,47,'test-inverse','not-a-real-credential');
 INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',1);
 INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps) VALUES(3,'test','3','inverse','inverse','https://test.invalid','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
 INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc) VALUES(1,47,'test','https://test.invalid','auto','stoarama','fixture',decode('00','hex'));
 INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at) VALUES(30,47,1,'inverse','https://test.invalid','* * * * *','Asia/Tokyo','paused',3,now());
 INSERT INTO recording_qualification_runs(id,account_id,definition_version,definition_jsonb,target_recording_count,window_sequence_start_at) VALUES(1,47,'inverse-fixture','{}',50,'2026-06-01');`)
	// The last two sources carry fixed delivery holds; build() never reaches them.
	const count = 1400
	store := &inverseFakeStore{objects: map[string]r2.ObjectInfo{}}
	sdk, providerDeletes := inverseSDKTestStore(t, store)
	store.endpointSHA = sdk.EndpointSHA256()
	for n := range count {
		key := fmt.Sprintf("managed/acct-47/ordinary/%04d.mp4", n)
		etag := fmt.Sprintf("etag-%04d", n)
		store.objects[key] = r2.ObjectInfo{Key: key, ETag: etag, SizeBytes: 100, LastModified: inverseRetirementCutoff().Add(-1500 * time.Millisecond)}
		exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path) VALUES($1,30,1,'https://test.invalid','stoarama',$2,100,$3,repeat('b',64),'2026-06-01','2026-06-01','2026-06-01 00:01:00','ordinary/'||$4)`, 9000000+n, key, etag, fmt.Sprintf("%04d.mp4", n))
	}
	exec(`INSERT INTO recording_collation_hours(id,hour_id,batch_id,generation,recording_id,local_date,delivery_hour,status,manifest_object_key,manifest_sha256,manifest,source_clip_count,included_clip_count,excluded_clip_count,join_count,split_count)
 VALUES(1,'inverse__recording-30__date-2026-06-01__hour-01__generation-2','inverse',2,30,'2026-06-01',1,'collated','joined/inverse/manifest.json',repeat('a',64),'{}',1,1,0,0,0);
 INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 VALUES(10000001,1,1,1,'joined/inverse/objects/'||repeat('a',64)||'.mp4','joined/ordinary/test-output.mp4',100,repeat('a',64),'2026-06-01','2026-06-01 00:01',ARRAY[9000000]::bigint[],now());
 INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 SELECT 10000000+n,1,n,10010,'joined/inverse/objects/'||lpad(to_hex(n),64,'0')||'.mp4','joined/ordinary/output-'||n||'.mp4',100,lpad(to_hex(n),64,'0'),'2026-06-01','2026-06-01 00:01',ARRAY[10000000+n]::bigint[],now() FROM generate_series(2,10010) n;
 ANALYZE recording_collation_outputs;`)
	exec(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, store.EndpointSHA256())
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `SET LOCAL session_replication_role=replica; UPDATE r2_retirement_writer_fences SET activated_at='2026-11-12T14:28:47.037031Z'; UPDATE r2_retirement_writer_fences SET drain_not_before=clock_timestamp()-interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var activated, deadline time.Time
	if err = pool.QueryRow(ctx, `SELECT activated_at,drain_not_before FROM r2_retirement_writer_fences`).Scan(&activated, &deadline); err != nil {
		t.Fatal(err)
	}
	drain := inverseDrainEvidence{Proof: inversePUTCompletionProof, Bucket: "stoarama", EndpointSHA256: store.EndpointSHA256(), FenceActivatedAt: activated, DrainNotBefore: deadline, ObservedAt: time.Now().UTC(), DeployedWriterCoverage: "test fixture all admissions checked", AcceptedPUTCompletion: "test fixture no provider PUTs accepted"}
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
			store.mu.Lock()
			object := store.objects[key]
			store.mu.Unlock()
			entries = append(entries, inverseEntry{ObjectInfo: object, Class: inverseDeletable})
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
	cleanupPlan := func(sourceID int64, mode string, withItem, withHold bool) int64 {
		t.Helper()
		var planID int64
		err := pool.QueryRow(ctx, `INSERT INTO nas_cleanup_plans(account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode) VALUES(47,13,$1,repeat('d',64),'[]',repeat('e',64),$2) RETURNING id`, fmt.Sprintf("inverse-%s-%d", mode, sourceID), mode).Scan(&planID)
		if err != nil {
			t.Fatal(err)
		}
		if withItem {
			exec(`INSERT INTO nas_cleanup_items(plan_id,ordinal,state) VALUES($1,1,'keep')`, planID)
		}
		if withHold {
			exec(`INSERT INTO nas_cleanup_source_holds(plan_id,clip_id) VALUES($1,$2)`, planID, sourceID)
		}
		return planID
	}
	latentCleanup := func(sourceID int64, withItem bool) int64 { return cleanupPlan(sourceID, "retired", withItem, true) }

	const insertDryScope = `INSERT INTO recording_joined_dry_run_scopes(dry_run_id,recording_id,priority_ordinal,local_date,date_ordinal,recording_job_id,high_water_clip_id)
 SELECT id,30,1,'2026-06-01',1,(convert_from(skeleton_bytes,'UTF8')::jsonb#>>'{recordings,0,snapshot_days,0,recording_job_id}')::bigint,(convert_from(skeleton_bytes,'UTF8')::jsonb#>>'{recordings,0,snapshot_days,0,high_water_clip_id}')::bigint FROM recording_joined_dry_runs WHERE id=$1::uuid`
	dryScope := func(sourceID int64) string {
		t.Helper()
		var jobID int64
		err := pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,completed_at) VALUES(30,'2026-06-01'::timestamptz+($1::bigint-9000000)*interval '1 minute','2026-06-01'::timestamptz+($1::bigint-9000000)*interval '1 minute',60,'done','inverse-scope-'||$1::bigint,'2026-06-02') RETURNING id`, sourceID).Scan(&jobID)
		if err != nil {
			t.Fatal(err)
		}
		exec(`UPDATE recording_clips SET recording_job_id=$2 WHERE id=$1`, sourceID, jobID)
		batchID := fmt.Sprintf("inverse-scope-%d", sourceID)
		input, _ := json.Marshal(map[string]any{"batch_id": batchID, "generation": 1, "connection_id": 13, "qualification_run_id": 1, "apply": false})
		skeleton, _ := json.Marshal(map[string]any{"batch_id": batchID, "generation": 1, "connection_id": 13, "selection_authority": map[string]any{"qualification_run_id": 1}, "recordings": []any{map[string]any{"frozen_recording": map[string]any{"recording_id": 30, "priority_ordinal": 1}, "snapshot_days": []any{map[string]any{"local_date": "2026-06-01", "date_ordinal": 1, "recording_job_id": jobID, "high_water_clip_id": sourceID}}}}})
		var runID string
		err = pool.QueryRow(ctx, `INSERT INTO recording_joined_dry_runs(account_id,connection_id,batch_id,generation,qualification_run_id,input_bytes,input_sha256,skeleton_bytes,skeleton_sha256) VALUES(47,13,'inverse-scope-'||$1::bigint,1,1,$3,encode(sha256($3::bytea),'hex'),$2,encode(sha256($2::bytea),'hex')) RETURNING id::text`, sourceID, skeleton, input).Scan(&runID)
		if err != nil {
			t.Fatal(err)
		}
		return runID
	}
	t.Run("owner_policy_without_completion_claim", func(t *testing.T) {
		a, _ := build(1)
		a.ledger.close()
		inverseUseOwnerFenceEvidence(t, &a.opts)
		before := providerDeletes()
		result, err := applyInverse(ctx, pool, sdk, a.opts)
		if err != nil || result.Deleted != 1 || providerDeletes()-before != 1 {
			t.Fatalf("owner-policy batch %+v err %v", result, err)
		}
	})
	t.Run("changed_provider_identity_never_reaches_DELETE", func(t *testing.T) {
		for _, field := range []string{"etag", "size", "last_modified", "after_partition"} {
			t.Run(field, func(t *testing.T) {
				a, entries := build(1)
				defer a.ledger.close()
				e := entries[0]
				store.mu.Lock()
				o := store.objects[e.Key]
				switch field {
				case "etag":
					o.ETag = "changed"
				case "size":
					o.SizeBytes++
				case "last_modified":
					o.LastModified = o.LastModified.Add(-time.Hour)
				case "after_partition":
					a.summary.CreatedAt = o.LastModified.Add(-time.Second)
				}
				store.objects[e.Key] = o
				store.mu.Unlock()
				before := providerDeletes()
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)
				if err = a.checkWireHead(ctx, tx, e, inverseRawTarget{SHA: strings.Repeat("b", 64), ETag: e.ETag}); err == nil || providerDeletes() != before {
					t.Fatal("changed identity reached wire", err)
				}
			})
		}
	})
	t.Run("success", func(t *testing.T) {
		a, _ := build(2)
		a.ledger.close()
		s, err := applyInverse(ctx, pool, store, a.opts)
		if err != nil || s.Deleted != 2 {
			t.Fatalf("%+v %v", s, err)
		}
		var receipts int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM r2_inverse_events`).Scan(&receipts); err != nil || receipts != 3 {
			t.Fatal(receipts, err)
		}
	})
	t.Run("durable_history_cannot_be_erased_or_rolled_back", func(t *testing.T) {
		for _, query := range []string{`DELETE FROM r2_inverse_attempts`, `TRUNCATE r2_inverse_attempts`, `UPDATE r2_inverse_attempts SET issued_at=now()`, `DELETE FROM r2_inverse_intents`, `UPDATE r2_inverse_intents SET size_bytes=1`, `TRUNCATE r2_inverse_intents CASCADE`, `DELETE FROM r2_inverse_events`, `TRUNCATE r2_inverse_events`, `UPDATE r2_inverse_budget_minutes SET charged_bytes=1`, `DELETE FROM r2_inverse_budget_minutes`, `TRUNCATE r2_inverse_budget_minutes`, `UPDATE r2_inverse_ke_epoch SET epoch=0`, `DELETE FROM r2_inverse_ke_epoch`, `TRUNCATE r2_inverse_ke_epoch`} {
			if _, err := pool.Exec(ctx, query); err == nil {
				t.Fatal("durable authority could be erased", query)
			}
		}
		var events int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM r2_inverse_events`).Scan(&events); err != nil || events != 3 {
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
	t.Run("unissued_intent_stays_held_when_object_vanishes", func(t *testing.T) {
		a, entries := build(1)
		if _, err := a.reserve(ctx, entries[0]); err != nil {
			t.Fatal(err)
		}
		a.ledger.close()
		store.mu.Lock()
		object := store.objects[entries[0].Key]
		delete(store.objects, entries[0].Key)
		store.mu.Unlock()
		before := store.calls
		s, err := applyInverse(ctx, pool, store, a.opts)
		store.mu.Lock()
		store.objects[entries[0].Key] = object
		store.mu.Unlock()
		var absent int
		var purged bool
		if qerr := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM r2_inverse_events WHERE object_key=$1),(SELECT purged_at IS NOT NULL FROM recording_clips WHERE object_key=$1)`, entries[0].Key).Scan(&absent, &purged); qerr != nil {
			t.Fatal(qerr)
		}
		if err == nil || !strings.Contains(err.Error(), "never issued a DELETE") || s.Deleted != 0 || absent != 0 || purged || store.calls != before {
			t.Fatalf("%+v %v absent%d purged%t calls%d", s, err, absent, purged, store.calls)
		}
	})
	t.Run("held_key_replaced_at_the_provider_stops_startup", func(t *testing.T) {
		a, entries := build(1)
		if _, err := a.reserve(ctx, entries[0]); err != nil {
			t.Fatal(err)
		}
		a.ledger.close()
		store.mu.Lock()
		original := store.objects[entries[0].Key]
		replaced := original
		replaced.ETag = "replaced-etag"
		store.objects[entries[0].Key] = replaced
		store.mu.Unlock()
		before := store.calls
		_, err := applyInverse(ctx, pool, store, a.opts)
		store.mu.Lock()
		store.objects[entries[0].Key] = original
		store.mu.Unlock()
		if err == nil || !strings.Contains(err.Error(), "names a different object") || store.calls != before {
			t.Fatalf("replaced held object: %v calls %d", err, store.calls-before)
		}
	})
	t.Run("attempt_without_a_sent_DELETE_stays_held_when_removed_outside", func(t *testing.T) {
		// An attempt row commits before the wire. If the process stops there and
		// the object later disappears for any other reason, nothing may retire it.
		a, entries := build(1)
		if _, err := a.reserve(ctx, entries[0]); err != nil {
			t.Fatal(err)
		}
		a.ledger.close()
		if _, err := pool.Exec(ctx, `INSERT INTO r2_inverse_attempts(bucket,object_key) VALUES('stoarama',$1)`, entries[0].Key); err != nil {
			t.Fatal(err)
		}
		store.mu.Lock()
		delete(store.objects, entries[0].Key)
		store.mu.Unlock()
		before := store.calls
		s, err := applyInverse(ctx, pool, store, a.opts)
		var events, receipts int
		var purged bool
		if qerr := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM r2_inverse_events WHERE object_key=$1),(SELECT count(*) FROM r2_object_retirements WHERE object_key=$1),(SELECT purged_at IS NOT NULL FROM recording_clips WHERE object_key=$1)`, entries[0].Key).Scan(&events, &receipts, &purged); qerr != nil {
			t.Fatal(qerr)
		}
		if err != nil || s.AmbiguousHeld < 1 || store.calls != before || events != 0 || receipts != 0 || purged {
			t.Fatalf("%+v %v calls%d events%d receipts%d purged%t", s, err, store.calls, events, receipts, purged)
		}
	})
	t.Run("lost_response_stays_held_without_retransmit", func(t *testing.T) {
		// The object is gone, but an attempt row cannot prove this executor
		// removed it: the key stays held, unretired and never resent.
		a, entries := build(1)
		a.ledger.close()
		store.setLost(true)
		_, err := applyInverse(ctx, pool, store, a.opts)
		store.setLost(false)
		if !errors.Is(err, errInverseWireAmbiguous) {
			t.Fatalf("lost response: %v", err)
		}
		before := store.calls
		s, err := applyInverse(ctx, pool, store, a.opts)
		var events, receipts int
		if qerr := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM r2_inverse_events WHERE object_key=$1),(SELECT count(*) FROM r2_object_retirements WHERE object_key=$1)`, entries[0].Key).Scan(&events, &receipts); qerr != nil {
			t.Fatal(qerr)
		}
		if err != nil || s.AmbiguousHeld < 1 || store.calls != before || events != 0 || receipts != 0 {
			t.Fatalf("%+v %v calls%d events%d receipts%d", s, err, store.calls, events, receipts)
		}
	})
	owner := filepath.Join("..", "..", "..", "docs", "records", "r2-owner-fence-policy-20261118.md")
	unresolved := func(t *testing.T, manifest string) []string {
		t.Helper()
		keys := []string{}
		if err := pool.QueryRow(ctx, `SELECT coalesce(array_agg(i.object_key ORDER BY i.object_key),'{}') FROM r2_inverse_intents i
 WHERE i.manifest_sha256=$1 AND NOT EXISTS(SELECT 1 FROM r2_inverse_events v WHERE v.bucket=i.bucket AND v.object_key=i.object_key AND v.event='absent')`, manifest).Scan(&keys); err != nil {
			t.Fatal(err)
		}
		return keys
	}
	// setWire installs server answers; the barrier hook (if any) runs on the
	// server before it answers each DELETE.
	setWire := func(modes map[string]string, hook func(string)) {
		store.mu.Lock()
		store.wire, store.onDelete, store.onAnswered = modes, hook, nil
		store.mu.Unlock()
	}
	t.Run("lost_response_holds_only_its_key", func(t *testing.T) {
		// Production shape: R2 drops one DELETE connection (EOF) while a sibling
		// DELETE is provably on the wire. The sibling must finish and publish;
		// only the lost key stays unresolved, held for good and never resent.
		a, entries := build(40)
		a.ledger.close()
		dir := t.TempDir()
		lost := entries[2].Key
		siblingOnWire, lostAnswered := make(chan struct{}), make(chan struct{})
		var sibling sync.Once
		setWire(map[string]string{lost: "eof"}, func(key string) {
			if key == lost {
				waitFor(t, siblingOnWire, "a sibling DELETE on the wire")
				return
			}
			sibling.Do(func() {
				close(siblingOnWire)
				waitFor(t, lostAnswered, "the lost DELETE's dropped connection")
				// Give the lost DELETE's worker time to see its error.
				time.Sleep(300 * time.Millisecond)
			})
		})
		store.mu.Lock()
		store.onAnswered = func(key string) {
			if key == lost {
				close(lostAnswered)
			}
		}
		store.mu.Unlock()
		done, first, err := runInverseJobBatch(ctx, pool, sdk, a.opts, owner, filepath.Join(dir, "evidence-1.json"))
		setWire(nil, nil)
		if err != nil || done || !slices.Equal(first.WireAmbiguous, []string{lost}) || first.Deleted < 1 || first.Deleted >= 39 {
			t.Fatalf("lost-response batch: done %v %+v %v", done, first, err)
		}
		if got := unresolved(t, a.opts.ManifestSHA); !slices.Equal(got, []string{lost}) {
			t.Fatalf("in-flight deletes were stranded: %v", got)
		}
		before := providerDeletes()
		done, second, err := runInverseJobBatch(ctx, pool, sdk, a.opts, owner, filepath.Join(dir, "evidence-2.json"))
		if err != nil || done || second.Deleted != 39-first.Deleted || providerDeletes()-before != second.Deleted {
			t.Fatalf("next batch: done %v %+v %v deletes %d", done, second, err, providerDeletes()-before)
		}
		if done, last, err := runInverseJobBatch(ctx, pool, sdk, a.opts, owner, filepath.Join(dir, "evidence-3.json")); err != nil || !done || last.Deleted != 0 {
			t.Fatalf("final batch: done %v %+v %v", done, last, err)
		}
		if got := unresolved(t, a.opts.ManifestSHA); !slices.Equal(got, []string{lost}) {
			t.Fatalf("lost key was settled or resent: %v", got)
		}
	})
	t.Run("received_status_or_veto_stops_the_job", func(t *testing.T) {
		dir := t.TempDir()
		for _, mode := range []string{"403", "503", "503-truncated", "503-headers-cut"} {
			a, entries := build(2)
			a.ledger.close()
			setWire(map[string]string{entries[0].Key: mode}, nil)
			_, _, err := runInverseJobBatch(ctx, pool, sdk, a.opts, owner, filepath.Join(dir, "evidence-"+mode+".json"))
			setWire(nil, nil)
			if err == nil || errors.Is(err, errInverseWireAmbiguous) {
				t.Fatalf("%s answer continued the job: %v", mode, err)
			}
		}
		// A veto before the wire.
		a, _ := build(1)
		a.ledger.close()
		store.beforeDelete = func(string) error { return errors.New("vetoed before the wire") }
		_, _, err := runInverseJobBatch(ctx, pool, store, a.opts, owner, filepath.Join(dir, "evidence-veto.json"))
		store.beforeDelete = nil
		if err == nil || errors.Is(err, errInverseWireAmbiguous) {
			t.Fatalf("pre-wire veto: %v", err)
		}
		// A lost response in a batch that deleted nothing.
		b, entries := build(1)
		b.ledger.close()
		setWire(map[string]string{entries[0].Key: "eof"}, nil)
		_, _, err = runInverseJobBatch(ctx, pool, sdk, b.opts, owner, filepath.Join(dir, "evidence-empty.json"))
		setWire(nil, nil)
		if !errors.Is(err, errInverseWireAmbiguous) {
			t.Fatalf("lost response with no deletes continued: %v", err)
		}
	})
	t.Run("lost_response_with_a_sibling_403_stops_the_job", func(t *testing.T) {
		a, entries := build(2)
		a.ledger.close()
		lost, refused := entries[0].Key, entries[1].Key
		arrived := map[string]chan struct{}{lost: make(chan struct{}), refused: make(chan struct{})}
		setWire(map[string]string{lost: "eof", refused: "403"}, func(key string) {
			close(arrived[key])
			for other, ch := range arrived {
				if other != key {
					waitFor(t, ch, "both DELETEs on the wire")
				}
			}
		})
		_, result, err := runInverseJobBatch(ctx, pool, sdk, a.opts, owner, filepath.Join(t.TempDir(), "evidence.json"))
		setWire(nil, nil)
		if err == nil || errors.Is(err, errInverseWireAmbiguous) || !slices.Equal(result.WireAmbiguous, []string{lost}) {
			t.Fatalf("mixed failure: %+v %v", result, err)
		}
	})
	t.Run("response_timeout_is_a_lost_response", func(t *testing.T) {
		a, entries := build(1)
		a.ledger.close()
		setWire(map[string]string{entries[0].Key: "slow"}, nil)
		timed, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := applyInverse(timed, pool, sdk, a.opts)
		cancel()
		setWire(nil, nil)
		if !errors.Is(err, errInverseWireAmbiguous) {
			t.Fatalf("response timeout: %v", err)
		}
	})
	t.Run("closed_admission_aborts_a_key_waiting_on_its_reservation", func(t *testing.T) {
		// Reservations serialize on one protection lock, so a sibling's hard
		// failure reaches a waiting key through admission: the wait aborts and
		// nothing durable is written for that key.
		a, entries := build(1)
		defer a.ledger.close()
		e := entries[0]
		blocker, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback(ctx)
		var pid int
		if err = blocker.QueryRow(ctx, `SELECT pg_backend_pid() FROM (SELECT lock_nas_r2_copy_key('stoarama',$1)) l`, e.Key).Scan(&pid); err != nil {
			t.Fatal(err)
		}
		admit, closeAdmission := context.WithCancel(ctx)
		done := make(chan error, 1)
		before := providerDeletes()
		go func() { _, err := a.delete(ctx, admit, &inverseSendGate{}, e); done <- err }()
		deadline := time.Now().Add(10 * time.Second)
		for {
			var waiting bool
			if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("key never waited on its reservation lock")
			}
			time.Sleep(10 * time.Millisecond)
		}
		closeAdmission()
		select {
		case err = <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("closed admission did not abort the reservation wait")
		}
		var intents int
		if qerr := pool.QueryRow(ctx, `SELECT count(*) FROM r2_inverse_intents WHERE object_key=$1`, e.Key).Scan(&intents); qerr != nil {
			t.Fatal(qerr)
		}
		if !errors.Is(err, errInverseAdmissionClosed) || intents != 0 || providerDeletes() != before {
			t.Fatalf("admission: err %v intents %d deletes %d", err, intents, providerDeletes()-before)
		}
	})
	t.Run("closed_send_gate_stops_a_reserved_key_before_its_first_DELETE", func(t *testing.T) {
		a, entries := build(1)
		defer a.ledger.close()
		closed := &inverseSendGate{}
		closed.close()
		before := store.calls
		_, err := a.delete(ctx, ctx, closed, entries[0])
		var intents, attempts int
		if qerr := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM r2_inverse_intents WHERE object_key=$1),(SELECT count(*) FROM r2_inverse_attempts WHERE object_key=$1)`, entries[0].Key).Scan(&intents, &attempts); qerr != nil {
			t.Fatal(qerr)
		}
		// Reserved and authorized, never sent: the key stays held for good.
		if !errors.Is(err, errInverseAdmissionClosed) || store.calls != before || intents != 1 || attempts != 1 {
			t.Fatalf("send gate: err %v deletes %d intents %d attempts %d", err, store.calls-before, intents, attempts)
		}
	})
	t.Run("stitch_hold_admission_conflicts_only_on_its_key", func(t *testing.T) {
		// Another executor's outer lock never fences stitch hold admission.
		batch, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer batch.Release()
		if _, err = batch.Exec(ctx, `SELECT pg_advisory_lock($1)`, r2RetireAdvisoryLockKey); err != nil {
			t.Fatal(err)
		}
		defer batch.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, r2RetireAdvisoryLockKey)
		a, entries := build(7)
		defer a.ledger.close()
		held, free, attempted, wired, other, unrelated, behind := entries[0], entries[1], entries[2], entries[3], entries[4], entries[5], entries[6]
		var hourID int64
		exec(`INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until) VALUES(70,3,'2026-06-01','2026-06-02','key locks',now()+interval '1 day')`)
		if err = pool.QueryRow(ctx, `INSERT INTO nas_stitch_r2_hours(scope_id,recording_id,hour_at,plan,plan_sha256,input_bytes) VALUES(70,30,'2026-06-01','{}',repeat('f',64),100) RETURNING id`).Scan(&hourID); err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at)
 SELECT 13,id,30,display_path,100,repeat('b',64),'present',now(),now() FROM recording_clips WHERE object_key=$1`, e.Key)
		}
		admit := func(ctx context.Context, tx pgx.Tx, e inverseEntry) error {
			_, err := tx.Exec(ctx, `INSERT INTO nas_stitch_r2_input_holds(hour_id,bucket,object_key,catalog_clip_id,sha256,size_bytes,etag)
 SELECT $1,bucket,object_key,id,repeat('b',64),100,etag FROM recording_clips WHERE object_key=$2`, hourID, e.Key)
			return err
		}
		admitAlone := func(e inverseEntry) error {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if err = admit(ctx, tx, e); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}
		// Executors older than any Go-side key lock only write the ledgers.
		stale := []string{
			`INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
 SELECT 'raw',id,bucket,object_key,100,repeat('b',64),repeat('a',64) FROM recording_clips WHERE object_key=$1`,
			`INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256)
 SELECT 'raw',id,3,bucket,object_key,100,repeat('b',64),'outside_keep28_no_nas_copy','none',repeat('a',64),repeat('a',64) FROM recording_clips WHERE object_key=$1`,
		}
		prompt := func() (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, 5*time.Second) }

		// A committed reservation and pre-wire attempt veto later admission.
		if _, err = a.reserve(ctx, attempted); err != nil {
			t.Fatal(err)
		}
		wire, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		// The attempt commits on a second connection while this wire owns the
		// copy key; a ledger trigger that waited for that key would hang here.
		waitless, cancel := prompt()
		if _, err = a.authorize(waitless, wire, attempted, &inverseSendGate{}); err != nil {
			t.Fatal(err)
		}
		cancel()
		wire.Rollback(ctx)
		if err = admitAlone(attempted); err == nil || !strings.Contains(err.Error(), "already has R2 retirement history") {
			t.Fatalf("committed pre-wire attempt did not veto hold admission: %v", err)
		}

		// An open RAW wire owns its key and row; admission elsewhere never waits.
		if _, err = a.reserve(ctx, wired); err != nil {
			t.Fatal(err)
		}
		wire, err = pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = a.prepareWireTarget(ctx, wire, wired); err != nil {
			t.Fatal(err)
		}
		importing, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		defer importing.Rollback(ctx)
		waitless, cancel = prompt()
		if err = admit(waitless, importing, other); err != nil {
			t.Fatalf("hold admission waited on an unrelated open wire: %v", err)
		}
		cancel()
		if err = importing.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if err = admitAlone(wired); err == nil || !strings.Contains(err.Error(), "already has R2 retirement history") {
			t.Fatalf("hold admitted a key with an open wire: %v", err)
		}
		wire.Rollback(ctx)

		// An open import fences only its own key. Same-key reserve never waits
		// on it while owning the wide protection locks: it rolls back and skips.
		durable := func(e inverseEntry) int {
			var n int
			if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM r2_retirement_reservations WHERE object_key=$1)+(SELECT count(*) FROM r2_inverse_intents WHERE object_key=$1)`, e.Key).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n
		}
		importing, err = pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		defer importing.Rollback(ctx)
		if err = admit(ctx, importing, held); err != nil {
			t.Fatal(err)
		}
		waitless, cancel = prompt()
		if _, err = a.reserve(waitless, held); !errors.Is(err, errInverseStitchConflict) || !strings.Contains(err.Error(), "hold admission in progress") || durable(held) != 0 {
			t.Fatalf("same-key RAW reserve did not skip an open import: %v", err)
		}
		for _, query := range stale {
			if _, err = pool.Exec(waitless, query, held.Key); err == nil || !strings.Contains(err.Error(), "hold admission is busy") {
				t.Fatalf("stale ledger INSERT crossed an open import: %v", err)
			}
		}
		cancel()
		before := store.calls
		waitless, cancel = prompt()
		if ok, err := a.delete(waitless, waitless, &inverseSendGate{}, free); !ok || err != nil || store.calls-before != 1 {
			t.Fatalf("unrelated RAW reserve and wire waited on an open import: %v %v", ok, err)
		}
		cancel()
		if err = importing.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err = a.reserve(ctx, held); !errors.Is(err, errInverseStitchConflict) || !strings.Contains(err.Error(), "committed hold") || durable(held) != 0 {
			t.Fatalf("RAW did not skip a committed hold: %v", err)
		}
		for _, query := range stale {
			if _, err = pool.Exec(ctx, query, held.Key); err == nil || !strings.Contains(err.Error(), "active R2 stitch input hold") {
				t.Fatalf("stale ledger INSERT crossed a committed hold: %v", err)
			}
		}

		// A certificate owns its held inputs' copy keys and rows. Same-key
		// reserve skips at once; an unrelated key proceeds meanwhile.
		certifying, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer certifying.Rollback(ctx)
		if _, err = certifying.Exec(ctx, `SELECT lock_nas_r2_copy_key('stoarama',$1),(SELECT count(*) FROM (SELECT 1 FROM recording_clips WHERE object_key=$1 FOR SHARE) c)`, held.Key); err != nil {
			t.Fatal(err)
		}
		waitless, cancel = prompt()
		if _, err = a.reserve(waitless, held); !errors.Is(err, errInverseStitchConflict) || !strings.Contains(err.Error(), "committed hold") {
			t.Fatalf("same-key RAW reserve did not skip an open certificate: %v", err)
		}
		before = store.calls
		if ok, err := a.delete(waitless, waitless, &inverseSendGate{}, unrelated); !ok || err != nil || store.calls-before != 1 {
			t.Fatalf("unrelated RAW key waited on an open certificate: %v %v", ok, err)
		}
		cancel()
		certifying.Rollback(ctx)

		// Any other owner of an unheld key is awaited with no protection lock
		// held: an unrelated reserve started behind that wait proceeds.
		busy, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer busy.Rollback(ctx)
		if _, err = busy.Exec(ctx, `SELECT 1 FROM recording_clips WHERE object_key=$1 FOR UPDATE`, other.Key); err != nil {
			t.Fatal(err)
		}
		awaited := make(chan error, 1)
		go func() { _, err := a.reserve(ctx, other); awaited <- err }()
		waitBlockedBy(t, pool, stitchBackendPID(t, busy), "same-key reserve behind another row owner")
		var wide bool
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND (classid::bigint*4294967296+objid::bigint)=ANY(ARRAY[7343226011103,7343226011101]::bigint[]))`).Scan(&wide); err != nil || wide {
			t.Fatalf("waiting reserve kept its protection locks: %v %v", wide, err)
		}
		waitless, cancel = prompt()
		if _, err = a.reserve(waitless, behind); err != nil || durable(behind) != 2 {
			t.Fatalf("unrelated reserve queued behind a same-key wait: %v", err)
		}
		cancel()
		busy.Rollback(ctx)
		if err = within(t, awaited, "awaited reserve"); err != nil || durable(other) != 2 {
			t.Fatalf("awaited reserve did not complete: %v", err)
		}
	})
	t.Run("send_gate_closed_after_authorize_stops_the_first_write", func(t *testing.T) {
		// The gate closes while this DELETE is past every check and dialing:
		// its first request byte is refused, so nothing reaches the provider.
		a, entries := build(1)
		defer a.ledger.close()
		e := entries[0]
		a.store = sdk
		if _, err := a.reserve(ctx, e); err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		gate := &inverseSendGate{}
		authority, err := a.authorize(ctx, tx, e, gate)
		if err != nil {
			t.Fatal(err)
		}
		gate.close()
		before := providerDeletes()
		err = sdk.deleteOnce(ctx, authority)
		if !errors.Is(err, errInverseAdmissionClosed) || errors.Is(err, errInverseWireAmbiguous) || providerDeletes() != before {
			t.Fatalf("closed gate: %v deletes %d", err, providerDeletes()-before)
		}
	})
	t.Run("delete_transport_never_speaks_HTTP2", func(t *testing.T) {
		// An HTTP/2-capable TLS server must still be spoken to in HTTP/1.1, on
		// a fresh connection for every DELETE.
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
		for _, client := range []*http.Client{{Transport: tr}, {Transport: (&inverseWire{gate: &inverseSendGate{}}).transport(tr)}} {
			req, _ := http.NewRequest(http.MethodDelete, server.URL+"/stoarama/key", nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.ProtoMajor != 1 || resp.StatusCode != 204 {
				t.Fatalf("DELETE answered over %s with %d", resp.Proto, resp.StatusCode)
			}
		}
		protosMu.Lock()
		defer protosMu.Unlock()
		if !slices.Equal(protos, []string{"HTTP/1.1", "HTTP/1.1"}) || connections.Load() != 2 {
			t.Fatalf("server saw %v over %d connections", protos, connections.Load())
		}
	})
	t.Run("protection_changed_during_HEAD", func(t *testing.T) {
		a, entries := build(1)
		a.ledger.close()
		target := entries[0].Key
		var once sync.Once
		store.beforeHead = func(_ context.Context, key string) error {
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
		for n, kind := range []string{"key", "source", "content_alias", "normalized_path", "output_dependency", "output_key", "output_content", "output_path", "output_cached_source"} {
			t.Run(kind, func(t *testing.T) {
				a, entries := build(1)
				defer a.ledger.close()
				e := entries[0]
				var id int64
				if err := pool.QueryRow(ctx, `SELECT id FROM recording_clips WHERE object_key=$1`, e.Key).Scan(&id); err != nil {
					t.Fatal(err)
				}
				placeholder := "datasets/test/first-" + strconv.FormatInt(id, 10)
				switch kind {
				case "key":
					exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',100,'metadata')`, e.Key)
				case "source":
					exec(`INSERT INTO delivery_nas_source_holds(clip_id) VALUES($1)`, id)
				case "content_alias":
					sha := fmt.Sprintf("%064x", id)
					exec(`UPDATE recording_clips SET sha256=$2 WHERE id=$1`, id, sha)
					exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256) VALUES('stoarama',$1,'hold',100,'metadata',$2)`, placeholder, sha)
				case "normalized_path":
					exec(`UPDATE recording_clips SET display_path='joined/'||display_path WHERE id=$1`, id)
					exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,relative_path) SELECT 'stoarama',$1,'hold',100,'metadata',regexp_replace(display_path,'^joined/','') FROM recording_clips WHERE id=$2`, placeholder, id)
				default:
					outputID := int64(10000000 + n + 2)
					exec(`UPDATE recording_collation_outputs SET source_clip_ids=ARRAY[$1]::bigint[] WHERE id=$2`, id, outputID)
					switch kind {
					case "output_dependency":
						exec(`INSERT INTO delivery_keep_output_dependencies(output_id,recording_id) VALUES($1,30)`, outputID)
					case "output_key":
						exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) SELECT 'stoarama',object_key,'hold',100,'metadata' FROM recording_collation_outputs WHERE id=$1`, outputID)
					case "output_content":
						exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256) SELECT 'stoarama',$1,'hold',size_bytes,'metadata',sha256 FROM recording_collation_outputs WHERE id=$2`, placeholder, outputID)
					case "output_cached_source":
						exec(`UPDATE recording_collation_outputs SET source_clip_ids=array_append(source_clip_ids,$2::bigint) WHERE id=$1`, outputID, int64(9000000+count-1))
						exec(`INSERT INTO delivery_nas_source_holds(clip_id) VALUES($1)`, int64(9000000+count-1))
					case "output_path":
						exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,relative_path) SELECT 'stoarama',$1,'hold',100,'metadata',regexp_replace(nas_relative_path,'^joined/','') FROM recording_collation_outputs WHERE id=$2`, placeholder, outputID)
					}
				}
				// No refresh_delivery_nas_source_holds: protection derives live closure.
				if _, err := a.reserve(ctx, e); err == nil {
					t.Fatal("pre-existing delivery protection lost to intent", kind)
				}
			})
		}
	})
	t.Run("late_admission_blocked_through_wire_then_intent_vetoes", func(t *testing.T) {
		for _, kind := range []string{"delivery_key", "delivery_source", "delivery_output", "delivery_path_enrichment", "collated_key", "held_output_source", "delivery_output_cached_source", "output_id", "cleanup_reactivate", "cleanup_insert_reactivate", "cleanup_source_admission", "cleanup_plan_admission", "joined_scope_admission", "copy_state", "copy_reason"} {
			t.Run(kind, func(t *testing.T) {
				a, entries := build(1)
				defer a.ledger.close()
				e := entries[0]
				var id int64
				if err := pool.QueryRow(ctx, `SELECT id FROM recording_clips WHERE object_key=$1`, e.Key).Scan(&id); err != nil {
					t.Fatal(err)
				}
				exec(`UPDATE recording_collation_outputs SET source_clip_ids=ARRAY[$1]::bigint[] WHERE id=10000001`, id)
				query := `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',100,'metadata')`
				var arg any = e.Key
				switch kind {
				case "copy_state", "copy_reason":
					exec(`INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state) SELECT 13,'raw',id,repeat('a',64),$2,jsonb_build_object('bucket',bucket,'object_key',object_key),'verified' FROM recording_clips WHERE id=$1`, id, policy)
					query = `UPDATE nas_r2_copy_items SET blocked_reason='checksum_mismatch' WHERE source_id=$1`
					if kind == "copy_state" {
						query = `UPDATE nas_r2_copy_items SET state='blocked',blocked_reason='checksum_mismatch' WHERE source_id=$1`
					}
					arg = id
				case "output_id":
					unusedID := int64(980000000) + id
					exec(`INSERT INTO delivery_keep_output_dependencies(output_id,recording_id) VALUES($1,30)`, unusedID)
					query = `UPDATE recording_collation_outputs SET id=$1 WHERE id=10000001`
					arg = unusedID
				case "joined_scope_admission":
					query = insertDryScope
					arg = dryScope(id)
				case "cleanup_source_admission":
					planID := cleanupPlan(id, "dry_run", false, false)
					query = fmt.Sprintf(`INSERT INTO nas_cleanup_source_holds(plan_id,clip_id) VALUES(%d,$1)`, planID)
					arg = id
				case "cleanup_plan_admission":
					query = `INSERT INTO nas_cleanup_plans(account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES(47,13,'inverse-manifest-'||$1::bigint,repeat('d',64),jsonb_build_array(jsonb_build_object('kind','raw','source_id',$1::bigint)),repeat('e',64))`
					arg = id
				case "cleanup_reactivate":
					arg = latentCleanup(id, true)
					query = `UPDATE nas_cleanup_items SET delete_ever_issued=true WHERE plan_id=$1`
				case "cleanup_insert_reactivate":
					arg = latentCleanup(id, false)
					query = `INSERT INTO nas_cleanup_items(plan_id,ordinal,state,delete_ever_issued) VALUES($1,1,'keep',true)`
				case "delivery_source":
					query = `INSERT INTO delivery_nas_source_holds(clip_id) VALUES($1)`
					arg = id
				case "delivery_output":
					query = `INSERT INTO delivery_keep_output_dependencies(output_id,recording_id) VALUES(10000001,$1)`
					arg = int64(30)
				case "delivery_path_enrichment":
					placeholder := "datasets/test/" + strconv.FormatInt(id, 10)
					exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',100,'metadata')`, placeholder)
					query = `UPDATE delivery_keep_holds SET relative_path=(SELECT display_path FROM recording_clips WHERE id=$1) WHERE object_key='` + placeholder + `'`
					arg = id
				case "collated_key":
					query = `UPDATE recording_collation_outputs SET object_key=$1 WHERE id=10000001`
				case "delivery_output_cached_source":
					exec(`UPDATE recording_collation_outputs SET source_clip_ids=array_append(source_clip_ids,$1::bigint) WHERE id=10000001`, int64(9000000+count-2))
					query = `INSERT INTO delivery_nas_source_holds(clip_id) VALUES($1)`
					arg = int64(9000000 + count - 2)
				case "held_output_source":
					exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256) SELECT 'stoarama','datasets/test/late-source-output','hold',size_bytes,'metadata',sha256 FROM recording_collation_outputs WHERE id=10000008 ON CONFLICT DO NOTHING`)
					query = `UPDATE recording_collation_outputs SET source_clip_ids=array_append(source_clip_ids,$1::bigint) WHERE id=10000008`
					arg = id
				}
				if err := pool.QueryRow(ctx, `SELECT epoch FROM r2_inverse_ke_epoch`).Scan(&a.keEpoch); err != nil {
					t.Fatal(err)
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
					if err == nil {
						return errors.New("late protection mutation committed")
					}
					var pe *pgconn.PgError
					if lateCtx.Err() == nil && !(errors.As(err, &pe) && pe.Code == "55P03") && !(kind == "output_id" && strings.Contains(err.Error(), "immutable")) && !strings.Contains(err.Error(), "cannot reactivate") {
						return fmt.Errorf("unexpected late admission refusal: %w", err)
					}
					return nil
				}
				before := store.calls
				ok, err := a.delete(ctx, ctx, &inverseSendGate{}, e)
				store.beforeDelete = nil
				if err != nil || !ok || store.calls != before+1 {
					t.Fatalf("wire result %v %v", ok, err)
				}
				_, err = pool.Exec(ctx, query, arg)
				expected := "reservation"
				if kind == "output_id" {
					expected = "immutable"
				}
				if kind == "cleanup_reactivate" || kind == "cleanup_insert_reactivate" {
					expected = "cannot reactivate"
				}
				if kind == "cleanup_source_admission" || kind == "cleanup_plan_admission" || kind == "joined_scope_admission" {
					expected = "retirement"
				}
				if err == nil || !strings.Contains(err.Error(), expected) {
					t.Fatal("post-wire protection adoption allowed", kind, err)
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
		if err = a.publishAbsent(ctx, e, a.opts.ManifestSHA); err == nil {
			t.Fatal("missing source published completion")
		}
		var events int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM r2_inverse_events WHERE object_key=$1`, e.Key).Scan(&events); err != nil || events != 0 {
			t.Fatal(events, err)
		}
	})
	t.Run("legacy_cleanup_scope_does_not_upgrade_to_admission", func(t *testing.T) {
		for _, exclusive := range []bool{false, true} {
			t.Run(strconv.FormatBool(exclusive), func(t *testing.T) {
				a, entries := build(1)
				defer a.ledger.close()
				e := entries[0]
				var sourceID int64
				if err := pool.QueryRow(ctx, `SELECT id FROM recording_clips WHERE object_key=$1`, e.Key).Scan(&sourceID); err != nil {
					t.Fatal(err)
				}
				planID := cleanupPlan(sourceID, "dry_run", true, false)
				legacy, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer legacy.Rollback(ctx)
				lockSQL := `SELECT pg_advisory_xact_lock_shared(7343226011101)`
				if exclusive {
					lockSQL = `SELECT pg_advisory_xact_lock(7343226011101)`
				}
				if _, err = legacy.Exec(ctx, lockSQL); err != nil {
					t.Fatal(err)
				}
				if _, err = legacy.Exec(ctx, `SELECT id FROM nas_cleanup_plans WHERE id=$1 FOR UPDATE`, planID); err != nil {
					t.Fatal(err)
				}
				if _, err = legacy.Exec(ctx, `SELECT id FROM nas_cleanup_items WHERE plan_id=$1 FOR UPDATE`, planID); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { _, err := a.delete(ctx, ctx, &inverseSendGate{}, e); done <- err }()
				deadline := time.Now().Add(3 * time.Second)
				for {
					var held bool
					if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND locktype='advisory' AND objsubid=1 AND classid::bigint::numeric*4294967296+objid::bigint::numeric=7343226011103 AND granted AND pid<>pg_backend_pid())`).Scan(&held); err != nil {
						t.Fatal(err)
					}
					if held {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("B1 never reached legacy public fence")
					}
					time.Sleep(10 * time.Millisecond)
				}
				progressCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				// Same parent/item order and mutations as observe/auto-pause/receipt paths.
				if _, err = legacy.Exec(progressCtx, `UPDATE nas_cleanup_items SET state='unverified' WHERE plan_id=$1`, planID); err != nil {
					t.Fatal("legacy item mutation blocked", err)
				}
				if _, err = legacy.Exec(progressCtx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=$1`, planID); err != nil {
					t.Fatal("legacy plan mutation upgraded locks", err)
				}
				var upgraded bool
				if err = legacy.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND locktype='advisory' AND pid=pg_backend_pid() AND objsubid=1 AND classid::bigint::numeric*4294967296+objid::bigint::numeric=7343226011103)`).Scan(&upgraded); err != nil || upgraded {
					t.Fatal("cleanup acquired admission lock", upgraded, err)
				}
				if err = legacy.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				select {
				case err = <-done:
					if err != nil {
						t.Fatal("B1 stranded by cleanup", err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("legacy/B1 lock cycle")
				}
			})
		}
	})
	t.Run("cleanup_admission_before_intent_wins", func(t *testing.T) {
		for _, manifestOnly := range []bool{false, true} {
			t.Run(strconv.FormatBool(manifestOnly), func(t *testing.T) {
				a, entries := build(1)
				defer a.ledger.close()
				e := entries[0]
				var id int64
				if err := pool.QueryRow(ctx, `SELECT id FROM recording_clips WHERE object_key=$1`, e.Key).Scan(&id); err != nil {
					t.Fatal(err)
				}
				if manifestOnly {
					exec(`INSERT INTO nas_cleanup_plans(account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES(47,13,'inverse-first-manifest-'||$1::bigint,repeat('d',64),jsonb_build_array(jsonb_build_object('kind','raw','source_id',$1::bigint)),repeat('e',64))`, id)
				} else {
					cleanupPlan(id, "dry_run", false, true)
				}
				if _, err := a.reserve(ctx, e); err == nil {
					t.Fatal("cleanup admission lost to intent")
				}
			})
		}
	})
	t.Run("joined_mutator_wins_barrier_before_B1_owns_legacy_locks", func(t *testing.T) {
		a, entries := build(1)
		defer a.ledger.close()
		e := entries[0]
		var id int64
		if err := pool.QueryRow(ctx, `SELECT id FROM recording_clips WHERE object_key=$1`, e.Key).Scan(&id); err != nil {
			t.Fatal(err)
		}
		runID := dryScope(id)
		mutator, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer mutator.Rollback(ctx)
		var mutatorPID int
		if err = mutator.QueryRow(ctx, `SELECT pg_backend_pid() FROM (SELECT epoch FROM r2_inverse_ke_epoch FOR UPDATE) e`).Scan(&mutatorPID); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { _, err := a.delete(ctx, ctx, &inverseSendGate{}, e); done <- err }()
		deadline := time.Now().Add(3 * time.Second)
		for {
			var waiting bool
			if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock' AND query LIKE '%SELECT epoch FROM r2_inverse_ke_epoch FOR SHARE%')`, mutatorPID).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("B1 did not wait on first barrier")
			}
			time.Sleep(10 * time.Millisecond)
		}
		var held bool
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND locktype='advisory' AND granted AND ((objsubid=1 AND (classid::bigint::numeric*4294967296+objid::bigint::numeric)=ANY(ARRAY[7343226011103,7343226011101]::numeric[])) OR(classid=137 AND objid=1 AND objsubid=2)))`).Scan(&held); err != nil || held {
			t.Fatal("waiting B1 owns later locks", held, err)
		}
		admissionCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if _, err = mutator.Exec(admissionCtx, insertDryScope, runID); err != nil {
			t.Fatal("joined admission blocked by waiting B1", err)
		}
		if err = mutator.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case err = <-done:
			if err == nil {
				t.Fatal("B1 ignored committed scope")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("joined/B1 lock cycle")
		}
		var intents int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM r2_inverse_intents WHERE object_key=$1`, e.Key).Scan(&intents); err != nil || intents != 0 {
			t.Fatal("scope winner left intent", intents, err)
		}
	})
	t.Run("repeatable_snapshot_cannot_adopt_after_intent", func(t *testing.T) {
		for _, level := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.Serializable} {
			t.Run(string(level), func(t *testing.T) {
				a, entries := build(1)
				defer a.ledger.close()
				e := entries[0]
				stale, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: level})
				if err != nil {
					t.Fatal(err)
				}
				defer stale.Rollback(ctx)
				var count int
				if err = stale.QueryRow(ctx, `SELECT count(*) FROM r2_inverse_intents WHERE object_key=$1`, e.Key).Scan(&count); err != nil || count != 0 {
					t.Fatal(count, err)
				}
				if _, err = a.delete(ctx, ctx, &inverseSendGate{}, e); err != nil {
					t.Fatal(err)
				}
				_, err = stale.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',100,'metadata')`, e.Key)
				var pe *pgconn.PgError
				if !errors.As(err, &pe) || pe.Code != "55000" {
					t.Fatal("stale snapshot admission not refused", err)
				}
			})
		}
	})
	t.Run("stale_snapshot_cannot_cross_COPY_reservation_boundary", func(t *testing.T) {
		const copyInsert = `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,blocked_reason)
 SELECT 13,'raw',id,repeat('a',64),$3,jsonb_build_object('bucket',bucket,'object_key',object_key),$2,CASE WHEN $2='blocked' THEN 'checksum_mismatch' ELSE '' END FROM recording_clips WHERE object_key=$1`
		for _, level := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.Serializable} {
			for _, admission := range []string{"pending", "blocked", "reservation"} {
				t.Run(string(level)+"/"+admission, func(t *testing.T) {
					a, entries := build(1)
					defer a.ledger.close()
					e := entries[0]
					stale, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: level})
					if err != nil {
						t.Fatal(err)
					}
					defer stale.Rollback(ctx)
					var count int
					if err = stale.QueryRow(ctx, `SELECT count(*) FROM r2_retirement_reservations WHERE object_key=$1`, e.Key).Scan(&count); err != nil || count != 0 {
						t.Fatal(count, err)
					}
					if admission == "reservation" {
						exec(copyInsert, e.Key, "blocked", policy)
						_, err = stale.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256) SELECT 'raw',id,bucket,object_key,size_bytes,sha256,repeat('a',64) FROM recording_clips WHERE object_key=$1`, e.Key)
					} else {
						if _, err = a.delete(ctx, ctx, &inverseSendGate{}, e); err != nil {
							t.Fatal(err)
						}
						_, err = stale.Exec(ctx, copyInsert, e.Key, admission, policy)
					}
					var pe *pgconn.PgError
					if !errors.As(err, &pe) || pe.Code != "55000" {
						t.Fatal("stale COPY/reservation admission not refused", err)
					}
				})
			}
		}
	})
	t.Run("COPY_update_admission_both_orderings", func(t *testing.T) {
		for _, reasonOnly := range []bool{false, true} {
			for _, reservationFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("reason_only=%t/reservation_first=%t", reasonOnly, reservationFirst), func(t *testing.T) {
					a, entries := build(1)
					defer a.ledger.close()
					e := entries[0]
					exec(`INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state) SELECT 13,'raw',id,repeat('a',64),$2,jsonb_build_object('bucket',bucket,'object_key',object_key),'verified' FROM recording_clips WHERE object_key=$1`, e.Key, policy)
					query := `UPDATE nas_r2_copy_items SET state='blocked',blocked_reason='checksum_mismatch' WHERE item->>'object_key'=$1`
					if reasonOnly {
						query = `UPDATE nas_r2_copy_items SET blocked_reason='final_conflict' WHERE item->>'object_key'=$1`
					}
					if reservationFirst {
						if _, err := a.reserve(ctx, e); err != nil {
							t.Fatal(err)
						}
						// A no-change verified update is not a new protection admission.
						exec(`UPDATE nas_r2_copy_items SET state='verified' WHERE item->>'object_key'=$1`, e.Key)
						if _, err := pool.Exec(ctx, query, e.Key); err == nil || !strings.Contains(err.Error(), "reservation") {
							t.Fatal("COPY UPDATE adopted reserved key", err)
						}
					} else {
						exec(query, e.Key)
						if _, err := a.reserve(ctx, e); err == nil {
							t.Fatal("COPY protection lost to reservation")
						}
					}
				})
			}
		}
	})
	t.Run("COPY_progress_with_real_reimport_has_no_lock_cycle", func(t *testing.T) {
		for _, transition := range []string{"pending_to_leased", "leased_to_verified", "leased_to_blocked", "verified_noop", "new_protection"} {
			t.Run(transition, func(t *testing.T) {
				a, entries := build(1)
				defer a.ledger.close()
				e := entries[0]
				exec(`UPDATE recording_clips SET clip_start_at=$2,clip_end_at=$3 WHERE object_key=$1`, e.Key, w.Start.Add(time.Hour), w.Start.Add(2*time.Hour))
				i := api.NASR2CopyItem{Kind: api.NASR2CopyRaw, StreamID: 3, Bucket: "stoarama", ObjectKey: e.Key}
				if err := pool.QueryRow(ctx, `SELECT id,recording_id,display_path,size_bytes,sha256,clip_start_at,clip_end_at FROM recording_clips WHERE object_key=$1`, e.Key).Scan(&i.SourceID, &i.RecordingID, &i.RelativePath, &i.SizeBytes, &i.SHA256, &i.StartAt, &i.EndAt); err != nil {
					t.Fatal(err)
				}
				// Manifest import receives JSON timestamps, not pgx's scan location.
				i.StartAt, i.EndAt = i.StartAt.UTC(), i.EndAt.UTC()
				h := nasCopyHeader{PolicySHA256: policy}
				if err := queueNASR2CopyItem(ctx, pool, h, strings.Repeat("a", 64), i, true); err != nil {
					t.Fatal("initial real import", err)
				}
				initial, nextState := "pending", "leased"
				switch transition {
				case "leased_to_verified":
					initial, nextState = "leased", "verified"
				case "leased_to_blocked":
					initial, nextState = "leased", "blocked"
				case "verified_noop":
					initial, nextState = "verified", "verified"
				case "new_protection":
					initial, nextState = "verified", "blocked"
				}
				exec(`UPDATE nas_r2_copy_items SET state=$2 WHERE source_id=$1`, i.SourceID, initial)
				handler, err := api.BeginNASR2CopyTx(ctx, pool)
				if err != nil {
					t.Fatal(err)
				}
				defer handler.Rollback(ctx)
				if _, err = handler.Exec(ctx, `SET LOCAL deadlock_timeout='50ms'`); err != nil {
					t.Fatal(err)
				}
				if _, err = handler.Exec(ctx, `SELECT id FROM nas_r2_copy_items WHERE source_id=$1 FOR UPDATE`, i.SourceID); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- queueNASR2CopyItem(ctx, pool, h, strings.Repeat("a", 64), i, true) }()
				deadline := time.Now().Add(3 * time.Second)
				for {
					var waiting bool
					if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE query LIKE 'INSERT INTO nas_r2_copy_items%' AND $1::integer=ANY(pg_blocking_pids(pid)))`, handler.Conn().PgConn().PID()).Scan(&waiting); err != nil {
						t.Fatal(err)
					}
					if waiting {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("real reimport did not reach handler row")
					}
					time.Sleep(5 * time.Millisecond)
				}
				progress, cancel := context.WithTimeout(ctx, time.Second)
				_, err = handler.Exec(progress, `UPDATE nas_r2_copy_items SET state=$2,blocked_reason=CASE WHEN $2='blocked' THEN 'checksum_mismatch' ELSE '' END WHERE source_id=$1`, i.SourceID, nextState)
				cancel()
				if transition == "new_protection" {
					var pe *pgconn.PgError
					if !errors.As(err, &pe) || pe.Code != "55P03" {
						t.Fatal("new admission must fail fast behind importer", err)
					}
					handler.Rollback(ctx)
				} else {
					if err != nil {
						t.Fatal("COPY progress deadlocked with real reimport", err)
					}
					if err = handler.Commit(ctx); err != nil {
						t.Fatal(err)
					}
				}
				select {
				case err = <-done:
					if err != nil {
						t.Fatal("real reimport failed", err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("reimport remained blocked")
				}
				if nextState == "leased" {
					exec(`UPDATE nas_r2_copy_items SET state='verified' WHERE source_id=$1`, i.SourceID)
				}
			})
		}
	})
	t.Run("pool_capacity_reserved_before_any_intent", func(t *testing.T) {
		a, entries := build(4)
		a.ledger.close()
		smallConfig := pool.Config()
		smallConfig.MaxConns = 4
		smallConfig.MinConns = 0
		small, err := pgxpool.NewWithConfig(ctx, smallConfig)
		if err != nil {
			t.Fatal(err)
		}
		defer small.Close()
		a.opts.Workers = 3
		if _, err = applyInverse(ctx, small, store, a.opts); err == nil || !strings.Contains(err.Error(), "capacity") {
			t.Fatal("oversubscribed pool admitted", err)
		}
		var count int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM r2_inverse_intents WHERE manifest_sha256=$1`, a.opts.ManifestSHA).Scan(&count); err != nil || count != 0 {
			t.Fatal("invalid pool wrote intent", count, err)
		}
		a.opts.Workers = 2
		bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		result, err := applyInverse(bounded, small, store, a.opts)
		if err != nil || result.Deleted != int64(len(entries)) {
			t.Fatalf("capacity ceiling %+v %v", result, err)
		}
	})
	t.Run("retired_item_reactivation_is_terminal", func(t *testing.T) {
		a, entries := build(1)
		defer a.ledger.close()
		var sourceID int64
		if err := pool.QueryRow(ctx, `SELECT id FROM recording_clips WHERE object_key=$1`, entries[0].Key).Scan(&sourceID); err != nil {
			t.Fatal(err)
		}
		planID := latentCleanup(sourceID, true)
		for _, query := range []string{`UPDATE nas_cleanup_items SET delete_ever_issued=true WHERE plan_id=$1`, `UPDATE nas_cleanup_items SET state='unverified' WHERE plan_id=$1`} {
			if _, err := pool.Exec(ctx, query, planID); err == nil || !strings.Contains(err.Error(), "cannot reactivate") {
				t.Fatal("retired hold reactivated", err)
			}
		}
	})
	t.Run("cleanup_retirement_and_first_hold_serialize", func(t *testing.T) {
		for _, retireFirst := range []bool{true, false} {
			t.Run(strconv.FormatBool(retireFirst), func(t *testing.T) {
				a, entries := build(1)
				defer a.ledger.close()
				e := entries[0]
				var id int64
				if err := pool.QueryRow(ctx, `SELECT id FROM recording_clips WHERE object_key=$1`, e.Key).Scan(&id); err != nil {
					t.Fatal(err)
				}
				planID := cleanupPlan(id, "paused", true, true)
				first, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer first.Rollback(ctx)
				if retireFirst {
					if _, err = first.Exec(ctx, `UPDATE nas_cleanup_plans SET mode='retired' WHERE id=$1`, planID); err != nil {
						t.Fatal(err)
					}
					_, err = pool.Exec(ctx, `UPDATE nas_cleanup_items SET delete_ever_issued=true WHERE plan_id=$1`, planID)
					var pe *pgconn.PgError
					if !errors.As(err, &pe) || pe.Code != "55P03" {
						t.Fatal("retire/flip could cycle", err)
					}
					if err = first.Commit(ctx); err != nil {
						t.Fatal(err)
					}
					if _, err = pool.Exec(ctx, `UPDATE nas_cleanup_items SET delete_ever_issued=true WHERE plan_id=$1`, planID); err == nil || !strings.Contains(err.Error(), "cannot reactivate") {
						t.Fatal("retired hold returned", err)
					}
				} else {
					if _, err = first.Exec(ctx, `UPDATE nas_cleanup_items SET delete_ever_issued=true WHERE plan_id=$1`, planID); err != nil {
						t.Fatal(err)
					}
					bounded, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
					_, err = pool.Exec(bounded, `UPDATE nas_cleanup_plans SET mode='retired' WHERE id=$1`, planID)
					cancel()
					if err == nil {
						t.Fatal("retirement crossed hold publication")
					}
					if err = first.Commit(ctx); err != nil {
						t.Fatal(err)
					}
					exec(`UPDATE nas_cleanup_plans SET mode='retired' WHERE id=$1`, planID)
					if _, err = a.reserve(ctx, e); err == nil {
						t.Fatal("retirement shed issued source hold")
					}
				}
			})
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
		a.opts.Budget24h = -1
		before := providerDeletes()
		if _, err := applyInverse(ctx, pool, sdk, a.opts); err == nil || providerDeletes() != before {
			t.Fatal("negative budget accepted", err)
		}
		a.opts.Budget24h = 1
		_, err := applyInverse(ctx, pool, sdk, a.opts)
		if err == nil || providerDeletes() != before {
			t.Fatal("budget exceeded", err)
		}
		a.opts.Budget24h = 0
		a.opts.MaxItems = 1
		result, err := applyInverse(ctx, pool, sdk, a.opts)
		if err != nil || result.Deleted != 1 || providerDeletes()-before != 1 {
			t.Fatalf("unlimited budget crossed item bound: %+v %v", result, err)
		}
		a.opts.MaxItems = 2
		a.opts.MaxBytes = 99
		result, err = applyInverse(ctx, pool, sdk, a.opts)
		if err != nil || result.Deleted != 0 || providerDeletes()-before != 1 {
			t.Fatalf("unlimited budget crossed byte bound: %+v %v", result, err)
		}
		a.opts.MaxBytes = 100
		result, err = applyInverse(ctx, pool, sdk, a.opts)
		if err != nil || result.Deleted != 1 || providerDeletes()-before != 2 {
			t.Fatalf("unlimited budget did not resume: %+v %v", result, err)
		}
	})
	t.Run("private_authority_cannot_replay_or_remint", func(t *testing.T) {
		for _, lost := range []bool{false, true} {
			t.Run(strconv.FormatBool(lost), func(t *testing.T) {
				a, entries := build(1)
				defer a.ledger.close()
				e := entries[0]
				a.store = sdk
				if _, err := a.reserve(ctx, e); err != nil {
					t.Fatal(err)
				}
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)
				authority, err := a.authorize(ctx, tx, e, &inverseSendGate{})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = a.authorize(ctx, tx, e, &inverseSendGate{}); err == nil {
					t.Fatal("durable authority reminted")
				}
				before := providerDeletes()
				store.setLost(lost)
				err = sdk.deleteOnce(ctx, authority)
				store.setLost(false)
				if (err != nil) != lost {
					t.Fatal("first wire result", err)
				}
				clone := *authority
				if err = sdk.deleteOnce(ctx, authority); err == nil {
					t.Fatal("wire authority replayed")
				}
				if err = sdk.deleteOnce(ctx, &clone); err == nil {
					t.Fatal("copied wire authority replayed")
				}
				if err = sdk.deleteOnce(ctx, &inverseDeleteAuthority{}); err == nil {
					t.Fatal("zero authority reached wire")
				}
				if providerDeletes()-before != 1 {
					t.Fatal("provider replay", providerDeletes()-before)
				}
			})
		}
	})
	t.Run("network_deadline_releases_locks_and_holds", func(t *testing.T) {
		for _, afterDelete := range []bool{false, true} {
			t.Run(strconv.FormatBool(afterDelete), func(t *testing.T) {
				a, entries := build(1)
				defer a.ledger.close()
				e := entries[0]
				timed, cancel := context.WithCancel(ctx)
				store.beforeHead = func(headCtx context.Context, key string) error {
					if key != e.Key {
						return nil
					}
					deadline, bounded := headCtx.Deadline()
					if !bounded || time.Until(deadline) > inverseNetworkTimeout {
						return errors.New("network context has no bounded deadline")
					}
					store.mu.Lock()
					_, present := store.objects[key]
					store.mu.Unlock()
					if !afterDelete || !present {
						cancel()
						return headCtx.Err()
					}
					return nil
				}
				_, err := a.delete(timed, ctx, &inverseSendGate{}, e)
				cancel()
				store.beforeHead = nil
				if err == nil {
					t.Fatal("stalled network ignored deadline")
				}
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				lockCtx, lockCancel := context.WithTimeout(ctx, time.Second)
				_, err = tx.Exec(lockCtx, `SELECT epoch FROM r2_inverse_ke_epoch FOR UPDATE NOWAIT`)
				if err == nil {
					err = inverseProtectionLocks(lockCtx, tx, false)
				}
				lockCancel()
				tx.Rollback(ctx)
				if err != nil {
					t.Fatal("network timeout retained protection locks", err)
				}
				before := store.calls
				a.ledger.close()
				result, err := applyInverse(ctx, pool, store, a.opts)
				if err != nil || store.calls != before || (afterDelete && result.AmbiguousHeld < 1) {
					t.Fatalf("restart %+v %v", result, err)
				}
			})
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
		a.opts.Workers = 16
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
func inverseSDKTestStore(t *testing.T, store *inverseFakeStore) (*inverseSDKStore, func() int64) {
	t.Helper()
	var deleted atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet && req.URL.Path == "/stoarama" && req.URL.Query().Has("uploads") {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`)
			return
		}
		key := strings.TrimPrefix(req.URL.Path, "/stoarama/")
		if req.Method == http.MethodDelete {
			deleted.Add(1)
			store.mu.Lock()
			mode, hook, answered := store.wire[key], store.onDelete, store.onAnswered
			if store.lost {
				mode = "eof"
			}
			if mode == "" || mode == "eof" || mode == "slow" {
				delete(store.objects, key)
			}
			store.mu.Unlock()
			if hook != nil {
				hook(key)
			}
			if answered != nil {
				defer answered(key)
			}
			switch mode {
			case "eof":
				c, _, _ := w.(http.Hijacker).Hijack()
				c.Close()
			case "503-headers-cut":
				c, _, _ := w.(http.Hijacker).Hijack()
				c.Write([]byte("HTTP/1.1 503 Service Unavailable\r\nContent-Ty"))
				c.Close()
			case "slow":
				<-req.Context().Done()
			case "403", "503":
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(map[string]int{"403": 403, "503": 503}[mode])
				fmt.Fprint(w, `<Error><Code>Refused</Code><Message>test</Message></Error>`)
			case "503-truncated":
				w.Header().Set("Content-Type", "application/xml")
				w.Header().Set("Content-Length", "500")
				w.WriteHeader(503)
				fmt.Fprint(w, `<Error><Code>Slow`)
			default:
				w.WriteHeader(204)
			}
			return
		}
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
			if !store.headWithoutSHA {
				w.Header().Set("x-amz-meta-sha256", strings.Repeat("b", 64))
			}
			w.WriteHeader(200)
		default:
			w.WriteHeader(400)
		}
	}))
	t.Cleanup(server.Close)
	client, err := newInverseSDKStore(context.Background(), r2.Config{Bucket: "stoarama", Endpoint: server.URL, AccessKey: "test", SecretKey: "test", Region: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	return client, deleted.Load
}

type failingWriteConn struct {
	net.Conn
	err error
}

func (c failingWriteConn) Write(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	return len(p), nil
}

// A lost response needs a request its connection accepted in full and no
// response byte; a failed write never qualifies, even with nothing read back.
func TestR2InverseWireLostNeedsAcceptedRequest(t *testing.T) {
	sent := &inverseWire{gate: &inverseSendGate{}}
	(&inverseWireConn{Conn: failingWriteConn{}, wire: sent}).Write([]byte("DELETE"))
	failed := &inverseWire{gate: &inverseSendGate{}}
	(&inverseWireConn{Conn: failingWriteConn{err: syscall.EPIPE}, wire: failed}).Write([]byte("DELETE"))
	answered := &inverseWire{gate: &inverseSendGate{}}
	(&inverseWireConn{Conn: failingWriteConn{}, wire: answered}).Write([]byte("DELETE"))
	answered.answered.Store(true)
	if !sent.lost() || failed.lost() || answered.lost() || (&inverseWire{}).lost() {
		t.Fatalf("lost: sent %v failed %v answered %v unsent %v", sent.lost(), failed.lost(), answered.lost(), (&inverseWire{}).lost())
	}
}

type shortWriteConn struct{ net.Conn }

func (shortWriteConn) Write([]byte) (int, error) { return 0, nil }

// A zero-byte write carries no request byte: an empty one neither passes a
// closed gate nor counts as sent, and one that wrote nothing of its bytes is a
// failed write. Neither is a lost response.
func TestR2InverseWireZeroByteWriteIsNotLost(t *testing.T) {
	gate := &inverseSendGate{}
	gate.close()
	empty := &inverseWire{gate: gate}
	conn := &inverseWireConn{Conn: failingWriteConn{}, wire: empty}
	if n, err := conn.Write(nil); n != 0 || err != nil || empty.wrote.Load() || empty.lost() {
		t.Fatalf("empty write: %d %v wrote %v", n, err, empty.wrote.Load())
	}
	if _, err := conn.Write([]byte("DELETE")); !errors.Is(err, errInverseAdmissionClosed) || empty.lost() {
		t.Fatalf("first write after an empty one passed a closed gate: %v", err)
	}
	short := &inverseWire{gate: &inverseSendGate{}}
	(&inverseWireConn{Conn: shortWriteConn{}, wire: short}).Write([]byte("DELETE"))
	if short.lost() || !short.writeFailed.Load() {
		t.Fatal("a write that sent no byte counted as a lost response")
	}
}

// lateWriteConn resumes a write only after the wire deadline has passed.
type lateWriteConn struct {
	net.Conn
	until time.Time
}

func (c lateWriteConn) Write(p []byte) (int, error) {
	time.Sleep(time.Until(c.until) + 20*time.Millisecond)
	return c.Conn.Write(p)
}

// deadlineBlindConn ignores socket deadlines, as a wrapped conn might.
type deadlineBlindConn struct{ net.Conn }

func (deadlineBlindConn) SetWriteDeadline(time.Time) error { return nil }

// No first request byte leaves at or after the wire deadline: a write that
// starts late is refused even by a conn blind to deadlines, and one that
// stalls past it is refused by the socket. Neither wrote, so neither is a
// lost response.
func TestR2InverseWireDeadlineRefusesFirstByte(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	for _, stalled := range []bool{false, true} {
		client, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		server, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		wire := &inverseWire{gate: &inverseSendGate{}, deadline: time.Now().Add(-time.Millisecond)}
		var conn net.Conn = deadlineBlindConn{client}
		if stalled {
			wire.deadline = time.Now().Add(100 * time.Millisecond)
			conn = lateWriteConn{Conn: client, until: wire.deadline}
		}
		_, writeErr := wire.track(conn).Write([]byte("DELETE /stoarama/key HTTP/1.1\r\n\r\n"))
		client.Close()
		received, _ := io.ReadAll(server)
		server.Close()
		if writeErr == nil || len(received) != 0 || wire.wrote.Load() || wire.lost() {
			t.Fatalf("stalled=%v: %v, server received %q", stalled, writeErr, received)
		}
	}
}

// The job holds only the very keys the executor reports: equal counts with a
// different held key stop it.
func TestR2InverseJobReadbackComparesHeldKeys(t *testing.T) {
	lost := fmt.Errorf("%w: test", errInverseWireAmbiguous)
	result := inverseApplySummary{Deleted: 1, Bytes: 100, WireAmbiguous: []string{"managed/a"}}
	readback := inverseBatchReadback{Intents: 2, Absent: 1, AbsentBytes: 100, Attempts: 2, Unresolved: []string{"managed/a"}}
	if done, err := readback.settle(result, lost); err != nil || done {
		t.Fatalf("matching readback: %v %v", done, err)
	}
	readback.Unresolved = []string{"managed/b"}
	if _, err := readback.settle(result, lost); err == nil || !strings.Contains(err.Error(), "evidence differs") {
		t.Fatalf("different held key continued the job: %v", err)
	}
	// Done means nothing new was selected: a batch whose selected keys were all
	// skipped for short custody, deleting none, is not done.
	if done, err := (inverseBatchReadback{Unresolved: []string{}}).settle(inverseApplySummary{Selected: 1, CustodyShort: r2RetireStat{Objects: 1, Bytes: 100}, WireAmbiguous: []string{}}, nil); err != nil || done {
		t.Fatalf("skipped-only batch: done %v %v", done, err)
	}
	if done, err := (inverseBatchReadback{Unresolved: []string{}}).settle(inverseApplySummary{WireAmbiguous: []string{}}, nil); err != nil || !done {
		t.Fatalf("empty batch: done %v %v", done, err)
	}
}

// Job totals sum every batch's counts; held keys are the latest snapshot.
func TestR2InverseJobTotalsAddBatches(t *testing.T) {
	total := inverseApplySummary{}
	total.addBatch(inverseApplySummary{Selected: 3, Deleted: 2, Bytes: 200, CustodyShort: r2RetireStat{Objects: 1, Bytes: 100}, WireAmbiguous: []string{"managed/a"}, LastModifiedHeld: r2RetireStat{Objects: 2, Bytes: 200}})
	total.addBatch(inverseApplySummary{Selected: 1, Deleted: 1, Bytes: 100, AmbiguousHeld: 1, HeldAbsent: []string{"managed/a"}, LastModifiedHeld: r2RetireStat{Objects: 2, Bytes: 200}})
	// last_modified_held comes from the partition every batch rereads: a snapshot.
	if total.Selected != 4 || total.Deleted != 3 || total.Bytes != 300 || total.CustodyShort != (r2RetireStat{Objects: 1, Bytes: 100}) || total.LastModifiedHeld != (r2RetireStat{Objects: 2, Bytes: 200}) || !slices.Equal(total.WireAmbiguous, []string{"managed/a"}) || total.AmbiguousHeld != 1 || !slices.Equal(total.HeldAbsent, []string{"managed/a"}) {
		t.Fatalf("job totals: %+v", total)
	}
}

type blockingWriteConn struct {
	net.Conn
	release chan struct{}
}

func (c blockingWriteConn) Write(p []byte) (int, error) {
	<-c.release
	return len(p), nil
}

// Closing the gate waits for a first write already under way, and refuses one
// that has not started.
func TestR2InverseSendGateOrdersFirstWrites(t *testing.T) {
	gate := &inverseSendGate{}
	release := make(chan struct{})
	under := &inverseWire{gate: gate}
	wrote := make(chan error, 1)
	go func() {
		_, err := (&inverseWireConn{Conn: blockingWriteConn{release: release}, wire: under}).Write([]byte("DELETE"))
		wrote <- err
	}()
	// Wait until the first write holds the gate.
	for gate.mu.TryLock() {
		gate.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	closed := make(chan struct{})
	go func() { gate.close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("gate closed under a first write in progress")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	<-closed
	if err := <-wrote; err != nil || !under.wrote.Load() {
		t.Fatalf("first write under way: %v", err)
	}
	late := &inverseWire{gate: gate}
	if _, err := (&inverseWireConn{Conn: failingWriteConn{}, wire: late}).Write([]byte("DELETE")); !errors.Is(err, errInverseAdmissionClosed) || late.lost() {
		t.Fatalf("first write after close: %v", err)
	}
}
