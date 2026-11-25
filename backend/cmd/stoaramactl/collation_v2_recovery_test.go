package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/collation"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// recoveryBucket is a create-only fake R2: PUT honours If-None-Match, a key in
// fail answers 500 (a lost wire write), and nothing is ever deleted.
type recoveryBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
	fail    map[string]bool
}

func (f *recoveryBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	etag := func(b []byte) string { s := sha256.Sum256(b); return `"` + hex.EncodeToString(s[:16]) + `"` }
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	switch r.Method {
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		if f.fail[key] {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if _, exists := f.objects[key]; exists && r.Header.Get("If-None-Match") == "*" {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		f.objects[key] = body
		w.Header().Set("ETag", etag(body))
	case http.MethodHead, http.MethodGet:
		body, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if m := r.Header.Get("If-Match"); m != "" && m != etag(body) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("ETag", etag(body))
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}
	default:
		http.Error(w, "unexpected "+r.Method, http.StatusBadRequest)
	}
}

func TestR2WriterFenceCollationV2RecoveryScope(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "collation_recovery")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	refused := func(what, want string, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: want refusal %q, got %v", what, want, err)
		}
	}
	exec(`
INSERT INTO accounts(id,email) VALUES(47,'collation-recovery@example.invalid');
INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(78,'test','78','scoped','scoped','https://storage.example.test','off','Asia/Seoul','hls','video_manifest','video_live','continuous_video',30);
INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc)
 VALUES(1,47,'test','https://storage.example.test','auto','bucket','fixture',decode('00','hex'));
INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(31,47,1,'scoped','https://storage.example.test','* * * * *','Asia/Seoul','paused',78,now()),
       (32,47,1,'unscoped','https://storage.example.test','* * * * *','Asia/Seoul','paused',78,now())`)
	matureR2WriterFence(t, pool, "bucket")
	if err := r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA); err != nil {
		t.Fatal(err)
	}
	// The controller approves recording 31's Seoul day 2026-11-01 under the fence.
	exec(`INSERT INTO collation_v2_recovery_scope(recording_id,local_date,reason) VALUES(31,'2026-11-01','best 14 recovery')`)

	bucket := &recoveryBucket{objects: map[string][]byte{}, fail: map[string]bool{}}
	server := httptest.NewServer(bucket)
	defer server.Close()
	client, err := r2.New(ctx, r2.Config{WriterDB: pool, AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	store := collation.R2Store{Client: client}
	const batch = "best14-recovery"
	seoul, _ := time.LoadLocation("Asia/Seoul")
	hour := func(recording int64, date string, h int) collation.HourWork {
		t.Helper()
		id, err := collation.HourIDFor(batch, recording, date, h)
		if err != nil {
			t.Fatal(err)
		}
		return collation.HourWork{BatchID: batch, HourID: id, RecordingID: recording, LocalDate: date, DeliveryHour: h}
	}
	part := func(name string) (string, []byte, string) {
		body := []byte("stitched " + name)
		sum := sha256.Sum256(body)
		sha := hex.EncodeToString(sum[:])
		return collation.ObjectKey(batch, sha), body, sha
	}
	publish := func(s collation.Store, key string, body []byte, sha string) error {
		path := t.TempDir() + "/part.mp4"
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		return s.PublishVerified(ctx, key, "video/mp4", path, int64(len(body)), sha)
	}

	// run: a scoped hour PUTs its part and manifest past the fence.
	scoped := hour(31, "2026-11-01", 1)
	rs := store.CollationRecoveryStore(scoped)
	partKey, partBody, partSHA := part("hour 1")
	if err := publish(rs, partKey, partBody, partSHA); err != nil {
		t.Fatal("scoped part refused:", err)
	}
	// 08:30 in Seoul is still the previous UTC day: the local date is the
	// recording's, never UTC's.
	start := time.Date(2026, 11, 1, 8, 30, 0, 0, seoul)
	manifest := collation.HourManifest{Generation: collation.Generation, Status: collation.StatusCollated, BatchID: batch, HourID: scoped.HourID,
		RecordingID: 31, LocalDate: "2026-11-01", DeliveryHour: 1, Clips: []collation.ClipDisposition{},
		Outputs: []collation.Output{{Part: 1, Parts: 1, ObjectKey: partKey, NASRelativePath: "scoped/hour-01.mp4", SizeBytes: int64(len(partBody)),
			SHA256: partSHA, StartUTC: start.UTC(), EndUTC: start.Add(30 * time.Minute).UTC(), SourceClipIDs: []int64{901}, R2VerifiedAt: time.Now()}}}
	manifestBody, _ := json.Marshal(manifest)
	manifestKey := collation.ManifestKey(batch, scoped.HourID)
	if err := rs.PutManifestIfAbsent(ctx, manifestKey, manifestBody); err != nil {
		t.Fatal("scoped manifest refused:", err)
	}
	var completed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM r2_retirement_writer_operations WHERE collation_recovery_hour_id=$1 AND completed_at IS NOT NULL`, scoped.HourID).Scan(&completed); err != nil || completed != 2 {
		t.Fatalf("recovery intents not durable and completed: %d %v", completed, err)
	}
	// A scoped hour bound client still writes nothing outside its own layout, and
	// the unbound client and unscoped hours stay fenced.
	refused("plain client", "admission", store.PutManifestIfAbsent(ctx, collation.ManifestKey(batch, hour(31, "2026-11-01", 2).HourID), manifestBody))
	_, err = client.ForCollationRecovery(scoped.HourID).PutBytes(ctx, "managed/acct-47/x.mp4", "video/mp4", []byte("x"))
	refused("key outside the hour layout", "admission", err)
	// A bound client makes no unconditional write, even at its own hour's key.
	spareKey, spareBody, _ := part("spare")
	_, err = client.ForCollationRecovery(scoped.HourID).PutBytes(ctx, spareKey, "video/mp4", spareBody)
	refused("unconditional PUT by a recovery client", "create-only", err)
	_, err = client.ForCollationRecovery(scoped.HourID).PresignPut(ctx, spareKey, "video/mp4", time.Hour)
	refused("capability by a recovery client", "create-only", err)
	otherKey, otherBody, otherSHA := part("unscoped")
	refused("unscoped recording", "admission", publish(store.CollationRecoveryStore(hour(32, "2026-11-01", 1)), otherKey, otherBody, otherSHA))
	refused("unscoped date", "admission", publish(store.CollationRecoveryStore(hour(31, "2026-11-02", 1)), otherKey, otherBody, otherSHA))
	bucket.mu.Lock()
	_, leaked := bucket.objects[otherKey]
	_, spareLeaked := bucket.objects[spareKey]
	bucket.mu.Unlock()
	if leaked || spareLeaked {
		t.Fatal("refused write reached R2")
	}

	// A lost recovery PUT keeps its durable intent: every R2 delete batch stays
	// blocked behind it, exactly as for any other writer.
	lostKey, lostBody, lostSHA := part("lost")
	lostStore := store.CollationRecoveryStore(hour(31, "2026-11-01", 3))
	bucket.mu.Lock()
	bucket.fail[lostKey] = true
	bucket.mu.Unlock()
	if err := publish(lostStore, lostKey, lostBody, lostSHA); err == nil {
		t.Fatal("failed PUT reported success")
	}
	refused("delete gate during a recovery intent", "have not drained", r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA))
	var drained bool
	if err := pool.QueryRow(ctx, `SELECT r2_retirement_writers_drained('bucket')`).Scan(&drained); err != nil || drained {
		t.Fatalf("recovery intent hidden from writers_drained: %v %v", drained, err)
	}
	// Only a later PUT that verifies the key's exact bytes completes the lost
	// attempts' intents with its own.
	bucket.mu.Lock()
	delete(bucket.fail, lostKey)
	bucket.mu.Unlock()
	if err := publish(lostStore, lostKey, lostBody, lostSHA); err != nil {
		t.Fatal(err)
	}
	if err := r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA); err != nil {
		t.Fatal("verified retry left lost intents blocking:", err)
	}

	// register: the scoped hour and its part catalogue past the fence.
	if err := insertCollationHour(ctx, pool, scoped, manifest, manifestKey, manifestBody); err != nil {
		t.Fatal("scoped register refused:", err)
	}
	// Once catalogued, the hour's keys are no longer new: run refuses them.
	refused("catalogued part re-PUT", "admission", publish(rs, partKey, partBody, partSHA))
	refused("catalogued manifest re-PUT", "admission", rs.PutManifestIfAbsent(ctx, manifestKey, manifestBody))

	register := func(w collation.HourWork, out collation.Output) error {
		m := manifest
		m.HourID, m.DeliveryHour, m.Outputs = w.HourID, w.DeliveryHour, []collation.Output{out}
		body, _ := json.Marshal(m)
		return insertCollationHour(ctx, pool, w, m, collation.ManifestKey(batch, w.HourID), body)
	}
	newPart := func(name string, start time.Time) collation.Output {
		key, body, sha := part(name)
		return collation.Output{Part: 1, Parts: 1, ObjectKey: key, NASRelativePath: "scoped/" + name + ".mp4", SizeBytes: int64(len(body)), SHA256: sha,
			StartUTC: start.UTC(), EndUTC: start.Add(time.Minute).UTC(), SourceClipIDs: []int64{902}, R2VerifiedAt: time.Now()}
	}
	refused("unscoped recording register", "fenced", register(hour(32, "2026-11-01", 1), newPart("r32", start)))
	refused("unscoped date register", "fenced", register(hour(31, "2026-11-02", 1), newPart("d2", start.AddDate(0, 0, 1))))
	reused := newPart("hour 2", start.Add(time.Hour))
	reused.ObjectKey, reused.SHA256, reused.SizeBytes = partKey, partSHA, int64(len(partBody))
	refused("existing object_key", "fenced", register(hour(31, "2026-11-01", 2), reused))
	// 00:30 on 2 November in Seoul is still 1 November in UTC: refused.
	refused("part outside the local date", "fenced", register(hour(31, "2026-11-01", 2), newPart("utc day", time.Date(2026, 11, 2, 0, 30, 0, 0, seoul))))
	// A registered hour never gains a part later, even inside the scope.
	late := newPart("late", start)
	_, err = pool.Exec(ctx, `INSERT INTO recording_collation_outputs(collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
		SELECT id,2,2,$2,$3,$4,$5,$6,$7,$8,now() FROM recording_collation_hours WHERE hour_id=$1`,
		scoped.HourID, late.ObjectKey, late.NASRelativePath, late.SizeBytes, late.SHA256, late.StartUTC, late.EndUTC, late.SourceClipIDs)
	refused("late part on a registered hour", "fenced", err)
	if err := register(hour(31, "2026-11-01", 2), newPart("hour 2", start.Add(time.Hour))); err != nil {
		t.Fatal("second scoped hour refused:", err)
	}

	// Add-only: nothing the scope admitted can change or leave.
	for name, sql := range map[string]string{
		"hour update":     `UPDATE recording_collation_hours SET status='gap_only' WHERE recording_id=31`,
		"hour delete":     `DELETE FROM recording_collation_hours WHERE recording_id=31`,
		"output update":   `UPDATE recording_collation_outputs SET r2_verified_at=now()`,
		"output delete":   `DELETE FROM recording_collation_outputs`,
		"output truncate": `TRUNCATE recording_collation_outputs CASCADE`,
	} {
		_, err := pool.Exec(ctx, sql)
		refused(name, "collation v2 recovery", err)
	}
	var hours, outputs int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM recording_collation_hours),(SELECT count(*) FROM recording_collation_outputs)`).Scan(&hours, &outputs); err != nil || hours != 2 || outputs != 2 {
		t.Fatalf("catalog changed: hours=%d outputs=%d %v", hours, outputs, err)
	}
}
