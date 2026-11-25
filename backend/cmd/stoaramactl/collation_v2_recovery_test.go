package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
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

// recoveryBucket is a create-only fake R2: PUT honours If-None-Match and
// refuses a body that differs from its x-amz-checksum-sha256 (400 BadDigest);
// a key in fail answers 500 (a lost wire write), one in headFail answers HEAD
// 500, one in race is absent to HEAD but lands its foreign bytes at the PUT
// (412). Nothing is ever deleted.
type recoveryBucket struct {
	mu          sync.Mutex
	objects     map[string][]byte
	fail        map[string]bool
	headFail    map[string]bool
	race        map[string][]byte
	puts        map[string]int
	checksummed map[string]bool
}

func (f *recoveryBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	etag := func(b []byte) string { s := sha256.Sum256(b); return `"` + hex.EncodeToString(s[:16]) + `"` }
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		prefix := r.URL.Query().Get("prefix")
		var contents strings.Builder
		for k, body := range f.objects {
			if strings.HasPrefix(k, prefix) {
				fmt.Fprintf(&contents, `<Contents><Key>%s</Key><Size>%d</Size><ETag>%s</ETag><LastModified>2026-11-25T00:00:00.000Z</LastModified></Contents>`, k, len(body), etag(body))
			}
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<ListBucketResult><Name>bucket</Name><Prefix>%s</Prefix><IsTruncated>false</IsTruncated>%s</ListBucketResult>`, prefix, contents.String())
		return
	}
	switch r.Method {
	case http.MethodPut:
		f.puts[key]++
		body, _ := io.ReadAll(r.Body)
		if sum := r.Header.Get("x-amz-checksum-sha256"); sum != "" {
			digest := sha256.Sum256(body)
			if sum != base64.StdEncoding.EncodeToString(digest[:]) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `<Error><Code>BadDigest</Code></Error>`)
				return
			}
			f.checksummed[key] = true
		}
		if foreign, ok := f.race[key]; ok {
			delete(f.race, key)
			f.objects[key] = foreign
		}
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
		if r.Method == http.MethodHead && f.headFail[key] {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
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

	const batch = "recovery-best14"
	seoul, _ := time.LoadLocation("Asia/Seoul")
	hourIn := func(b string, recording int64, date string, h int) collation.HourWork {
		t.Helper()
		id, err := collation.HourIDFor(b, recording, date, h)
		if err != nil {
			t.Fatal(err)
		}
		return collation.HourWork{BatchID: b, HourID: id, RecordingID: recording, LocalDate: date, DeliveryHour: h}
	}
	hour := func(recording int64, date string, h int) collation.HourWork { return hourIn(batch, recording, date, h) }
	partIn := func(b, name string) (string, []byte, string) {
		body := []byte("stitched " + name)
		sum := sha256.Sum256(body)
		sha := hex.EncodeToString(sum[:])
		return collation.ObjectKey(b, sha), body, sha
	}
	part := func(name string) (string, []byte, string) { return partIn(batch, name) }
	start := time.Date(2026, 11, 1, 8, 30, 0, 0, seoul)
	newPart := func(name string, start time.Time) collation.Output {
		key, body, sha := part(name)
		return collation.Output{Part: 1, Parts: 1, ObjectKey: key, NASRelativePath: "scoped/" + name + ".mp4", SizeBytes: int64(len(body)), SHA256: sha,
			StartUTC: start.UTC(), EndUTC: start.Add(time.Minute).UTC(), SourceClipIDs: []int64{902}, R2VerifiedAt: time.Now()}
	}
	register := func(w collation.HourWork, out collation.Output) error {
		m := collation.HourManifest{Generation: collation.Generation, Status: collation.StatusCollated, BatchID: w.BatchID, HourID: w.HourID,
			RecordingID: w.RecordingID, LocalDate: w.LocalDate, DeliveryHour: w.DeliveryHour, Clips: []collation.ClipDisposition{}, Outputs: []collation.Output{out}}
		body, _ := json.Marshal(m)
		return insertCollationHour(ctx, pool, w, m, collation.ManifestKey(w.BatchID, w.HourID), body)
	}

	// Before the fence, an earlier writer catalogued an hour in recovery-stale.
	stale := newPart("stale", start)
	stale.ObjectKey, _, _ = partIn("recovery-stale", "stale")
	if err := register(hourIn("recovery-stale", 32, "2026-11-01", 1), stale); err != nil {
		t.Fatal(err)
	}
	// Registered hours in an existing batch, as the R2 lane left them: the
	// 2026-11-05 part is gone from R2, and a 2026-11-06 part still has a
	// retirement pending.
	const oldBatch = "goodplus-20260821"
	restoreOut := newPart("restore", time.Date(2026, 11, 5, 8, 30, 0, 0, seoul))
	restoreOut.ObjectKey, _, _ = partIn(oldBatch, "restore")
	restoreHour := hourIn(oldBatch, 32, "2026-11-05", 1)
	if err := register(restoreHour, restoreOut); err != nil {
		t.Fatal(err)
	}
	pendingOut := newPart("pending", time.Date(2026, 11, 6, 8, 30, 0, 0, seoul))
	pendingOut.ObjectKey, _, _ = partIn(oldBatch, "pending")
	if err := register(hourIn(oldBatch, 32, "2026-11-06", 1), pendingOut); err != nil {
		t.Fatal(err)
	}
	unscopedOut := newPart("unscoped restore", time.Date(2026, 11, 7, 8, 30, 0, 0, seoul))
	unscopedOut.ObjectKey, _, _ = partIn(oldBatch, "unscoped restore")
	if err := register(hourIn(oldBatch, 32, "2026-11-07", 1), unscopedOut); err != nil {
		t.Fatal(err)
	}
	// A registered row whose columns disagree with its own hour_id: its hour_id
	// names scoped recording 32, its recording_id is 31.
	driftOut := newPart("drift", time.Date(2026, 11, 5, 9, 30, 0, 0, seoul))
	driftOut.ObjectKey, _, _ = partIn(oldBatch, "drift")
	driftHour := hourIn(oldBatch, 32, "2026-11-05", 2)
	driftHour.RecordingID = 31
	if err := register(driftHour, driftOut); err != nil {
		t.Fatal(err)
	}
	exec(`INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
		SELECT 'collated',id,'bucket',object_key,size_bytes,sha256,repeat('b',64) FROM recording_collation_outputs WHERE object_key=$1`, pendingOut.ObjectKey)
	matureR2WriterFence(t, pool, "bucket")
	if err := r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA); err != nil {
		t.Fatal(err)
	}

	// The controller approves recording 31's Seoul days in a fresh recovery-
	// batch, and recording 32's 2026-11-05 as a restore in its own batch. A
	// recovery- batch any earlier row names is never fresh, and no scope row
	// lands over a pending retirement.
	scopeOf := func(b string, recording int64, date string) error {
		_, err := pool.Exec(ctx, `INSERT INTO collation_v2_recovery_scope(batch_id,recording_id,local_date,reason) VALUES($1,$2,$3,'best 14 recovery')`, b, recording, date)
		return err
	}
	scope := func(b, date string) error { return scopeOf(b, 31, date) }
	refused("stale batch", "not fresh", scope("recovery-stale", "2026-11-01"))
	refused("scope over a pending retirement", "pending R2 retirement", scopeOf(oldBatch, 32, "2026-11-06"))
	for _, err := range []error{scope(batch, "2026-11-01"), scope(batch, "2026-11-03"), scopeOf(oldBatch, 32, "2026-11-05")} {
		if err != nil {
			t.Fatal(err)
		}
	}
	// Scope rows are revoked, never deleted or rewritten: a hold outlives its admission.
	for name, sql := range map[string]string{
		"scope delete":   `DELETE FROM collation_v2_recovery_scope`,
		"scope truncate": `TRUNCATE collation_v2_recovery_scope`,
		"scope rewrite":  `UPDATE collation_v2_recovery_scope SET local_date='2026-11-02' WHERE local_date='2026-11-01'`,
	} {
		_, err := pool.Exec(ctx, sql)
		refused(name, "collation v2 recovery scope", err)
	}

	bucket := &recoveryBucket{objects: map[string][]byte{}, fail: map[string]bool{}, headFail: map[string]bool{},
		race: map[string][]byte{}, puts: map[string]int{}, checksummed: map[string]bool{}}
	server := httptest.NewServer(bucket)
	defer server.Close()
	client, err := r2.New(ctx, r2.Config{WriterDB: pool, AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	store := collation.R2Store{Client: client}
	inBucket := func(key string) bool {
		bucket.mu.Lock()
		defer bucket.mu.Unlock()
		_, ok := bucket.objects[key]
		return ok
	}
	publish := func(s collation.Store, key string, body []byte, sha string) error {
		path := t.TempDir() + "/part.mp4"
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		return s.PublishVerified(ctx, key, "video/mp4", path, int64(len(body)), sha)
	}
	intents := func(key string) (open, done int) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE completed_at IS NULL),count(*) FILTER (WHERE completed_at IS NOT NULL)
			FROM r2_retirement_writer_operations WHERE object_key=$1`, key).Scan(&open, &done); err != nil {
			t.Fatal(err)
		}
		return open, done
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
	manifest := collation.HourManifest{Generation: collation.Generation, Status: collation.StatusCollated, BatchID: batch, HourID: scoped.HourID,
		RecordingID: 31, LocalDate: "2026-11-01", DeliveryHour: 1, Clips: []collation.ClipDisposition{},
		Outputs: []collation.Output{{Part: 1, Parts: 1, ObjectKey: partKey, NASRelativePath: "scoped/hour-01.mp4", SizeBytes: int64(len(partBody)),
			SHA256: partSHA, StartUTC: start.UTC(), EndUTC: start.Add(30 * time.Minute).UTC(), SourceClipIDs: []int64{901}, R2VerifiedAt: time.Now()}}}
	manifestBody, _ := json.Marshal(manifest)
	manifestKey := collation.ManifestKey(batch, scoped.HourID)
	if err := rs.PutManifestIfAbsent(ctx, manifestKey, manifestBody); err != nil {
		t.Fatal("scoped manifest refused:", err)
	}
	for _, key := range []string{partKey, manifestKey} {
		if open, done := intents(key); open != 0 || done != 1 {
			t.Fatalf("%s: recovery intent not durable and completed: open=%d done=%d", key, open, done)
		}
		bucket.mu.Lock()
		bound := bucket.checksummed[key]
		bucket.mu.Unlock()
		if !bound {
			t.Fatalf("%s: recovery PUT not bound to its admitted sha256", key)
		}
	}

	// plan --recovery-batch LISTs an unscoped batch's prefix: any object there
	// means the batch is not fresh. A scoped batch's objects are its own.
	bucket.mu.Lock()
	bucket.objects["joined/recovery-used/objects/x.mp4"] = []byte("older")
	bucket.mu.Unlock()
	refused("unscoped batch with R2 objects", "not fresh", requireFreshRecoveryBatch(ctx, pool, store, "recovery-used"))
	for _, b := range []string{batch, "recovery-unused"} {
		if err := requireFreshRecoveryBatch(ctx, pool, store, b); err != nil {
			t.Fatalf("%s: %v", b, err)
		}
	}

	// Every path that leaves no possible write behind completes its intent at
	// once, so nothing blocks deletes for good.
	noPut := func(name string, w collation.HourWork, key string, body []byte, sha string, want string) {
		t.Helper()
		_, _, err := client.ForCollationRecovery(w.HourID).PutReaderIfAbsentVerified(ctx, key, "video/mp4", bytes.NewReader(body), int64(len(body)), sha)
		refused(name, want, err)
		if open, done := intents(key); open != 0 || done != 1 {
			t.Fatalf("%s: intent open=%d done=%d", name, open, done)
		}
	}
	// A body that changed after admission (same length) never lands: R2
	// refuses it against the admitted sha256 before committing anything.
	tornKey, tornBody, tornSHA := part("torn")
	torn := append([]byte(nil), tornBody...)
	torn[0] ^= 0xff
	noPut("changed body", hour(31, "2026-11-01", 5), tornKey, torn, tornSHA, "BadDigest")
	if inBucket(tornKey) {
		t.Fatal("a body other than the admitted bytes landed")
	}
	// Foreign bytes already at a new hour's manifest key: no PUT is sent.
	foreignManifest := collation.ManifestKey(batch, hour(31, "2026-11-01", 6).HourID)
	bucket.mu.Lock()
	bucket.objects[foreignManifest] = []byte(`{"someone":"else"}`)
	bucket.mu.Unlock()
	noPut("foreign bytes at the key", hour(31, "2026-11-01", 6), foreignManifest, manifestBody, sha256Hex(manifestBody), "content mismatch")
	// HEAD fails: no PUT is sent.
	headKey, headBody, headSHA := part("head fails")
	bucket.mu.Lock()
	bucket.headFail[headKey] = true
	bucket.mu.Unlock()
	noPut("HEAD failure", hour(31, "2026-11-01", 7), headKey, headBody, headSHA, "collation recovery HEAD")
	// Another run's bytes land between our HEAD and PUT: our PUT gets 412.
	raceKey := collation.ManifestKey(batch, hour(31, "2026-11-01", 8).HourID)
	bucket.mu.Lock()
	bucket.race[raceKey] = []byte(`{"created_at":"another run"}`)
	bucket.mu.Unlock()
	noPut("412 over another run's bytes", hour(31, "2026-11-01", 8), raceKey, manifestBody, sha256Hex(manifestBody), "content mismatch")
	bucket.mu.Lock()
	sentNoPut := bucket.puts[foreignManifest] + bucket.puts[headKey]
	bucket.mu.Unlock()
	if sentNoPut != 0 {
		t.Fatal("a PUT was sent over a present key or after a failed HEAD")
	}
	// Every key a scope row covers is an owner keep hold, which every R2
	// retirement ledger refuses; keys outside it are not.
	var held, unheld bool
	if err := pool.QueryRow(ctx, `SELECT r2_owner_keep_held('bucket',$1) AND r2_owner_keep_held('bucket',$2) AND r2_owner_keep_held('bucket',$3),
		r2_owner_keep_held('bucket',$4)`, partKey, manifestKey, restoreOut.ObjectKey, unscopedOut.ObjectKey).Scan(&held, &unheld); err != nil || !held || unheld {
		t.Fatalf("scope holds: held=%v unheld=%v %v", held, unheld, err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
		SELECT 'collated',id,'bucket',object_key,size_bytes,sha256,repeat('b',64) FROM recording_collation_outputs WHERE object_key=$1`, restoreOut.ObjectKey)
	refused("retirement of a scoped key", "OWNER KEEP HOLD", err)

	// Restore: a registered hour's catalogued bytes go back at their own key,
	// create-only, and only while absent. It writes no catalog row.
	restoreBody := []byte("stitched restore")
	restorer := client.ForCollationRecovery(restoreHour.HourID)
	putRestore := func(c *r2.Client, key string, body []byte) error {
		_, _, err := c.PutReaderIfAbsentVerified(ctx, key, "video/mp4", bytes.NewReader(body), int64(len(body)), sha256Hex(body))
		return err
	}
	refused("restore with other bytes", "admission refused", putRestore(restorer, restoreOut.ObjectKey, []byte("not the catalogued bytes")))
	otherNew, _, _ := partIn(oldBatch, "invented")
	refused("restore of a key the hour never catalogued", "admission refused", putRestore(restorer, otherNew, []byte("stitched invented")))
	refused("restore out of scope", "admission refused",
		putRestore(client.ForCollationRecovery(hourIn(oldBatch, 32, "2026-11-07", 1).HourID), unscopedOut.ObjectKey, []byte("stitched unscoped restore")))
	refused("restore of a row whose columns disagree with its hour_id", "admission refused",
		putRestore(client.ForCollationRecovery(driftHour.HourID), driftOut.ObjectKey, []byte("stitched drift")))
	otherBucket, err := r2.New(ctx, r2.Config{WriterDB: pool, AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "other", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	refused("restore into a bucket other than the fenced one", "admission refused",
		putRestore(otherBucket.ForCollationRecovery(restoreHour.HourID), restoreOut.ObjectKey, restoreBody))
	if err := putRestore(restorer, restoreOut.ObjectKey, restoreBody); err != nil {
		t.Fatal("restore of an absent catalogued key refused:", err)
	}
	// A present key gets no PUT at all: its bytes are read back, never written.
	if err := putRestore(restorer, restoreOut.ObjectKey, restoreBody); err != nil {
		t.Fatal("present catalogued key not verified:", err)
	}
	bucket.mu.Lock()
	restorePuts := bucket.puts[restoreOut.ObjectKey]
	bucket.mu.Unlock()
	if restorePuts != 1 {
		t.Fatalf("restore sent %d PUTs, want exactly 1 for the absent key", restorePuts)
	}
	if open, done := intents(restoreOut.ObjectKey); open != 0 || done != 2 {
		t.Fatalf("restore intents: open=%d done=%d", open, done)
	}
	var restoreRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_collation_hours WHERE batch_id=$1`, oldBatch).Scan(&restoreRows); err != nil || restoreRows != 4 {
		t.Fatalf("restore changed the catalog: %d %v", restoreRows, err)
	}
	// A new recovery hour never shadows a recording, date and hour that another
	// batch has already registered.
	if err := scopeOf(batch, 32, "2026-11-05"); err != nil {
		t.Fatal(err)
	}
	shadowKey, shadowBody, _ := part("shadow")
	refused("new hour over a registered generation-2 hour", "admission refused",
		putRestore(client.ForCollationRecovery(hour(32, "2026-11-05", 1).HourID), shadowKey, shadowBody))
	refused("new hour in a restore-only batch", "fenced", register(hourIn(oldBatch, 32, "2026-11-05", 2), newPart("restore new", time.Date(2026, 11, 5, 9, 30, 0, 0, seoul))))

	// The database itself refuses every create-only PUT outside the hour's own
	// layout: no intent is admitted and nothing reaches R2.
	bound := client.ForCollationRecovery(scoped.HourID)
	otherBatchKey, otherBatchBody, otherBatchSHA := partIn("goodplus-20260821", "adopt")
	_, misnamedBody, misnamedSHA := part("misnamed")
	misnamedKey, _, _ := part("not its bytes")
	for name, put := range map[string]struct {
		key  string
		body []byte
		sha  string
	}{
		"managed key":         {"managed/acct-47/x.mp4", otherBatchBody, otherBatchSHA},
		"other batch":         {otherBatchKey, otherBatchBody, otherBatchSHA},
		"other hour manifest": {collation.ManifestKey(batch, hour(31, "2026-11-01", 2).HourID), manifestBody, sha256Hex(manifestBody)},
		"objects key not sha": {misnamedKey, misnamedBody, misnamedSHA},
		"stale batch key":     {stale.ObjectKey, otherBatchBody, otherBatchSHA},
	} {
		_, _, err := bound.PutReaderIfAbsentVerified(ctx, put.key, "video/mp4", bytes.NewReader(put.body), int64(len(put.body)), put.sha)
		refused(name, "admission refused", err)
		if open, done := intents(put.key); open+done != 0 || inBucket(put.key) {
			t.Fatalf("%s: refused write left an intent or object", name)
		}
	}
	// A new recovery hour never reaches a restored key in another batch.
	refused("restore key bound to a new hour", "admission refused", putRestore(bound, restoreOut.ObjectKey, restoreBody))
	// The unbound client, unscoped hours and every generic write stay fenced.
	refused("plain client", "admission", store.PutManifestIfAbsent(ctx, collation.ManifestKey(batch, hour(31, "2026-11-01", 2).HourID), manifestBody))
	spareKey, spareBody, _ := part("spare")
	_, err = bound.PutBytes(ctx, spareKey, "video/mp4", spareBody)
	refused("unconditional PUT by a recovery client", "create-only", err)
	_, err = bound.PresignPut(ctx, spareKey, "video/mp4", time.Hour)
	refused("capability by a recovery client", "create-only", err)
	otherKey, otherBody, otherSHA := part("unscoped")
	refused("unscoped recording", "admission", publish(store.CollationRecoveryStore(hour(32, "2026-11-01", 1)), otherKey, otherBody, otherSHA))
	refused("unscoped date", "admission", publish(store.CollationRecoveryStore(hour(31, "2026-11-02", 1)), otherKey, otherBody, otherSHA))
	if inBucket(otherKey) || inBucket(spareKey) {
		t.Fatal("refused write reached R2")
	}

	// Wire time checks the scope again: a day withdrawn after its intent
	// committed sends no PUT, and that intent completes at once.
	exec(`CREATE FUNCTION test_withdraw_scope() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 UPDATE collation_v2_recovery_scope SET revoked_at=now() WHERE local_date='2026-11-03'; RETURN NULL; END $$;
CREATE TRIGGER test_withdraw_scope AFTER INSERT ON r2_retirement_writer_operations FOR EACH ROW EXECUTE FUNCTION test_withdraw_scope()`)
	withdrawnKey, withdrawnBody, withdrawnSHA := part("withdrawn")
	_, _, err = client.ForCollationRecovery(hour(31, "2026-11-03", 1).HourID).PutReaderIfAbsentVerified(ctx, withdrawnKey, "video/mp4",
		bytes.NewReader(withdrawnBody), int64(len(withdrawnBody)), withdrawnSHA)
	refused("withdrawn scope at wire time", "no PUT sent", err)
	exec(`DROP TRIGGER test_withdraw_scope ON r2_retirement_writer_operations; DROP FUNCTION test_withdraw_scope()`)
	if open, done := intents(withdrawnKey); open != 0 || done != 1 || inBucket(withdrawnKey) {
		t.Fatalf("withdrawn write: open=%d done=%d sent=%v", open, done, inBucket(withdrawnKey))
	}

	// A verified PUT whose own intent does not complete fails fast.
	stuckKey, stuckBody, stuckSHA := part("stuck")
	exec(`CREATE FUNCTION test_skip_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TRIGGER test_skip_completion BEFORE UPDATE ON r2_retirement_writer_operations FOR EACH ROW WHEN (OLD.object_key='` + stuckKey + `') EXECUTE FUNCTION test_skip_completion()`)
	_, _, err = client.ForCollationRecovery(hour(31, "2026-11-01", 9).HourID).PutReaderIfAbsentVerified(ctx, stuckKey, "video/mp4",
		bytes.NewReader(stuckBody), int64(len(stuckBody)), stuckSHA)
	refused("verified PUT whose intent did not complete", "did not complete", err)
	exec(`DROP TRIGGER test_skip_completion ON r2_retirement_writer_operations; DROP FUNCTION test_skip_completion()`)
	if report, err := client.SettleCollationRecoveryIntents(ctx, true); err != nil || report.Settled != 1 || len(report.Verified) != 1 {
		t.Fatalf("settle the stuck intent: %+v %v", report, err)
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
	// Only a later PUT that reads the key's exact bytes back settles the lost
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
	// Once catalogued, the hour's parts are restores: present, they are read
	// back and get no second PUT. Restores cover outputs only: its manifest is
	// never PUT again.
	if err := publish(rs, partKey, partBody, partSHA); err != nil {
		t.Fatal(err)
	}
	refused("registered manifest re-PUT", "admission", rs.PutManifestIfAbsent(ctx, manifestKey, manifestBody))
	bucket.mu.Lock()
	partPuts, manifestPuts := bucket.puts[partKey], bucket.puts[manifestKey]
	bucket.mu.Unlock()
	if partPuts != 1 || manifestPuts != 1 {
		t.Fatalf("catalogued keys rewritten: part=%d manifest=%d PUTs", partPuts, manifestPuts)
	}

	refused("unscoped recording register", "fenced", register(hour(32, "2026-11-01", 1), newPart("r32", start)))
	refused("unscoped date register", "fenced", register(hour(31, "2026-11-02", 1), newPart("d2", start.AddDate(0, 0, 1))))
	refused("stale batch register", "fenced", register(hourIn("recovery-stale", 31, "2026-11-01", 2), newPart("stale 2", start.Add(time.Hour))))
	reused := newPart("hour 2", start.Add(time.Hour))
	reused.ObjectKey, reused.SHA256, reused.SizeBytes = partKey, partSHA, int64(len(partBody))
	refused("existing object_key", "fenced", register(hour(31, "2026-11-01", 2), reused))
	misnamed := newPart("hour 2 misnamed", start.Add(time.Hour))
	misnamed.ObjectKey = misnamedKey
	refused("output key not its sha256", "fenced", register(hour(31, "2026-11-01", 2), misnamed))
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
		"output update":   `UPDATE recording_collation_outputs SET r2_verified_at=now() WHERE object_key LIKE 'joined/recovery-best14/%'`,
		"output delete":   `DELETE FROM recording_collation_outputs WHERE object_key LIKE 'joined/recovery-best14/%'`,
		"output truncate": `TRUNCATE recording_collation_outputs CASCADE`,
	} {
		_, err := pool.Exec(ctx, sql)
		refused(name, "collation v2 recovery", err)
	}
	var hours, outputs int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM recording_collation_hours WHERE batch_id=$1),
		(SELECT count(*) FROM recording_collation_outputs WHERE object_key LIKE 'joined/recovery-best14/%')`, batch).Scan(&hours, &outputs); err != nil || hours != 2 || outputs != 2 {
		t.Fatalf("catalog changed: hours=%d outputs=%d %v", hours, outputs, err)
	}

	// settle-recovery: crashes left four unfinished intents. One key holds its
	// exact bytes, one is missing (its PUT was lost and its day then revoked),
	// one holds foreign bytes, and one holds its bytes but its PUT is still in
	// flight (its wire transaction share-locks the row). Every intent whose PUT
	// is over settles, whatever its key holds: each key is held for good.
	seed := func(name string, held []byte) (string, int64) {
		key, body, sha := part(name)
		if held != nil {
			bucket.mu.Lock()
			bucket.objects[key] = held
			bucket.mu.Unlock()
		} else if name != "missing" {
			bucket.mu.Lock()
			bucket.objects[key] = body
			bucket.mu.Unlock()
		}
		injectR2WriterState(t, pool, fmt.Sprintf(`INSERT INTO r2_retirement_writer_operations(bucket,collation_recovery_hour_id,object_key,size_bytes,sha256)
			VALUES('bucket','%s','%s',%d,'%s')`, hour(31, "2026-11-03", 4).HourID, key, len(body), sha))
		var id int64
		if err := pool.QueryRow(ctx, `SELECT id FROM r2_retirement_writer_operations WHERE object_key=$1`, key).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return key, id
	}
	crashedKey, _ := seed("crashed", nil)
	missingKey, _ := seed("missing", nil)
	foreignKey, _ := seed("foreign", []byte("someone else's bytes"))
	liveKey, liveID := seed("in flight", nil)
	wire, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer wire.Rollback(context.Background())
	if _, err := wire.Exec(ctx, `SELECT 1 FROM r2_retirement_writer_operations WHERE id=$1 FOR SHARE`, liveID); err != nil {
		t.Fatal(err)
	}
	report, err := client.SettleCollationRecoveryIntents(ctx, false)
	if err != nil || report.Keys != 4 || report.Settled != 0 || len(report.Verified) != 2 ||
		len(report.Absent) != 1 || report.Absent[0] != missingKey || len(report.Foreign) != 1 || report.Foreign[0] != foreignKey {
		t.Fatalf("dry run: %+v %v", report, err)
	}
	refused("delete gate before settling", "have not drained", r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA))
	if report, err = client.SettleCollationRecoveryIntents(ctx, true); err != nil || report.Settled != 3 {
		t.Fatalf("apply: %+v %v", report, err)
	}
	for _, key := range []string{crashedKey, missingKey, foreignKey} {
		if open, _ := intents(key); open != 0 {
			t.Fatalf("%s: finished intent not settled", key)
		}
	}
	if open, _ := intents(liveKey); open != 1 {
		t.Fatal("settlement completed a PUT still in flight")
	}
	refused("delete gate behind a PUT in flight", "have not drained", r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA))
	_ = wire.Rollback(ctx)
	if report, err = client.SettleCollationRecoveryIntents(ctx, true); err != nil || report.Settled != 1 {
		t.Fatalf("apply after the PUT ended: %+v %v", report, err)
	}
	if err := r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA); err != nil {
		t.Fatal("settled intents still block deletes:", err)
	}
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
