package r2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// These witnesses run the production retirement end to end: a genuine
// certified hour, the real client, the real one-shot intent on its own
// connection, and a loopback provider that answers the exact multipart
// inventory, HEAD and DELETE the production wire sends. Nothing takes the
// retirement's locks by hand and nothing writes an absence stamp: every stamp
// below is written by RetireStitchStage itself. Only the HTTP boundary and the
// pre-existing certified rows are fixtures.

// stageLiveEndpointFence is the shared pair's first lock (1103): the one every
// cleanup and owner-keep admission takes shared, and that a new owner keep
// prefix takes exclusively.
const stageLiveEndpointFence int64 = 7343226011103

// liveStageProvider is one stage generation at a loopback provider. It answers
// the exact-key multipart inventory, the generation HEAD and exactly one
// DELETE, and can hold that DELETE so a race is observed rather than raced for.
type liveStageProvider struct {
	key, etag, sha string
	size           int64
	modified       time.Time

	deletes                      atomic.Int32
	postDeleteEmptyLists         atomic.Int32
	postDeleteNotFoundAfterEmpty atomic.Int32
	// liveInventoryAfterDelete keeps a live upload listed for this exact key
	// after the DELETE, so publication must refuse although the object is gone.
	liveInventoryAfterDelete atomic.Bool
	deleteMarkerAfterDelete  atomic.Bool
	holdDelete               chan struct{}
	entered                  sync.Once
	deleteEntered            chan struct{}
	deleted                  atomic.Bool
	srv                      *httptest.Server
}

func newLiveStageProvider(t *testing.T, key, sha string, size int64) *liveStageProvider {
	t.Helper()
	p := &liveStageProvider{key: key, etag: "stage-etag", sha: sha, size: size,
		modified: time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC), deleteEntered: make(chan struct{})}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.Method != http.MethodGet && r.URL.Path != "/stoarama/"+p.key {
			t.Errorf("%s asked for %q, want %q", r.Method, r.URL.Path, "/stoarama/"+p.key)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodGet:
			if prefix := r.URL.Query().Get("prefix"); prefix != p.key {
				t.Errorf("multipart inventory asked for %q, want %q", prefix, p.key)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if p.deleted.Load() {
				if p.liveInventoryAfterDelete.Load() {
					fmt.Fprintf(w, "<ListMultipartUploadsResult><Bucket>stoarama</Bucket><Prefix>%s</Prefix><IsTruncated>false</IsTruncated><Upload><Key>%s</Key><UploadId>still-live</UploadId></Upload></ListMultipartUploadsResult>", p.key, p.key)
					return
				}
				p.postDeleteEmptyLists.Add(1)
			}
			fmt.Fprintf(w, "<ListMultipartUploadsResult><Bucket>stoarama</Bucket><Prefix>%s</Prefix><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>", p.key)
		case http.MethodHead:
			if p.deleted.Load() {
				if p.postDeleteEmptyLists.Load() > 0 {
					p.postDeleteNotFoundAfterEmpty.Add(1)
				}
				if p.deleteMarkerAfterDelete.Load() {
					w.Header().Set("X-Amz-Delete-Marker", "true")
				}
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", `"`+p.etag+`"`)
			w.Header().Set("Last-Modified", p.modified.Format(http.TimeFormat))
			w.Header().Set("Content-Length", strconv.FormatInt(p.size, 10))
			w.Header().Set("X-Amz-Meta-Sha256", p.sha)
		case http.MethodDelete:
			p.deletes.Add(1)
			if p.holdDelete != nil {
				p.entered.Do(func() { close(p.deleteEntered) })
				select {
				case <-p.holdDelete:
				case <-r.Context().Done():
					return
				}
			}
			p.deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("stage provider saw an unexpected %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(func() {
		p.releaseDelete()
		p.srv.Close()
	})
	return p
}

func (p *liveStageProvider) releaseDelete() {
	if p.holdDelete != nil {
		select {
		case <-p.holdDelete:
		default:
			close(p.holdDelete)
		}
	}
}

func sha256Hex(v string) string {
	sum := sha256.Sum256([]byte(strings.TrimRight(v, "/")))
	return hex.EncodeToString(sum[:])
}

// liveStageWire is the production wire at the HTTP boundary: one exact
// no-redirect HEAD and one exact DELETE, with the retirement's own recheck run
// before the first request byte.
type liveStageWire struct{ endpoint string }

func (w liveStageWire) EndpointSHA256() string { return sha256Hex(w.endpoint) }

func (w liveStageWire) client() *http.Client {
	return &http.Client{Transport: http.DefaultTransport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}

func (w liveStageWire) ExactHead(ctx context.Context, target StitchStageTarget, deadline time.Time) (ObjectHead, error) {
	if target.EndpointSHA256 != w.EndpointSHA256() {
		return ObjectHead{}, fmt.Errorf("wire endpoint differs: %q", target.EndpointSHA256)
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, w.endpoint+"/"+target.Bucket+"/"+target.Key, nil)
	if err != nil {
		return ObjectHead{}, err
	}
	resp, err := w.client().Do(req)
	if err != nil {
		return ObjectHead{}, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ObjectHead{}, fmt.Errorf("head status %d", resp.StatusCode)
	}
	modified, _ := http.ParseTime(resp.Header.Get("Last-Modified"))
	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	return ObjectHead{LastModified: modified, ETag: strings.Trim(resp.Header.Get("ETag"), `"`),
		SizeBytes: size, SHA256Metadata: resp.Header.Get("X-Amz-Meta-Sha256")}, nil
}

func (w liveStageWire) DeleteOnce(ctx context.Context, target StitchStageTarget, deadline time.Time, recheck func(context.Context) error) error {
	if target.EndpointSHA256 != w.EndpointSHA256() {
		return fmt.Errorf("wire endpoint differs: %q", target.EndpointSHA256)
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := recheck(ctx); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, w.endpoint+"/"+target.Bucket+"/"+target.Key+"?x-id=DeleteObject", nil)
	if err != nil {
		return err
	}
	resp, err := w.client().Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("delete status %d", resp.StatusCode)
	}
	return nil
}

// liveStage is one certified hour with exactly one retirable staged output, its
// exact NAS custody receipt and the matured writer fence the evidence binds.
type liveStage struct {
	fixture     *stageFixture
	provider    *liveStageProvider
	objectID    int64
	hourID      int64
	attempt     int
	endpoint    string
	objectKey   string
	sha         string
	evidence    StitchStageEvidence
	picks       StitchStagePicks
	certificate string
}

// certifyLiveStage seeds what certification leaves behind for one hour, then
// builds the production client and wire for it. The rows below are data: they
// are written with replica role so the append-only evidence guards do not have
// to be replayed, and every behaviour under test is the production path.
func certifyLiveStage(t *testing.T, name string) *liveStage {
	t.Helper()
	f := newStageFixtureBase(t, name)
	const hourID, objectID, attempt, size = int64(500), int64(601), 2, int64(100)
	sha := fmt.Sprintf("%064d", objectID)
	f.seedHour(t, hourID)
	const clipID, sourceKey, sourceSize = int64(701), "managed/acct-47/stitch-raw.mp4", int64(200)
	sourceSHA := fmt.Sprintf("%064d", clipID)
	f.insert(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,
 size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path)
 SELECT $2,recording_id,1,'https://storage.example.test','stoarama',$3,$4,'source-etag',$5,
 hour_at,hour_at,hour_at+interval '1 minute','stitch/source.mp4'
 FROM nas_stitch_r2_hours WHERE id=$1`, hourID, clipID, sourceKey, sourceSize, sourceSHA)
	f.insert(`INSERT INTO nas_stitch_r2_input_holds(hour_id,bucket,object_key,catalog_clip_id,sha256,size_bytes,etag,released_at)
 VALUES($1,'stoarama',$2,$3,$4,$5,'source-etag',clock_timestamp())`, hourID, sourceKey, clipID, sourceSHA, sourceSize)
	f.insert(`UPDATE nas_stitch_r2_hours SET state='certified',input_bytes=$4,
 plan=jsonb_set(plan,'{clips}',jsonb_build_array(jsonb_build_object(
  'source_id',$2::bigint,'bucket','stoarama','object_key',$3::text,'size_bytes',$4::bigint,'sha256',$5::text,'etag','source-etag')))
 WHERE id=$1`, hourID, clipID, sourceKey, sourceSize, sourceSHA)

	// The generation is fixed here, so the generated key is known before the
	// provider exists and the row can be seeded once with that provider's own
	// endpoint. The stage identity guard forbids changing it afterwards.
	const generation = "f5238abd-6063-4441-a712-a9647d10b986"
	objectKey := stitchStageKeyPrefix + generation + "/" + sha + ".mp4"
	provider := newLiveStageProvider(t, objectKey, sha, size)
	endpoint := sha256Hex(provider.srv.URL)
	f.insert(`INSERT INTO nas_stitch_r2_stage_objects(id,hour_id,attempt,api_key_id,bucket,endpoint_sha256,generation,
 relative_path,sha256,size_bytes,state,upload_id,etag,last_modified,grants_drain_at)
 OVERRIDING SYSTEM VALUE VALUES($1,$2,$3,47,'stoarama',$4,$5,'live/run-000.mp4',$6,$7,'staged','up','stage-etag',
 clock_timestamp()-interval '1 hour',clock_timestamp()-interval '1 second')`,
		objectID, hourID, attempt, endpoint, generation, sha, size)
	var storedKey string
	if err := f.pool.QueryRow(f.ctx, `SELECT object_key FROM nas_stitch_r2_stage_objects WHERE id=$1`, objectID).Scan(&storedKey); err != nil {
		t.Fatal(err)
	}
	if storedKey != objectKey {
		t.Fatalf("staged key %q, want the deterministic %q", storedKey, objectKey)
	}
	// The provider must serve exactly the generation the row recorded. Truncate
	// like the provider's HTTP date, which is whole seconds.
	var modified time.Time
	if err := f.pool.QueryRow(f.ctx, `SELECT last_modified FROM nas_stitch_r2_stage_objects WHERE id=$1`, objectID).Scan(&modified); err != nil {
		t.Fatal(err)
	}
	provider.modified = modified.UTC().Truncate(time.Second)
	f.insert(`UPDATE nas_stitch_r2_stage_objects SET last_modified=$2 WHERE id=$1`, objectID, provider.modified)

	// The exact NAS custody receipt for that generation and the certificate that
	// resolved the hour. seedHour's digests are the ones the certificate names.
	planSHA, manifestSHA := strings.Repeat("a", 64), strings.Repeat("b", 64)
	receipt := fmt.Sprintf(`{"relative_path":"live/run-000.mp4","size_bytes":%d,"sha256":%q,"fsynced":true,
 "identity":[%d,1,2,3,0]}`, size, sha, size)
	item := fmt.Sprintf(`{"kind":"stitch_output","source_id":%d,"bucket":"stoarama","object_key":%q,
 "relative_path":"live/run-000.mp4","size_bytes":%d,"sha256":%q}`, objectID, objectKey, size, sha)
	f.insert(`INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,
 completion,committed_bytes,etag,stitch_receipt,stitch_result_sha256,verified_at)
 VALUES(13,'stitch_output',$1,$2,$3,$4,'verified','downloaded',$5,'stage-etag',$6,repeat('a',64),clock_timestamp())`,
		objectID, manifestSHA, strings.Repeat("c", 64), item, size, receipt)
	f.insert(`INSERT INTO nas_stitch_r2_certificates(hour_id,connection_id,attempt,plan_sha256,manifest_sha256,receipt)
 VALUES($1,13,$2,$3,$4,'{"objects":[]}'::jsonb)`, hourID, attempt, planSHA, manifestSHA)

	// The provider's own endpoint is the endpoint the whole retirement binds, so
	// the committed row, the client, the evidence and the wire all agree.
	f.insert(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256,drain_not_before)
 VALUES('stoarama',$1,clock_timestamp()-interval '1 hour')`, endpoint)
	var activated, drain time.Time
	if err := f.pool.QueryRow(f.ctx, `SELECT activated_at,drain_not_before FROM r2_retirement_writer_fences
 WHERE bucket='stoarama' AND endpoint_sha256=$1`, endpoint).Scan(&activated, &drain); err != nil {
		t.Fatal(err)
	}
	picksSHA := fmt.Sprintf("%064d", 6161)
	evidence := StitchStageEvidence{Version: 1, Bucket: "stoarama", EndpointSHA256: endpoint,
		ObjectID: objectID, ObjectKey: objectKey, SHA256: sha, SizeBytes: size,
		HourID: hourID, Attempt: attempt, PlanSHA256: planSHA, ManifestSHA256: manifestSHA,
		FenceActivatedAt: activated, DrainNotBefore: drain,
		DeployedWriterCoverage: "fixture: this disposable schema", ExternalWriterRace: StitchStageExternalWriterRace,
		PicksSHA256: picksSHA, DecidedStreamIDs: []int64{3}, KEStreamIDs: []int64{}}
	certificate := fmt.Sprintf(`{"hour_id":%d,"attempt":%d,"plan_sha256":%q,"manifest_sha256":%q}`,
		hourID, attempt, planSHA, manifestSHA)
	return &liveStage{fixture: f, provider: provider, objectID: objectID, hourID: hourID, attempt: attempt,
		endpoint: endpoint, objectKey: objectKey, sha: sha, evidence: evidence,
		picks:       StitchStagePicks{SHA256: picksSHA, Decided: []int64{3}, KE: []int64{}},
		certificate: certificate}
}

// client builds the production client for this fixture's provider endpoint.
func (s *liveStage) client(t *testing.T) *Client {
	t.Helper()
	c, err := New(s.fixture.ctx, Config{Bucket: "stoarama", Endpoint: s.provider.srv.URL, Region: "auto",
		AccessKey: "test", SecretKey: "test", WriterDB: s.fixture.pool})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// wire binds the loopback provider's own endpoint.
func (s *liveStage) wire() StitchStageWire { return liveStageWire{endpoint: s.provider.srv.URL} }

// retire runs the production retirement once.
func (s *liveStage) retire(t *testing.T, client *Client) error {
	t.Helper()
	return client.RetireStitchStage(s.fixture.ctx, s.objectID, s.sha, s.evidence, s.picks, s.wire(), false)
}

// provenance reports the durable retirement row as (intents, resolved, stamps).
func (s *liveStage) provenance(t *testing.T) (int, bool, *time.Time, *time.Time, *time.Time) {
	t.Helper()
	var rows int
	var resolved bool
	var absence, strict, wire *time.Time
	if err := s.fixture.pool.QueryRow(s.fixture.ctx, `SELECT count(*)
 FROM nas_stitch_r2_stage_retirements WHERE object_id=$1`, s.objectID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		return rows, false, nil, nil, nil
	}
	if err := s.fixture.pool.QueryRow(s.fixture.ctx, `SELECT
 nas_stitch_r2_stage_retirement_resolved(absence_observed_at,strict_absence_observed_at,wire_absence_observed_at),
 absence_observed_at,strict_absence_observed_at,wire_absence_observed_at
 FROM nas_stitch_r2_stage_retirements WHERE object_id=$1`, s.objectID).
		Scan(&resolved, &absence, &strict, &wire); err != nil {
		t.Fatal(err)
	}
	return rows, resolved, absence, strict, wire
}

// nasGate asks the certified NAS cleanup gate for this hour's exact certificate.
func (s *liveStage) nasGate(t *testing.T) bool {
	t.Helper()
	var gate bool
	if err := s.fixture.pool.QueryRow(s.fixture.ctx,
		`SELECT nas_cleanup_certified_stitch_staging_absent($1::jsonb)`, s.certificate).Scan(&gate); err != nil {
		t.Fatal(err)
	}
	return gate
}

func (s *liveStage) assertSingleWire(t *testing.T) {
	t.Helper()
	if got := s.provider.deletes.Load(); got != 1 {
		t.Fatalf("retirement sent %d DELETEs", got)
	}
	if lists, heads := s.provider.postDeleteEmptyLists.Load(), s.provider.postDeleteNotFoundAfterEmpty.Load(); lists != 1 || heads != 1 {
		t.Fatalf("post-DELETE proof: %d empty inventories and %d exact 404 HEADs after the empty inventory", lists, heads)
	}
}

// A single invocation that sees its own answered DELETE, then an empty
// exact-key multipart inventory, then a strict HEAD 404 publishes all three
// stamps atomically. The same invocation with a live upload still listed for
// that exact key must publish nothing: that inventory check is load-bearing.
func TestR2StitchStageSameInvocationWritesEveryStamp(t *testing.T) {
	s := certifyLiveStage(t, "live_positive")
	if err := s.retire(t, s.client(t)); err != nil {
		t.Fatalf("same-invocation retirement: %v", err)
	}
	rows, resolved, absence, strict, wire := s.provenance(t)
	if rows != 1 || !resolved || absence == nil || strict == nil || wire == nil ||
		!absence.Equal(*strict) || !absence.Equal(*wire) {
		t.Fatalf("retirement did not write one atomic provenance: rows=%d resolved=%v %v/%v/%v",
			rows, resolved, absence, strict, wire)
	}
	s.assertSingleWire(t)
	if !s.nasGate(t) {
		t.Fatal("the production stamp did not clear the certified NAS gate")
	}

	// Mutation: identical invocation, but the post-DELETE inventory still lists
	// a live upload for the exact key. The DELETE happened and the key reads
	// 404, so only the inventory check stands between that and a published
	// absence.
	blocked := certifyLiveStage(t, "live_inventory")
	blocked.provider.liveInventoryAfterDelete.Store(true)
	err := blocked.retire(t, blocked.client(t))
	if err == nil || !strings.Contains(err.Error(), "absence unconfirmed") {
		t.Fatalf("a live exact-key upload published an absence: %v", err)
	}
	blocked.assertHeldAfterWire(t)
}

func TestR2StitchStageDeleteMarkerDoesNotPublishAbsence(t *testing.T) {
	s := certifyLiveStage(t, "live_delete_marker")
	s.provider.deleteMarkerAfterDelete.Store(true)
	if err := s.retire(t, s.client(t)); err == nil || !strings.Contains(err.Error(), "absence remains unconfirmed") {
		t.Fatalf("a delete-marker 404 published an absence: %v", err)
	}
	s.assertSingleWire(t)
	s.assertHeldAfterWire(t)
}

func (s *liveStage) assertHeldAfterWire(t *testing.T) {
	t.Helper()
	rows, resolved, absence, strict, wire := s.provenance(t)
	if rows != 1 || resolved || absence != nil || strict != nil || wire != nil {
		t.Fatalf("withheld absence wrote provenance: rows=%d resolved=%v %v/%v/%v",
			rows, resolved, absence, strict, wire)
	}
	var issued bool
	if err := s.fixture.pool.QueryRow(s.fixture.ctx, `SELECT issued_at IS NOT NULL
 FROM nas_stitch_r2_stage_retirements WHERE object_id=$1`, s.objectID).Scan(&issued); err != nil || !issued {
		t.Fatalf("a withheld absence lost its one-shot intent: %v %v", issued, err)
	}
	if got := s.provider.deletes.Load(); got != 1 {
		t.Fatalf("the withheld retirement sent %d DELETEs", got)
	}
	if s.nasGate(t) {
		t.Fatal("unconfirmed absence cleared the certified NAS gate")
	}
}

// stageAdvisoryHolders lists the backends holding key in the given advisory
// mode, so a wait is proven from pg_locks rather than inferred from timing.
// Advisory locks are cluster-wide, so both queries bind this database: another
// test's database must never be mistaken for the retirement under test.
func stageAdvisoryHolders(t *testing.T, pool *pgxpool.Pool, key int64, mode string) []uint32 {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT pid FROM pg_locks WHERE locktype='advisory'
 AND objsubid=1 AND classid=$1::oid AND objid=$2::oid AND mode=$3 AND granted
 AND database=(SELECT oid::oid FROM pg_database WHERE datname=current_database())`,
		key>>32, key&0xffffffff, mode)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var pids []uint32
	for rows.Next() {
		var pid uint32
		if err := rows.Scan(&pid); err != nil {
			t.Fatal(err)
		}
		pids = append(pids, pid)
	}
	return pids
}

// stageQueuedForLock returns the backend queued for key in mode, failing if none
// appears. A queued row exists only while some grant of the same key blocks it,
// so the caller's wait is for this exact lock rather than an inferred one.
func stageQueuedForLock(t *testing.T, pool *pgxpool.Pool, key int64, mode string) uint32 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		var pid uint32
		err := pool.QueryRow(ctx, `SELECT pid FROM pg_locks WHERE locktype='advisory'
 AND objsubid=1 AND classid=$1::oid AND objid=$2::oid AND mode=$3 AND NOT granted
 AND database=(SELECT oid::oid FROM pg_database WHERE datname=current_database())`,
			key>>32, key&0xffffffff, mode).Scan(&pid)
		if err == nil {
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("no backend queued for advisory lock %d %s: %v", key, mode, err)
		}
	}
}

// stageWaitsForLock waits until pid has an ungranted advisory lock request for
// key in mode. That row exists only while pid is queued behind a granted holder
// of the same key, so it proves the wait is for this exact lock rather than for
// some unrelated resource.
func stageWaitsForLock(t *testing.T, pool *pgxpool.Pool, pid uint32, key int64, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		var queued bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory'
 AND objsubid=1 AND classid=$1::oid AND objid=$2::oid AND mode=$3 AND pid=$4 AND NOT granted
 AND database=(SELECT oid::oid FROM pg_database WHERE datname=current_database()))`,
			key>>32, key&0xffffffff, mode, pid).Scan(&queued)
		if err == nil && queued {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("backend %d never queued for advisory lock %d %s: %v", pid, key, mode, err)
		}
	}
}

// Both orders of the one real race must be observed, not raced for: a new owner
// prefix admission against the production retirement, each on its own
// independent attempt transaction.
//
// Hold first: the admission owns the exclusive protection lock, so the real
// retirement blocks on it and sends zero DELETEs.
//
// Retirement first: the real retirement owns that lock shared, so the real
// admission blocks; the retirement alone then publishes one durable wire
// authority with the provider's key verified absent, and only after that commit
// may the admission proceed.
func TestR2StitchStageRetirementRacesOwnerPrefixAdmission(t *testing.T) {
	t.Run("hold_wins_sends_no_delete", func(t *testing.T) {
		s := certifyLiveStage(t, "live_hold")
		client := s.client(t)
		admission, err := s.fixture.pool.Begin(s.fixture.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer admission.Rollback(context.Background())
		if _, err = admission.Exec(s.fixture.ctx, `INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision)
 VALUES('stoarama','raw/stream/3/','fixture source hold before retirement')`); err != nil {
			t.Fatal(err)
		}
		admissionPID := stageBackendPID(t, admission)
		if holders := stageAdvisoryHolders(t, s.fixture.pool, stageLiveEndpointFence, "ExclusiveLock"); len(holders) != 1 || holders[0] != admissionPID {
			t.Fatalf("owner admission does not own the protection lock: %v (pid %d)", holders, admissionPID)
		}
		retired := make(chan error, 1)
		go func() { retired <- s.retire(t, client) }()
		// The retirement is queued for its shared 1103 behind the admission's
		// exclusive one, which is the real block, not a timing inference.
		stageQueuedForLock(t, s.fixture.pool, stageLiveEndpointFence, "ShareLock")
		if got := s.provider.deletes.Load(); got != 0 {
			t.Fatalf("a blocked retirement sent %d DELETEs", got)
		}
		if err = admission.Commit(s.fixture.ctx); err != nil {
			t.Fatal(err)
		}
		if err = <-retired; err == nil || !strings.Contains(err.Error(), "stitch stage source provenance is protected") {
			t.Fatalf("retirement did not refuse the source owner hold: %v", err)
		}
		if got := s.provider.deletes.Load(); got != 0 {
			t.Fatalf("a hold-covered stage key was deleted: %d", got)
		}
		if rows, resolved, absence, strict, wire := s.provenance(t); rows != 0 || resolved || absence != nil || strict != nil || wire != nil {
			t.Fatalf("a hold-covered retirement wrote durable authority: rows=%d resolved=%v", rows, resolved)
		}
	})

	t.Run("retirement_wins_publishes_one_wire_authority", func(t *testing.T) {
		s := certifyLiveStage(t, "live_retire")
		client := s.client(t)
		s.provider.holdDelete = make(chan struct{})
		retired := make(chan error, 1)
		go func() { retired <- s.retire(t, client) }()
		select {
		case <-s.provider.deleteEntered:
		case <-time.After(20 * time.Second):
			t.Fatal("the retirement never reached the wire")
		}
		// The one-shot intent is committed on its own connection and is durable
		// to every other session while the DELETE is still in flight.
		var intents int
		var issuedAt time.Time
		if err := s.fixture.pool.QueryRow(s.fixture.ctx, `SELECT count(*),max(issued_at)
 FROM nas_stitch_r2_stage_retirements WHERE object_id=$1`, s.objectID).Scan(&intents, &issuedAt); err != nil ||
			intents != 1 || issuedAt.IsZero() {
			t.Fatalf("independent attempt transaction is not durable: %d %v %v", intents, issuedAt, err)
		}
		// The retirement holds 1103 shared before it reaches the wire, and the
		// intent insert's own trigger takes it shared too; both are this
		// retirement's sessions. What must be true is that a real admission
		// queues behind that exact lock, which the wait below proves.
		if holders := stageAdvisoryHolders(t, s.fixture.pool, stageLiveEndpointFence, "ShareLock"); len(holders) == 0 {
			t.Fatal("the retirement does not hold the protection lock shared")
		}

		// A real owner admission for the same prefix now blocks on it.
		admission, err := s.fixture.pool.Begin(s.fixture.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer admission.Rollback(context.Background())
		admissionPID := stageBackendPID(t, admission)
		admitted := make(chan error, 1)
		go func() {
			_, err := admission.Exec(context.Background(), `INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision)
 VALUES('stoarama','raw/stream/3/','fixture source hold after retirement')`)
			admitted <- err
		}()
		stageWaitsForLock(t, s.fixture.pool, admissionPID, stageLiveEndpointFence, "ExclusiveLock")
		if got := s.provider.deletes.Load(); got != 1 {
			t.Fatalf("the retirement sent %d DELETEs while claimed by one invocation", got)
		}
		s.provider.releaseDelete()
		if err = <-retired; err != nil {
			t.Fatalf("the winning retirement failed: %v", err)
		}
		if err = <-admitted; err != nil {
			t.Fatalf("the admission did not proceed after the retirement committed: %v", err)
		}
		if err = admission.Commit(s.fixture.ctx); err != nil {
			t.Fatalf("commit source hold after retirement: %v", err)
		}
		rows, resolved, absence, strict, wire := s.provenance(t)
		if rows != 1 || !resolved || absence == nil || strict == nil || wire == nil ||
			!absence.Equal(*strict) || !absence.Equal(*wire) {
			t.Fatalf("the winning retirement did not publish one wire authority: rows=%d resolved=%v %v/%v/%v",
				rows, resolved, absence, strict, wire)
		}
		s.assertSingleWire(t)
		if !s.nasGate(t) {
			t.Fatal("the winning retirement did not clear the certified NAS gate")
		}
	})
}
