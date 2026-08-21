package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/secretbox"
)

func TestValidateJoinedAck(t *testing.T) {
	valid := joinedAckRequest{ArtifactID: 1, RelativePath: "site/May/Monday/hour_01_part_01_0800-0900.mp4", SizeBytes: 10, SHA256: strings.Repeat("a", 64)}
	if err := validateJoinedAck(valid); err != nil {
		t.Fatalf("valid acknowledgment rejected: %v", err)
	}
	for _, bad := range []joinedAckRequest{
		{},
		{ArtifactID: 1, RelativePath: "/absolute.mp4", SizeBytes: 1, SHA256: strings.Repeat("a", 64)},
		{ArtifactID: 1, RelativePath: "../escape.mp4", SizeBytes: 1, SHA256: strings.Repeat("a", 64)},
		{ArtifactID: 1, RelativePath: "ok.mp4", SizeBytes: 0, SHA256: strings.Repeat("a", 64)},
		{ArtifactID: 1, RelativePath: "ok.mp4", SizeBytes: 1, SHA256: "bad"},
	} {
		if err := validateJoinedAck(bad); err == nil {
			t.Fatalf("invalid acknowledgment accepted: %+v", bad)
		}
	}
}

func assertJoinedCapabilityExpiry(t *testing.T, rec *httptest.ResponseRecorder, max time.Duration, notAfter *time.Time, createOnly bool) {
	t.Helper()
	var response struct {
		ExpiresAt time.Time           `json:"expires_at"`
		Request   joinedSignedRequest `json:"request"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode capability response: %v body=%s", err, rec.Body.String())
	}
	query, err := url.ParseQuery(response.Request.RawQuery)
	if err != nil {
		t.Fatal(err)
	}
	seconds, err := strconv.Atoi(query.Get("X-Amz-Expires"))
	if err != nil || seconds < 1 || time.Duration(seconds)*time.Second > max {
		t.Fatalf("signed capability lifetime=%q max=%s err=%v", query.Get("X-Amz-Expires"), max, err)
	}
	signedAt, err := time.Parse("20060102T150405Z", query.Get("X-Amz-Date"))
	signedExpiry := signedAt.Add(time.Duration(seconds) * time.Second)
	if err != nil || !response.ExpiresAt.Equal(signedExpiry) {
		t.Fatalf("response expiry=%s differs from signer expiry=%s err=%v", response.ExpiresAt,
			signedAt.Add(time.Duration(seconds)*time.Second), err)
	}
	if notAfter != nil && signedExpiry.After(*notAfter) {
		t.Fatalf("signed read expiry=%s exceeds DB lease=%s", signedExpiry, *notAfter)
	}
	if createOnly && response.Request.RequiredHeaders["If-None-Match"] != "*" {
		t.Fatalf("PUT capability is not create-only: %+v", response.Request.RequiredHeaders)
	}
}

type joinedOutputStoreStub struct {
	head              r2.ObjectHead
	extraGetExpirySec int
	extraPutExpirySec int
}

func (s joinedOutputStoreStub) Head(context.Context, string) (r2.ObjectHead, error) {
	return s.head, nil
}

func (s joinedOutputStoreStub) PresignPutCreateOnlyRequest(_ context.Context, key, contentType string, sizeBytes int64, _ string, ttl time.Duration) (r2.PresignedRequest, error) {
	query := url.Values{
		"X-Amz-Date":    {time.Now().UTC().Format("20060102T150405Z")},
		"X-Amz-Expires": {strconv.Itoa(int(ttl.Seconds()) + s.extraPutExpirySec)},
	}
	return r2.PresignedRequest{
		Method: http.MethodPut,
		URL:    "https://output.example.test/joined-output/" + key + "?" + query.Encode(),
		Headers: http.Header{
			"Content-Length": {strconv.FormatInt(sizeBytes, 10)},
			"Content-Type":   {contentType},
			"If-None-Match":  {"*"},
		},
	}, nil
}

func (s joinedOutputStoreStub) PresignGetExactRequest(_ context.Context, key, etag, versionID string, ttl time.Duration) (r2.PresignedRequest, error) {
	query := url.Values{
		"X-Amz-Date":    {time.Now().UTC().Format("20060102T150405Z")},
		"X-Amz-Expires": {strconv.Itoa(int(ttl.Seconds()) + s.extraGetExpirySec)},
		"versionId":     {versionID},
	}
	return r2.PresignedRequest{
		Method:  http.MethodGet,
		URL:     "https://output.example.test/joined-output/" + key + "?" + query.Encode(),
		Headers: http.Header{"If-Match": {`"` + etag + `"`}},
	}, nil
}

func TestJoinedCapabilityEnvelopeRejectsChangedAuthorityAndExpiredLease(t *testing.T) {
	capability := r2.PresignedRequest{Method: http.MethodGet, URL: "https://storage.example.test/bucket/exact?signature=1", Headers: http.Header{"If-Match": []string{`"etag"`}}}
	got, err := joinedSignedRequestFrom(capability, "storage.example.test")
	if err != nil || got.Authority != "storage.example.test" || got.RequiredHeaders["If-Match"] != `"etag"` {
		t.Fatalf("exact capability rejected: %+v %v", got, err)
	}
	for _, raw := range []string{
		"http://storage.example.test/bucket/exact?signature=1",
		"https://other.example.test/bucket/exact?signature=1",
		"https://user@storage.example.test/bucket/exact?signature=1",
		"https://storage.example.test:8443/bucket/exact?signature=1",
	} {
		capability.URL = raw
		if _, err := joinedSignedRequestFrom(capability, "storage.example.test"); err == nil {
			t.Fatalf("changed storage authority accepted: %s", raw)
		}
	}
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	if _, err := joinedCapabilityTTL(time.Minute, now.Add(500*time.Millisecond), now, false); err == nil {
		t.Fatal("expired publication lease minted a capability")
	}
	if ttl, err := joinedCapabilityTTL(time.Hour, now.Add(5*time.Minute), now, true); err != nil || ttl != time.Hour {
		t.Fatalf("exact create-only capability may safely outlive the DB lease: ttl=%s err=%v", ttl, err)
	}
	if ttl, err := joinedCapabilityTTL(15*time.Minute, now.Add(5*time.Minute), now, false); err != nil || ttl != 5*time.Minute {
		t.Fatalf("read capability must end with the DB lease: ttl=%s err=%v", ttl, err)
	}
	if ttl, err := joinedCapabilityTTL(15*time.Minute, now.Add(30*time.Minute), now, false); err != nil || ttl != 15*time.Minute {
		t.Fatalf("read capability exceeded configured maximum: ttl=%s err=%v", ttl, err)
	}
	signedExpiry, err := joinedSignedRequestExpiry(joinedSignedRequest{RawQuery: "X-Amz-Date=20260821T120000Z&X-Amz-Expires=300"}, now.Add(10*time.Minute))
	if err != nil || !signedExpiry.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("signed request expiry=%s err=%v", signedExpiry, err)
	}
	if _, err := joinedSignedRequestExpiry(joinedSignedRequest{RawQuery: "X-Amz-Date=20260821T120000Z&X-Amz-Expires=600"}, now.Add(5*time.Minute)); err == nil {
		t.Fatal("read capability whose signed expiry outlives the DB lease was accepted")
	}
	if signedExpiry, err := joinedSignedRequestExpiry(joinedSignedRequest{RawQuery: "X-Amz-Date=20260821T120000Z&X-Amz-Expires=3600"}, now.Add(time.Hour)); err != nil || !signedExpiry.Equal(now.Add(time.Hour)) {
		t.Fatalf("create-only signed expiry=%s err=%v", signedExpiry, err)
	}
}

func TestJoinedWorkerAuthIsShortLivedAndRouteScoped(t *testing.T) {
	claim := uuid.New()
	token, err := joinedauth.MintOperation("joined-signing-key", "batch-test", joinedauth.SubjectHour, "hour-test",
		claim, joinedauth.OperationPreflight, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{cfg: config.Config{ServiceToken: "generic-service-key", JoinedWorkerBootstrapToken: "joined-bootstrap-key", JoinedWorkerSigningKey: "joined-signing-key"}}
	misconfigured := &Server{cfg: config.Config{JoinedRecordingEnabled: true, ServiceToken: "shared-secret",
		JoinedWorkerBootstrapToken: "shared-secret", JoinedWorkerSigningKey: "shared-secret"}}
	if misconfigured.joinedControlPlaneReady() {
		t.Fatal("joined control plane accepted shared generic/bootstrap/signing credentials")
	}
	sharedReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/token", strings.NewReader(`{"protocol_version":1}`))
	sharedReq.Header.Set("Authorization", "Bearer shared-secret")
	sharedRec := httptest.NewRecorder()
	misconfigured.requireJoinedWorkerBootstrapAuth(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("shared generic credential reached joined bootstrap route")
	})).ServeHTTP(sharedRec, sharedReq)
	if sharedRec.Code != http.StatusUnauthorized {
		t.Fatalf("shared joined bootstrap status=%d", sharedRec.Code)
	}
	sharedOperation, err := joinedauth.MintOperation("shared-secret", "batch-test", joinedauth.SubjectHour, "hour-test",
		claim, joinedauth.OperationPreflight, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	sharedReq = httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/heartbeat", strings.NewReader(`{"protocol_version":1}`))
	sharedReq.Header.Set("Authorization", "Bearer "+sharedOperation)
	sharedRec = httptest.NewRecorder()
	misconfigured.requireJoinedWorkerAuth(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("shared signing credential reached joined operation route")
	})).ServeHTTP(sharedRec, sharedReq)
	if sharedRec.Code != http.StatusUnauthorized {
		t.Fatalf("shared joined operation status=%d", sharedRec.Code)
	}
	reached := false
	handler := s.requireJoinedWorkerAuth(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		reached = true
		claims, ok := joinedWorkerClaimsFromContext(r.Context())
		if !ok || claims.SubjectKind != joinedauth.SubjectHour || claims.SubjectID != "hour-test" ||
			claims.LeaseToken != claim.String() || claims.Operation != joinedauth.OperationPreflight {
			t.Fatalf("joined claims=%+v ok=%v", claims, ok)
		}
	}))
	for _, tc := range []struct {
		name, authorization string
		want                int
	}{
		{name: "missing", want: http.StatusUnauthorized},
		{name: "invalid", authorization: "Bearer invalid", want: http.StatusUnauthorized},
		{name: "generic service token", authorization: "Bearer generic-service-key", want: http.StatusUnauthorized},
		{name: "valid", authorization: "Bearer " + token, want: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reached = false
			req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/capabilities/source", nil)
			req.Header.Set("Authorization", tc.authorization)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.want || reached != (tc.want == http.StatusOK) {
				t.Fatalf("status=%d reached=%v", rec.Code, reached)
			}
		})
	}
	// A joined job token is HMAC-derived, not equal to the generic service
	// secret, and therefore cannot authenticate requireServiceAuth routes.
	req := httptest.NewRequest(http.MethodPost, "/api/v1/processing/worker-heartbeat", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	s.requireServiceAuth(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("joined job token reached an unrelated service route")
	})).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("joined job token service-route status=%d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/processing/worker-heartbeat", nil)
	req.Header.Set("Authorization", "Bearer joined-bootstrap-key")
	rec = httptest.NewRecorder()
	s.requireServiceAuth(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("joined bootstrap token reached an unrelated service route")
	})).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("joined bootstrap service-route status=%d", rec.Code)
	}
	for _, tc := range []struct {
		name, token string
		want        int
	}{
		{name: "missing", want: http.StatusUnauthorized},
		{name: "generic service token", token: "generic-service-key", want: http.StatusUnauthorized},
		{name: "joined bootstrap", token: "joined-bootstrap-key", want: http.StatusOK},
	} {
		t.Run("bootstrap "+tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/claim", nil)
			req.Header.Set("Authorization", "Bearer "+tc.token)
			rec := httptest.NewRecorder()
			s.requireJoinedWorkerBootstrapAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})).ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status=%d want=%d", rec.Code, tc.want)
			}
		})
	}
	// A token minted while Release B was enabled is inert immediately after the
	// global kill switch is turned off. Nil DB/storage dependencies make this a
	// regression proof that every route returns before either can be touched.
	for _, tc := range []struct {
		name, path, body string
		handler          http.HandlerFunc
	}{
		{name: "heartbeat", path: "/api/v1/recording/joined/heartbeat", body: `{"scope_kind":"hour","scope_id":"hour-test"}`, handler: s.handleJoinedHeartbeat},
		{name: "source capability", path: "/api/v1/recording/joined/capabilities/source", body: `{"hour_id":"hour-test","clip_id":1}`, handler: s.handleJoinedSourceCapability},
		{name: "artifact capability", path: "/api/v1/recording/joined/capabilities/artifact", body: `{"scope_kind":"hour","scope_id":"hour-test","artifact_id":1,"operation":"put"}`, handler: s.handleJoinedArtifactCapability},
	} {
		t.Run("disabled "+tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()
			s.requireJoinedWorkerAuth(tc.handler).ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
	s.cfg.JoinedRecordingEnabled = true
	bootstrapLeaseRequest := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/token", strings.NewReader(`{"lease_id":"forbidden"}`))
	bootstrapLeaseRequest.Header.Set("Authorization", "Bearer joined-bootstrap-key")
	bootstrapLeaseResponse := httptest.NewRecorder()
	s.requireJoinedWorkerBootstrapAuth(http.HandlerFunc(s.handleJoinedToken)).ServeHTTP(bootstrapLeaseResponse, bootstrapLeaseRequest)
	if bootstrapLeaseResponse.Code != http.StatusBadRequest {
		t.Fatalf("response-only lease_id bootstrap status=%d body=%s", bootstrapLeaseResponse.Code, bootstrapLeaseResponse.Body.String())
	}
	claimAuth, err := joinedauth.MintClaim(s.cfg.JoinedWorkerSigningKey, "batch-test", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path, body, auth string
		handler                http.HandlerFunc
	}{
		{name: "claim", path: "/api/v1/recording/joined/claim", body: `{"worker_id":"worker","lease_id":"forbidden"}`, auth: claimAuth, handler: s.handleJoinedClaim},
		{name: "heartbeat", path: "/api/v1/recording/joined/heartbeat", body: `{"scope_kind":"hour","scope_id":"hour-test","lease_id":"forbidden"}`, auth: token, handler: s.handleJoinedHeartbeat},
		{name: "source capability", path: "/api/v1/recording/joined/capabilities/source", body: `{"hour_id":"hour-test","clip_id":1,"lease_id":"forbidden"}`, auth: token, handler: s.handleJoinedSourceCapability},
		{name: "artifact capability", path: "/api/v1/recording/joined/capabilities/artifact", body: `{"scope_kind":"hour","scope_id":"hour-test","artifact_id":1,"operation":"put","lease_id":"forbidden"}`, auth: token, handler: s.handleJoinedArtifactCapability},
	} {
		t.Run("response-only lease_id "+tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+tc.auth)
			rec := httptest.NewRecorder()
			s.requireJoinedWorkerAuth(tc.handler).ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

// This exercises the real migrations in a disposable PostgreSQL schema when
// STOARAMA_TEST_DATABASE_URL is configured by CI or a local developer.
func TestJoinedHourSealPublishFeedAndExactAck(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	secrets, err := secretbox.NewFromBase64Key("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}
	sealedStorageSecret, err := secrets.Encrypt([]byte("storage-secret"))
	if err != nil {
		t.Fatal(err)
	}
	s.secrets = secrets
	s.cfg.R2SignGetTTL = time.Minute
	s.cfg.R2SignPutTTL = time.Minute
	s.cfg.JoinedRecordingEnabled = true
	s.cfg.JoinedWorkerBootstrapToken = "joined-bootstrap-key"
	s.cfg.JoinedWorkerSigningKey = "joined-worker-signing-key"
	s.cfg.R2Endpoint = "https://output.example.test"
	s.cfg.R2Bucket = "joined-output"
	s.cfg.R2Region = "auto"
	s.cfg.R2AccessKeyID = "output-key"
	s.cfg.R2SecretAccessKey = "output-secret"
	s.r2, err = r2.New(context.Background(), r2.Config{Endpoint: s.cfg.R2Endpoint, Region: s.cfg.R2Region,
		Bucket: s.cfg.R2Bucket, AccessKey: s.cfg.R2AccessKeyID, SecretKey: s.cfg.R2SecretAccessKey})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, accountID := seedUserOrg(t, pool, "joined@example.test", false)

	var storageID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO storage_destinations(account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc,status)
		VALUES($1,'joined-test','https://example.test','auto','clips','key',$2,'verified') RETURNING id`, accountID, sealedStorageSecret).Scan(&storageID); err != nil {
		t.Fatal(err)
	}
	var apiKeyID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO account_api_keys(account_id,key_prefix,secret_hash,label,scopes)
		VALUES($1,'sir_joined','joined-test-hash','NAS',ARRAY['stoarama.pull']) RETURNING id`, accountID).Scan(&apiKeyID); err != nil {
		t.Fatal(err)
	}
	var connectionID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,label,api_key_id,joined_protocol_version)
		VALUES($1,'nas_pull','NAS',$2,1) RETURNING id`, accountID, apiKeyID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	start := time.Date(now.Year(), now.Month(), now.Day(), 8, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	var recordingID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,cron_timezone,mode,daily_window_start,daily_window_end,delivery,start_at,end_at)
		VALUES($1,$2,'joined-recording','https://example.test/live.m3u8','UTC','continuous','08:00','20:00','nas_pull',$3,$4)
		RETURNING id`, accountID, storageID, start.Add(-14*24*time.Hour), start.Add(24*time.Hour)).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	jobIDs := make([]int64, 0, 14)
	localDates := make([]string, 0, 14)
	var jobID int64
	for day := -13; day <= 0; day++ {
		scheduled := start.AddDate(0, 0, day)
		if err := pool.QueryRow(ctx, `
			INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at,completed_at)
			VALUES($1,$2,$2,60,'done',$3,'continuous_window',$4,$4) RETURNING id`, recordingID, scheduled,
			fmt.Sprintf("reccont:%d:%d", recordingID, scheduled.Unix()), scheduled.Add(12*time.Hour)).Scan(&jobID); err != nil {
			t.Fatal(err)
		}
		jobIDs = append(jobIDs, jobID)
		localDates = append(localDates, scheduled.Format("2006-01-02"))
	}
	sourceSHA := strings.Repeat("d", 64)
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_clips(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,
		  size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at)
		VALUES (9001,$1,$2,$3,'https://example.test','clips','raw/9001.mp4',100,'source-etag',$4,$5,$5,$6),
		       (9002,$1,$2,$3,'https://example.test','clips','raw/9002.mp4',100,'source-etag-2',$4,$5,$6,$7)`,
		recordingID, jobID, storageID, sourceSHA, start, start.Add(time.Hour), start.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}

	batchID, batchSHA := "batch-20260821", strings.Repeat("a", 64)
	sourceFrozenAt := time.Now().UTC()
	hours := make([]int64, 2)
	hourCanonicalIDs := make([]string, 2)
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_joined_hours(account_id,connection_id,recording_id,batch_id,batch_expected_hours,
		  batch_expected_source_clips,batch_expected_source_bytes,frozen_denominator_sha256,source_frozen_at,local_date,delivery_hour,
		  local_clock_hour,local_timezone,hour_start_at,hour_end_at,batch_queue_order,priority_tier,priority_order,priority_facts,
		  source_clip_count,source_bytes,source_manifest_sha256,generation,canonical_hour_id,delivery_start_at,delivery_end_at,
		  authoritative_local_dates,authoritative_recording_job_ids,qualification_facts,qualification_sha256,
		  qualification_policy_version)
		SELECT $1,$2,$3::bigint,$4::text,168,2,200,$5,$13,d,dh,dh+7,'UTC',
		  (d::timestamp+make_interval(hours=>dh+7)) AT TIME ZONE 'UTC',
		  (d::timestamp+make_interval(hours=>dh+8)) AT TIME ZONE 'UTC',1,1,
		  CASE WHEN d=$6::date AND dh IN(1,2) THEN dh ELSE 1000+day_ordinal*12+dh END,'{}',
		  CASE WHEN d=$6::date AND dh IN(1,2) THEN 1 ELSE 0 END,
		  CASE WHEN d=$6::date AND dh IN(1,2) THEN 100 ELSE 0 END,$7,1,
		  $4::text||'__recording-'||($3::bigint)::text||'__date-'||d::text||'__hour-'||lpad(dh::text,2,'0')||'__generation-1',
		  $8,$9,$10::date[],$11::bigint[],
		  '{"status":"good_plus"}',$12,'good-plus-v1'
		FROM unnest($10::date[]) WITH ORDINALITY dates(d,day_ordinal) CROSS JOIN generate_series(1,12) dh`,
		accountID, connectionID, recordingID, batchID, batchSHA, localDates[13], sourceSHA,
		start.AddDate(0, 0, -13), start.Add(12*time.Hour), localDates, jobIDs, strings.Repeat("9", 64), sourceFrozenAt); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT id,canonical_hour_id FROM recording_joined_hours WHERE connection_id=$1 AND batch_id=$2
		AND local_date=$3::date AND delivery_hour=1`, connectionID, batchID, localDates[13]).Scan(&hours[0], &hourCanonicalIDs[0]); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT id,canonical_hour_id FROM recording_joined_hours WHERE connection_id=$1 AND batch_id=$2
		AND local_date=$3::date AND delivery_hour=2`, connectionID, batchID, localDates[13]).Scan(&hours[1], &hourCanonicalIDs[1]); err != nil {
		t.Fatal(err)
	}
	var badJobID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,
		idempotency_key,kind,window_end_at,completed_at) VALUES($1,$2,$2,60,'done',$3,'continuous_window',$4,$4) RETURNING id`,
		recordingID, start, fmt.Sprintf("bad-scope:%d:%d", recordingID, start.Unix()), start.Add(2*time.Hour)).Scan(&badJobID); err != nil {
		t.Fatal(err)
	}
	badJobIDs := append([]int64(nil), jobIDs...)
	badJobIDs[len(badJobIDs)-1] = badJobID
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_joined_hours(account_id,connection_id,recording_id,batch_id,batch_expected_hours,
		  batch_expected_source_clips,batch_expected_source_bytes,frozen_denominator_sha256,source_frozen_at,local_date,delivery_hour,
		  local_clock_hour,local_timezone,hour_start_at,hour_end_at,batch_queue_order,priority_tier,priority_order,priority_facts,
		  source_clip_count,source_bytes,source_manifest_sha256,generation,canonical_hour_id,delivery_start_at,delivery_end_at,
		  authoritative_local_dates,authoritative_recording_job_ids,qualification_facts,qualification_sha256,
		  qualification_policy_version)
		SELECT account_id,connection_id,recording_id,'bad-scope',1,0,0,$2,source_frozen_at,local_date,delivery_hour,local_clock_hour,
		  local_timezone,hour_start_at,hour_end_at,batch_queue_order,priority_tier,priority_order,priority_facts,0,0,source_manifest_sha256,
		  generation,'bad-scope__recording-'||recording_id::text||'__date-'||local_date::text||'__hour-'||lpad(delivery_hour::text,2,'0')||'__generation-1',
		  delivery_start_at,delivery_end_at,authoritative_local_dates,$3::bigint[],qualification_facts,
		  qualification_sha256,qualification_policy_version FROM recording_joined_hours WHERE id=$1`,
		hours[0], strings.Repeat("1", 64), badJobIDs); err == nil {
		t.Fatal("arbitrary non-12-hour job list was accepted as a 14-day qualification scope")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_stream_days(account_id,connection_id,recording_id,batch_id,
		local_date,recording_job_id,source_clip_count,source_bytes,source_manifest_sha256,adjacency_manifest_sha256,
		first_source_clip_id,last_source_clip_id,previous_frozen_clip_id,cross_day_facts,adjacency_facts,sealed_at)
		SELECT $1,$2,$3,$4,d,j,
		  CASE WHEN d=$5::date THEN 2 ELSE 0 END,CASE WHEN d=$5::date THEN 200 ELSE 0 END,$6,$7,
		  CASE WHEN d=$5::date THEN 9001 END,CASE WHEN d=$5::date THEN 9002 END,NULL,
		  CASE WHEN d=$5::date THEN '{"boundary":"cross_day","verdict":"first"}'::jsonb
		    ELSE '{"boundary":"cross_day","verdict":"no_sources"}'::jsonb END,
		  CASE WHEN d=$5::date THEN '{"schema_version":1,"pair_count":1}'::jsonb
		    ELSE '{"schema_version":1,"pair_count":0}'::jsonb END,now()
		FROM unnest($8::date[],$9::bigint[]) paired(d,j)`, accountID, connectionID, recordingID, batchID,
		localDates[13], sourceSHA, strings.Repeat("e", 64), localDates, jobIDs); err != nil {
		t.Fatal(err)
	}
	var streamDayID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM recording_joined_stream_days WHERE connection_id=$1 AND batch_id=$2
		AND recording_id=$3 AND local_date=$4::date`, connectionID, batchID, recordingID, localDates[13]).Scan(&streamDayID); err != nil {
		t.Fatal(err)
	}
	for i := range hours {
		hourStart := start.Add(time.Duration(i) * time.Hour)
		clipID := int64(9001 + i)
		etag := "source-etag"
		if i == 1 {
			etag = "source-etag-2"
		}
		var previousClipID any
		adjacencyFacts := fmt.Sprintf(`{"schema_version":1,"verdict":"first","boundary":"first","reason":"first_source",
			"previous_clip_id":null,"next_clip_id":%d,"previous_presentation_end_utc":null,
			"next_presentation_start_utc":%q,"signed_gap_nanoseconds":null}`, clipID, hourStart.Format(time.RFC3339Nano))
		if i == 1 {
			previousClipID = int64(9001)
			adjacencyFacts = fmt.Sprintf(`{"schema_version":1,"verdict":"exact","boundary":"cross_hour","reason":"continuous",
				"previous_clip_id":9001,"next_clip_id":%d,"previous_presentation_end_utc":%q,
				"next_presentation_start_utc":%q,"signed_gap_nanoseconds":0,
				"certification_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`,
				clipID, hourStart.Format(time.RFC3339Nano), hourStart.Format(time.RFC3339Nano))
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO recording_joined_hour_sources(hour_id,stream_day_id,account_id,connection_id,batch_id,ordinal,clip_id,
			  recording_job_id,storage_destination_id,provider,endpoint,region,bucket,object_key,size_bytes,sha256,etag,version_id,
			  clip_start_at,clip_end_at,released_at,stream_day_clip_ordinal,previous_clip_id,adjacency_facts,allocation_facts)
			VALUES($1,$2,$3,$4,$5,1,$6,$7,$8,'s3_compatible','https://example.test','auto','clips',$9,100,$10,$11,$12,$13,$14,NULL,$15,$16,$17,
			  '{"boundary_rule":"closest_verified_seam","crosses_hour_start":false,"crosses_hour_end":false,
			    "candidate_count":1,"winning_distance_us":0,"tie_breaker":"earlier_seam",
			    "decision_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}')`,
			hours[i], streamDayID, accountID, connectionID, batchID, clipID, jobID, storageID,
			fmt.Sprintf("raw/%d.mp4", clipID), sourceSHA, etag, fmt.Sprintf("source-version-%d", i+1),
			hourStart, hourStart.Add(time.Hour), i+1, previousClipID, adjacencyFacts); err != nil {
			t.Fatal(err)
		}
	}

	principal := accountPrincipal{AccountID: accountID, APIKeyID: &apiKeyID, KeyScopes: []string{accountScopePull}}
	callFeed := func() map[string]any {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/account/joined", nil)
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
		rec := httptest.NewRecorder()
		s.handleAccountJoined(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("feed status=%d body=%s", rec.Code, rec.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body
	}
	callAck := func(ack joinedAckRequest) map[string]any {
		ackBody, _ := json.Marshal(ack)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", bytes.NewReader(ackBody))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
		rec := httptest.NewRecorder()
		s.handleAccountJoinedAck(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("ack status=%d body=%s", rec.Code, rec.Body.String())
		}
		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return body
	}
	withConcurrentProtocolRevocation := func(call func() error) {
		t.Helper()
		revokeTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var blockerPID int
		if err := revokeTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
			t.Fatal(err)
		}
		if _, err := revokeTx.Exec(ctx, `UPDATE connections SET joined_protocol_version=0 WHERE id=$1`, connectionID); err != nil {
			t.Fatal(err)
		}
		started, result := make(chan struct{}), make(chan error, 1)
		go func() {
			close(started)
			result <- call()
		}()
		<-started
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for {
			var blocked bool
			if err := pool.QueryRow(waitCtx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity a
				WHERE $1=ANY(pg_blocking_pids(a.pid)))`, blockerPID).Scan(&blocked); err != nil {
				t.Fatal(err)
			}
			if blocked {
				break
			}
			select {
			case err := <-result:
				t.Fatalf("joined request returned before final protocol fence: %v", err)
			case <-waitCtx.Done():
				t.Fatal("joined request never reached final protocol fence")
			default:
			}
		}
		if err := revokeTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=1 WHERE id=$1`, connectionID); err != nil {
			t.Fatal(err)
		}
	}
	if item := callFeed()["item"]; item != nil {
		t.Fatalf("pending hour leaked into feed: %v", item)
	}

	// Allocation ledgers have their own sealed -> fenced publication -> exact
	// ACK lifecycle and must arrive before any dependent hour manifest.
	ledgerRows, err := pool.Query(ctx, `SELECT id,recording_id,local_date,source_clip_count,source_bytes,
		adjacency_manifest_sha256,seal_token FROM recording_joined_stream_days
		WHERE connection_id=$1 AND batch_id=$2 ORDER BY recording_id,local_date`, connectionID, batchID)
	if err != nil {
		t.Fatal(err)
	}
	type ledgerFixture struct {
		dayID, artifactID, size int64
		path, sha               string
	}
	var ledgers []ledgerFixture
	for ledgerRows.Next() {
		var dayID, recordingID, sourceBytes int64
		var sourceCount int
		var localDate time.Time
		var sha string
		var seal uuid.UUID
		if err := ledgerRows.Scan(&dayID, &recordingID, &localDate, &sourceCount, &sourceBytes, &sha, &seal); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("coverage/ledgers/%d/%s.json", recordingID, localDate.Format("2006-01-02"))
		var artifactID int64
		if err := pool.QueryRow(ctx, `INSERT INTO recording_joined_outputs(stream_day_id,account_id,connection_id,batch_id,
			artifact_kind,content_type,hour_seal_token,relative_path,content_id,expected_size_bytes,expected_sha256,
			object_key,source_clip_count,source_bytes,coverage_start_at,coverage_end_at,plan_facts)
			VALUES($1,$2,$3,$4,'allocation_ledger','application/json',$5,$6,$7,200,$7,'joined/'||$4||'/'||$6,
			  $8,$9,(SELECT min(hour_start_at) FROM recording_joined_hours WHERE connection_id=$3 AND batch_id=$4 AND recording_id=$11 AND local_date=$10::date),
			  (SELECT max(hour_end_at) FROM recording_joined_hours WHERE connection_id=$3 AND batch_id=$4 AND recording_id=$11 AND local_date=$10::date),
			  jsonb_build_object('schema_version',1,'recording_id',($11::bigint)::text,'local_date',$10::text)) RETURNING id`,
			dayID, accountID, connectionID, batchID, seal, path, sha, sourceCount, sourceBytes,
			localDate.Format("2006-01-02"), recordingID).Scan(&artifactID); err != nil {
			t.Fatal(err)
		}
		publishToken := uuid.New()
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_stream_days SET state='publishing',publish_attempt_count=1,
			publish_claim_token=$2,publish_claimed_by='ledger-worker',publish_lease_expires_at=now()+interval '10 minutes',
			publish_heartbeat_at=now() WHERE id=$1`, dayID, publishToken); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_outputs SET state='published',size_bytes=200,sha256=$2,
			r2_etag='ledger-etag',r2_version_id='ledger-version',published_at=now() WHERE id=$1`, artifactID, sha); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_stream_days SET state='published',publish_claim_token=NULL,
			publish_claimed_by=NULL,publish_lease_expires_at=NULL,publish_heartbeat_at=NULL,published_at=now() WHERE id=$1`, dayID); err != nil {
			t.Fatal(err)
		}
		ledgers = append(ledgers, ledgerFixture{dayID: dayID, artifactID: artifactID, size: 200, path: path, sha: sha})
	}
	ledgerRows.Close()
	if len(ledgers) != 14 {
		t.Fatalf("ledger count=%d want 14", len(ledgers))
	}
	withConcurrentProtocolRevocation(func() error {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/account/joined", nil)
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
		rec := httptest.NewRecorder()
		s.handleAccountJoined(rec, req)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"item":null`) {
			return fmt.Errorf("protocol-v0 feed status=%d body=%s", rec.Code, rec.Body.String())
		}
		return nil
	})
	for _, ledger := range ledgers {
		firstLedger, _ := callFeed()["item"].(map[string]any)
		if firstLedger["kind"] != "allocation_ledger" || firstLedger["artifact_id"] != float64(ledger.artifactID) || firstLedger["hour_id"] != nil {
			t.Fatalf("ledger was not first in its independent feed lifecycle: %v", firstLedger)
		}
		callAck(joinedAckRequest{ArtifactID: ledger.artifactID, RelativePath: ledger.path, SizeBytes: ledger.size, SHA256: ledger.sha})
	}

	tx1, _ := pool.Begin(ctx)
	defer func() { _ = tx1.Rollback(ctx) }()
	tx2, _ := pool.Begin(ctx)
	defer func() { _ = tx2.Rollback(ctx) }()
	var first, second int64
	if err := tx1.QueryRow(ctx, `SELECT id FROM recording_joined_hours WHERE state='pending' ORDER BY priority_order FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if err := tx2.QueryRow(ctx, `SELECT id FROM recording_joined_hours WHERE state='pending' ORDER BY priority_order FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&second); err != nil {
		t.Fatal(err)
	}
	if first != hours[0] || second != hours[1] {
		t.Fatalf("parallel hour claims=%d,%d want=%d,%d", first, second, hours[0], hours[1])
	}
	_ = tx1.Rollback(ctx)
	_ = tx2.Rollback(ctx)

	publicationClaim := uuid.New()
	tokenReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/token",
		strings.NewReader(fmt.Sprintf(`{"protocol_version":1,"batch_id":%q}`, batchID)))
	tokenReq.Header.Set("Authorization", "Bearer joined-bootstrap-key")
	tokenRec := httptest.NewRecorder()
	s.requireJoinedWorkerBootstrapAuth(http.HandlerFunc(s.handleJoinedToken)).ServeHTTP(tokenRec, tokenReq)
	var tokenResponse struct {
		Item *struct {
			ClaimToken string `json:"claim_token"`
		} `json:"item"`
	}
	if tokenRec.Code != http.StatusOK || json.Unmarshal(tokenRec.Body.Bytes(), &tokenResponse) != nil || tokenResponse.Item == nil {
		t.Fatalf("joined token status=%d body=%s", tokenRec.Code, tokenRec.Body.String())
	}
	foreignTokenReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/token",
		strings.NewReader(`{"protocol_version":1,"batch_id":"foreign-batch"}`))
	foreignTokenReq.Header.Set("Authorization", "Bearer joined-bootstrap-key")
	foreignTokenRec := httptest.NewRecorder()
	s.requireJoinedWorkerBootstrapAuth(http.HandlerFunc(s.handleJoinedToken)).ServeHTTP(foreignTokenRec, foreignTokenReq)
	if foreignTokenRec.Code != http.StatusOK || !strings.Contains(foreignTokenRec.Body.String(), `"item":null`) {
		t.Fatalf("foreign batch bootstrap status=%d body=%s", foreignTokenRec.Code, foreignTokenRec.Body.String())
	}
	claimBody, _ := json.Marshal(joinedClaimRequest{ProtocolVersion: joinedWorkerProtocolVersion, BatchID: batchID, WorkerID: "worker-1"})
	foreignClaimBody, _ := json.Marshal(joinedClaimRequest{ProtocolVersion: joinedWorkerProtocolVersion, BatchID: "foreign-batch", WorkerID: "worker-1"})
	foreignClaimReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/claim", bytes.NewReader(foreignClaimBody))
	foreignClaimReq.Header.Set("Authorization", "Bearer "+tokenResponse.Item.ClaimToken)
	foreignClaimRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedClaim)).ServeHTTP(foreignClaimRec, foreignClaimReq)
	if foreignClaimRec.Code != http.StatusForbidden {
		t.Fatalf("foreign batch claim status=%d body=%s", foreignClaimRec.Code, foreignClaimRec.Body.String())
	}
	// A protocol revocation that wins the connection-row lock must prevent the
	// pending claim from mutating an hour or returning storage authority.
	revokeTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := revokeTx.Exec(ctx, `UPDATE connections SET joined_protocol_version=0 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	raceDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/claim", bytes.NewReader(claimBody))
		req.Header.Set("Authorization", "Bearer "+tokenResponse.Item.ClaimToken)
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedClaim)).ServeHTTP(rec, req)
		raceDone <- rec
	}()
	if err := revokeTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var racedClaim *httptest.ResponseRecorder
	select {
	case racedClaim = <-raceDone:
	case <-time.After(5 * time.Second):
		t.Fatal("protocol-revoked claim deadlocked")
	}
	if racedClaim.Code != http.StatusOK || !strings.Contains(racedClaim.Body.String(), `"item":null`) {
		t.Fatalf("revoked claim status=%d body=%s", racedClaim.Code, racedClaim.Body.String())
	}
	var racedLeases int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_joined_hours WHERE connection_id=$1 AND state='leased'`, connectionID).Scan(&racedLeases); err != nil || racedLeases != 0 {
		t.Fatalf("protocol revocation committed %d claims: err=%v", racedLeases, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=1 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	claimReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/claim", bytes.NewReader(claimBody))
	claimReq.Header.Set("Authorization", "Bearer "+tokenResponse.Item.ClaimToken)
	claimRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedClaim)).ServeHTTP(claimRec, claimReq)
	if claimRec.Code != http.StatusOK {
		t.Fatalf("joined claim status=%d body=%s", claimRec.Code, claimRec.Body.String())
	}
	var claimed struct {
		Item *joinedClaimItem `json:"item"`
	}
	if err := json.Unmarshal(claimRec.Body.Bytes(), &claimed); err != nil || claimed.Item == nil ||
		claimed.Item.HourID != hourCanonicalIDs[0] || len(claimed.Item.Sources) != 1 || claimed.Item.LeaseID == "" {
		t.Fatalf("joined source-only claim=%+v err=%v", claimed.Item, err)
	}
	var claim uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT claim_token FROM recording_joined_hours WHERE id=$1`, hours[0]).Scan(&claim); err != nil {
		t.Fatal(err)
	}
	if claimed.Item.LeaseID != joinedauth.LeaseID(claim) || claimed.Item.LeaseID == claim.String() {
		t.Fatalf("claim lease_id=%q fence=%q", claimed.Item.LeaseID, claim)
	}
	heartbeatBody, _ := json.Marshal(joinedHeartbeatRequest{ProtocolVersion: joinedWorkerProtocolVersion, ScopeKind: joinedauth.SubjectHour, ScopeID: claimed.Item.HourID})
	heartbeatReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/heartbeat", bytes.NewReader(heartbeatBody))
	heartbeatReq.Header.Set("Authorization", "Bearer "+claimed.Item.OperationToken)
	heartbeatRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedHeartbeat)).ServeHTTP(heartbeatRec, heartbeatReq)
	if heartbeatRec.Code != http.StatusOK {
		t.Fatalf("joined heartbeat status=%d body=%s", heartbeatRec.Code, heartbeatRec.Body.String())
	}
	var heartbeatResponse struct {
		LeaseID        string    `json:"lease_id"`
		OperationToken string    `json:"operation_token"`
		ScopeKind      string    `json:"scope_kind"`
		ScopeID        string    `json:"scope_id"`
		ExpiresAt      time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(heartbeatRec.Body.Bytes(), &heartbeatResponse); err != nil || heartbeatResponse.LeaseID != joinedauth.LeaseID(claim) ||
		heartbeatResponse.OperationToken == "" || heartbeatResponse.ScopeKind != joinedauth.SubjectHour ||
		heartbeatResponse.ScopeID != claimed.Item.HourID {
		t.Fatalf("joined heartbeat did not preserve lease and refresh auth: %+v err=%v", heartbeatResponse, err)
	}
	var databaseLeaseExpires time.Time
	if err := pool.QueryRow(ctx, `SELECT lease_expires_at FROM recording_joined_hours WHERE id=$1`, hours[0]).Scan(&databaseLeaseExpires); err != nil {
		t.Fatal(err)
	}
	renewedClaims, err := joinedauth.Verify(s.cfg.JoinedWorkerSigningKey, heartbeatResponse.OperationToken, time.Now().UTC())
	if err != nil || !heartbeatResponse.ExpiresAt.Equal(databaseLeaseExpires) || renewedClaims.ExpiresAt != databaseLeaseExpires.Unix() {
		t.Fatalf("heartbeat auth expiry differs from DB lease: response=%s db=%s claims=%d err=%v",
			heartbeatResponse.ExpiresAt, databaseLeaseExpires, renewedClaims.ExpiresAt, err)
	}
	callSourceCapability := func(claimToken uuid.UUID, sourceOperation string) *httptest.ResponseRecorder {
		t.Helper()
		operation := joinedauth.OperationPreflight
		if claimToken == publicationClaim {
			operation = joinedauth.OperationPublish
		}
		jobToken, err := joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, batchID, joinedauth.SubjectHour,
			hourCanonicalIDs[0], claimToken, operation, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(joinedSourceCapabilityRequest{ProtocolVersion: joinedWorkerProtocolVersion, HourID: hourCanonicalIDs[0], ClipID: 9001, Operation: sourceOperation})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/capabilities/source", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+jobToken)
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedSourceCapability)).ServeHTTP(rec, req)
		return rec
	}
	preflightSourceCapability := callSourceCapability(claim, "get")
	if preflightSourceCapability.Code != http.StatusOK || !strings.Contains(preflightSourceCapability.Body.String(), `"operation":"get"`) {
		t.Fatalf("preflight source capability status=%d body=%s", preflightSourceCapability.Code, preflightSourceCapability.Body.String())
	}
	assertJoinedCapabilityExpiry(t, preflightSourceCapability, 15*time.Minute, &databaseLeaseExpires, false)
	preflightHeadCapability := callSourceCapability(claim, "head")
	if preflightHeadCapability.Code != http.StatusOK || !strings.Contains(preflightHeadCapability.Body.String(), `"operation":"head"`) ||
		!strings.Contains(preflightHeadCapability.Body.String(), `"method":"HEAD"`) {
		t.Fatalf("preflight source HEAD capability status=%d body=%s", preflightHeadCapability.Code, preflightHeadCapability.Body.String())
	}
	assertJoinedCapabilityExpiry(t, preflightHeadCapability, 15*time.Minute, &databaseLeaseExpires, false)
	outputSHA, planSHA := strings.Repeat("b", 64), strings.Repeat("7", 64)
	hourID := fmt.Sprintf("%s__recording-%d__date-%s__hour-01__generation-1", batchID, recordingID, localDates[13])
	contentID := outputSHA
	relativePath := "site/May/Monday/hour_01_part_01_0800-0900.mp4"
	objectKey := "joined/" + batchID + "/objects/" + contentID + ".mp4"
	sourceClaimSHA := sourceSHA
	preflightTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = preflightTx.Rollback(ctx) }()
	var outputID, manifestID int64
	if err := preflightTx.QueryRow(ctx, `
		INSERT INTO recording_joined_outputs(hour_id,account_id,connection_id,batch_id,artifact_kind,content_type,
		  hour_seal_token,part_ordinal,relative_path,content_id,expected_size_bytes,expected_sha256,object_key,
		  source_claim_sha256,source_clip_count,source_bytes,
		  coverage_start_at,coverage_end_at,plan_facts)
		VALUES($1,$2,$3,$4,'media','video/mp4',$5,1,$6,$7,1000,$7,$8,$9,1,100,$10,$11,
		  '{"policy_version":"test-v1","ffmpeg_version":"test","ffprobe_version":"test","maximality_evidence":[]}') RETURNING id`,
		hours[0], accountID, connectionID, batchID, claim, relativePath, contentID, objectKey, sourceClaimSHA,
		start, start.Add(time.Hour)).Scan(&outputID); err != nil {
		t.Fatal(err)
	}
	if _, err := preflightTx.Exec(ctx, `INSERT INTO recording_joined_output_sources(output_id,hour_id,ordinal,clip_id,
		hour_source_ordinal,seam_facts)
		VALUES($1,$2,1,9001,1,'{"verdict":"not_applicable"}')`, outputID, hours[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := preflightTx.Exec(ctx, `INSERT INTO recording_joined_hour_source_dispositions(hour_id,clip_id,disposition,output_id,evidence_facts)
		VALUES($1,9001,'included',$2,'{}')`, hours[0], outputID); err != nil {
		t.Fatal(err)
	}
	manifestPath := "coverage/hours/" + hourID + ".json"
	if err := preflightTx.QueryRow(ctx, `INSERT INTO recording_joined_outputs(hour_id,account_id,connection_id,batch_id,
		artifact_kind,content_type,hour_seal_token,relative_path,content_id,expected_size_bytes,expected_sha256,
		object_key,source_clip_count,source_bytes,coverage_start_at,coverage_end_at,plan_facts)
		VALUES($1::bigint,$2,$3,$4,'hour_manifest','application/json',$5,$6,$7,500,$7,$8,1,100,$9,$10,
		  jsonb_build_object('schema_version',1,'quarantine_reason_code','','batch_id',$4::text,'hour_id',$13::text,
		    'source_manifest_sha256',$11::text,
		    'local_date',$12::text,'delivery_hour','1','ledger_artifact_id',(SELECT o.id::text FROM recording_joined_outputs o
		      WHERE o.stream_day_id=(SELECT stream_day_id FROM recording_joined_hour_sources WHERE hour_id=$1 LIMIT 1) AND o.artifact_kind='allocation_ledger'),
		    'ledger_relative_path',(SELECT o.relative_path FROM recording_joined_outputs o
		      WHERE o.stream_day_id=(SELECT stream_day_id FROM recording_joined_hour_sources WHERE hour_id=$1 LIMIT 1) AND o.artifact_kind='allocation_ledger'),
		    'ledger_sha256',(SELECT o.expected_sha256 FROM recording_joined_outputs o
		      WHERE o.stream_day_id=(SELECT stream_day_id FROM recording_joined_hour_sources WHERE hour_id=$1 LIMIT 1) AND o.artifact_kind='allocation_ledger'))) RETURNING id`, hours[0], accountID, connectionID, batchID, claim, manifestPath,
		planSHA, "joined/"+batchID+"/coverage/hours/"+hourID+".json", start, start.Add(time.Hour), sourceSHA, localDates[13], hourID).Scan(&manifestID); err != nil {
		t.Fatal(err)
	}
	if _, err := preflightTx.Exec(ctx, `UPDATE recording_joined_hours SET planned_output_count=1,hour_manifest_sha256=$2,
		state='sealed',seal_token=claim_token,claim_token=NULL,claimed_by=NULL,lease_expires_at=NULL,heartbeat_at=NULL,
		sealed_at=now() WHERE id=$1`, hours[0], planSHA); err != nil {
		t.Fatal(err)
	}
	if err := preflightTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if rec := callSourceCapability(claim, "get"); rec.Code != http.StatusConflict {
		t.Fatalf("pre-seal token retained source authority status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='publishing',publish_attempt_count=1,
		publish_claim_token=$2,publish_claimed_by='publisher-1',publish_lease_expires_at=now()+interval '10 minutes',
		publish_heartbeat_at=now() WHERE id=$1`, hours[0], publicationClaim); err != nil {
		t.Fatal(err)
	}
	publicationSourceCapability := callSourceCapability(publicationClaim, "get")
	if publicationSourceCapability.Code != http.StatusOK || !strings.Contains(publicationSourceCapability.Body.String(), `"operation":"get"`) {
		t.Fatalf("publication rebuild source capability status=%d body=%s", publicationSourceCapability.Code, publicationSourceCapability.Body.String())
	}
	var publicationLeaseExpires time.Time
	if err := pool.QueryRow(ctx, `SELECT publish_lease_expires_at FROM recording_joined_hours WHERE id=$1`, hours[0]).Scan(&publicationLeaseExpires); err != nil {
		t.Fatal(err)
	}
	assertJoinedCapabilityExpiry(t, publicationSourceCapability, 15*time.Minute, &publicationLeaseExpires, false)
	callArtifactPutCapability := func(claimToken uuid.UUID, extra string) *httptest.ResponseRecorder {
		t.Helper()
		jobToken, err := joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, batchID, joinedauth.SubjectHour,
			hourCanonicalIDs[0], claimToken, joinedauth.OperationPublish, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		body := fmt.Sprintf(`{"protocol_version":1,"scope_kind":"hour","scope_id":%q,"artifact_id":%d,"operation":"put"%s}`,
			hourCanonicalIDs[0], outputID, extra)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/capabilities/artifact", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+jobToken)
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedArtifactCapability)).ServeHTTP(rec, req)
		return rec
	}
	if rec := callArtifactPutCapability(claim, ""); rec.Code != http.StatusConflict {
		t.Fatalf("pre-seal token retained output authority status=%d body=%s", rec.Code, rec.Body.String())
	}
	artifactCapability := callArtifactPutCapability(publicationClaim, "")
	if artifactCapability.Code != http.StatusOK || !strings.Contains(artifactCapability.Body.String(), objectKey) ||
		!strings.Contains(artifactCapability.Body.String(), `"If-None-Match":"*"`) {
		t.Fatalf("sealed output capability status=%d body=%s", artifactCapability.Code, artifactCapability.Body.String())
	}
	assertJoinedCapabilityExpiry(t, artifactCapability, time.Hour, nil, true)
	s.joinedOutputStorage = joinedOutputStoreStub{extraPutExpirySec: 60}
	skewedArtifactPutCapability := callArtifactPutCapability(publicationClaim, "")
	s.joinedOutputStorage = nil
	if skewedArtifactPutCapability.Code != http.StatusBadGateway || strings.Contains(skewedArtifactPutCapability.Body.String(), `"request"`) {
		t.Fatalf("signed PUT beyond one-hour cap returned authority status=%d body=%s",
			skewedArtifactPutCapability.Code, skewedArtifactPutCapability.Body.String())
	}
	callArtifactReadCapability := func() *httptest.ResponseRecorder {
		t.Helper()
		jobToken, err := joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, batchID, joinedauth.SubjectHour,
			hourCanonicalIDs[0], publicationClaim, joinedauth.OperationPublish, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		body := fmt.Sprintf(`{"protocol_version":1,"scope_kind":"hour","scope_id":%q,"artifact_id":%d,"operation":"read"}`,
			hourCanonicalIDs[0], outputID)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/capabilities/artifact", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+jobToken)
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedArtifactCapability)).ServeHTTP(rec, req)
		return rec
	}
	s.joinedOutputStorage = joinedOutputStoreStub{head: r2.ObjectHead{ETag: "output-etag", VersionID: "output-version", SizeBytes: 1000}}
	artifactReadCapability := callArtifactReadCapability()
	s.joinedOutputStorage = nil
	if artifactReadCapability.Code != http.StatusOK || !strings.Contains(artifactReadCapability.Body.String(), `"method":"GET"`) {
		t.Fatalf("sealed output read capability status=%d body=%s", artifactReadCapability.Code, artifactReadCapability.Body.String())
	}
	assertJoinedCapabilityExpiry(t, artifactReadCapability, 15*time.Minute, &publicationLeaseExpires, false)
	s.joinedOutputStorage = joinedOutputStoreStub{
		head:              r2.ObjectHead{ETag: "output-etag", VersionID: "output-version", SizeBytes: 1000},
		extraGetExpirySec: 60,
	}
	skewedArtifactReadCapability := callArtifactReadCapability()
	s.joinedOutputStorage = nil
	if skewedArtifactReadCapability.Code != http.StatusBadGateway || strings.Contains(skewedArtifactReadCapability.Body.String(), `"request"`) {
		t.Fatalf("signed read beyond DB lease returned authority status=%d body=%s",
			skewedArtifactReadCapability.Code, skewedArtifactReadCapability.Body.String())
	}
	publicationToken, err := joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, batchID, joinedauth.SubjectHour,
		hourCanonicalIDs[0], publicationClaim, joinedauth.OperationPublish, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	withConcurrentProtocolRevocation(func() error {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/heartbeat", bytes.NewReader(heartbeatBody))
		req.Header.Set("Authorization", "Bearer "+publicationToken)
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedHeartbeat)).ServeHTTP(rec, req)
		if rec.Code != http.StatusConflict {
			return fmt.Errorf("protocol-v0 heartbeat status=%d body=%s", rec.Code, rec.Body.String())
		}
		return nil
	})
	withConcurrentProtocolRevocation(func() error {
		rec := callSourceCapability(publicationClaim, "get")
		if rec.Code != http.StatusConflict || strings.Contains(rec.Body.String(), `"request"`) {
			return fmt.Errorf("source capability survived concurrent protocol revocation: status=%d body=%s", rec.Code, rec.Body.String())
		}
		return nil
	})
	withConcurrentProtocolRevocation(func() error {
		rec := callArtifactPutCapability(publicationClaim, "")
		if rec.Code != http.StatusConflict || strings.Contains(rec.Body.String(), `"request"`) {
			return fmt.Errorf("artifact capability survived concurrent protocol revocation: status=%d body=%s", rec.Code, rec.Body.String())
		}
		return nil
	})
	downloadStorage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("ETag", `"ledger-etag"`)
		w.Header().Set("Content-Length", "200")
		w.Header().Set("x-amz-version-id", "ledger-version")
		w.WriteHeader(http.StatusOK)
	}))
	defer downloadStorage.Close()
	downloadR2, err := r2.New(ctx, r2.Config{Endpoint: downloadStorage.URL, Region: "auto", Bucket: "joined-output",
		AccessKey: "download-key", SecretKey: "download-secret"})
	if err != nil {
		t.Fatal(err)
	}
	savedOutputR2 := s.r2
	s.r2 = downloadR2
	withConcurrentProtocolRevocation(func() error {
		ledger := ledgers[0]
		routeContext := chi.NewRouteContext()
		routeContext.URLParams.Add("joinedId", fmt.Sprint(ledger.artifactID))
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/account/joined/%d/download", ledger.artifactID), nil)
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeContext))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
		rec := httptest.NewRecorder()
		s.handleAccountJoinedDownload(rec, req)
		if rec.Code != http.StatusConflict || strings.Contains(rec.Body.String(), `"url"`) {
			return fmt.Errorf("joined download survived concurrent protocol revocation: status=%d body=%s", rec.Code, rec.Body.String())
		}
		return nil
	})
	s.r2 = savedOutputR2
	// Protocol revocation is immediate even for an otherwise valid, unexpired
	// publication token. Nil dependencies prove neither storage path is reached.
	if _, err := pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=0 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	savedR2, savedSecrets := s.r2, s.secrets
	s.r2, s.secrets = nil, nil
	protocolHeartbeat := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/heartbeat", bytes.NewReader(heartbeatBody))
	protocolHeartbeat.Header.Set("Authorization", "Bearer "+publicationToken)
	protocolHeartbeatRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedHeartbeat)).ServeHTTP(protocolHeartbeatRec, protocolHeartbeat)
	if protocolHeartbeatRec.Code != http.StatusConflict {
		t.Fatalf("protocol-v0 heartbeat status=%d body=%s", protocolHeartbeatRec.Code, protocolHeartbeatRec.Body.String())
	}
	if rec := callSourceCapability(publicationClaim, "get"); rec.Code != http.StatusConflict {
		t.Fatalf("protocol-v0 source capability status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := callArtifactPutCapability(publicationClaim, ""); rec.Code != http.StatusConflict {
		t.Fatalf("protocol-v0 artifact capability status=%d body=%s", rec.Code, rec.Body.String())
	}
	s.r2, s.secrets = savedR2, savedSecrets
	if _, err := pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=1 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	if rec := callArtifactPutCapability(publicationClaim, `,"object_key":"attacker/key"`); rec.Code != http.StatusBadRequest {
		t.Fatalf("caller-supplied object coordinates status=%d body=%s", rec.Code, rec.Body.String())
	}

	wrong := claim // the pre-seal claim is not a publication lease
	ct, err := pool.Exec(ctx, `UPDATE recording_joined_outputs o SET state='published',size_bytes=1000,sha256=$3,
		r2_etag='output-etag',r2_version_id='output-version',published_at=now()
		WHERE o.id=$1 AND EXISTS(SELECT 1 FROM recording_joined_hours h WHERE h.id=o.hour_id AND h.state='publishing'
		  AND h.publish_claim_token=$2 AND h.publish_lease_expires_at>now())`, outputID, wrong, outputSHA)
	if err != nil || ct.RowsAffected() != 0 {
		t.Fatalf("stale publish fence rows=%d err=%v", ct.RowsAffected(), err)
	}
	ct, err = pool.Exec(ctx, `UPDATE recording_joined_outputs o SET state='published',size_bytes=1000,sha256=$3,
		r2_etag='output-etag',r2_version_id='output-version',published_at=now()
		WHERE o.id=$1 AND EXISTS(SELECT 1 FROM recording_joined_hours h WHERE h.id=o.hour_id AND h.state='publishing'
		  AND h.publish_claim_token=$2 AND h.publish_lease_expires_at>now())`, outputID, publicationClaim, outputSHA)
	if err != nil || ct.RowsAffected() != 1 {
		t.Fatalf("publish rows=%d err=%v", ct.RowsAffected(), err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_outputs SET state='published',size_bytes=500,sha256=$2,
		r2_etag='manifest-etag',r2_version_id='manifest-version',published_at=now() WHERE id=$1`, manifestID, planSHA); err != nil {
		t.Fatal(err)
	}
	if item := callFeed()["item"]; item != nil {
		t.Fatalf("part became visible before whole hour published: %v", item)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='published',publish_claim_token=NULL,publish_claimed_by=NULL,
		publish_lease_expires_at=NULL,publish_heartbeat_at=NULL,published_at=now() WHERE id=$1`, hours[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=0 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	if item := callFeed()["item"]; item != nil {
		t.Fatalf("joined artifacts leaked before capability activation: %v", item)
	}
	dormantAck, _ := json.Marshal(joinedAckRequest{ArtifactID: manifestID, RelativePath: manifestPath, SizeBytes: 500, SHA256: planSHA})
	dormantReq := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", bytes.NewReader(dormantAck))
	dormantReq = dormantReq.WithContext(context.WithValue(dormantReq.Context(), accountPrincipalContextKey, principal))
	dormantRec := httptest.NewRecorder()
	s.handleAccountJoinedAck(dormantRec, dormantReq)
	if dormantRec.Code != http.StatusNotFound {
		t.Fatalf("dormant joined ACK status=%d body=%s", dormantRec.Code, dormantRec.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET joined_protocol_version=1 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	firstItem, _ := callFeed()["item"].(map[string]any)
	secondItem, _ := callFeed()["item"].(map[string]any)
	if firstItem["kind"] != "hour_manifest" || firstItem["artifact_id"] != secondItem["artifact_id"] ||
		firstItem["ledger_artifact_id"] == nil || firstItem["ledger_relative_path"] == nil || firstItem["ledger_sha256"] == nil {
		t.Fatalf("published manifest did not repeat first: %v / %v", firstItem, secondItem)
	}
	manifestAck := joinedAckRequest{ArtifactID: manifestID, RelativePath: manifestPath, SizeBytes: 500, SHA256: planSHA}
	if already, _ := callAck(manifestAck)["already_verified"].(bool); already {
		t.Fatal("first ack reported already verified")
	}
	if already, _ := callAck(manifestAck)["already_verified"].(bool); !already {
		t.Fatal("idempotent ack was not recognized")
	}
	mediaItem, _ := callFeed()["item"].(map[string]any)
	if mediaItem["kind"] != "media" || mediaItem["hour_manifest_id"] != float64(manifestID) ||
		mediaItem["hour_manifest_relative_path"] != manifestPath || mediaItem["hour_manifest_sha256"] != planSHA {
		t.Fatalf("media was not exactly bound to installed hour manifest: %v", mediaItem)
	}
	mediaAck := joinedAckRequest{ArtifactID: outputID, RelativePath: relativePath, SizeBytes: 1000, SHA256: outputSHA}
	if already, _ := callAck(mediaAck)["already_verified"].(bool); already {
		t.Fatal("first media ack reported already verified")
	}
	if item := callFeed()["item"]; item != nil {
		t.Fatalf("acknowledged output remained in feed: %v", item)
	}
	var gapHourID int64
	var gapHourCanonical string
	if err := pool.QueryRow(ctx, `SELECT id,canonical_hour_id FROM recording_joined_hours WHERE connection_id=$1 AND batch_id=$2
		AND source_clip_count=0 ORDER BY local_date,delivery_hour LIMIT 1`, connectionID, batchID).Scan(&gapHourID, &gapHourCanonical); err != nil {
		t.Fatal(err)
	}
	gapClaim, gapPublicationClaim := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='leased',attempt_count=1,claim_token=$2,
		claimed_by='worker-gap',lease_expires_at=now()+interval '10 minutes',heartbeat_at=now() WHERE id=$1`, gapHourID, gapClaim); err != nil {
		t.Fatal(err)
	}
	gapSHA := strings.Repeat("8", 64)
	gapManifestPath := "coverage/hours/" + gapHourCanonical + ".json"
	var gapManifestID int64
	gapPlanTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gapPlanTx.Rollback(ctx) }()
	if err := gapPlanTx.QueryRow(ctx, `INSERT INTO recording_joined_outputs(hour_id,account_id,connection_id,batch_id,
		artifact_kind,content_type,hour_seal_token,relative_path,content_id,expected_size_bytes,expected_sha256,
		object_key,source_clip_count,source_bytes,coverage_start_at,coverage_end_at,plan_facts)
		SELECT id,account_id,connection_id,batch_id,'hour_manifest','application/json',$2,
		  $4::text,$3,300,$3,'joined/'||batch_id||'/coverage/hours/'||$5::text||'.json',0,0,
		  hour_start_at,hour_end_at,jsonb_build_object('schema_version',1,'quarantine_reason_code','',
		    'batch_id',batch_id,'hour_id',$5::text,
		    'source_manifest_sha256',source_manifest_sha256,
		    'local_date',local_date::text,'delivery_hour',delivery_hour::text,
		    'ledger_artifact_id',(SELECT o.id::text FROM recording_joined_outputs o JOIN recording_joined_stream_days d ON d.id=o.stream_day_id
		      WHERE o.artifact_kind='allocation_ledger' AND d.connection_id=recording_joined_hours.connection_id
		        AND d.batch_id=recording_joined_hours.batch_id AND d.recording_id=recording_joined_hours.recording_id AND d.local_date=recording_joined_hours.local_date),
		    'ledger_relative_path',(SELECT o.relative_path FROM recording_joined_outputs o JOIN recording_joined_stream_days d ON d.id=o.stream_day_id
		      WHERE o.artifact_kind='allocation_ledger' AND d.connection_id=recording_joined_hours.connection_id
		        AND d.batch_id=recording_joined_hours.batch_id AND d.recording_id=recording_joined_hours.recording_id AND d.local_date=recording_joined_hours.local_date),
		    'ledger_sha256',(SELECT o.expected_sha256 FROM recording_joined_outputs o JOIN recording_joined_stream_days d ON d.id=o.stream_day_id
		      WHERE o.artifact_kind='allocation_ledger' AND d.connection_id=recording_joined_hours.connection_id
		        AND d.batch_id=recording_joined_hours.batch_id AND d.recording_id=recording_joined_hours.recording_id AND d.local_date=recording_joined_hours.local_date))
		  FROM recording_joined_hours WHERE id=$1 RETURNING id`,
		gapHourID, gapClaim, gapSHA, gapManifestPath, gapHourCanonical).Scan(&gapManifestID); err != nil {
		t.Fatal(err)
	}
	if _, err := gapPlanTx.Exec(ctx, `UPDATE recording_joined_hours SET planned_output_count=0,
		state='sealed',hour_manifest_sha256=$2,seal_token=claim_token,claim_token=NULL,claimed_by=NULL,
		lease_expires_at=NULL,heartbeat_at=NULL,sealed_at=now() WHERE id=$1`, gapHourID, gapSHA); err != nil {
		t.Fatal(err)
	}
	if err := gapPlanTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='publishing',publish_attempt_count=1,
		publish_claim_token=$2,publish_claimed_by='gap-publisher',publish_lease_expires_at=now()+interval '10 minutes',
		publish_heartbeat_at=now() WHERE id=$1`, gapHourID, gapPublicationClaim); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_outputs SET state='published',size_bytes=300,sha256=$2,
		r2_etag='gap-etag',r2_version_id='gap-version',published_at=now() WHERE id=$1`, gapManifestID, gapSHA); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='published',publish_claim_token=NULL,publish_claimed_by=NULL,
		publish_lease_expires_at=NULL,publish_heartbeat_at=NULL,published_at=now() WHERE id=$1`, gapHourID); err != nil {
		t.Fatal(err)
	}
	gapItem, _ := callFeed()["item"].(map[string]any)
	if gapItem["kind"] != "hour_manifest" {
		t.Fatalf("gap-only hour did not produce its immutable manifest: %v", gapItem)
	}
	if already, _ := callAck(joinedAckRequest{ArtifactID: gapManifestID, RelativePath: gapManifestPath,
		SizeBytes: 300, SHA256: gapSHA})["already_verified"].(bool); already {
		t.Fatal("first gap manifest ack reported already verified")
	}
	partialClaim := uuid.New()
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='leased',attempt_count=1,claim_token=$2,
		claimed_by='worker-partial',lease_expires_at=now()+interval '10 minutes',heartbeat_at=now() WHERE id=$1`,
		hours[1], partialClaim); err != nil {
		t.Fatal(err)
	}
	partialTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	partialSHA := strings.Repeat("0", 64)
	if _, err := partialTx.Exec(ctx, `INSERT INTO recording_joined_outputs(hour_id,account_id,connection_id,batch_id,
		artifact_kind,content_type,hour_seal_token,part_ordinal,relative_path,content_id,expected_size_bytes,expected_sha256,object_key,source_claim_sha256,
		source_clip_count,source_bytes,coverage_start_at,coverage_end_at,plan_facts)
		VALUES($1,$2,$3,$4,'media','video/mp4',$5,1,'partial.mp4',$6,1000,$6,$7,$8,1,100,$9,$10,
		  '{"policy_version":"test-v1","ffmpeg_version":"test","ffprobe_version":"test"}')`,
		hours[1], accountID, connectionID, batchID, partialClaim, partialSHA,
		"joined/"+batchID+"/objects/"+partialSHA+".mp4", sourceSHA, start.Add(time.Hour), start.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := partialTx.Commit(ctx); err == nil {
		t.Fatal("partial append-only output plan committed without its hour seal")
	}
	quarantineSHA := strings.Repeat("c", 64)
	quarantinePublicationClaim := uuid.New()
	quarantineHourID := fmt.Sprintf("%s__recording-%d__date-%s__hour-02__generation-1", batchID, recordingID, localDates[13])
	quarantinePath := "coverage/hours/" + quarantineHourID + ".json"
	quarantineTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = quarantineTx.Rollback(ctx) }()
	var quarantineManifestID int64
	if _, err := quarantineTx.Exec(ctx, `INSERT INTO recording_joined_hour_source_dispositions(
		hour_id,clip_id,disposition,reason_code,evidence_sha256,evidence_facts)
		VALUES($1,9002,'quarantined','decode_failed',$2,jsonb_build_object('schema_version',1,
		  'classification','deterministic_media_failure','isolated_build_count',2,'source_claim_sha256',$3::text,
		  'policy_sha256',$4::text,'media_tool_sha256',$5::text,'expected_facts_sha256',$6::text,
		  'observed_facts_sha256',$7::text,'repeat_digest_sha256',$2::text))`, hours[1], strings.Repeat("4", 64),
		sourceSHA, strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64), strings.Repeat("5", 64)); err != nil {
		t.Fatal(err)
	}
	if err := quarantineTx.QueryRow(ctx, `INSERT INTO recording_joined_outputs(hour_id,account_id,connection_id,batch_id,
		artifact_kind,content_type,hour_seal_token,relative_path,content_id,expected_size_bytes,expected_sha256,
		object_key,source_clip_count,source_bytes,coverage_start_at,coverage_end_at,plan_facts)
		SELECT id,account_id,connection_id,batch_id,'hour_manifest','application/json',$2,$3,$4,400,$4,
		  'joined/'||batch_id||'/coverage/hours/'||$5||'.json',source_clip_count,source_bytes,hour_start_at,hour_end_at,
		  jsonb_build_object('schema_version',1,'quarantine_reason_code','decode_failed','batch_id',batch_id,
		    'hour_id',$5,'source_manifest_sha256',source_manifest_sha256,'local_date',local_date::text,
		    'delivery_hour',delivery_hour,'ledger_artifact_id',(SELECT o.id::text FROM recording_joined_outputs o JOIN recording_joined_stream_days d ON d.id=o.stream_day_id
		      WHERE o.artifact_kind='allocation_ledger' AND d.connection_id=recording_joined_hours.connection_id
		        AND d.batch_id=recording_joined_hours.batch_id AND d.recording_id=recording_joined_hours.recording_id AND d.local_date=recording_joined_hours.local_date),
		    'ledger_relative_path',(SELECT o.relative_path FROM recording_joined_outputs o JOIN recording_joined_stream_days d ON d.id=o.stream_day_id
		      WHERE o.artifact_kind='allocation_ledger' AND d.connection_id=recording_joined_hours.connection_id
		        AND d.batch_id=recording_joined_hours.batch_id AND d.recording_id=recording_joined_hours.recording_id AND d.local_date=recording_joined_hours.local_date),
		    'ledger_sha256',(SELECT o.expected_sha256 FROM recording_joined_outputs o JOIN recording_joined_stream_days d ON d.id=o.stream_day_id
		      WHERE o.artifact_kind='allocation_ledger' AND d.connection_id=recording_joined_hours.connection_id
		        AND d.batch_id=recording_joined_hours.batch_id AND d.recording_id=recording_joined_hours.recording_id AND d.local_date=recording_joined_hours.local_date))
		FROM recording_joined_hours WHERE id=$1 RETURNING id`, hours[1], partialClaim, quarantinePath, quarantineSHA,
		quarantineHourID).Scan(&quarantineManifestID); err != nil {
		t.Fatal(err)
	}
	if _, err := quarantineTx.Exec(ctx, `UPDATE recording_joined_hours SET planned_output_count=0,
		state='sealed',hour_manifest_sha256=$2,seal_token=claim_token,claim_token=NULL,claimed_by=NULL,
		lease_expires_at=NULL,heartbeat_at=NULL,
		quarantine_reason_code='decode_failed',sealed_at=now() WHERE id=$1`,
		hours[1], quarantineSHA); err != nil {
		t.Fatal(err)
	}
	if err := quarantineTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='publishing',publish_attempt_count=1,
		publish_claim_token=$2,publish_claimed_by='quarantine-publisher',publish_lease_expires_at=now()+interval '10 minutes',
		publish_heartbeat_at=now() WHERE id=$1`, hours[1], quarantinePublicationClaim); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_outputs SET state='published',size_bytes=400,sha256=$2,
		r2_etag='quarantine-etag',r2_version_id='quarantine-version',published_at=now() WHERE id=$1`,
		quarantineManifestID, quarantineSHA); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='published',publish_claim_token=NULL,publish_claimed_by=NULL,
		publish_lease_expires_at=NULL,publish_heartbeat_at=NULL,published_at=now() WHERE id=$1`, hours[1]); err != nil {
		t.Fatal(err)
	}
	quarantineItem, _ := callFeed()["item"].(map[string]any)
	if quarantineItem["kind"] != "hour_manifest" {
		t.Fatalf("nonempty quarantined hour did not publish its manifest: %v", quarantineItem)
	}
	callAck(joinedAckRequest{ArtifactID: quarantineManifestID, RelativePath: quarantinePath, SizeBytes: 400, SHA256: quarantineSHA})
	var reclaimHourID int64
	var reclaimHourCanonical string
	if err := pool.QueryRow(ctx, `SELECT id,canonical_hour_id FROM recording_joined_hours WHERE connection_id=$1 AND batch_id=$2
		AND state='pending' AND source_clip_count=0 ORDER BY priority_order LIMIT 1`, connectionID, batchID).Scan(&reclaimHourID, &reclaimHourCanonical); err != nil {
		t.Fatal(err)
	}
	oldLeaseToken, initialPublishToken := uuid.New(), uuid.New()
	var oldLeaseExpires time.Time
	if err := pool.QueryRow(ctx, `UPDATE recording_joined_hours SET state='leased',attempt_count=1,claim_token=$2,
		claimed_by='worker-old',lease_expires_at=now()+interval '1 microsecond',heartbeat_at=now() WHERE id=$1
		RETURNING lease_expires_at`, reclaimHourID, oldLeaseToken).Scan(&oldLeaseExpires); err != nil {
		t.Fatal(err)
	}
	var leaseExpired bool
	if err := pool.QueryRow(ctx, `SELECT lease_expires_at<=now() FROM recording_joined_hours WHERE id=$1`, reclaimHourID).Scan(&leaseExpired); err != nil || !leaseExpired {
		t.Fatalf("normal fixture lease did not expire before reclaim: expired=%v err=%v", leaseExpired, err)
	}
	oldOperationToken, err := joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, batchID, joinedauth.SubjectHour,
		reclaimHourCanonical, oldLeaseToken, joinedauth.OperationPreflight, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	const claimBarrierKey int64 = 771337
	if _, err := pool.Exec(ctx, `CREATE FUNCTION joined_test_claim_barrier() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		  IF NEW.claimed_by='worker-reclaim' AND OLD.state='leased' AND OLD.lease_expires_at<=now() THEN
		    PERFORM pg_advisory_xact_lock(771337);
		  END IF;
		  RETURN NEW;
		END $$;
		CREATE TRIGGER joined_test_claim_barrier AFTER UPDATE ON recording_joined_hours
		FOR EACH ROW EXECUTE FUNCTION joined_test_claim_barrier()`); err != nil {
		t.Fatal(err)
	}
	barrierConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer barrierConn.Release()
	if _, err := barrierConn.Exec(ctx, `SELECT pg_advisory_lock($1)`, claimBarrierKey); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = barrierConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, claimBarrierKey)
	}()
	var blockerPID int
	if err := barrierConn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		t.Fatal(err)
	}
	reclaimBody, _ := json.Marshal(joinedClaimRequest{ProtocolVersion: joinedWorkerProtocolVersion, BatchID: batchID, WorkerID: "worker-reclaim"})
	reclaimDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		reclaimReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/claim", bytes.NewReader(reclaimBody))
		reclaimReq.Header.Set("Authorization", "Bearer "+tokenResponse.Item.ClaimToken)
		reclaimRec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedClaim)).ServeHTTP(reclaimRec, reclaimReq)
		reclaimDone <- reclaimRec
	}()
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var reclaimPID int
	for reclaimPID == 0 {
		if err := pool.QueryRow(waitCtx, `SELECT COALESCE((SELECT pid FROM pg_stat_activity a
			WHERE $1=ANY(pg_blocking_pids(a.pid)) ORDER BY pid LIMIT 1),0)`, blockerPID).Scan(&reclaimPID); err != nil {
			t.Fatal(err)
		}
		select {
		case rec := <-reclaimDone:
			t.Fatalf("reclaim returned before its commit barrier: status=%d body=%s", rec.Code, rec.Body.String())
		default:
		}
	}
	staleHeartbeatBody, _ := json.Marshal(joinedHeartbeatRequest{ProtocolVersion: joinedWorkerProtocolVersion,
		ScopeKind: joinedauth.SubjectHour, ScopeID: reclaimHourCanonical})
	staleHeartbeatDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		staleHeartbeatReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/heartbeat", bytes.NewReader(staleHeartbeatBody))
		staleHeartbeatReq.Header.Set("Authorization", "Bearer "+oldOperationToken)
		staleHeartbeatRec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedHeartbeat)).ServeHTTP(staleHeartbeatRec, staleHeartbeatReq)
		staleHeartbeatDone <- staleHeartbeatRec
	}()
	for {
		var blocked bool
		if err := pool.QueryRow(waitCtx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity a
			WHERE $1=ANY(pg_blocking_pids(a.pid)))`, reclaimPID).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case rec := <-staleHeartbeatDone:
			t.Fatalf("stale heartbeat returned before reclaim committed: status=%d body=%s", rec.Code, rec.Body.String())
		default:
		}
	}
	if _, err := barrierConn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, claimBarrierKey); err != nil {
		t.Fatal(err)
	}
	var reclaimRec, staleHeartbeatRec *httptest.ResponseRecorder
	select {
	case reclaimRec = <-reclaimDone:
	case <-waitCtx.Done():
		t.Fatal("actual reclaim deadlocked")
	}
	select {
	case staleHeartbeatRec = <-staleHeartbeatDone:
	case <-waitCtx.Done():
		t.Fatal("stale heartbeat deadlocked after reclaim")
	}
	var reclaimed struct {
		Item *joinedClaimItem `json:"item"`
	}
	if reclaimRec.Code != http.StatusOK || json.Unmarshal(reclaimRec.Body.Bytes(), &reclaimed) != nil ||
		reclaimed.Item == nil || reclaimed.Item.HourID != reclaimHourCanonical || reclaimed.Item.LeaseID == joinedauth.LeaseID(oldLeaseToken) {
		t.Fatalf("actual expired hour reclaim status=%d body=%s", reclaimRec.Code, reclaimRec.Body.String())
	}
	var reclaimedLeaseToken uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT claim_token FROM recording_joined_hours WHERE id=$1`, reclaimHourID).Scan(&reclaimedLeaseToken); err != nil {
		t.Fatal(err)
	}
	if staleHeartbeatRec.Code != http.StatusConflict || strings.Contains(staleHeartbeatRec.Body.String(), `"operation_token"`) {
		t.Fatalf("expired lease token survived actual reclaim status=%d body=%s", staleHeartbeatRec.Code, staleHeartbeatRec.Body.String())
	}
	reclaimSHA := strings.Repeat("5", 64)
	reclaimPath := "coverage/hours/" + reclaimHourCanonical + ".json"
	reclaimPlanTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reclaimPlanTx.Rollback(ctx) }()
	var reclaimManifestID int64
	if err := reclaimPlanTx.QueryRow(ctx, `INSERT INTO recording_joined_outputs(hour_id,account_id,connection_id,batch_id,
		artifact_kind,content_type,hour_seal_token,relative_path,content_id,expected_size_bytes,expected_sha256,
		object_key,source_clip_count,source_bytes,coverage_start_at,coverage_end_at,plan_facts)
		SELECT id,account_id,connection_id,batch_id,'hour_manifest','application/json',$2,$3,$4,250,$4,
		  'joined/'||batch_id||'/coverage/hours/'||canonical_hour_id||'.json',0,0,hour_start_at,hour_end_at,
		  jsonb_build_object('schema_version',1,'quarantine_reason_code','','batch_id',batch_id,'hour_id',canonical_hour_id,
		    'source_manifest_sha256',source_manifest_sha256,
		    'local_date',local_date::text,'delivery_hour',delivery_hour,
		    'ledger_artifact_id',(SELECT o.id::text FROM recording_joined_outputs o JOIN recording_joined_stream_days d ON d.id=o.stream_day_id
		      WHERE o.artifact_kind='allocation_ledger' AND d.connection_id=recording_joined_hours.connection_id
		        AND d.batch_id=recording_joined_hours.batch_id AND d.recording_id=recording_joined_hours.recording_id AND d.local_date=recording_joined_hours.local_date),
		    'ledger_relative_path',(SELECT o.relative_path FROM recording_joined_outputs o JOIN recording_joined_stream_days d ON d.id=o.stream_day_id
		      WHERE o.artifact_kind='allocation_ledger' AND d.connection_id=recording_joined_hours.connection_id
		        AND d.batch_id=recording_joined_hours.batch_id AND d.recording_id=recording_joined_hours.recording_id AND d.local_date=recording_joined_hours.local_date),
		    'ledger_sha256',(SELECT o.expected_sha256 FROM recording_joined_outputs o JOIN recording_joined_stream_days d ON d.id=o.stream_day_id
		      WHERE o.artifact_kind='allocation_ledger' AND d.connection_id=recording_joined_hours.connection_id
		        AND d.batch_id=recording_joined_hours.batch_id AND d.recording_id=recording_joined_hours.recording_id AND d.local_date=recording_joined_hours.local_date))
		FROM recording_joined_hours WHERE id=$1 RETURNING id`, reclaimHourID, reclaimedLeaseToken, reclaimPath, reclaimSHA).Scan(&reclaimManifestID); err != nil {
		t.Fatal(err)
	}
	if _, err := reclaimPlanTx.Exec(ctx, `UPDATE recording_joined_hours SET planned_output_count=0,
		state='sealed',hour_manifest_sha256=$2,seal_token=claim_token,claim_token=NULL,claimed_by=NULL,
		lease_expires_at=NULL,heartbeat_at=NULL,sealed_at=now() WHERE id=$1`, reclaimHourID, reclaimSHA); err != nil {
		t.Fatal(err)
	}
	if err := reclaimPlanTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='publishing',publish_attempt_count=1,
		publish_claim_token=$2,publish_claimed_by='publisher-old',publish_lease_expires_at=now()+interval '10 minutes',
		publish_heartbeat_at=now() WHERE id=$1`, reclaimHourID, initialPublishToken); err != nil {
		t.Fatal(err)
	}
	ct, err = pool.Exec(ctx, `UPDATE recording_joined_outputs o SET state='published',size_bytes=250,sha256=$3,
		r2_etag='reclaim-etag',r2_version_id='reclaim-version',published_at=now()
		WHERE o.id=$1 AND EXISTS(SELECT 1 FROM recording_joined_hours h WHERE h.id=o.hour_id AND h.state='publishing'
		  AND h.publish_claim_token=$2 AND h.publish_lease_expires_at>now())`, reclaimManifestID, initialPublishToken, reclaimSHA)
	if err != nil || ct.RowsAffected() != 1 {
		t.Fatalf("reclaimed sealed manifest publish rows=%d err=%v", ct.RowsAffected(), err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='published',publish_claim_token=NULL,publish_claimed_by=NULL,
		publish_lease_expires_at=NULL,publish_heartbeat_at=NULL,published_at=now() WHERE id=$1`, reclaimHourID); err != nil {
		t.Fatal(err)
	}
	callAck(joinedAckRequest{ArtifactID: reclaimManifestID, RelativePath: reclaimPath, SizeBytes: 250, SHA256: reclaimSHA})
	var files, pulledBytes int64
	if err := pool.QueryRow(ctx, `SELECT joined_files_pulled,joined_bytes_pulled FROM connections WHERE id=$1`, connectionID).Scan(&files, &pulledBytes); err != nil {
		t.Fatal(err)
	}
	if files != 19 || pulledBytes != 5250 {
		t.Fatalf("joined totals files=%d bytes=%d", files, pulledBytes)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_outputs SET relative_path='mutated.mp4' WHERE id=$1`, outputID); err == nil {
		t.Fatal("published output identity mutation succeeded")
	}
	indexSHA := strings.Repeat("f", 64)
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_outputs(account_id,connection_id,batch_id,artifact_kind,
		content_type,relative_path,content_id,expected_size_bytes,expected_sha256,object_key,source_clip_count,source_bytes,plan_facts)
		VALUES($1,$2,$3,'batch_index','application/json','coverage/batch.json',$4,1000,$4,
		  'joined/'||$3||'/coverage/batch.json',0,0,jsonb_build_object('schema_version',1,'frozen_denominator_sha256',$5::text,
		  'expected_ledgers','14','expected_hours','168','source_clip_count','2','source_bytes','200'))`,
		accountID, connectionID, batchID, indexSHA, batchSHA); err == nil {
		t.Fatal("partial batch produced a final index before all 168 hours and 14 ledgers were exactly acknowledged")
	}
}
