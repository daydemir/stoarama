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

	"github.com/google/uuid"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

func joinedCanaryScopeForTest(batch string, included ...string) string {
	hours := append([]string(nil), included...)
	for i := 1; len(hours) < 3; i++ {
		candidate := fmt.Sprintf("%s__recording-99999%d__date-2026-08-01__hour-%02d__generation-1", batch, i, i)
		duplicate := false
		for _, hour := range hours {
			if candidate == hour {
				duplicate = true
				break
			}
		}
		if !duplicate {
			hours = append(hours, candidate)
		}
	}
	return strings.Join(hours, ",")
}

func mintJoinedClaimForTest(t *testing.T, s *Server, batchID string) string {
	t.Helper()
	scope, err := s.joinedWorkScopeIdentity()
	if err != nil {
		t.Fatal(err)
	}
	token, err := joinedauth.MintClaim(s.cfg.JoinedWorkerSigningKey, batchID, scope, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestJoinedBootstrapAndClaimScopeDriftFailBeforeDatabaseMutation(t *testing.T) {
	t.Parallel()
	const batchID = "tier1-generation-1"
	firstHour := batchID + "__recording-1__date-2026-08-01__hour-01__generation-1"
	s := &Server{cfg: config.Config{
		JoinedRecordingControlPlaneEnabled: true,
		JoinedRecordingProtocolVersion:     1,
		JoinedRecordingWorkScope:           config.JoinedWorkScopeCanary,
		JoinedRecordingBatchID:             batchID,
		JoinedRecordingCanaryHourIDs:       joinedCanaryScopeForTest(batchID, firstHour),
		JoinedWorkerBootstrapToken:         "joined-bootstrap-credential-32bytes",
		JoinedWorkerSigningKey:             "joined-signing-credential-32-bytes",
	}, joinedCredentialCheck: func(context.Context) error { return nil }}
	frozen, err := joinedrecording.NewWorkScopeIdentity(batchID, joinedrecording.WorkScopeFrozenBatch, nil)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapBody, _ := json.Marshal(joinedrecording.WorkerBootstrapRequest{ProtocolVersion: 1,
		BatchID: batchID, WorkScopeIdentity: frozen})
	bootstrapReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/token", bytes.NewReader(bootstrapBody))
	bootstrapRec := httptest.NewRecorder()
	s.handleJoinedToken(bootstrapRec, bootstrapReq)
	if bootstrapRec.Code != http.StatusForbidden {
		t.Fatalf("bootstrap scope drift status=%d body=%s", bootstrapRec.Code, bootstrapRec.Body.String())
	}
	claim := mintJoinedClaimForTest(t, s, batchID)
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	s.cfg.JoinedRecordingCanaryHourIDs = ""
	for _, tc := range []struct {
		path    string
		handler http.HandlerFunc
	}{
		{path: "/api/v1/recording/joined/publication/claim", handler: s.handleJoinedPublicationClaim},
		{path: "/api/v1/recording/joined/claim", handler: s.handleJoinedClaim},
	} {
		body, _ := json.Marshal(joinedrecording.WorkClaimRequest{ProtocolVersion: 1, BatchID: batchID, WorkerID: "drift-worker"})
		req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+claim)
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(tc.handler).ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("scope drift path=%s status=%d body=%s", tc.path, rec.Code, rec.Body.String())
		}
	}
}

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
	heads             map[string]r2.ObjectHead
	extraGetExpirySec int
	extraPutExpirySec int
	headKeys          *[]string
}

func (s joinedOutputStoreStub) Head(_ context.Context, key string) (r2.ObjectHead, error) {
	if s.headKeys != nil {
		*s.headKeys = append(*s.headKeys, key)
	}
	if head, ok := s.heads[key]; ok {
		return head, nil
	}
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
	const bootstrapCredential = "joined-bootstrap-credential-32bytes"
	const signingCredential = "joined-signing-credential-32-bytes"
	claim := uuid.New()
	token, err := joinedauth.MintOperation(signingCredential, "batch-test", joinedauth.SubjectHour, "hour-test",
		claim, joinedauth.OperationPreflight, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{cfg: config.Config{ServiceToken: "generic-service-key", JoinedWorkerBootstrapToken: bootstrapCredential, JoinedWorkerSigningKey: signingCredential},
		joinedCredentialCheck: func(context.Context) error { return nil }}
	misconfigured := &Server{cfg: config.Config{JoinedRecordingControlPlaneEnabled: true, ServiceToken: "shared-secret",
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
	req.Header.Set("Authorization", "Bearer "+bootstrapCredential)
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
		{name: "joined bootstrap", token: bootstrapCredential, want: http.StatusOK},
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
	s.cfg.JoinedRecordingControlPlaneEnabled = true
	s.cfg.JoinedRecordingProtocolVersion = 1
	s.cfg.JoinedRecordingBatchID = "batch-test"
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeCanary
	s.cfg.JoinedRecordingCanaryHourIDs = joinedCanaryScopeForTest("batch-test", "batch-test__recording-1__date-2026-08-01__hour-01__generation-1")
	bootstrapLeaseRequest := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/token", strings.NewReader(`{"lease_id":"forbidden"}`))
	bootstrapLeaseRequest.Header.Set("Authorization", "Bearer "+bootstrapCredential)
	bootstrapLeaseResponse := httptest.NewRecorder()
	s.requireJoinedWorkerBootstrapAuth(http.HandlerFunc(s.handleJoinedToken)).ServeHTTP(bootstrapLeaseResponse, bootstrapLeaseRequest)
	if bootstrapLeaseResponse.Code != http.StatusBadRequest {
		t.Fatalf("response-only lease_id bootstrap status=%d body=%s", bootstrapLeaseResponse.Code, bootstrapLeaseResponse.Body.String())
	}
	claimAuth := mintJoinedClaimForTest(t, s, "batch-test")
	canaryHourID := strings.Split(s.cfg.JoinedRecordingCanaryHourIDs, ",")[0]
	canaryToken, err := joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, "batch-test", joinedauth.SubjectHour,
		canaryHourID, claim, joinedauth.OperationPreflight, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path, body, auth string
		handler                http.HandlerFunc
	}{
		{name: "claim", path: "/api/v1/recording/joined/claim", body: `{"worker_id":"worker","lease_id":"forbidden"}`, auth: claimAuth, handler: s.handleJoinedClaim},
		{name: "heartbeat", path: "/api/v1/recording/joined/heartbeat", body: `{"scope_kind":"hour","scope_id":"` + canaryHourID + `","lease_id":"forbidden"}`, auth: canaryToken, handler: s.handleJoinedHeartbeat},
		{name: "source capability", path: "/api/v1/recording/joined/capabilities/source", body: `{"hour_id":"` + canaryHourID + `","clip_id":1,"lease_id":"forbidden"}`, auth: canaryToken, handler: s.handleJoinedSourceCapability},
		{name: "artifact capability", path: "/api/v1/recording/joined/capabilities/artifact", body: `{"scope_kind":"hour","scope_id":"` + canaryHourID + `","artifact_id":1,"operation":"put","lease_id":"forbidden"}`, auth: canaryToken, handler: s.handleJoinedArtifactCapability},
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

func TestJoinedOperationTokenIsFencedByCurrentExactCanaryScope(t *testing.T) {
	const batch = "tier1-2026-08"
	inside := batch + "__recording-377__date-2026-08-01__hour-01__generation-1"
	cfg := config.Config{JoinedRecordingControlPlaneEnabled: true, JoinedRecordingProtocolVersion: 1,
		JoinedRecordingBatchID: batch, JoinedRecordingCanaryHourIDs: joinedCanaryScopeForTest(batch, inside),
		JoinedRecordingWorkScope:   config.JoinedWorkScopeCanary,
		JoinedWorkerBootstrapToken: "joined-bootstrap-credential-32bytes",
		JoinedWorkerSigningKey:     "joined-signing-credential-32-bytes"}
	s := &Server{cfg: cfg, joinedCredentialCheck: func(context.Context) error { return nil }}
	call := func(hourID string) int {
		token, err := joinedauth.MintOperation(cfg.JoinedWorkerSigningKey, batch, joinedauth.SubjectHour, hourID,
			uuid.New(), joinedauth.OperationPublish, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/heartbeat", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })).ServeHTTP(rec, req)
		return rec.Code
	}
	if status := call(inside); status != http.StatusNoContent {
		t.Fatalf("current canary operation status=%d", status)
	}
	outside := batch + "__recording-377__date-2026-08-01__hour-04__generation-1"
	if status := call(outside); status != http.StatusUnauthorized {
		t.Fatalf("outside canary operation status=%d", status)
	}
}

func TestJoinedStorageAuthorityRequiresExactHTTPSRoot(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{
		"http://storage.example.test", "https://user@storage.example.test", "https://storage.example.test/path",
		"https://storage.example.test?query=1", "https://storage.example.test/#fragment",
	} {
		if _, err := joinedOutputAuthority(raw); err == nil {
			t.Fatalf("accepted non-root joined storage authority %q", raw)
		}
	}
	if got, err := joinedOutputAuthority("https://storage.example.test/"); err != nil || got != "storage.example.test" {
		t.Fatalf("root authority got=%q err=%v", got, err)
	}
}

func TestSameFrozenJoinedSourcesComparesReleaseTimeValues(t *testing.T) {
	released := time.Date(2026, 8, 21, 1, 2, 3, 4, time.UTC)
	sameValue := released
	frozen := []joinedrecording.SourceClip{{ClipID: 1, ReleasedAt: &released}}
	accounted := []joinedrecording.SourceClip{{ClipID: 1, ReleasedAt: &sameValue}}
	if !sameFrozenJoinedSources(frozen, accounted) {
		t.Fatal("equal release timestamps at different addresses did not match")
	}
	changed := sameValue.Add(time.Nanosecond)
	accounted[0].ReleasedAt = &changed
	if sameFrozenJoinedSources(frozen, accounted) {
		t.Fatal("different release timestamps matched")
	}
	accounted[0].ReleasedAt = nil
	if sameFrozenJoinedSources(frozen, accounted) {
		t.Fatal("nil and non-nil release timestamps matched")
	}
}
