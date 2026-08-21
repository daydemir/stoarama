package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

type joinedFreezeStoreStub struct {
	bucket string
	mu     sync.Mutex
	heads  int
}

func (s *joinedFreezeStoreStub) Bucket() string { return s.bucket }

func (s *joinedFreezeStoreStub) PresignHeadExactRequest(_ context.Context, key, etag, _ string, _ time.Duration) (r2.PresignedRequest, error) {
	s.mu.Lock()
	s.heads++
	s.mu.Unlock()
	return r2.PresignedRequest{Method: http.MethodHead,
		URL:     joinedTestSourceEndpoint + "/" + s.bucket + "/" + key + "?X-Amz-Signature=test",
		Headers: http.Header{"If-Match": []string{`"` + etag + `"`}}}, nil
}

func (s *joinedFreezeStoreStub) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heads
}

type joinedFreezeTransportStub struct {
	mu   sync.Mutex
	etag string
}

func (s *joinedFreezeTransportStub) RoundTrip(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	etag := s.etag
	s.mu.Unlock()
	headers := make(http.Header)
	headers.Set("ETag", `"`+etag+`"`)
	headers.Set("Content-Length", "10")
	headers.Set("x-amz-version-id", "source-version-one")
	return &http.Response{StatusCode: http.StatusOK, Header: headers, ContentLength: 10,
		Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
}

func (s *joinedFreezeTransportStub) setETag(etag string) {
	s.mu.Lock()
	s.etag = etag
	s.mu.Unlock()
}

func TestJoinedStreamDayHEADSealIsAtomicIdempotentAndAdminOnly(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-stream-day@example.test")
	defer fixture.cleanup()
	req := fixture.req
	req.Apply, req.ExpectedRequestSHA256 = true, fixture.plan.RequestSHA256
	if response, _ := fixture.call(req); response.Code != http.StatusOK {
		t.Fatalf("apply status=%d body=%s", response.Code, response.Body.String())
	}
	fixture.s.cfg.ServiceToken = "generic-service-credential-32-bytes"
	fixture.s.cfg.R2Endpoint = joinedTestSourceEndpoint
	fixture.s.cfg.R2Region = "auto"
	fixture.s.cfg.R2Bucket = "clips"
	store := &joinedFreezeStoreStub{bucket: "clips"}
	transport := &joinedFreezeTransportStub{etag: "wrong-etag"}
	fixture.s.joinedFreezeSourceStore = store
	fixture.s.joinedFreezeTransport = transport

	dayRequest := joinedSealStreamDayRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		BatchID: req.BatchID, RecordingID: joinedrecording.Tier1RecordingIDs[0], LocalDate: "2026-08-01"}
	body, _ := json.Marshal(dayRequest)
	call := func(cookie bool) *httptest.ResponseRecorder {
		httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/recording/joined/stream-days/seal", bytes.NewReader(body))
		if cookie {
			httpReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: fixture.sessionToken})
		}
		recorder := httptest.NewRecorder()
		fixture.s.requireAdminAuth(http.HandlerFunc(fixture.s.handleAdminJoinedSealStreamDay)).ServeHTTP(recorder, httpReq)
		return recorder
	}

	claim, err := joinedauth.MintClaim(fixture.s.cfg.JoinedWorkerSigningKey, req.BatchID, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	operation, err := joinedauth.MintOperation(fixture.s.cfg.JoinedWorkerSigningKey, req.BatchID, joinedauth.SubjectHour,
		"foreign-hour", uuid.New(), joinedauth.OperationPreflight, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for name, token := range map[string]string{"missing": "", "generic service": fixture.s.cfg.ServiceToken,
		"joined bootstrap": fixture.s.cfg.JoinedWorkerBootstrapToken, "joined claim": claim, "joined operation": operation} {
		t.Run("rejects "+name, func(t *testing.T) {
			httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/recording/joined/stream-days/seal", bytes.NewReader(body))
			if token != "" {
				httpReq.Header.Set("Authorization", "Bearer "+token)
			}
			recorder := httptest.NewRecorder()
			fixture.s.requireAdminAuth(http.HandlerFunc(fixture.s.handleAdminJoinedSealStreamDay)).ServeHTTP(recorder, httpReq)
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
	if store.count() != 0 {
		t.Fatal("unauthorized day seal reached storage")
	}

	if response := call(true); response.Code != http.StatusConflict {
		t.Fatalf("drifted HEAD status=%d body=%s", response.Code, response.Body.String())
	}
	var state string
	var hours, sources, boundaries, ledgers int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT d.state,
		(SELECT count(*) FROM recording_joined_hours h WHERE h.stream_day_id=d.id),
		(SELECT count(*) FROM recording_joined_sources s WHERE s.stream_day_id=d.id),
		(SELECT count(*) FROM recording_joined_day_boundaries b WHERE b.stream_day_id=d.id),
		(SELECT count(*) FROM recording_joined_artifacts a WHERE a.stream_day_id=d.id)
		FROM recording_joined_stream_days d WHERE d.batch_id=$1 AND d.recording_id=$2 AND d.local_date=$3`,
		dayRequest.BatchID, dayRequest.RecordingID, dayRequest.LocalDate).Scan(&state, &hours, &sources, &boundaries, &ledgers); err != nil ||
		state != "pending" || hours != 0 || sources != 0 || boundaries != 0 || ledgers != 0 {
		t.Fatalf("failed HEAD leaked state=%s hours=%d sources=%d boundaries=%d ledgers=%d err=%v",
			state, hours, sources, boundaries, ledgers, err)
	}

	transport.setETag("etag-one")
	sealed := call(true)
	var response joinedSealStreamDayResponse
	if sealed.Code != http.StatusOK || json.Unmarshal(sealed.Body.Bytes(), &response) != nil || response.AlreadySealed ||
		response.SourceCount != 1 || response.SourceBytes != 10 || response.LedgerArtifactID <= 0 {
		t.Fatalf("seal status=%d body=%s", sealed.Code, sealed.Body.String())
	}
	if err := fixture.pool.QueryRow(context.Background(), `SELECT d.state,
		(SELECT count(*) FROM recording_joined_hours h WHERE h.stream_day_id=d.id),
		(SELECT count(*) FROM recording_joined_sources s WHERE s.stream_day_id=d.id),
		(SELECT count(*) FROM recording_joined_day_boundaries b WHERE b.stream_day_id=d.id),
		(SELECT count(*) FROM recording_joined_artifacts a WHERE a.stream_day_id=d.id)
		FROM recording_joined_stream_days d WHERE d.batch_id=$1 AND d.recording_id=$2 AND d.local_date=$3`,
		dayRequest.BatchID, dayRequest.RecordingID, dayRequest.LocalDate).Scan(&state, &hours, &sources, &boundaries, &ledgers); err != nil ||
		state != "sealed" || hours != 12 || sources != 1 || boundaries != 13 || ledgers != 1 {
		t.Fatalf("sealed state=%s hours=%d sources=%d boundaries=%d ledgers=%d err=%v",
			state, hours, sources, boundaries, ledgers, err)
	}
	headsBeforeReplay := store.count()
	replayed := call(true)
	var replay joinedSealStreamDayResponse
	if replayed.Code != http.StatusOK || json.Unmarshal(replayed.Body.Bytes(), &replay) != nil || !replay.AlreadySealed ||
		replay.SealRequestSHA != response.SealRequestSHA || replay.LedgerArtifactID != response.LedgerArtifactID {
		t.Fatalf("replay status=%d body=%s", replayed.Code, replayed.Body.String())
	}
	if store.count() != headsBeforeReplay {
		t.Fatal("exact sealed retry repeated source HEAD")
	}
	t.Log("JOINED_STREAM_DAY_HEAD_SEAL_EXECUTED")
}
