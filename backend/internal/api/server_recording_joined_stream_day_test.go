package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/daydemir/stoarama/backend/internal/config"
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

type joinedHeadStep struct {
	status int
	etag   string
	err    error
}

type joinedScriptedHeadTransport struct {
	mu    sync.Mutex
	steps []joinedHeadStep
	calls int
}

func (s *joinedScriptedHeadTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	index := s.calls
	s.calls++
	step := joinedHeadStep{status: http.StatusOK, etag: strings.Trim(req.Header.Get("If-Match"), `"`)}
	if index < len(s.steps) {
		step = s.steps[index]
	}
	s.mu.Unlock()
	if step.err != nil {
		return nil, step.err
	}
	if step.status == 0 {
		step.status = http.StatusOK
	}
	if step.etag == "" {
		step.etag = strings.Trim(req.Header.Get("If-Match"), `"`)
	}
	headers := make(http.Header)
	headers.Set("ETag", `"`+step.etag+`"`)
	headers.Set("Content-Length", "10")
	headers.Set("x-amz-version-id", "source-version-one")
	return &http.Response{StatusCode: step.status, Header: headers, ContentLength: 10,
		Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
}

func (s *joinedScriptedHeadTransport) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type joinedConcurrentHeadTransport struct {
	mu      sync.Mutex
	active  int
	maximum int
	started chan struct{}
	release chan struct{}
}

type joinedPairHeadTransport struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
}

func (s *joinedPairHeadTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	index := s.calls
	s.calls++
	s.mu.Unlock()
	s.started <- struct{}{}
	select {
	case <-req.Context().Done():
		return nil, req.Context().Err()
	case <-s.release:
	}
	etag := "other"
	if index == 0 {
		etag = strings.Trim(req.Header.Get("If-Match"), `"`)
	}
	headers := make(http.Header)
	headers.Set("ETag", `"`+etag+`"`)
	headers.Set("Content-Length", "10")
	headers.Set("x-amz-version-id", "source-version-one")
	return &http.Response{StatusCode: http.StatusOK, Header: headers, ContentLength: 10,
		Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
}

func (s *joinedConcurrentHeadTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	s.active++
	if s.active > s.maximum {
		s.maximum = s.active
	}
	s.mu.Unlock()
	s.started <- struct{}{}
	select {
	case <-req.Context().Done():
		s.mu.Lock()
		s.active--
		s.mu.Unlock()
		return nil, req.Context().Err()
	case <-s.release:
	}
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
	headers := make(http.Header)
	headers.Set("ETag", req.Header.Get("If-Match"))
	headers.Set("Content-Length", "10")
	headers.Set("x-amz-version-id", "source-version-one")
	return &http.Response{StatusCode: http.StatusOK, Header: headers, ContentLength: 10,
		Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
}

func joinedHeadTestSnapshot(ordinal int) joinedStreamDaySnapshot {
	start := time.Date(2026, 8, 1, 12, ordinal, 0, 0, time.UTC)
	return joinedStreamDaySnapshot{ID: int64(ordinal + 1), Source: joinedrecording.SourceClip{
		ClipID: int64(ordinal + 1), RecordingID: 377, RecordingJobID: 1, StorageDestinationID: 1,
		Provider: "r2", Endpoint: joinedTestSourceEndpoint, Region: "auto", Bucket: "clips",
		StartUTC: start, EndUTC: start.Add(time.Minute), Object: joinedrecording.ObjectIdentity{
			Key: "raw/source.mp4", ETag: "etag-one", SizeBytes: 10,
			SHA256: strings.Repeat("a", 64)}}}
}

func TestJoinedStreamDayHEADRetriesOnlyTransientFailures(t *testing.T) {
	snapshot := joinedHeadTestSnapshot(0)
	store := &joinedFreezeStoreStub{bucket: "clips"}
	wait := func(context.Context, time.Duration) error { return nil }
	for _, test := range []struct {
		name      string
		steps     []joinedHeadStep
		wantCalls int
		wantOK    bool
	}{
		{name: "network recovers", steps: []joinedHeadStep{{err: io.ErrUnexpectedEOF}, {}}, wantCalls: 2, wantOK: true},
		{name: "timeout recovers", steps: []joinedHeadStep{{err: context.DeadlineExceeded}, {}}, wantCalls: 2, wantOK: true},
		{name: "429 recovers", steps: []joinedHeadStep{{status: http.StatusTooManyRequests}, {}}, wantCalls: 2, wantOK: true},
		{name: "500 recovers", steps: []joinedHeadStep{{status: http.StatusInternalServerError}, {}}, wantCalls: 2, wantOK: true},
		{name: "transient exhausts", steps: []joinedHeadStep{{status: http.StatusServiceUnavailable},
			{status: http.StatusServiceUnavailable}, {status: http.StatusServiceUnavailable}}, wantCalls: 3},
		{name: "redirect denied", steps: []joinedHeadStep{{status: http.StatusTemporaryRedirect}}, wantCalls: 1},
		{name: "other 4xx denied", steps: []joinedHeadStep{{status: http.StatusForbidden}}, wantCalls: 1},
		{name: "non HTTP status class denied", steps: []joinedHeadStep{{status: 600}}, wantCalls: 1},
		{name: "identity drift denied", steps: []joinedHeadStep{{status: http.StatusOK, etag: "other"}}, wantCalls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &joinedScriptedHeadTransport{steps: test.steps}
			s := &Server{cfg: config.Config{}, joinedFreezeTransport: transport}
			s.cfg.R2Endpoint, s.cfg.R2Region, s.cfg.R2Bucket = joinedTestSourceEndpoint, "auto", "clips"
			_, err := s.headJoinedStreamDaySourceRetry(context.Background(), store, snapshot, wait)
			if (err == nil) != test.wantOK || transport.count() != test.wantCalls {
				t.Fatalf("error=%v calls=%d want_ok=%v want_calls=%d", err, transport.count(), test.wantOK, test.wantCalls)
			}
		})
	}
}

func TestJoinedStreamDayHEADCancellationStopsBackoff(t *testing.T) {
	transport := &joinedScriptedHeadTransport{steps: []joinedHeadStep{{status: http.StatusTooManyRequests}, {}}}
	s := &Server{cfg: config.Config{}, joinedFreezeTransport: transport,
		joinedFreezeSourceStore: &joinedFreezeStoreStub{bucket: "clips"}}
	s.cfg.R2Endpoint, s.cfg.R2Region, s.cfg.R2Bucket = joinedTestSourceEndpoint, "auto", "clips"
	ctx, cancel := context.WithCancel(context.Background())
	waited := false
	wait := func(waitCtx context.Context, _ time.Duration) error {
		waited = true
		cancel()
		<-waitCtx.Done()
		return waitCtx.Err()
	}
	_, err := s.headJoinedStreamDaySourceRetry(ctx, &joinedFreezeStoreStub{bucket: "clips"},
		joinedHeadTestSnapshot(0), wait)
	if !waited || !errors.Is(err, context.Canceled) || transport.count() != 1 {
		t.Fatalf("waited=%v error=%v calls=%d", waited, err, transport.count())
	}
}

func TestJoinedStreamDayHEADConcurrencyIsCappedAtFour(t *testing.T) {
	transport := &joinedConcurrentHeadTransport{started: make(chan struct{}, 9), release: make(chan struct{})}
	s := &Server{cfg: config.Config{}, joinedFreezeTransport: transport,
		joinedFreezeSourceStore: &joinedFreezeStoreStub{bucket: "clips"}}
	s.cfg.R2Endpoint, s.cfg.R2Region, s.cfg.R2Bucket = joinedTestSourceEndpoint, "auto", "clips"
	plan := joinedStreamDayPlan{Sources: make([]joinedStreamDaySnapshot, 9)}
	for i := range plan.Sources {
		plan.Sources[i] = joinedHeadTestSnapshot(i)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.headJoinedStreamDaySources(context.Background(), plan)
		done <- err
	}()
	for i := 0; i < joinedStreamDayHeadConcurrency; i++ {
		<-transport.started
	}
	transport.mu.Lock()
	active, maximum := transport.active, transport.maximum
	transport.mu.Unlock()
	if active != joinedStreamDayHeadConcurrency || maximum != joinedStreamDayHeadConcurrency {
		t.Fatalf("active=%d maximum=%d", active, maximum)
	}
	close(transport.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	maximum = transport.maximum
	transport.mu.Unlock()
	if maximum > joinedStreamDayHeadConcurrency {
		t.Fatalf("maximum concurrent HEADs=%d", maximum)
	}
}

func TestJoinedStreamDayGapOnlyNeedsNoObjectStore(t *testing.T) {
	observations, err := (&Server{}).headJoinedStreamDaySources(context.Background(), joinedStreamDayPlan{})
	if err != nil || observations == nil || len(observations) != 0 {
		t.Fatalf("observations=%v error=%v", observations, err)
	}
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

	pair := &joinedPairHeadTransport{started: make(chan struct{}, 2), release: make(chan struct{})}
	fixture.s.joinedFreezeTransport = pair
	concurrentResults := make(chan *httptest.ResponseRecorder, 1)
	go func() { concurrentResults <- call(true) }()
	<-pair.started
	rejected := call(true)
	select {
	case <-pair.started:
		t.Fatal("rejected concurrent stream-day seal performed a source HEAD")
	default:
	}
	close(pair.release)
	sealed := <-concurrentResults
	if sealed.Code != http.StatusOK || rejected.Code != http.StatusConflict {
		t.Fatalf("concurrent differing retry statuses=%d,%d bodies=%s / %s", sealed.Code, rejected.Code,
			sealed.Body.String(), rejected.Body.String())
	}
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
	gapRequest := joinedSealStreamDayRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		BatchID: req.BatchID, RecordingID: joinedrecording.Tier1RecordingIDs[0], LocalDate: "2026-08-02"}
	gapBody, _ := json.Marshal(gapRequest)
	gapHTTP := httptest.NewRequest(http.MethodPost, "/api/v1/admin/recording/joined/stream-days/seal", bytes.NewReader(gapBody))
	gapHTTP.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: fixture.sessionToken})
	gapRecorder := httptest.NewRecorder()
	fixture.s.requireAdminAuth(http.HandlerFunc(fixture.s.handleAdminJoinedSealStreamDay)).ServeHTTP(gapRecorder, gapHTTP)
	var gapResponse joinedSealStreamDayResponse
	if gapRecorder.Code != http.StatusOK || json.Unmarshal(gapRecorder.Body.Bytes(), &gapResponse) != nil ||
		gapResponse.SourceCount != 0 || gapResponse.SourceBytes != 0 || store.count() != headsBeforeReplay {
		t.Fatalf("gap-only seal status=%d heads=%d body=%s", gapRecorder.Code, store.count(), gapRecorder.Body.String())
	}
	if err := fixture.pool.QueryRow(context.Background(), `SELECT d.state,
		(SELECT count(*) FROM recording_joined_hours h WHERE h.stream_day_id=d.id),
		(SELECT count(*) FROM recording_joined_sources s WHERE s.stream_day_id=d.id),
		(SELECT count(*) FROM recording_joined_day_boundaries b WHERE b.stream_day_id=d.id)
		FROM recording_joined_stream_days d WHERE d.batch_id=$1 AND d.recording_id=$2 AND d.local_date=$3`,
		gapRequest.BatchID, gapRequest.RecordingID, gapRequest.LocalDate).Scan(&state, &hours, &sources, &boundaries); err != nil ||
		state != "sealed" || hours != 12 || sources != 0 || boundaries != 13 {
		t.Fatalf("gap-only state=%s hours=%d sources=%d boundaries=%d err=%v", state, hours, sources, boundaries, err)
	}
	identicalRequest := gapRequest
	identicalRequest.LocalDate = "2026-08-03"
	identicalBody, _ := json.Marshal(identicalRequest)
	start := make(chan struct{})
	identicalResults := make(chan *httptest.ResponseRecorder, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/recording/joined/stream-days/seal",
				bytes.NewReader(identicalBody))
			httpReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: fixture.sessionToken})
			recorder := httptest.NewRecorder()
			fixture.s.requireAdminAuth(http.HandlerFunc(fixture.s.handleAdminJoinedSealStreamDay)).ServeHTTP(recorder, httpReq)
			identicalResults <- recorder
		}()
	}
	close(start)
	identicalFirst, identicalSecond := <-identicalResults, <-identicalResults
	success, retry := identicalFirst, identicalSecond
	if success.Code != http.StatusOK {
		success, retry = identicalSecond, identicalFirst
	}
	if success.Code != http.StatusOK || (retry.Code != http.StatusOK && retry.Code != http.StatusConflict) {
		t.Fatalf("concurrent identical retries first=%d %s second=%d %s", identicalFirst.Code,
			identicalFirst.Body.String(), identicalSecond.Code, identicalSecond.Body.String())
	}
	if retry.Code == http.StatusConflict {
		httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/recording/joined/stream-days/seal", bytes.NewReader(identicalBody))
		httpReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: fixture.sessionToken})
		retry = httptest.NewRecorder()
		fixture.s.requireAdminAuth(http.HandlerFunc(fixture.s.handleAdminJoinedSealStreamDay)).ServeHTTP(retry, httpReq)
	}
	var successIdentity, retryIdentity joinedSealStreamDayResponse
	if retry.Code != http.StatusOK || json.Unmarshal(success.Body.Bytes(), &successIdentity) != nil ||
		json.Unmarshal(retry.Body.Bytes(), &retryIdentity) != nil ||
		successIdentity.SealRequestSHA != retryIdentity.SealRequestSHA ||
		successIdentity.LedgerArtifactID != retryIdentity.LedgerArtifactID ||
		successIdentity.AlreadySealed || !retryIdentity.AlreadySealed {
		t.Fatalf("identical retry replay first=%d %s second=%d %s", success.Code,
			success.Body.String(), retry.Code, retry.Body.String())
	}
	t.Log("JOINED_STREAM_DAY_HEAD_SEAL_EXECUTED")
}
