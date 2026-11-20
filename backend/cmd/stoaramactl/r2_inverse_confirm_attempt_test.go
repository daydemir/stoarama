package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

type headConnKey struct{}

// headReplayServer answers the first HEAD on each connection with a transient
// status and, on any later HEAD on that same connection, closes it without
// answering: the provider accepted the request but never processed it. A
// keep-alive client reuses that connection, and Go's HTTP transport replays the
// lost request on a fresh connection, so one executor attempt costs two
// requests. A client that opens a fresh connection per attempt never reuses
// one, so one attempt is exactly one request.
type headReplayServer struct {
	requests atomic.Int64
	mu       sync.Mutex
	perConn  map[net.Conn]int
}

func (s *headReplayServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.requests.Add(1)
	conn, _ := req.Context().Value(headConnKey{}).(net.Conn)
	s.mu.Lock()
	s.perConn[conn]++
	onConn := s.perConn[conn]
	s.mu.Unlock()
	if onConn > 1 {
		c, _, _ := w.(http.Hijacker).Hijack()
		c.Close()
		return
	}
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusServiceUnavailable)
}

func newHeadReplayServer(t *testing.T) (*httptest.Server, *headReplayServer) {
	t.Helper()
	h := &headReplayServer{perConn: map[net.Conn]int{}}
	server := httptest.NewUnstartedServer(h)
	server.Config.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		return context.WithValue(ctx, headConnKey{}, c)
	}
	server.Start()
	t.Cleanup(server.Close)
	return server, h
}

func newHeadReplayStore(t *testing.T, endpoint string) *inverseSDKStore {
	t.Helper()
	client, err := newInverseSDKStore(context.Background(), r2.Config{Bucket: "stoarama", Endpoint: endpoint, AccessKey: "test", SecretKey: "test", Region: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// The confirming HEAD sends exactly one request per executor attempt. A pooled
// keep-alive transport replays a request whose reused connection the provider
// closed: the reviewers counted 5-8 requests for 4 attempts. The production
// client takes a fresh connection per attempt, so 4 attempts send exactly 4
// requests even against this provider.
func TestR2InverseConfirmSendsOneRequestPerAttempt(t *testing.T) {
	server, h := newHeadReplayServer(t)
	sdk := newHeadReplayStore(t, server.URL)
	// Prove the fixture catches the regression: the pooled transport this
	// client replaces replays, so it would see more than one request per attempt.
	pooled := newHeadReplayStore(t, server.URL)
	pooled.confirm = &http.Client{Transport: awshttp.NewBuildableClient().GetTransport(), Timeout: inverseConfirmAttemptTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if err := pooled.confirmAbsent(context.Background(), "managed/acct-47/ordinary/0001.mp4"); err == nil {
		t.Fatal("a persistent 503 confirmed an absence")
	}
	replayed := h.requests.Load()
	if replayed <= inverseConfirmAttempts {
		t.Fatalf("fixture sent %d requests for %d attempts: it cannot detect a replay", replayed, inverseConfirmAttempts)
	}
	h.requests.Store(0)
	started := time.Now()
	err := sdk.confirmAbsent(context.Background(), "managed/acct-47/ordinary/0001.mp4")
	if err == nil || h.requests.Load() != inverseConfirmAttempts {
		t.Fatalf("%d requests for %d attempts in %v: %v", h.requests.Load(), inverseConfirmAttempts, time.Since(started), err)
	}
	if elapsed := time.Since(started); elapsed > inverseConfirmTimeout {
		t.Fatalf("confirmation took %v, past its own %v bound", elapsed, inverseConfirmTimeout)
	}
	// No keep-alive and no HTTP/2: nothing a reused connection could replay on.
	transport, ok := sdk.confirm.Transport.(*http.Transport)
	if !ok || !transport.DisableKeepAlives || transport.ForceAttemptHTTP2 {
		t.Fatalf("confirming transport keeps connections alive or offers HTTP/2: %#v", sdk.confirm.Transport)
	}
}
