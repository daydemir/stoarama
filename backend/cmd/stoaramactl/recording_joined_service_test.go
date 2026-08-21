package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestJoinedWorkerClaimsPublicationBeforePreflight(t *testing.T) {
	t.Parallel()
	const (
		batchID   = "tier1-2026-08"
		workerID  = "worker-1"
		bootstrap = "bootstrap-token-kept-secret"
		claim     = "claim-token-kept-secret-value"
	)
	paths := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		if got := r.Header.Get("Authorization"); got != "Bearer "+map[string]string{
			"/api/v1/recording/joined/token":             bootstrap,
			"/api/v1/recording/joined/publication/claim": claim,
			"/api/v1/recording/joined/claim":             claim,
		}[r.URL.Path] {
			t.Errorf("authorization on %s = %q", r.URL.Path, got)
		}
		switch r.URL.Path {
		case "/api/v1/recording/joined/token":
			var request joinedrecording.WorkerBootstrapRequest
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.BatchID != batchID {
				t.Errorf("unexpected bootstrap request: %+v", request)
			}
			writeJoinedTestJSON(t, w, joinedrecording.WorkerBootstrapResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: batchID, ClaimToken: claim, ExpiresAt: time.Now().Add(time.Hour)})
		case "/api/v1/recording/joined/publication/claim", "/api/v1/recording/joined/claim":
			var request joinedrecording.WorkClaimRequest
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.BatchID != batchID || request.WorkerID != workerID {
				t.Errorf("unexpected claim request: %+v", request)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	api, err := newJoinedAPIClient(server.URL, bootstrap, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{cfg: config.Config{}, api: api}
	worked, err := service.runWorkerOnce(context.Background(), joinedWorkerRequest{BatchID: batchID, WorkerID: workerID, ScratchRoot: t.TempDir()})
	if err != nil || worked {
		t.Fatalf("run once: worked=%v err=%v", worked, err)
	}
	for _, want := range []string{"/api/v1/recording/joined/token", "/api/v1/recording/joined/publication/claim", "/api/v1/recording/joined/claim"} {
		select {
		case got := <-paths:
			if got != want {
				t.Fatalf("claim order: got %q want %q", got, want)
			}
		default:
			t.Fatalf("missing request %q", want)
		}
	}
}

func TestJoinedWorkerIdleCancellationStopsCleanly(t *testing.T) {
	t.Parallel()
	requested := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/recording/joined/token" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		requested <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	api, err := newJoinedAPIClient(server.URL, "bootstrap-token-kept-secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	service := &remoteJoinedOperatorService{api: api, idlePoll: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- service.RunWorker(ctx, joinedWorkerRequest{BatchID: "tier1-2026-08", WorkerID: "worker-1"})
	}()
	select {
	case <-requested:
	case <-time.After(time.Second):
		t.Fatal("worker did not poll")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("cancelled idle worker: %v", err)
	}
}

func TestJoinedAPIRejectsRedirectAndDoesNotExposeResponseBody(t *testing.T) {
	t.Parallel()
	const secret = "response-body-secret-must-not-leak"
	for _, tc := range []struct {
		name   string
		status int
	}{
		{name: "redirect", status: http.StatusTemporaryRedirect},
		{name: "failure", status: http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "/redirect-target")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(secret))
			}))
			defer server.Close()
			api, err := newJoinedAPIClient(server.URL, "bootstrap-token-kept-secret", server.Client())
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = api.bootstrap(context.Background(), "tier1-2026-08")
			if err == nil || strings.Contains(err.Error(), secret) {
				t.Fatalf("unsafe API error: %v", err)
			}
		})
	}
}

func TestJoinedWorkerFailsClosedForReclaimedHour(t *testing.T) {
	t.Parallel()
	service := &remoteJoinedOperatorService{}
	err := service.publishClaim(context.Background(), joinedrecording.PublicationClaimResponse{Kind: "hour", Hour: &joinedrecording.WorkerClaim{HourID: "sealed-hour"}})
	if err == nil || !strings.Contains(err.Error(), "cannot be rebuilt") {
		t.Fatalf("reclaimed hour did not fail closed: %v", err)
	}
}

func writeJoinedTestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}
