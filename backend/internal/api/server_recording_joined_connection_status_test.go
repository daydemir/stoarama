package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func TestJoinedConnectionStatusIsAuthenticatedReadOnlyAndScoped(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()

	const (
		connectionID = 13
		batchID      = "goodplus-20260821-generation-1"
		bootstrap    = "joined-bootstrap-credential-32bytes"
	)
	lastSeen := time.Now().UTC().Add(-2 * time.Second)
	errorAt := lastSeen.Add(-time.Second)
	clientError := strings.Repeat("diagnostic-", 20)
	if _, err := pool.Exec(context.Background(), `INSERT INTO connections
		(id,account_id,kind,last_seen_at,poll_interval_sec,client_version,client_phase,client_previous_exit,
		 client_last_error,client_last_error_at,joined_protocol_version)
		VALUES($1,1,'nas_pull',$2,60,'pull-test','degraded','clean',$3,$4,0)`, connectionID, lastSeen, clientError, errorAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `CREATE TABLE recording_joined_batches
		(id BIGSERIAL PRIMARY KEY, batch_id TEXT NOT NULL, connection_id BIGINT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO recording_joined_batches(batch_id,connection_id) VALUES($1,$2)`, batchID, connectionID); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool, cfg: config.Config{
		JoinedRecordingControlPlaneEnabled: true,
		JoinedRecordingBatchID:             batchID,
		JoinedRecordingWorkScope:           config.JoinedWorkScopeCanary,
		JoinedRecordingCanaryHourIDs:       joinedCanaryScopeForTest(batchID),
		JoinedRecordingConnectionID:        connectionID,
		JoinedRecordingProtocolVersion:     1,
		JoinedRecordingProtocolGeneration:  1,
		JoinedWorkerBootstrapToken:         bootstrap,
		JoinedWorkerSigningKey:             "joined-signing-credential-32-bytes",
	}, joinedCredentialCheck: func(context.Context) error { return nil }}

	call := func(path, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerBootstrapAuth(http.HandlerFunc(s.handleJoinedConnectionStatus)).ServeHTTP(rec, req)
		return rec
	}

	if rec := call("/api/v1/recording/joined/connection-status?batch_id="+batchID, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing auth status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := call("/api/v1/recording/joined/connection-status?batch_id=other-generation-1", bootstrap); rec.Code != http.StatusForbidden {
		t.Fatalf("wrong batch status=%d body=%s", rec.Code, rec.Body.String())
	}
	rec := call("/api/v1/recording/joined/connection-status?batch_id="+batchID, bootstrap)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid status=%d body=%s", rec.Code, rec.Body.String())
	}
	var got joinedConnectionStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.ConnectionID != connectionID || got.ExpectedProtocolVersion != 1 || got.ExpectedProtocolGeneration != 1 ||
		got.ServerDesiredProtocolVersion != 1 || got.ServerDesiredProtocolGeneration != 1 ||
		got.ObservedProtocolVersion != 0 || got.HeartbeatStale || got.ClientVersion != "pull-test" || got.ClientPhase != "degraded" ||
		!got.ClientErrorPresent || got.ClientErrorAt == nil || !got.ClientErrorAt.Equal(errorAt) ||
		got.ClientErrorClass != "nas_pull" || got.ClientErrorSHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte(clientError))) {
		t.Fatalf("unexpected status: %+v", got)
	}
	if strings.Contains(rec.Body.String(), "diagnostic-") {
		t.Fatal("diagnostic response exposed raw client error")
	}

	var protocol int
	if err := pool.QueryRow(context.Background(), `SELECT joined_protocol_version FROM connections WHERE id=$1`, connectionID).Scan(&protocol); err != nil {
		t.Fatal(err)
	}
	if protocol != 0 {
		t.Fatalf("diagnostic mutated connection protocol: %d", protocol)
	}
	if rec := call("/api/v1/recording/joined/connection-status?batch_id="+batchID, bootstrap); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("rate limit status=%d body=%s", rec.Code, rec.Body.String())
	}

	// Staleness is derived from telemetry and never changes the row.
	if _, err := pool.Exec(context.Background(), `UPDATE connections SET last_seen_at=NULL WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	s.joinedConnectionStatusAt = time.Time{}
	rec = call("/api/v1/recording/joined/connection-status?batch_id="+batchID, bootstrap)
	if rec.Code != http.StatusOK {
		t.Fatalf("missing heartbeat status=%d body=%s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !got.HeartbeatStale || got.HeartbeatAgeSeconds != nil {
		t.Fatalf("missing heartbeat was not marked stale: %+v", got)
	}

	s.cfg.JoinedRecordingConnectionID = 99
	s.joinedConnectionStatusAt = time.Time{}
	rec = call("/api/v1/recording/joined/connection-status?batch_id="+batchID, bootstrap)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing connection status=%d body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), bootstrap) {
		t.Fatal("diagnostic response echoed bootstrap credential")
	}
}

func TestJoinedClientErrorDiagnosticIsClassifiedAndOpaque(t *testing.T) {
	joined := "joined delivery: signed URL must not escape"
	if got := joinedClientErrorClass(joined); got != "joined_delivery" {
		t.Fatalf("joined class=%q", got)
	}
	if got := joinedClientErrorClass("raw pull failed"); got != "nas_pull" {
		t.Fatalf("NAS class=%q", got)
	}
	if got := joinedClientErrorClass("  "); got != "" {
		t.Fatalf("empty class=%q", got)
	}
	want := fmt.Sprintf("%x", sha256.Sum256([]byte(joined)))
	if got := joinedClientErrorSHA256(joined); got != want || strings.Contains(got, "signed URL") {
		t.Fatalf("opaque digest=%q want=%q", got, want)
	}
}
