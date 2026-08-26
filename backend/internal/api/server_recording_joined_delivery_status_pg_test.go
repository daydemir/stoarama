package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func joinedDeliveryStatusTestPool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run joined delivery-status DB regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("joined_delivery_status_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
		CREATE TABLE connections(id bigint PRIMARY KEY, joined_protocol_version integer NOT NULL);
		CREATE TABLE recording_joined_batches(id bigint PRIMARY KEY,batch_id text NOT NULL,connection_id bigint NOT NULL);
		CREATE TABLE recording_joined_hours(id bigint PRIMARY KEY,batch_record_id bigint NOT NULL,hour_id text NOT NULL);
		CREATE TABLE recording_joined_artifacts(
		 id bigint PRIMARY KEY,batch_record_id bigint NOT NULL,connection_id bigint NOT NULL,batch_id text NOT NULL,
		 hour_record_id bigint,artifact_kind text NOT NULL,relative_path text NOT NULL,expected_size_bytes bigint NOT NULL,
		 expected_sha256 text NOT NULL,publication_state text,published_at timestamptz);
		CREATE TABLE recording_joined_artifact_acks(
		 artifact_id bigint NOT NULL,connection_id bigint NOT NULL,relative_path text NOT NULL,size_bytes bigint NOT NULL,
		 sha256 text NOT NULL,verified_at timestamptz NOT NULL,PRIMARY KEY(artifact_id,connection_id));`)
	if err != nil {
		pool.Close()
		_, _ = admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		admin.Close()
		t.Fatal(err)
	}
	cleanup := func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		admin.Close()
	}
	return pool, cleanup
}

func TestJoinedDeliveryStatusReadsExactAppendOnlyAckWithoutWriting(t *testing.T) {
	pool, cleanup := joinedDeliveryStatusTestPool(t)
	defer cleanup()
	ctx := context.Background()
	const batchID = "goodplus-20260821-generation-1"
	const hourID = batchID + "__recording-420__date-2026-08-15__hour-06__generation-1"
	const outsideHourID = batchID + "__recording-421__date-2026-08-20__hour-01__generation-1"
	sha := strings.Repeat("a", 64)
	seeds := []struct {
		query string
		args  []any
	}{
		{query: `INSERT INTO connections VALUES(13,1)`},
		{query: `INSERT INTO recording_joined_batches VALUES(1,$1,13),(2,'foreign-generation-1',13)`, args: []any{batchID}},
		{query: `INSERT INTO recording_joined_hours VALUES(101,1,$1),(102,1,$2),(103,2,$1)`, args: []any{hourID, outsideHourID}},
		{query: `INSERT INTO recording_joined_artifacts VALUES
		 (492,1,13,$1,101,'media','plaza/August/Saturday/part-01.mp4',1234,$2,NULL,now()),
		 (493,1,13,$1,102,'media','outside/August/Saturday/part-01.mp4',1234,$2,NULL,now()),
		 (494,2,13,'foreign-generation-1',103,'media','foreign/August/Saturday/part-01.mp4',1234,$2,NULL,now())`, args: []any{batchID, sha}},
	}
	for _, seed := range seeds {
		if _, err := pool.Exec(ctx, seed.query, seed.args...); err != nil {
			t.Fatal(err)
		}
	}
	s := &Server{pool: pool, cfg: config.Config{
		JoinedRecordingControlPlaneEnabled: true, JoinedRecordingProtocolVersion: 1,
		JoinedRecordingProtocolGeneration: 1, JoinedRecordingConnectionID: 13, JoinedRecordingBatchID: batchID,
		JoinedRecordingWorkScope: config.JoinedWorkScopeSingleCanary, JoinedRecordingCanaryHourIDs: hourID,
		JoinedOperatorToken: "operator-token-32-bytes-long-000000", JoinedWorkerBootstrapToken: "bootstrap-token-32-bytes-long-000",
		JoinedWorkerSigningKey: "signing-key-32-bytes-long-0000000", ServiceToken: "service-token-32-bytes-long-000000",
	}}
	call := func(artifactID string) *httptest.ResponseRecorder {
		s.joinedDeliveryStatusAt = time.Time{}
		req := httptest.NewRequest(http.MethodGet, "/api/v1/recording/joined/delivery-status?batch_id="+batchID+"&artifact_id="+artifactID, nil)
		rec := httptest.NewRecorder()
		s.handleJoinedDeliveryStatus(rec, req)
		return rec
	}
	assertCounts := func(wantArtifacts, wantAcks int) {
		var artifacts, acks int
		if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM recording_joined_artifacts),(SELECT count(*) FROM recording_joined_artifact_acks)`).Scan(&artifacts, &acks); err != nil {
			t.Fatal(err)
		}
		if artifacts != wantArtifacts || acks != wantAcks {
			t.Fatalf("artifacts=%d acks=%d", artifacts, acks)
		}
	}
	assertCounts(3, 0)
	unacked := call("492")
	if unacked.Code != http.StatusOK {
		t.Fatalf("unacked status=%d body=%s", unacked.Code, unacked.Body.String())
	}
	var status joinedDeliveryStatusResponse
	if err := json.Unmarshal(unacked.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Acknowledged || status.IdentityMatches || status.HourID != hourID {
		t.Fatalf("unacked=%+v", status)
	}
	assertCounts(3, 0)
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_artifact_acks VALUES(492,13,'plaza/August/Saturday/part-01.mp4',1234,$1,now())`, sha); err != nil {
		t.Fatal(err)
	}
	acked := call("492")
	if acked.Code != http.StatusOK {
		t.Fatalf("acked status=%d body=%s", acked.Code, acked.Body.String())
	}
	if err := json.Unmarshal(acked.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if !status.Acknowledged || !status.IdentityMatches || status.VerifiedAt == nil {
		t.Fatalf("acked=%+v", status)
	}
	assertCounts(3, 1)
	for _, id := range []string{"493", "494", "999"} {
		if rec := call(id); rec.Code != http.StatusNotFound {
			t.Fatalf("artifact %s status=%d body=%s", id, rec.Code, rec.Body.String())
		}
	}
	assertCounts(3, 1)
}
