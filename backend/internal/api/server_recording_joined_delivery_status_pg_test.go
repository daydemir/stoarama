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

	"github.com/go-chi/chi/v5"
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
		CREATE TABLE connections(id bigint PRIMARY KEY,account_id bigint NOT NULL DEFAULT 1,api_key_id bigint,kind text NOT NULL DEFAULT 'nas_pull',joined_protocol_version integer NOT NULL,
		 joined_last_attempt_artifact_id bigint,joined_last_blocker text NOT NULL DEFAULT '',joined_last_attempt_at timestamptz,joined_retry_at timestamptz,
		 last_cursor_id bigint NOT NULL DEFAULT 0,clips_pulled bigint NOT NULL DEFAULT 0,bytes_pulled bigint NOT NULL DEFAULT 0,
		 client_last_success_at timestamptz,nas_batch_completed_at timestamptz,nas_batch_clips integer NOT NULL DEFAULT 0,
		 nas_batch_bytes bigint NOT NULL DEFAULT 0,nas_batch_failures integer NOT NULL DEFAULT 0,
		 joined_files_pulled bigint NOT NULL DEFAULT 0,joined_bytes_pulled bigint NOT NULL DEFAULT 0);
		CREATE TABLE recordings(id bigint PRIMARY KEY,account_id bigint NOT NULL,delivery text NOT NULL);
		CREATE TABLE recording_clips(id bigint PRIMARY KEY,recording_id bigint NOT NULL,size_bytes bigint NOT NULL,
		 created_at timestamptz NOT NULL,purged_at timestamptz,released_at timestamptz);
		CREATE TABLE recording_joined_batches(id bigint PRIMARY KEY,batch_id text NOT NULL,connection_id bigint NOT NULL);
		CREATE TABLE recording_joined_hours(id bigint PRIMARY KEY,batch_record_id bigint NOT NULL,hour_id text NOT NULL,priority_ordinal integer NOT NULL);
		CREATE TABLE recording_joined_artifacts(
		 id bigint PRIMARY KEY,batch_record_id bigint NOT NULL,account_id bigint NOT NULL DEFAULT 1,connection_id bigint NOT NULL,batch_id text NOT NULL,
		 stream_day_id bigint,hour_record_id bigint,artifact_kind text NOT NULL,ordinal integer NOT NULL DEFAULT 1,relative_path text NOT NULL,expected_size_bytes bigint NOT NULL,
		 expected_sha256 text NOT NULL,content_type text NOT NULL DEFAULT 'application/json',object_key text NOT NULL DEFAULT '',etag text NOT NULL DEFAULT '',
		 version_id text NOT NULL DEFAULT '',publication_state text,published_at timestamptz);
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

func TestJoinedNASDeliveryAndFeedHeadExcludePersistedForeignBatch(t *testing.T) {
	pool, cleanup := joinedDeliveryStatusTestPool(t)
	defer cleanup()
	ctx := context.Background()
	const batchID = "goodplus-20260821-generation-1"
	const hourID = batchID + "__recording-420__date-2026-08-15__hour-06__generation-1"
	sha := strings.Repeat("b", 64)
	seeds := []struct {
		query string
		args  []any
	}{
		{query: `INSERT INTO connections(id,account_id,api_key_id,joined_protocol_version) VALUES(13,1,7,1)`},
		{query: `INSERT INTO recording_joined_batches VALUES(1,'foreign-generation-1',13),(2,$1,13)`, args: []any{batchID}},
		{query: `INSERT INTO recording_joined_hours VALUES(101,2,$1,1)`, args: []any{hourID}},
		{query: `INSERT INTO recording_joined_artifacts(id,batch_record_id,account_id,connection_id,batch_id,artifact_kind,
		 stream_day_id,hour_record_id,relative_path,object_key,expected_size_bytes,expected_sha256,publication_state,published_at) VALUES
		 (490,2,1,13,$1,'allocation_ledger',10,NULL,'coverage/ledgers/current.json','joined/current.json',100,$2,'published',now()),
		 (491,2,1,13,$1,'hour_manifest',10,101,'coverage/hours/current.json','joined/current-hour.json',100,$2,'published',now()),
		 (480,1,1,13,'foreign-generation-1','allocation_ledger',20,NULL,'coverage/ledgers/foreign.json','joined/foreign.json',100,$2,'published',now())`, args: []any{batchID, sha}},
	}
	for _, seed := range seeds {
		if _, err := pool.Exec(ctx, seed.query, seed.args...); err != nil {
			t.Fatal(err)
		}
	}
	s := &Server{pool: pool, cfg: config.Config{
		JoinedRecordingControlPlaneEnabled: true, JoinedRecordingNASDeliveryEnabled: true,
		JoinedRecordingProtocolVersion: 1, JoinedRecordingProtocolGeneration: 1,
		JoinedRecordingConnectionID: 13, JoinedRecordingBatchID: batchID,
		JoinedRecordingWorkScope: config.JoinedWorkScopeSingleCanary, JoinedRecordingCanaryHourIDs: hourID,
		JoinedWorkerBootstrapToken: "delivery-bootstrap-token-32-bytes-000",
		JoinedWorkerSigningKey:     "delivery-signing-token-32-bytes-0000",
	}}
	apiKeyID := int64(7)
	principal := accountPrincipal{AccountID: 1, APIKeyID: &apiKeyID, KeyScopes: []string{accountScopePull}}
	feedReq := httptest.NewRequest(http.MethodGet, "/api/v1/account/joined", nil)
	feedReq = feedReq.WithContext(context.WithValue(feedReq.Context(), accountPrincipalContextKey, principal))
	feedRec := httptest.NewRecorder()
	s.handleAccountJoined(feedRec, feedReq)
	var feed struct {
		Item *joinedArtifactItem `json:"item"`
	}
	if feedRec.Code != http.StatusOK || json.Unmarshal(feedRec.Body.Bytes(), &feed) != nil || feed.Item == nil ||
		feed.Item.ArtifactID != 490 || feed.Item.BatchID != batchID {
		t.Fatalf("exact-batch feed status=%d body=%s", feedRec.Code, feedRec.Body.String())
	}
	downloadReq := httptest.NewRequest(http.MethodGet, "/api/v1/account/joined/480/download", nil)
	downloadRoute := chi.NewRouteContext()
	downloadRoute.URLParams.Add("joinedId", "480")
	downloadReq = downloadReq.WithContext(context.WithValue(context.WithValue(downloadReq.Context(),
		accountPrincipalContextKey, principal), chi.RouteCtxKey, downloadRoute))
	downloadRec := httptest.NewRecorder()
	s.handleAccountJoinedDownload(downloadRec, downloadReq)
	ackBody, _ := json.Marshal(joinedAckRequest{ArtifactID: 480, RelativePath: "coverage/ledgers/foreign.json",
		SizeBytes: 100, SHA256: sha})
	ackReq := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", strings.NewReader(string(ackBody)))
	ackReq = ackReq.WithContext(context.WithValue(ackReq.Context(), accountPrincipalContextKey, principal))
	ackRec := httptest.NewRecorder()
	s.handleAccountJoinedAck(ackRec, ackReq)
	var ackCount int
	var files, bytesPulled int64
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM recording_joined_artifact_acks),joined_files_pulled,
		joined_bytes_pulled FROM connections WHERE id=13`).Scan(&ackCount, &files, &bytesPulled); err != nil ||
		downloadRec.Code != http.StatusNotFound || ackRec.Code != http.StatusNotFound || ackCount != 0 || files != 0 || bytesPulled != 0 {
		t.Fatalf("foreign batch delivery mutated download=%d ack=%d rows=%d counters=%d/%d err=%v",
			downloadRec.Code, ackRec.Code, ackCount, files, bytesPulled, err)
	}
	s.joinedDeliveryStatusAt = time.Time{}
	statusReq := httptest.NewRequest(http.MethodGet,
		"/api/v1/recording/joined/delivery-status?batch_id="+batchID+"&artifact_id=491", nil)
	statusRec := httptest.NewRecorder()
	s.handleJoinedDeliveryStatus(statusRec, statusReq)
	var status joinedDeliveryStatusResponse
	if statusRec.Code != http.StatusOK || json.Unmarshal(statusRec.Body.Bytes(), &status) != nil || status.FeedHead == nil ||
		status.FeedHead.ArtifactID != 490 || status.FeedHead.BatchID != batchID {
		t.Fatalf("exact-batch FeedHead status=%d body=%s", statusRec.Code, statusRec.Body.String())
	}
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
		{query: `INSERT INTO connections(id,joined_protocol_version,last_cursor_id,clips_pulled,bytes_pulled,
		 client_last_success_at,nas_batch_completed_at,nas_batch_clips,nas_batch_bytes,nas_batch_failures,joined_files_pulled,joined_bytes_pulled)
		 VALUES(13,1,100,90,9000,now(),now(),4,400,0,7,700)`},
		{query: `INSERT INTO recordings VALUES(1,1,'nas_pull'); INSERT INTO recording_clips VALUES(101,1,250,now()-interval '10 minutes',NULL,NULL)`},
		{query: `INSERT INTO recording_joined_batches VALUES(1,$1,13),(2,'foreign-generation-1',13)`, args: []any{batchID}},
		{query: `INSERT INTO recording_joined_hours VALUES(101,1,$1,1),(102,1,$2,2),(103,2,$1,1)`, args: []any{hourID, outsideHourID}},
		{query: `INSERT INTO recording_joined_artifacts(id,batch_record_id,connection_id,batch_id,stream_day_id,hour_record_id,artifact_kind,relative_path,expected_size_bytes,expected_sha256,publication_state,published_at) VALUES
		 (492,1,13,$1,10,101,'media','plaza/August/Saturday/part-01.mp4',1234,$2,NULL,now()),
		 (493,1,13,$1,11,102,'media','outside/August/Saturday/part-01.mp4',1234,$2,NULL,now()),
		 (494,2,13,'foreign-generation-1',12,103,'media','foreign/August/Saturday/part-01.mp4',1234,$2,NULL,now()),
		 (480,2,13,'foreign-generation-1',12,NULL,'allocation_ledger','coverage/ledgers/foreign.json',100,$2,'published',now())`, args: []any{batchID, sha}},
	}
	for _, seed := range seeds {
		if _, err := pool.Exec(ctx, seed.query, seed.args...); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET joined_last_attempt_artifact_id=480,
		joined_last_blocker='download_error',joined_last_attempt_at=now(),joined_retry_at=now()+interval '1 minute' WHERE id=13`); err != nil {
		t.Fatal(err)
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
	assertCounts(4, 0)
	unacked := call("492")
	if unacked.Code != http.StatusOK {
		t.Fatalf("unacked status=%d body=%s", unacked.Code, unacked.Body.String())
	}
	var status joinedDeliveryStatusResponse
	if err := json.Unmarshal(unacked.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Acknowledged || status.IdentityMatches || status.HourID != hourID || status.FeedHead != nil ||
		status.LastAttemptArtifactID == nil || *status.LastAttemptArtifactID != 480 || status.TelemetryMatchesHead ||
		status.LastAttemptBlockerClass != "present" || status.LastAttemptBlockerSHA256 == "" ||
		status.RawDelivery.LastCursorID != 100 || status.RawDelivery.ClipsPulled != 90 ||
		status.RawDelivery.PendingClips != 1 || status.RawDelivery.PendingBytes != 250 ||
		status.RawDelivery.JoinedFilesPulled != 7 || status.RawDelivery.JoinedBytesPulled != 700 {
		t.Fatalf("unacked=%+v", status)
	}
	if strings.Contains(unacked.Body.String(), "download_error") {
		t.Fatal("status exposed raw NAS blocker")
	}
	assertCounts(4, 0)
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_artifact_acks VALUES(480,13,'coverage/ledgers/foreign.json',100,$1,now())`, sha); err != nil {
		t.Fatal(err)
	}
	advanced := call("492")
	if advanced.Code != http.StatusOK {
		t.Fatalf("advanced status=%d body=%s", advanced.Code, advanced.Body.String())
	}
	status = joinedDeliveryStatusResponse{}
	if err := json.Unmarshal(advanced.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.FeedHead != nil || status.TelemetryMatchesHead {
		t.Fatalf("feed did not advance to empty: %+v", status)
	}
	assertCounts(4, 1)
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_artifact_acks VALUES(492,13,'plaza/August/Saturday/part-01.mp4',1234,$1,now())`, sha); err != nil {
		t.Fatal(err)
	}
	acked := call("492")
	if acked.Code != http.StatusOK {
		t.Fatalf("acked status=%d body=%s", acked.Code, acked.Body.String())
	}
	status = joinedDeliveryStatusResponse{}
	if err := json.Unmarshal(acked.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if !status.Acknowledged || !status.IdentityMatches || status.VerifiedAt == nil {
		t.Fatalf("acked=%+v", status)
	}
	assertCounts(4, 2)
	for _, id := range []string{"493", "494", "999"} {
		if rec := call(id); rec.Code != http.StatusNotFound {
			t.Fatalf("artifact %s status=%d body=%s", id, rec.Code, rec.Body.String())
		}
	}
	assertCounts(4, 2)
}
