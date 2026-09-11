package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testJoinedExactRetryGrant(t *testing.T, s *Server, pool *pgxpool.Pool, batch, hour string, race func(*testing.T, string) []int, setHour func(*testing.T, string, int)) {
	t.Helper()
	ctx := context.Background()
	// Only this disposable PostgreSQL fixture may manufacture legacy states.
	requireJoinedTestReplicationRole(t, pool)
	fixtureSQL := func(sql string, args ...any) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role=replica"); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	s.cfg.JoinedOperatorToken = "exact-retry-operator-token-32-bytes"
	request := joinedrecording.ExactRetryGrantRequest{ProtocolVersion: 1, BatchID: batch, HourID: hour, ExpectedAttemptCount: 1, Reason: "Reviewed expired preseal failure"}
	call := func(req joinedrecording.ExactRetryGrantRequest, token string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(req)
		r := httptest.NewRequest(http.MethodPut, "/api/v1/recording/joined/retry-grant", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		out := httptest.NewRecorder()
		s.requireJoinedOperatorAuth(http.HandlerFunc(s.handleJoinedExactRetryGrant)).ServeHTTP(out, r)
		return out
	}
	if got := call(request, s.cfg.JoinedWorkerBootstrapToken); got.Code != http.StatusUnauthorized {
		t.Fatalf("worker granted retry: %d", got.Code)
	}
	if got := call(request, s.cfg.ServiceToken); got.Code != http.StatusUnauthorized {
		t.Fatalf("service token granted retry: %d", got.Code)
	}
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeSingleCanary
	if got := call(request, s.cfg.JoinedOperatorToken); got.Code != http.StatusForbidden {
		t.Fatalf("non-frozen scope granted retry: %d", got.Code)
	}
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	for _, tc := range []struct {
		name string
		edit func(*joinedrecording.ExactRetryGrantRequest)
		code int
	}{
		{"stale attempt", func(r *joinedrecording.ExactRetryGrantRequest) { r.ExpectedAttemptCount = 2 }, 409},
		{"max attempts", func(r *joinedrecording.ExactRetryGrantRequest) { r.ExpectedAttemptCount = 8 }, 400},
		{"wrong batch", func(r *joinedrecording.ExactRetryGrantRequest) { r.BatchID = "other" }, 403},
		{"wrong hour", func(r *joinedrecording.ExactRetryGrantRequest) { r.HourID = "other" }, 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := request
			tc.edit(&r)
			if got := call(r, s.cfg.JoinedOperatorToken); got.Code != tc.code {
				t.Fatalf("status=%d body=%s", got.Code, got.Body.String())
			}
		})
	}
	for _, state := range []string{"pending", "terminal_failed"} {
		setHour(t, state, 1)
		if got := call(request, s.cfg.JoinedOperatorToken); got.Code != 409 {
			t.Fatalf("%s accepted: %d", state, got.Code)
		}
	}
	setHour(t, "leased", 2)
	missing := request
	missing.ExpectedAttemptCount = 2
	if got := call(missing, s.cfg.JoinedOperatorToken); got.Code != 409 {
		t.Fatalf("unacknowledged attempt accepted: %d", got.Code)
	}
	setHour(t, "leased", 1)
	for _, column := range []string{"lease_expires_at", "next_attempt_at"} {
		fixtureSQL("UPDATE recording_joined_hours SET "+column+"=now()+interval '1 hour' WHERE hour_id=$1", hour)
		if got := call(request, s.cfg.JoinedOperatorToken); got.Code != 409 {
			t.Fatalf("future %s accepted: %d", column, got.Code)
		}
		setHour(t, "leased", 1)
	}
	fixtureSQL(`UPDATE recording_joined_worker_failures SET retry_at=now()+interval '1 hour' WHERE scope_id=$1`, hour)
	if got := call(request, s.cfg.JoinedOperatorToken); got.Code != 409 {
		t.Fatalf("not due failure accepted: %d", got.Code)
	}
	fixtureSQL(`UPDATE recording_joined_worker_failures SET retry_at=now()-interval '1 minute' WHERE scope_id=$1`, hour)
	artifact := func() {
		fixtureSQL(`INSERT INTO recording_joined_artifacts
		 (batch_record_id,account_id,connection_id,batch_id,scope_kind,scope_id,stream_day_id,hour_record_id,
		 artifact_kind,ordinal,relative_path,object_key,content_type,content_id,expected_size_bytes,expected_sha256)
		 SELECT batch_record_id,account_id,connection_id,batch_id,'hour',hour_id,stream_day_id,id,
		 'media',99,'retry-evidence.mp4','joined/'||batch_id||'/objects/'||repeat('b',64)||'.mp4',
		 'video/mp4',repeat('b',64),1,repeat('b',64) FROM recording_joined_hours WHERE hour_id=$1`, hour)
	}
	removeArtifact := func() {
		fixtureSQL(`DELETE FROM recording_joined_artifacts WHERE hour_record_id=(SELECT id FROM recording_joined_hours WHERE hour_id=$1)`, hour)
	}
	artifact()
	if got := call(request, s.cfg.JoinedOperatorToken); got.Code != 409 {
		t.Fatalf("artifact evidence grant accepted: %d", got.Code)
	}
	removeArtifact()
	var failureBefore string
	if err := pool.QueryRow(ctx, `SELECT row_to_json(f)::text FROM recording_joined_worker_failures f WHERE scope_id=$1`, hour).Scan(&failureBefore); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	grantCodes := make(chan int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); grantCodes <- call(request, s.cfg.JoinedOperatorToken).Code }()
	}
	wg.Wait()
	close(grantCodes)
	for code := range grantCodes {
		if code != 200 {
			t.Fatalf("concurrent idempotent grant status=%d", code)
		}
	}
	changed := request
	changed.Reason = "different review"
	if got := call(changed, s.cfg.JoinedOperatorToken); got.Code != 409 {
		t.Fatalf("grant audit evidence replaced: %d", got.Code)
	}
	// Even an already approved grant cannot bypass subsequently discovered output.
	for _, patch := range []string{`{"source_only_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`, `{"canonical_plan":{}}`, `{"manifest_bytes":"\\x01"}`, `{"manifest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`, `{"sealed_at":"2026-01-01T00:00:00Z"}`, `{"claim_token":"00000000-0000-0000-0000-000000000098"}`, `{"attempt_count":8}`} {
		var available bool
		if err := pool.QueryRow(ctx, `SELECT joined_exact_retry_available(jsonb_populate_record(h,$2::jsonb)) FROM recording_joined_hours h WHERE hour_id=$1`, hour, patch).Scan(&available); err != nil || available {
			t.Fatalf("unsafe grant patch=%s available=%v err=%v", patch, available, err)
		}
	}
	token := joinedGapBootstrapToken(t, s, batch)
	artifact()
	blockedArtifacts := race(t, token)
	if blockedArtifacts[0] != 204 || blockedArtifacts[1] != 204 {
		t.Fatalf("approved grant bypassed artifacts: %v", blockedArtifacts)
	}
	removeArtifact()
	fixtureSQL(`INSERT INTO recording_joined_hour_dispositions(hour_record_id,source_id,disposition,reason_code,quarantine_evidence)
		SELECT h.id,s.id,'quarantined','fixture_evidence','{}'::jsonb FROM recording_joined_hours h JOIN recording_joined_sources s ON s.hour_record_id=h.id WHERE h.hour_id=$1 LIMIT 1`, hour)
	blocked := race(t, token)
	if blocked[0] != 204 || blocked[1] != 204 {
		t.Fatalf("disposition evidence bypassed: %v", blocked)
	}
	fixtureSQL(`DELETE FROM recording_joined_hour_dispositions WHERE hour_record_id=(SELECT id FROM recording_joined_hours WHERE hour_id=$1)`, hour)
	codes := race(t, token)
	sort.Ints(codes)
	if codes[0] != http.StatusOK || codes[1] != http.StatusNoContent {
		t.Fatalf("one grant raced into claims: %v", codes)
	}
	replay := call(request, s.cfg.JoinedOperatorToken)
	var grant joinedrecording.ExactRetryGrant
	if replay.Code != http.StatusOK || json.Unmarshal(replay.Body.Bytes(), &grant) != nil || grant.ConsumedAt == nil {
		t.Fatalf("grant consumption absent: %d %s", replay.Code, replay.Body.String())
	}
	var failureAfter string
	if err := pool.QueryRow(ctx, `SELECT row_to_json(f)::text FROM recording_joined_worker_failures f WHERE scope_id=$1`, hour).Scan(&failureAfter); err != nil || failureBefore != failureAfter {
		t.Fatalf("failure evidence changed: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM recording_joined_exact_retry_grants WHERE hour_record_id=(SELECT id FROM recording_joined_hours WHERE hour_id=$1)`, hour); err == nil {
		t.Fatal("grant evidence was deletable")
	}
	setHour(t, "leased", 2)
	codes = race(t, token)
	if codes[0] != http.StatusNoContent || codes[1] != http.StatusNoContent {
		t.Fatalf("grant reused after expiry: %v", codes)
	}
}
