package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSnapshotPromotedRelayManifestBindsImmutableBytes(t *testing.T) {
	live := []byte(`{"version":"new123","previous_version":"old123","relay":{},"ytdlp":{},"deno":{}}`)
	objects := map[string][]byte{
		relayReleasePrefix + "latest.json":            live,
		relayReleasePrefix + "latest-new123.json":     live,
		relayReleasePrefix + "latest-new123.json.sig": []byte("signed"),
	}
	open := func(_ context.Context, key string) (io.ReadCloser, error) {
		body, ok := objects[key]
		if !ok {
			return nil, fmt.Errorf("missing %s", key)
		}
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	snapshot, err := snapshotPromotedRelayManifest(context.Background(), "new123", "old123", open)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != "new123" || snapshot.PreviousVersion != "old123" || snapshot.Name != "latest-new123.json" || len(snapshot.SHA256) != 64 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if _, err := snapshotPromotedRelayManifest(context.Background(), "new123", "candidate999", open); err == nil || !strings.Contains(err.Error(), "immediate successor") {
		t.Fatalf("unproven ancestry accepted: %v", err)
	}
	objects[relayReleasePrefix+"latest-new123.json"] = []byte(`{"version":"new123","relay":{"changed":{}}}`)
	if _, err := snapshotPromotedRelayManifest(context.Background(), "new123", "old123", open); err == nil || !strings.Contains(err.Error(), "differs") {
		t.Fatalf("immutable substitution accepted: %v", err)
	}
}

func TestRelayUpdateIntentCreateReplayAndHeartbeatCompletion(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	userID, accountID := seedUserOrg(t, pool, "relay-update@example.com", false)
	insertSession(t, pool, accountID, userID, "relay-update-session")
	var sessionID int64
	if err := pool.QueryRow(context.Background(), `SELECT id FROM account_sessions WHERE session_hash=$1`, hashSecret("relay-update-session")).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	var nodeID int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO nodes(account_id,node_type,display_name,status,last_heartbeat_at,capabilities_jsonb)
		VALUES ($1,'relay','update-relay','active',now(),'{"relay_version":"old123","relay_update_intents_v1":true}') RETURNING id
	`, accountID).Scan(&nodeID); err != nil {
		t.Fatal(err)
	}
	var snapshotCalls atomic.Int64
	s.relayUpdateSnapshot = func(_ context.Context, expected, fromVersion string) (relayManifestSnapshot, error) {
		snapshotCalls.Add(1)
		if expected != "new123" {
			return relayManifestSnapshot{}, fmt.Errorf("not current")
		}
		return relayManifestSnapshot{Version: expected, Name: "latest-new123.json", SHA256: strings.Repeat("a", 64), PreviousVersion: fromVersion}, nil
	}
	requestID := "00000000-0000-4000-8000-000000000142"
	callCreate := func(p accountPrincipal, id, expected string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(relayUpdateIntentRequest{RequestID: id, ExpectedPromotedVersion: expected})
		req := withPrincipal(httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/account/nodes/%d/relay-update-intents", nodeID), bytes.NewReader(body)), p, fmt.Sprint(nodeID))
		rec := httptest.NewRecorder()
		s.handleAccountRelayUpdateIntentCreate(rec, req)
		return rec
	}
	owner := accountPrincipal{AccountID: accountID, UserID: userID, Email: "relay-update@example.com", MemberRole: "owner", SessionID: &sessionID}
	if rec := callCreate(accountPrincipal{AccountID: accountID, UserID: userID, MemberRole: "member"}, requestID, "new123"); rec.Code != http.StatusForbidden {
		t.Fatalf("member status=%d body=%s", rec.Code, rec.Body.String())
	}
	var legacyNodeID int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO nodes(account_id,node_type,display_name,status,last_heartbeat_at,capabilities_jsonb)
		VALUES ($1,'relay','legacy-relay','active',now(),'{"relay_version":"old123"}') RETURNING id
	`, accountID).Scan(&legacyNodeID); err != nil {
		t.Fatal(err)
	}
	legacyBody, _ := json.Marshal(relayUpdateIntentRequest{RequestID: "00000000-0000-4000-8000-000000000145", ExpectedPromotedVersion: "new123"})
	legacyReq := withPrincipal(httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/account/nodes/%d/relay-update-intents", legacyNodeID), bytes.NewReader(legacyBody)), owner, fmt.Sprint(legacyNodeID))
	legacyRec := httptest.NewRecorder()
	s.handleAccountRelayUpdateIntentCreate(legacyRec, legacyReq)
	if legacyRec.Code != http.StatusConflict {
		t.Fatalf("legacy relay status=%d body=%s", legacyRec.Code, legacyRec.Body.String())
	}
	if rec := callCreate(owner, "00000000-0000-4000-8000-000000000143", "other123"); rec.Code != http.StatusConflict {
		t.Fatalf("noncurrent version status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := callCreate(owner, requestID, "new123"); rec.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", rec.Code, rec.Body.String())
	}
	s.relayUpdateSnapshot = func(context.Context, string, string) (relayManifestSnapshot, error) {
		t.Fatal("exact replay consulted mutable manifest")
		return relayManifestSnapshot{}, nil
	}
	if rec := callCreate(owner, requestID, "new123"); rec.Code != http.StatusOK {
		t.Fatalf("replay status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := callCreate(owner, requestID, "other123"); rec.Code != http.StatusConflict {
		t.Fatalf("mismatched replay status=%d body=%s", rec.Code, rec.Body.String())
	}
	if snapshotCalls.Load() != 2 {
		t.Fatalf("snapshot calls=%d want 2", snapshotCalls.Load())
	}

	heartbeat := func(reported string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(nodeHeartbeatRequest{CapabilitiesJSON: map[string]any{"relay_version": reported}})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/node/heartbeat", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: nodeID, AccountID: accountID, NodeType: nodeTypeRelay}))
		rec := httptest.NewRecorder()
		s.handleNodeHeartbeat(rec, req)
		return rec
	}
	if rec := heartbeat("old123"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"relay_update_intent"`) {
		t.Fatalf("pending heartbeat status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := heartbeat("new123"); rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), `"relay_update_intent"`) {
		t.Fatalf("completion heartbeat status=%d body=%s", rec.Code, rec.Body.String())
	}
	var status, completedVersion string
	if err := pool.QueryRow(context.Background(), `SELECT status,completed_version FROM relay_update_intents WHERE account_id=$1 AND request_id=$2`, accountID, requestID).Scan(&status, &completedVersion); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || completedVersion != "new123" {
		t.Fatalf("status=%q completed_version=%q", status, completedVersion)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE relay_update_intents SET target_version='tampered' WHERE account_id=$1 AND request_id=$2`, accountID, requestID); err == nil {
		t.Fatal("immutable intent accepted target substitution")
	}
	if rec := callCreate(owner, requestID, "new123"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"completed"`) {
		t.Fatalf("terminal replay status=%d body=%s", rec.Code, rec.Body.String())
	}

	s.relayUpdateSnapshot = func(_ context.Context, expected, fromVersion string) (relayManifestSnapshot, error) {
		return relayManifestSnapshot{Version: expected, Name: "latest-" + expected + ".json", SHA256: strings.Repeat("b", 64), PreviousVersion: fromVersion}, nil
	}
	const concurrentID = "00000000-0000-4000-8000-000000000144"
	responses := make(chan int, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			responses <- callCreate(owner, concurrentID, "new456").Code
		}()
	}
	wg.Wait()
	close(responses)
	counts := map[int]int{}
	for code := range responses {
		counts[code]++
	}
	if counts[http.StatusCreated] != 1 || counts[http.StatusOK] != 1 {
		t.Fatalf("concurrent exact replay status counts=%v", counts)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE account_sessions SET revoked_at=now() WHERE id=$1`, sessionID); err != nil {
		t.Fatal(err)
	}
	if rec := callCreate(owner, concurrentID, "new456"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked-session replay status=%d body=%s", rec.Code, rec.Body.String())
	}
}
