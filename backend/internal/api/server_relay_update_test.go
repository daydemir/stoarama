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
	"sync/atomic"
	"testing"
	"time"
)

func TestSnapshotPromotedRelayManifestBindsImmutableBytes(t *testing.T) {
	live := []byte(`{"version":"new123","previous_version":"old123","relay":{"linux-arm64":{"artifact":"relay.tgz","sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}},"ytdlp":{},"deno":{}}`)
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
	snapshot, err := snapshotPromotedRelayManifest(context.Background(), "new123", "old123", "linux-arm64", open)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != "new123" || snapshot.PreviousVersion != "old123" || snapshot.Name != "latest-new123.json" || snapshot.ArtifactSHA256 != strings.Repeat("c", 64) || len(snapshot.SHA256) != 64 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if _, err := snapshotPromotedRelayManifest(context.Background(), "new123", "candidate999", "linux-arm64", open); err == nil || !strings.Contains(err.Error(), "immediate successor") {
		t.Fatalf("unproven ancestry accepted: %v", err)
	}
	objects[relayReleasePrefix+"latest-new123.json"] = []byte(`{"version":"new123","relay":{"changed":{}}}`)
	if _, err := snapshotPromotedRelayManifest(context.Background(), "new123", "old123", "linux-arm64", open); err == nil || !strings.Contains(err.Error(), "differs") {
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
		VALUES ($1,'relay','update-relay','active',now(),'{"relay_version":"old123","relay_update_intents_v1":true,"relay_update_runtime_target":"linux-arm64"}') RETURNING id
	`, accountID).Scan(&nodeID); err != nil {
		t.Fatal(err)
	}
	nodeSecret := "relay-update-node-token"
	var nodeTokenID, claimGeneration int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO node_tokens(node_id,key_prefix,secret_hash)
		VALUES($1,'relay-upd',$2) RETURNING id,recording_claim_generation
	`, nodeID, hashSecret(nodeSecret)).Scan(&nodeTokenID, &claimGeneration); err != nil {
		t.Fatal(err)
	}
	var snapshotCalls atomic.Int64
	s.relayUpdateSnapshot = func(_ context.Context, expected, fromVersion, runtimeTarget string) (relayManifestSnapshot, error) {
		snapshotCalls.Add(1)
		if expected != "new123" {
			return relayManifestSnapshot{}, fmt.Errorf("not current")
		}
		return relayManifestSnapshot{Version: expected, Name: "latest-new123.json", SHA256: strings.Repeat("a", 64), PreviousVersion: fromVersion, RuntimeTarget: runtimeTarget, ArtifactSHA256: strings.Repeat("b", 64)}, nil
	}
	requestID := "00000000-0000-4000-8000-000000000142"
	callCreate := func(p accountPrincipal, id, expected string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(relayUpdateIntentRequest{RequestID: id, ExpectedPromotedVersion: expected})
		req := withPrincipal(httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/account/nodes/%d/relay-update-intents", nodeID), bytes.NewReader(body)), p, fmt.Sprint(nodeID))
		rec := httptest.NewRecorder()
		s.handleAccountRelayUpdateIntentCreate(rec, req)
		return rec
	}
	owner := accountPrincipal{AccountID: accountID, UserID: userID, Email: "relay-update@example.com", MemberRole: "owner", SessionID: &sessionID, credentialSHA256: hashSecret("relay-update-session")}
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
	s.relayUpdateSnapshot = func(context.Context, string, string, string) (relayManifestSnapshot, error) {
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

	heartbeat := func(reported string, supports bool, result *relayUpdateAppliedResult) *httptest.ResponseRecorder {
		capabilities := map[string]any{"relay_version": reported, "relay_update_intents_v1": supports}
		if result != nil {
			capabilities["relay_update_result"] = result
		}
		body, _ := json.Marshal(nodeHeartbeatRequest{CapabilitiesJSON: capabilities})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/node/heartbeat", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), nodePrincipalContextKey, nodePrincipal{
			NodeID: nodeID, NodeTokenID: nodeTokenID, NodeClaimGeneration: claimGeneration,
			NodeClaimPurpose: "claim_current", AccountID: accountID, NodeType: nodeTypeRelay,
			credentialSHA256: hashSecret(nodeSecret),
		}))
		rec := httptest.NewRecorder()
		s.handleNodeHeartbeat(rec, req)
		return rec
	}
	if rec := heartbeat("old123", false, nil); rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), `"relay_update_intent"`) {
		t.Fatalf("disabled capability status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := heartbeat("old123", true, nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"relay_update_intent"`) {
		t.Fatalf("pending heartbeat status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := heartbeat("new123", true, nil); rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), `"relay_update_ack_request_id"`) {
		t.Fatalf("version-only completion status=%d body=%s", rec.Code, rec.Body.String())
	}
	result := &relayUpdateAppliedResult{
		RequestID: requestID, TargetVersion: "new123", ManifestName: "latest-new123.json",
		ManifestSHA256: strings.Repeat("a", 64), RelayArtifactSHA256: strings.Repeat("b", 64),
		InstalledSHA256: strings.Repeat("b", 64), AppliedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if rec := heartbeat("new123", true, result); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"relay_update_ack_request_id":"`+requestID+`"`) {
		t.Fatalf("proof completion status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := heartbeat("new123", true, result); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"relay_update_ack_request_id":"`+requestID+`"`) {
		t.Fatalf("proof replay status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(context.Background(), `UPDATE relay_update_intents SET target_version='tampered' WHERE account_id=$1 AND request_id=$2`, accountID, requestID); err == nil {
		t.Fatal("immutable intent accepted target substitution")
	}
	if _, err := pool.Exec(context.Background(), `DELETE FROM relay_update_intents WHERE account_id=$1 AND request_id=$2`, accountID, requestID); err == nil {
		t.Fatal("runtime deleted immutable intent")
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO relay_update_intents(account_id,node_id,request_id,node_version_at_request,runtime_target,target_version,manifest_name,manifest_sha256,relay_artifact_sha256,requested_by_user_id) VALUES($1,$2,gen_random_uuid(),'old123','linux-arm64','new123','latest-new123.json',repeat('a',64),repeat('b',64),$3)`, accountID, nodeID, userID); err == nil {
		t.Fatal("runtime forged relay update intent")
	}
	if rec := callCreate(owner, requestID, "new123"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"completed"`) {
		t.Fatalf("terminal replay status=%d body=%s", rec.Code, rec.Body.String())
	}

	s.relayUpdateSnapshot = func(_ context.Context, expected, fromVersion, runtimeTarget string) (relayManifestSnapshot, error) {
		return relayManifestSnapshot{Version: expected, Name: "latest-" + expected + ".json", SHA256: strings.Repeat("b", 64), PreviousVersion: fromVersion, RuntimeTarget: runtimeTarget, ArtifactSHA256: strings.Repeat("c", 64)}, nil
	}
	const concurrentID = "00000000-0000-4000-8000-000000000144"
	claimTx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claimTx.Exec(context.Background(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		_ = claimTx.Rollback(context.Background())
		t.Fatal(err)
	}
	created := make(chan *httptest.ResponseRecorder, 1)
	go func() { created <- callCreate(owner, concurrentID, "new456") }()
	select {
	case rec := <-created:
		_ = claimTx.Rollback(context.Background())
		t.Fatalf("update intent crossed an in-flight lease claim fence: status=%d body=%s", rec.Code, rec.Body.String())
	case <-time.After(75 * time.Millisecond):
	}
	if err := claimTx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case rec := <-created:
		if rec.Code != http.StatusCreated {
			t.Fatalf("claim-first update status=%d body=%s", rec.Code, rec.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("update intent did not resume after claim fence")
	}
	var nodeStatus string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM nodes WHERE id=$1`, nodeID).Scan(&nodeStatus); err != nil || nodeStatus != "disabled" {
		t.Fatalf("update-first node status=%q err=%v", nodeStatus, err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE nodes SET status='active' WHERE id=$1`, nodeID); err == nil {
		t.Fatal("runtime bypassed typed update drain state")
	}
	lease, err := s.leaseRelayRecordingJob(context.Background(), nodePrincipal{
		NodeID: nodeID, NodeTokenID: nodeTokenID, NodeClaimGeneration: claimGeneration,
		AccountID: accountID, NodeType: nodeTypeRelay,
	}, true, 60, true)
	if err != nil {
		t.Fatalf("lease after update reservation: %v", err)
	}
	if lease.JobID != 0 {
		t.Fatalf("disabled update-reserved node leased recording job %d", lease.JobID)
	}
	if rec := callCreate(owner, concurrentID, "new456"); rec.Code != http.StatusOK {
		t.Fatalf("exact replay status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(context.Background(), `UPDATE account_sessions SET revoked_at=now() WHERE id=$1`, sessionID); err != nil {
		t.Fatal(err)
	}
	if rec := callCreate(owner, concurrentID, "new456"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked-session replay status=%d body=%s", rec.Code, rec.Body.String())
	}
}
