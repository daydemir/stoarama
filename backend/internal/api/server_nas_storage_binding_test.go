package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestConnectionHeartbeatStorageBudgetBindsActualSample(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',123)`); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	call := func(version string, storage *connectionStorageStatus) {
		t.Helper()
		body, err := json.Marshal(connectionHeartbeatRequest{ClientVersion: version, ClientPhase: "idle", ClientPreviousExit: "unknown", ClientBootID: "storage-test", Storage: storage})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/heartbeat", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(123)}))
		rec := httptest.NewRecorder()
		s.handleAccountConnectionHeartbeat(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("heartbeat %s: %d %s", version, rec.Code, rec.Body.String())
		}
	}
	sample := &connectionStorageStatus{Available: true, TotalBytes: 1000, FreeBytes: 500}
	check := func(want string) {
		t.Helper()
		var bound string
		if err := pool.QueryRow(ctx, `SELECT nas_storage_client_version FROM connections WHERE id=13`).Scan(&bound); err != nil || bound != want {
			t.Fatalf("sample binding=%q want=%q error=%v", bound, want, err)
		}
	}
	for _, version := range []string{"r2-copy-v4", "v85r-nas-proof-v2.2", "r2-copy-v5-extra"} {
		call(version, sample)
		check("")
	}
	call("r2-copy-v4", sample)
	call(NASStorageBudgetClientVersion, nil)
	check("") // No old-sample promotion.
	call(NASStorageBudgetClientVersion, sample)
	check(NASStorageBudgetClientVersion)
	call("r2-copy-v4", nil)
	check(NASStorageBudgetClientVersion) // Binding belongs to sample, not latest heartbeat.
	call("r2-copy-v4", sample)
	check("")
	call(NASStorageBudgetClientVersion, sample)
	call(NASStorageBudgetClientVersion, &connectionStorageStatus{Available: false})
	check("")
}

func TestNASStorageBindingMigrationLeavesOldSamplesIneligible(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `ALTER TABLE connections DROP COLUMN nas_storage_client_version;INSERT INTO connections(account_id,kind,api_key_id,nas_storage_total_bytes,nas_storage_free_bytes,nas_storage_reported_at,client_version) VALUES(47,'nas_pull',123,1000,500,now(),'r2-copy-v5')`); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("../../../infra/sql/migrations/0203_nas_storage_client_binding.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	var tag string
	if err = pool.QueryRow(ctx, `SELECT nas_storage_client_version FROM connections WHERE api_key_id=123`).Scan(&tag); err != nil || tag != "" {
		t.Fatalf("old sample inherited client version: %q %v", tag, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version='r2-copy-v4'`); err == nil {
		t.Fatal("unreviewed budget binding accepted")
	}
}
