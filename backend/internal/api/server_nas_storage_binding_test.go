package api

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type storageBudgetObservation struct {
	version, nonce           string
	issued, budget, received *time.Time
	free                     *int64
}

func storageHeartbeatFixture(t *testing.T) (*Server, *pgxpool.Pool) {
	t.Helper()
	pool, cleanup := testAccountClipsPool(t)
	t.Cleanup(cleanup)
	if _, err := pool.Exec(context.Background(), `INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',123)`); err != nil {
		t.Fatal(err)
	}
	// Use the actual storage-proof constraints for heartbeat writes as well as
	// the dedicated migration-preservation oracle below.
	if _, err := pool.Exec(context.Background(), `ALTER TABLE connections DROP COLUMN nas_storage_client_version,DROP COLUMN nas_storage_challenge_nonce,DROP COLUMN nas_storage_challenge_issued_at,DROP COLUMN nas_storage_budget_at`); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"0203_nas_storage_client_binding.sql", "0208_nas_storage_v6_binding.sql"} {
		body, err := os.ReadFile("../../../infra/sql/migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(context.Background(), string(body)); err != nil {
			t.Fatal(err)
		}
	}
	return &Server{pool: pool}, pool
}
func storageHeartbeat(t *testing.T, s *Server, version string, sample *connectionStorageStatus) string {
	t.Helper()
	b, err := json.Marshal(connectionHeartbeatRequest{ClientVersion: version, ClientPhase: "idle", ClientPreviousExit: "unknown", ClientBootID: "storage-test", Storage: sample})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/heartbeat", bytes.NewReader(b))
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(123)}))
	rec := httptest.NewRecorder()
	s.handleAccountConnectionHeartbeat(rec, req)
	if rec.Code != 200 {
		t.Fatalf("heartbeat: %d %s", rec.Code, rec.Body.String())
	}
	var response connectionHeartbeatResponse
	if err = json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	nonce := response.StorageChallengeNonce
	if nasStorageBudgetVersion(version) != "" {
		if decoded, err := hex.DecodeString(nonce); err != nil || len(decoded) != 32 {
			t.Fatal("invalid v5 challenge shape")
		}
	} else if nonce != "" || bytes.Contains(rec.Body.Bytes(), []byte("storage_challenge_nonce")) {
		t.Fatal("private v5 field leaked into legacy heartbeat contract")
	}
	return nonce
}
func storageObservation(t *testing.T, pool *pgxpool.Pool) storageBudgetObservation {
	t.Helper()
	var o storageBudgetObservation
	if err := pool.QueryRow(context.Background(), `SELECT nas_storage_client_version,nas_storage_challenge_nonce,nas_storage_challenge_issued_at,nas_storage_budget_at,nas_storage_reported_at,nas_storage_free_bytes FROM connections WHERE id=13`).Scan(&o.version, &o.nonce, &o.issued, &o.budget, &o.received, &o.free); err != nil {
		t.Fatal(err)
	}
	return o
}
func storageSample(nonce string) *connectionStorageStatus {
	return &connectionStorageStatus{Available: true, TotalBytes: 3000000000000, FreeBytes: 2500000000000, ChallengeNonce: nonce}
}

func TestConnectionHeartbeatStorageBudgetBindsActualSample(t *testing.T) {
	s, pool := storageHeartbeatFixture(t)
	nonce := storageHeartbeat(t, s, NASStorageBudgetClientVersion, storageSample(""))
	if o := storageObservation(t, pool); o.version != "" || o.budget != nil || o.free == nil {
		t.Fatalf("first sample not display-only: %+v", o)
	}
	old := storageObservation(t, pool)
	next := storageHeartbeat(t, s, NASStorageBudgetClientVersion, storageSample(nonce))
	o := storageObservation(t, pool)
	if o.version != NASStorageBudgetClientVersion || o.budget == nil || !o.budget.Equal(*old.issued) || next == nonce || o.nonce != next {
		t.Fatalf("sample not bound to OLD issuance: %+v", o)
	}
	budget := *o.budget
	storageHeartbeat(t, s, "r2-copy-v4", nil)
	o = storageObservation(t, pool)
	if o.version != NASStorageBudgetClientVersion || o.budget == nil || !o.budget.Equal(budget) {
		t.Fatal("omitted storage changed sample proof")
	}
	for _, version := range []string{"r2-copy-v4", "v85r-nas-proof-v2.2", "r2-copy-v5-extra"} {
		nonce = storageObservation(t, pool).nonce
		storageHeartbeat(t, s, version, storageSample(nonce))
		o = storageObservation(t, pool)
		if o.version != "" || o.budget != nil || o.free == nil {
			t.Fatalf("legacy storage earned budget: %+v", o)
		}
	}
	nonce = storageObservation(t, pool).nonce
	storageHeartbeat(t, s, NASStorageBudgetClientVersion, storageSample(nonce))
	storageHeartbeat(t, s, NASStorageBudgetClientVersion, &connectionStorageStatus{Available: false})
	o = storageObservation(t, pool)
	if o.version != "" || o.budget != nil || o.free != nil {
		t.Fatal("unavailable storage kept proof")
	}
}

func TestConnectionHeartbeatStorageNonceLostResponseAndReplay(t *testing.T) {
	s, pool := storageHeartbeatFixture(t)
	nonce := storageHeartbeat(t, s, NASStorageBudgetClientVersion, nil)
	_ = storageHeartbeat(t, s, NASStorageBudgetClientVersion, storageSample(nonce)) // response lost
	replacement := storageHeartbeat(t, s, NASStorageBudgetClientVersion, storageSample(nonce))
	o := storageObservation(t, pool)
	if o.version != "" || o.budget != nil || o.free == nil {
		t.Fatal("lost-response old nonce earned budget")
	}
	good := storageHeartbeat(t, s, NASStorageBudgetClientVersion, storageSample(replacement))
	if o = storageObservation(t, pool); o.version != NASStorageBudgetClientVersion {
		t.Fatal("fresh response did not recover")
	}
	replay := storageHeartbeat(t, s, NASStorageBudgetClientVersion, storageSample(replacement))
	if o = storageObservation(t, pool); o.version != "" || o.budget != nil || replay == good {
		t.Fatal("replayed nonce earned budget or failed rotation")
	}
}

func TestConnectionHeartbeatStorageBufferedV5PreservesIssuance(t *testing.T) {
	for _, frame := range []struct {
		name     string
		age      time.Duration
		accepted bool
	}{
		{"issued0_sample1_delivery200", 200 * time.Second, false},
		{"issued60_sample64_delivery200", 140 * time.Second, true},
	} {
		t.Run(frame.name, func(t *testing.T) {
			s, pool := storageHeartbeatFixture(t)
			nonce := storageHeartbeat(t, s, NASStorageBudgetClientVersion, nil)
			// A complete genuine v5 payload was sampled before t65 and buffered until t200.
			sample := storageSample(nonce)
			issued := time.Now().UTC().Truncate(time.Microsecond).Add(-frame.age)
			if _, err := pool.Exec(context.Background(), `UPDATE connections SET nas_storage_challenge_issued_at=$1 WHERE id=13`, issued); err != nil {
				t.Fatal(err)
			}
			storageHeartbeat(t, s, NASStorageBudgetClientVersion, sample)
			o := storageObservation(t, pool)
			if o.received == nil || o.free == nil || *o.free != sample.FreeBytes {
				t.Fatal("buffered sample did not update display normally")
			}
			if frame.accepted {
				if o.version != NASStorageBudgetClientVersion || o.budget == nil || !o.budget.Equal(issued) {
					t.Fatal("t60 challenge restamped at t200")
				}
				// At t250 the accepted issuance is190s old, although receipt is50s old.
				if o.received.Add(50*time.Second).Sub(*o.budget) <= NASStorageBudgetMaxAge {
					t.Fatal("harder timeline did not expire at t250")
				}
			} else if o.version != "" || o.budget != nil {
				t.Fatal("expired genuine v5 nonce earned budget")
			}
		})
	}
}

func TestConnectionHeartbeatStorageNonceSingleUse(t *testing.T) {
	s, pool := storageHeartbeatFixture(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `CREATE TABLE storage_budget_events(at timestamptz);
 CREATE FUNCTION record_storage_budget_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.nas_storage_budget_at IS NOT NULL THEN INSERT INTO storage_budget_events VALUES(NEW.nas_storage_budget_at);END IF;RETURN NEW;END $$;
 CREATE TRIGGER storage_budget_event AFTER UPDATE ON connections FOR EACH ROW EXECUTE FUNCTION record_storage_budget_event()`); err != nil {
		t.Fatal(err)
	}
	nonce := storageHeartbeat(t, s, NASStorageBudgetClientVersion, nil)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); storageHeartbeat(t, s, NASStorageBudgetClientVersion, storageSample(nonce)) }()
	}
	wg.Wait()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM storage_budget_events`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("nonce accepted %d times: %v", count, err)
	}
}

func TestNASStorageBindingMigrationLeavesOldSamplesIneligible(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `ALTER TABLE connections DROP COLUMN nas_storage_client_version,DROP COLUMN nas_storage_challenge_nonce,DROP COLUMN nas_storage_challenge_issued_at,DROP COLUMN nas_storage_budget_at;
 INSERT INTO connections(account_id,kind,api_key_id,nas_storage_total_bytes,nas_storage_free_bytes,nas_storage_reported_at,client_version) VALUES(47,'nas_pull',123,1000,500,now(),'r2-copy-v5')`); err != nil {
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
	var budget *time.Time
	if err = pool.QueryRow(ctx, `SELECT nas_storage_client_version,nas_storage_budget_at FROM connections WHERE api_key_id=123`).Scan(&tag, &budget); err != nil || tag != "" || budget != nil {
		t.Fatal("old sample inherited proof")
	}
	for _, sql := range []string{`UPDATE connections SET nas_storage_client_version='r2-copy-v4'`, `UPDATE connections SET nas_storage_client_version='r2-copy-v5'`, `UPDATE connections SET nas_storage_challenge_issued_at=now()`} {
		if _, err = pool.Exec(ctx, sql); err == nil {
			t.Fatal("invalid proof binding accepted")
		}
	}
}

func TestConnectionHeartbeatStorageV6UsesSameNonceContract(t *testing.T) {
	s, pool := storageHeartbeatFixture(t)
	nonce := storageHeartbeat(t, s, NASR2CopyPagedClientVersion, storageSample(""))
	first := storageObservation(t, pool)
	if first.budget != nil || first.version != "" {
		t.Fatal("unproven v6 sample earned budget")
	}
	next := storageHeartbeat(t, s, NASR2CopyPagedClientVersion, storageSample(nonce))
	second := storageObservation(t, pool)
	if second.version != NASR2CopyPagedClientVersion || second.budget == nil || !second.budget.Equal(*first.issued) || next == nonce || !SupportsNASStorageBudget(second.version) {
		t.Fatal("v6 proof not bound to preceding challenge", second)
	}
	replacement := storageHeartbeat(t, s, NASR2CopyPagedClientVersion, storageSample(nonce))
	if o := storageObservation(t, pool); o.budget != nil || o.version != "" {
		t.Fatal("v6 replay retained proof")
	}
	storageHeartbeat(t, s, NASR2CopyPagedClientVersion, storageSample(replacement))
	storageHeartbeat(t, s, "r2-copy-v6-extra", storageSample(storageObservation(t, pool).nonce))
	if o := storageObservation(t, pool); o.budget != nil || o.version != "" {
		t.Fatal("unknown v6 suffix earned proof")
	}
}

func TestConnectionHeartbeatStorageV8UsesSameNonceContract(t *testing.T) {
	s, pool := storageHeartbeatFixture(t)
	body, err := os.ReadFile("../../../infra/sql/migrations/0235_nas_storage_v8_binding.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), string(body)); err != nil {
		t.Fatal(err)
	}
	nonce := storageHeartbeat(t, s, nasLowMemoryCopyClientVersion, storageSample(""))
	issued := storageObservation(t, pool).issued
	storageHeartbeat(t, s, nasLowMemoryCopyClientVersion, storageSample(nonce))
	observation := storageObservation(t, pool)
	if issued == nil || observation.version != nasLowMemoryCopyClientVersion ||
		observation.budget == nil || !observation.budget.Equal(*issued) {
		t.Fatal("v8 proof did not bind the preceding challenge", observation)
	}
}

func TestNASStorageBindingV6MigrationPreservesEvidence(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `ALTER TABLE connections DROP COLUMN nas_storage_client_version,DROP COLUMN nas_storage_challenge_nonce,DROP COLUMN nas_storage_challenge_issued_at,DROP COLUMN nas_storage_budget_at;`); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile("../../../infra/sql/migrations/0203_nas_storage_client_binding.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(old)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO connections(account_id,kind,api_key_id,nas_storage_client_version,nas_storage_budget_at) VALUES(47,'nas_pull',123,'r2-copy-v5',now()-interval '1 minute'),(47,'nas_pull',124,'',NULL)`); err != nil {
		t.Fatal(err)
	}
	snapshot := `SELECT jsonb_agg(jsonb_build_array(api_key_id,nas_storage_client_version,nas_storage_budget_at) ORDER BY api_key_id)::text FROM connections WHERE api_key_id IN(123,124)`
	var before, after string
	if err = pool.QueryRow(ctx, snapshot).Scan(&before); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile("../../../infra/sql/migrations/0208_nas_storage_v6_binding.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(body)); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, snapshot).Scan(&after); err != nil || before != after {
		t.Fatal("migration changed evidence", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version='r2-copy-v6',nas_storage_budget_at=now() WHERE api_key_id=123`); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{`UPDATE connections SET nas_storage_client_version='r2-copy-v6-extra' WHERE api_key_id=123`, `UPDATE connections SET nas_storage_budget_at=NULL WHERE api_key_id=123`, `UPDATE connections SET nas_storage_client_version='' WHERE api_key_id=123`, `UPDATE connections SET nas_storage_client_version='r2-copy-v6' WHERE api_key_id=124`} {
		if _, err = pool.Exec(ctx, sql); err == nil {
			t.Fatal("invalid v6 proof pair accepted")
		}
	}
}
