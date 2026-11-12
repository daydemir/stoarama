package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func installDeliveryFenceTestSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	body, err := os.ReadFile("../../../infra/sql/migrations/0185_nas_delivery_retirement_fence.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), string(body)); err != nil {
		t.Fatal(err)
	}
}

func TestNASDeliveryFenceRefusesOutsideAndDrains(t *testing.T) {
	f := newCleanupFixture(t, 1)
	_, windows, _ := Keep28Policy()
	window := windows[3]
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id,clip_start_at,clip_end_at) VALUES(901,2,$1,$1::timestamptz+interval '1 minute'),(902,2,$2,$2::timestamptz+interval '1 minute')`, window.Start.Add(-time.Hour), window.Start); err != nil {
		t.Fatal(err)
	}
	call := func(id int64) *httptest.ResponseRecorder {
		request := httptest.NewRequest("GET", "/api/v1/account/recordings/2/clips/901/download", nil)
		request = request.WithContext(context.WithValue(request.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(5)}))
		response := httptest.NewRecorder()
		f.s.fenceNASDelivery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if f.s.allowNASDelivery(w, r, "raw", id) {
				w.WriteHeader(200)
			}
		})).ServeHTTP(response, request)
		return response
	}
	if response := call(901); response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	var leases int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_delivery_download_leases`).Scan(&leases); err != nil || leases != 1 {
		t.Fatal(leases, err)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if err = ActivateNASDeliveryFence(f.ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if response := call(901); response.Code != 409 {
		t.Fatal("outside delivery allowed", response.Code, response.Body.String())
	}
	if response := call(902); response.Code != 200 {
		t.Fatal("kept delivery refused", response.Body.String())
	}
	if response := call(999999); response.Code != 409 {
		t.Fatal("unknown delivery allowed")
	}
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO nas_delivery_retirement_settlements VALUES(13,now(),'workers stopped and waited')`); err == nil {
		t.Fatal("unexpired capabilities settled")
	}
	// Test time travel is confined to the disposable database and bypasses the
	// immutable audit triggers; production can only wait for real expiry.
	if _, err = f.pool.Exec(f.ctx, `SET session_replication_role=replica;UPDATE nas_delivery_retirement_fences SET effective_at=now()-interval '9 days',legacy_expires_at=now()-interval '2 days';RESET session_replication_role`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO nas_delivery_retirement_settlements VALUES(13,now(),'workers stopped and waited')`); err == nil {
		t.Fatal("outstanding lease ignored")
	}
	if _, err = f.pool.Exec(f.ctx, `SET session_replication_role=replica;UPDATE nas_delivery_download_leases SET expires_at=now()-interval '1 minute';RESET session_replication_role`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO nas_delivery_retirement_settlements(connection_id,drain_acknowledgment) VALUES(13,'NAS workers stopped; every transfer waited and all URLs expired')`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(f.ctx, `DELETE FROM nas_delivery_retirement_fences`); err == nil {
		t.Fatal("fence mutable")
	}
}
