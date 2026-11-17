package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func redownloadCall(f *cleanupFixture, item NASR2CopyItem, out http.ResponseWriter) {
	policy, _, _ := Keep28Policy()
	data, _ := json.Marshal(nasRedownloadRequest{ClipID: item.SourceID, SHA256: item.SHA256, SizeBytes: item.SizeBytes, PolicySHA256: policy})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/redownload", strings.NewReader(string(data)))
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(5), KeyScopes: []string{accountScopePull}}))
	f.s.confineAccountScope(http.HandlerFunc(f.s.handleAccountConnectionRedownload)).ServeHTTP(out, req)
}

func requireRedownloadRefused(t *testing.T, out *httptest.ResponseRecorder) {
	t.Helper()
	if out.Code != http.StatusConflict || strings.Contains(out.Body.String(), `"url"`) {
		t.Fatalf("redownload granted: %d %s", out.Code, out.Body)
	}
}

func TestNASRedownloadMaintenanceRefusesGrant(t *testing.T) {
	f, item := copyTestFixture(t)
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_maintenance SET active=true`); err != nil {
		t.Fatal(err)
	}
	out := httptest.NewRecorder()
	redownloadCall(f, item, out)
	requireRedownloadRefused(t, out)
	if !strings.Contains(out.Body.String(), "maintenance") {
		t.Fatal("refusal omitted maintenance reason", out.Body)
	}
}

func TestNASRedownloadHEADInFlightMaintenanceRefusesGrant(t *testing.T) {
	f, item := copyTestFixture(t)
	started, release := blockCopyHEAD(f)
	defer release()
	out := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { redownloadCall(f, item, out); close(done) }()
	waitCopyHEAD(t, started)
	tx, err := BeginNASR2CopyTx(f.ctx, f.pool)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	// No COPY or legacy cleanup fence may span the network HEAD.
	for _, key := range []int64{nasR2CopyLock, nasCleanupAdmissionLock, nasCleanupPublicFence} {
		var held bool
		if err = tx.QueryRow(f.ctx, `SELECT pg_try_advisory_xact_lock($1)`, key).Scan(&held); err != nil || !held {
			t.Fatalf("HEAD holds fence %d: held=%t err=%v", key, held, err)
		}
	}
	if _, err = tx.Exec(f.ctx, `SELECT singleton FROM nas_cleanup_maintenance WHERE singleton FOR UPDATE NOWAIT;
 UPDATE nas_cleanup_maintenance SET active=true WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case <-done:
		requireRedownloadRefused(t, out)
	case <-time.After(3 * time.Second):
		t.Fatal("redownload did not finish after HEAD")
	}
}

func TestNASRedownloadRevalidatesSourceAfterHEAD(t *testing.T) {
	for name, query := range map[string]string{
		"key":    `UPDATE recording_clips SET object_key='managed/replaced.mp4' WHERE id=901`,
		"purged": `UPDATE recording_clips SET purged_at=clock_timestamp() WHERE id=901`,
		"held": `INSERT INTO nas_cleanup_plans(account_id,connection_id,label,report_sha256,manifest,plan_sha256)
 VALUES(47,13,'redownload-race',repeat('a',64),'[{"kind":"raw","source_id":901}]',repeat('b',64))`,
	} {
		t.Run(name, func(t *testing.T) {
			f, item := copyTestFixture(t)
			started, release := blockCopyHEAD(f)
			defer release()
			out, done := httptest.NewRecorder(), make(chan struct{})
			go func() { redownloadCall(f, item, out); close(done) }()
			waitCopyHEAD(t, started)
			if _, err := f.pool.Exec(f.ctx, query); err != nil {
				t.Fatal(err)
			}
			release()
			select {
			case <-done:
				requireRedownloadRefused(t, out)
			case <-time.After(3 * time.Second):
				t.Fatal("redownload did not revalidate source")
			}
		})
	}
}

type redownloadGrantWriter struct {
	*httptest.ResponseRecorder
	reached, resume chan struct{}
}

func (w *redownloadGrantWriter) WriteHeader(code int) {
	if code == http.StatusOK {
		close(w.reached)
		<-w.resume
	}
	w.ResponseRecorder.WriteHeader(code)
}

func TestNASRedownloadMaintenanceWaitsForGrant(t *testing.T) {
	f, item := copyTestFixture(t)
	out := &redownloadGrantWriter{ResponseRecorder: httptest.NewRecorder(), reached: make(chan struct{}), resume: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(out.resume) }) }
	defer release()
	done := make(chan struct{})
	go func() { redownloadCall(f, item, out); close(done) }()
	select {
	case <-out.reached:
	case <-time.After(3 * time.Second):
		t.Fatal("grant did not reach publication")
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, `SELECT singleton FROM nas_cleanup_maintenance WHERE singleton FOR UPDATE NOWAIT`)
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "55P03" {
		t.Fatalf("grant did not hold maintenance singleton: %v", err)
	}
	tx.Rollback(f.ctx)
	release()
	select {
	case <-done:
		if out.Code != http.StatusOK {
			t.Fatalf("in-flight grant failed: %d %s", out.Code, out.Body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("grant did not release its fences")
	}
}

func TestNASRedownloadMaintenanceCleanupLockOrder(t *testing.T) {
	f, item := copyTestFixture(t)
	cleanup, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup.Rollback(f.ctx)
	if _, err = cleanup.Exec(f.ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPublicFence); err != nil {
		t.Fatal(err)
	}
	if err = cleanupMaintenanceCheck(f.ctx, cleanup); err != nil {
		t.Fatal(err)
	}
	fenced := make(chan error, 1)
	go func() {
		tx, err := BeginNASR2CopyTx(f.ctx, f.pool)
		if err != nil {
			fenced <- err
			return
		}
		defer tx.Rollback(f.ctx)
		if _, err = tx.Exec(f.ctx, `SELECT pg_advisory_xact_lock($1)`, nasR2CopyLock); err == nil {
			_, err = tx.Exec(f.ctx, `UPDATE nas_cleanup_maintenance SET active=true WHERE singleton`)
		}
		if err == nil {
			err = tx.Commit(f.ctx)
		}
		fenced <- err
	}()
	copyLockWaiters(t, f, 1)
	out, done := httptest.NewRecorder(), make(chan struct{})
	go func() { redownloadCall(f, item, out); close(done) }()
	copyLockWaiters(t, f, 2)
	// If redownload took1103/public before1192, cleanup could not finish
	// while maintenance waits for its shared singleton: a three-way cycle.
	var held bool
	if err = cleanup.QueryRow(f.ctx, `SELECT pg_try_advisory_xact_lock($1)`, nasCleanupAdmissionLock).Scan(&held); err != nil || !held {
		t.Fatalf("redownload inverted COPY/cleanup order: held=%t err=%v", held, err)
	}
	if err = cleanup.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-fenced:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("maintenance did not drain cleanup")
	}
	select {
	case <-done:
		requireRedownloadRefused(t, out)
	case <-time.After(3 * time.Second):
		t.Fatal("redownload cycled with maintenance/cleanup")
	}
}
