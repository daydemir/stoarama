package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setCleanupMaster flips the durable master switch through the audited
// operator handler, exactly as `stoaramactl nas-cleanup master` does.
func setCleanupMaster(t testing.TB, s *Server, on bool) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"enabled": on, "actor": "test", "reason": "fixture"})
	w := httptest.NewRecorder()
	s.handleAdminNASCleanupMasterSet(w, httptest.NewRequest("POST", "/api/v1/service/nas-cleanup/master", bytes.NewReader(b)))
	if w.Code != 200 {
		t.Fatalf("set cleanup master %v: %d %s", on, w.Code, w.Body.String())
	}
}

func cleanupMasterCall(s *Server, body map[string]any) *httptest.ResponseRecorder {
	b, _ := json.Marshal(body)
	w := httptest.NewRecorder()
	s.handleAdminNASCleanupMasterSet(w, httptest.NewRequest("POST", "/api/v1/service/nas-cleanup/master", bytes.NewReader(b)))
	return w
}

type cleanupMasterHistoryRow struct {
	Actor, Reason string
	Old           *bool
	New           bool
}

func cleanupMasterHistory(t *testing.T, pool *pgxpool.Pool) []cleanupMasterHistoryRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT actor,reason,old_enabled,new_enabled FROM nas_cleanup_master_history ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (cleanupMasterHistoryRow, error) {
		var h cleanupMasterHistoryRow
		return h, r.Scan(&h.Actor, &h.Reason, &h.Old, &h.New)
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestNASCleanupMasterDefaultsOffAndAuditsEveryChange(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	if cleanupMasterOn(ctx, pool) {
		t.Fatal("master must ship OFF")
	}
	status := httptest.NewRecorder()
	s.handleAdminNASCleanupMasterStatus(status, httptest.NewRequest("GET", "/api/v1/service/nas-cleanup/master", nil))
	var m cleanupMasterState
	if status.Code != 200 || json.Unmarshal(status.Body.Bytes(), &m) != nil || m.Enabled || m.Actor != "migration 0229" {
		t.Fatalf("status %d %s", status.Code, status.Body.String())
	}
	for _, body := range []map[string]any{
		{"enabled": true, "actor": "test"},
		{"enabled": true, "actor": "test", "reason": ""},
		{"actor": "test", "reason": "no state"},
		{"enabled": true, "reason": "no actor"},
	} {
		if w := cleanupMasterCall(s, body); w.Code != 400 {
			t.Fatalf("incomplete change %v accepted: %d", body, w.Code)
		}
	}
	type change struct {
		Before, After cleanupMasterState
		Changed       bool
	}
	flip := func(on bool, reason string) change {
		t.Helper()
		w := cleanupMasterCall(s, map[string]any{"enabled": on, "actor": "ops@jelly", "reason": reason})
		var c change
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &c) != nil {
			t.Fatalf("flip: %d %s", w.Code, w.Body.String())
		}
		return c
	}
	if c := flip(true, "start plan 7"); !c.Changed || c.Before.Enabled || !c.After.Enabled || c.After.Actor != "ops@jelly" || c.After.Reason != "start plan 7" || !c.After.ChangedAt.After(c.Before.ChangedAt) {
		t.Fatalf("on: %+v", c)
	}
	if !cleanupMasterOn(ctx, pool) {
		t.Fatal("master ON not read live")
	}
	if c := flip(true, "again"); c.Changed || !c.Before.Enabled || c.After != c.Before {
		t.Fatalf("idempotent on changed state: %+v", c)
	}
	if c := flip(false, "deploy window"); !c.Changed || !c.Before.Enabled || c.After.Enabled {
		t.Fatalf("off: %+v", c)
	}
	on, off := true, false
	want := []cleanupMasterHistoryRow{
		{Actor: "migration 0229", Reason: "initial state: OFF", New: false},
		{Actor: "ops@jelly", Reason: "start plan 7", Old: &off, New: true},
		{Actor: "ops@jelly", Reason: "deploy window", Old: &on, New: false},
	}
	got := cleanupMasterHistory(t, pool)
	if len(got) != len(want) {
		t.Fatalf("history %+v", got)
	}
	for n := range want {
		if got[n].Actor != want[n].Actor || got[n].Reason != want[n].Reason || got[n].New != want[n].New || (got[n].Old == nil) != (want[n].Old == nil) || (got[n].Old != nil && *got[n].Old != *want[n].Old) {
			t.Fatalf("history[%d] = %+v, want %+v", n, got[n], want[n])
		}
	}
	for _, sql := range []string{
		`UPDATE nas_cleanup_master_history SET reason='rewritten'`,
		`DELETE FROM nas_cleanup_master_history`,
		`TRUNCATE nas_cleanup_master_history`,
		`INSERT INTO nas_cleanup_master_history(changed_at,actor,old_enabled,new_enabled,reason) VALUES(now(),'forged',false,true,'forged')`,
		`DELETE FROM nas_cleanup_master`,
		`TRUNCATE nas_cleanup_master`,
		`UPDATE nas_cleanup_master SET reason='silent edit'`,
		`INSERT INTO nas_cleanup_master(enabled,actor,reason) VALUES(true,'second','row')`,
	} {
		if _, err := pool.Exec(ctx, sql); err == nil {
			t.Fatalf("audit bypass accepted: %s", sql)
		}
	}
	if got := cleanupMasterHistory(t, pool); len(got) != len(want) {
		t.Fatalf("history changed: %+v", got)
	}
	var always int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_trigger WHERE tgrelid IN ('nas_cleanup_master'::regclass,'nas_cleanup_master_history'::regclass) AND NOT tgisinternal AND tgenabled='A'`).Scan(&always); err != nil || always != 5 {
		t.Fatalf("ENABLE ALWAYS triggers %d %v", always, err)
	}
}

func TestNASCleanupMasterFailsClosed(t *testing.T) {
	ctx := context.Background()
	for name, breakControl := range map[string]string{
		"missing row":       `ALTER TABLE nas_cleanup_master DISABLE TRIGGER USER;DELETE FROM nas_cleanup_master`,
		"unparseable value": `ALTER TABLE nas_cleanup_master DISABLE TRIGGER USER,ALTER COLUMN enabled DROP NOT NULL;UPDATE nas_cleanup_master SET enabled=NULL`,
		"unreadable table":  `ALTER TABLE nas_cleanup_master RENAME TO nas_cleanup_master_gone`,
	} {
		t.Run(name, func(t *testing.T) {
			pool := cleanupTestPool(t)
			s := &Server{pool: pool}
			setCleanupMaster(t, s, true)
			if _, err := pool.Exec(ctx, breakControl); err != nil {
				t.Fatal(err)
			}
			if cleanupMasterOn(ctx, pool) {
				t.Fatal("broken master control read as ON")
			}
			if w := cleanupCall(t, s.handleAdminNASCleanupControl, 47, 5, "id", "1", map[string]any{"plan_sha256": strings.Repeat("b", 64), "mode": "enabled", "reason": "fixture"}); w.Code != 409 || !strings.Contains(w.Body.String(), "master switch") {
				t.Fatalf("broken master allowed arming: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

// Flipping the durable master ON lets an approved, enabled plan execute;
// flipping it OFF mid-plan stops every further lease and permit at once, and
// waits for an in-flight permit decision that already read it ON.
func TestNASCleanupMasterGovernsAnEnabledPlanLive(t *testing.T) {
	f := newCleanupFixture(t, 24)
	p1, sha1, _ := f.admit("master-live", syntheticFiles(100, 4, 10))
	f.dryRun()
	if code, body := f.enableBatch([]int64{p1}, []string{sha1}, 2); code != 409 || !strings.Contains(body, "master switch") {
		t.Fatalf("master OFF allowed batch enable: %d %s", code, body)
	}
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{p1}, []string{sha1}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	execute := func() []cleanupTask {
		t.Helper()
		tasks := f.leaseV2(64, 1, 1<<40, "")
		if len(tasks) != 1 || tasks[0].ApprovedProof == nil {
			t.Fatalf("master ON did not lease execution: %+v", tasks)
		}
		return tasks
	}
	first := execute()
	r := f.observe(first, nil)
	if !r[0].Delete {
		t.Fatalf("master ON withheld the permit: %+v", r[0])
	}
	f.receipts(first, r, "deleted")
	second := execute()

	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !cleanupMasterOn(f.ctx, tx) {
		t.Fatal("in-flight decision read OFF")
	}
	flipped := make(chan int)
	go func() {
		flipped <- cleanupMasterCall(f.s, map[string]any{"enabled": false, "actor": "test", "reason": "mid-plan stop"}).Code
	}()
	select {
	case code := <-flipped:
		t.Fatalf("master OFF did not wait for an in-flight decision: %d", code)
	case <-time.After(300 * time.Millisecond):
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if code := <-flipped; code != 200 {
		t.Fatalf("master OFF: %d", code)
	}

	if r := f.observe(second, nil); r[0].Delete {
		t.Fatalf("master OFF mid-plan issued a permit: %+v", r[0])
	}
	for _, task := range f.leaseV2(64, 64, 1<<40, "") {
		if task.ApprovedProof != nil {
			t.Fatalf("master OFF mid-plan leased execution: %+v", task)
		}
	}
	w := cleanupCall(t, f.s.handleAdminNASCleanupStatus, 47, 5, "id", strconv.FormatInt(p1, 10), nil)
	var status map[string]any
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || status["master_deletion_enabled"] != false {
		t.Fatalf("plan status master: %d %s", w.Code, w.Body.String())
	}
}

// The Render env var is inert: no code path may read it again.
func TestNASCleanupMasterEnvVarIsGone(t *testing.T) {
	name := "NAS_CLEANUP_" + "DELETION_ENABLED"
	err := filepath.WalkDir("../../..", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "node_modules") {
			return filepath.SkipDir
		}
		switch filepath.Ext(path) {
		case ".go", ".py", ".sh", ".yaml", ".yml", ".js", ".ts", ".swift", ".toml":
		default:
			return nil
		}
		b, err := os.ReadFile(path)
		if err == nil && bytes.Contains(b, []byte(name)) {
			t.Errorf("%s still references %s", path, name)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}
