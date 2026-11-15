package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/google/uuid"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestR2JoinedWriterCancelRequiresBoundApplyAndDefaultsDryRun(t *testing.T) {
	o, err := parseR2JoinedCancelArgs([]string{"--batch-record-id", "5", "--out", "new.jsonl"})
	if err != nil || o.apply {
		t.Fatalf("defaults %+v %v", o, err)
	}
	for _, args := range [][]string{{"--batch-record-id", "5", "--out", "new", "--apply"}, {"--batch-record-id", "5", "--out", "new", "--owner-map", "x"}, {"--batch-record-id", "0", "--out", "new"}, {"--batch-record-id", "5", "--out", "new", "extra"}} {
		if _, err := parseR2JoinedCancelArgs(args); err == nil {
			t.Fatalf("incomplete authority accepted %v", args)
		}
	}
}

func TestR2JoinedOwnerStopNeedsExactHistoricalIdentityAndFreshPrimaryProof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "historical-primary-resource.json")
	body := []byte(`{"account_uuid":"original-primary-account","droplet":{"id":123,"name":"stoarama-joined-test"}}`)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	sha := sha256.Sum256(body)
	mapping := r2JoinedOwnerMapping{Owner: "stoarama-joined-test", Kind: r2JoinedDODestroyed, ResourceID: "123", HistoricalResourceFile: path, HistoricalResourceSHA256: hex.EncodeToString(sha[:])}
	owners := []string{mapping.Owner}
	status := http.StatusNotFound
	calls := 0
	read := func(_ context.Context, url string, _ string) (int, []byte, error) {
		calls++
		if strings.HasSuffix(url, "/account") {
			return http.StatusOK, []byte(`{"account":{"uuid":"original-primary-account"}}`), nil
		}
		return status, nil, nil
	}
	evidence, err := r2JoinedVerifyOwners(context.Background(), []r2JoinedOwnerMapping{mapping}, owners, read, "", "local-token-never-recorded")
	if err != nil || len(evidence) != 1 || evidence[0].ObservedAt.IsZero() || len(evidence[0].EvidenceSHA256) != 64 {
		t.Fatalf("proof %v %v", evidence, err)
	}
	b, _ := json.Marshal(evidence)
	if strings.Contains(string(b), "local-token") {
		t.Fatal("credential entered audit")
	}
	for _, code := range []int{200, 401, 403, 429, 500} {
		status = code
		if _, err := r2JoinedVerifyOwners(context.Background(), []r2JoinedOwnerMapping{mapping}, owners, read, "", "key"); err == nil {
			t.Fatalf("provider status%d became absence", code)
		}
	}
	status = 404
	wrong := mapping
	wrong.ResourceID = "124"
	prior := calls
	if _, err := r2JoinedVerifyOwners(context.Background(), []r2JoinedOwnerMapping{wrong}, owners, read, "", "key"); err == nil || calls != prior {
		t.Fatal("unbound resource reached provider")
	}
	wrong = mapping
	wrong.Owner = "another-owner"
	if _, err := r2JoinedVerifyOwners(context.Background(), []r2JoinedOwnerMapping{wrong}, owners, read, "", "key"); err == nil {
		t.Fatal("owner roster drift accepted")
	}
	wrong = mapping
	wrong.HistoricalResourceSHA256 = strings.Repeat("0", 64)
	if _, err := r2JoinedVerifyOwners(context.Background(), []r2JoinedOwnerMapping{wrong}, owners, read, "", "key"); err == nil {
		t.Fatal("changed primary association accepted")
	}
	if _, err := r2JoinedVerifyOwners(context.Background(), []r2JoinedOwnerMapping{mapping, mapping}, owners, read, "", "key"); err == nil {
		t.Fatal("duplicate mapping accepted")
	}
}

// Use complete deployed row shapes and the real ordinary lifecycle guards in
// an isolated local schema. No replication-role/trigger bypass seeds these
// observations. The production migration is then applied unchanged.
func TestR2JoinedWriterRetirementIsAtomicAndAudited(t *testing.T) {
	pool := joinedPurgeTestPool(t, "joined_writer_cancel")
	ctx := context.Background()
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	var base string
	if err = conn.QueryRow(ctx, `SELECT current_schema()`).Scan(&base); err != nil {
		t.Fatal(err)
	}
	schema := "cancel_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE SCHEMA " + schema)
	defer conn.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	for _, name := range []string{"recording_joined_batches", "recording_joined_hours", "recording_joined_artifacts", "recording_joined_stream_days", "recording_joined_admission_controls", "r2_retirement_writer_fences"} {
		exec("CREATE TABLE " + schema + "." + name + " AS TABLE " + base + "." + name + " WITH NO DATA")
	}
	exec("ALTER TABLE " + schema + ".recording_joined_batches ADD PRIMARY KEY(id)")
	exec("SET search_path=" + schema + "," + base)
	for _, name := range []string{"guard_recording_joined_hour_update", "guard_recording_joined_artifact_update", "guard_recording_joined_batch_update"} {
		var definition string
		if err := conn.QueryRow(ctx, "SELECT pg_get_functiondef($1::regproc)", base+"."+name).Scan(&definition); err != nil {
			t.Fatal(err)
		}
		exec(strings.Replace(definition, base+"."+name, schema+"."+name, 1))
	}
	exec(`CREATE TRIGGER recording_joined_hour_update_guard BEFORE UPDATE ON recording_joined_hours FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_hour_update()`)
	exec(`CREATE TRIGGER recording_joined_artifact_update_guard BEFORE UPDATE ON recording_joined_artifacts FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_artifact_update()`)
	exec(`CREATE TRIGGER recording_joined_batch_update_guard BEFORE UPDATE ON recording_joined_batches FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_batch_update()`)
	raw, err := os.ReadFile("../../../infra/sql/migrations/0195_r2_joined_writer_retirement.sql")
	if err != nil {
		t.Fatal(err)
	}
	exec(string(raw))
	exec(`INSERT INTO r2_retirement_writer_fences(bucket,activated_at,drain_not_before) VALUES('stoarama',now()-interval '2 days',now()+interval '5 days');
 INSERT INTO recording_joined_batches(id,account_id,connection_id,state,failure_reason_code) VALUES(5,47,13,'frozen','');
 INSERT INTO recording_joined_admission_controls(batch_record_id,claims_paused,updated_at) VALUES(5,false,now()-interval '1 day');
 INSERT INTO recording_joined_hours(id,batch_record_id,state,claimed_by,claim_token,lease_expires_at,heartbeat_at,attempt_count,failure_reason_code,updated_at) VALUES
 (1,5,'pending',NULL,NULL,NULL,NULL,0,'',now()-interval '1 day'),
 (2,5,'leased','stoarama-joined-test','00000000-0000-0000-0000-000000000001',now()-interval '1 day',now()-interval '1 day',1,'',now()-interval '1 day');
 INSERT INTO recording_joined_artifacts(id,batch_record_id,artifact_kind,publication_state,publication_attempt_count,failure_reason_code,updated_at) VALUES(3,5,'hour_manifest','sealed',0,'',now()-interval '1 day')`)
	if _, err := conn.Exec(ctx, `UPDATE recording_joined_hours SET state='terminal_failed',failure_reason_code='owner_retired' WHERE id=1`); err == nil {
		t.Fatal("unaudited pending transition accepted")
	}
	if _, err := conn.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='terminal_failed',failure_reason_code='owner_retired' WHERE id=3`); err == nil {
		t.Fatal("unaudited sealed transition accepted")
	}
	var snapshot string
	if err := conn.QueryRow(ctx, `SELECT r2_joined_writer_snapshot(5)`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	proof := func(owner string) string {
		return fmt.Sprintf(`[{"owner":%q,"kind":"do_destroyed","resource_id":"123","evidence_sha256":%q,"observed_at":%q}]`, owner, strings.Repeat("a", 64), time.Now().UTC().Format(time.RFC3339Nano))
	}
	attempt := func(snapshot, proof string, rollback bool) (int, error) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return 0, err
		}
		defer tx.Rollback(ctx)
		var count int
		err = tx.QueryRow(ctx, `SELECT r2_retire_dead_joined_batch($1,5,$2,$3,'independent-test-review',$4::jsonb)`, uuid.New(), snapshot, strings.Repeat("b", 64), proof).Scan(&count)
		if err != nil {
			return 0, err
		}
		if rollback {
			return count, nil
		}
		return count, tx.Commit(ctx)
	}
	for _, tc := range []struct{ snapshot, proof string }{{strings.Repeat("0", 64), proof("stoarama-joined-test")}, {snapshot, proof("other-owner")}, {snapshot, `[{"owner":"stoarama-joined-test","kind":"do_destroyed","resource_id":"123","evidence_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]`}} {
		if _, err := attempt(tc.snapshot, tc.proof, false); err == nil {
			t.Fatal("drift/missing evidence accepted")
		}
	}

	for _, live := range []bool{true, false} {
		lease := "now()-interval '1 day'"
		updated := "now()"
		if live {
			lease = "now()+interval '1 day'"
			updated = "now()-interval '1 day'"
		}
		exec("INSERT INTO recording_joined_hours(id,batch_record_id,state,claimed_by,claim_token,lease_expires_at,heartbeat_at,attempt_count,failure_reason_code,updated_at) VALUES(4,5,'leased','stoarama-joined-test','00000000-0000-0000-0000-000000000002'," + lease + ",now()-interval '1 day',1,''," + updated + ")")
		var changed string
		if err := conn.QueryRow(ctx, `SELECT r2_joined_writer_snapshot(5)`).Scan(&changed); err != nil {
			t.Fatal(err)
		}
		if _, err := attempt(changed, proof("stoarama-joined-test"), false); err == nil || !strings.Contains(err.Error(), "live/recent") {
			t.Fatalf("live lease/recent write was not vetoed: %v", err)
		}
		exec(`DELETE FROM recording_joined_hours WHERE id=4`)
	}
	if count, err := attempt(snapshot, proof("stoarama-joined-test"), true); err != nil || count != 5 {
		t.Fatalf("transactional trial count=%d err=%v", count, err)
	}
	var rows int
	var unchanged string
	if err := conn.QueryRow(ctx, `SELECT count(*),(SELECT r2_joined_writer_snapshot(5)) FROM r2_joined_writer_retirement_runs`).Scan(&rows, &unchanged); err != nil || rows != 0 || unchanged != snapshot {
		t.Fatalf("rollback left changes rows=%d digest=%s err=%v", rows, unchanged, err)
	}
	if count, err := attempt(snapshot, proof("stoarama-joined-test"), false); err != nil || count != 5 {
		t.Fatalf("atomic apply count=%d err=%v", count, err)
	}
	var pending int
	var paused bool
	if err := conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM recording_joined_hours WHERE state IN ('pending','leased'))+(SELECT count(*) FROM recording_joined_artifacts WHERE publication_state IN ('sealed','publishing'))+(SELECT count(*) FROM recording_joined_batches WHERE state='frozen'),(SELECT claims_paused FROM recording_joined_admission_controls WHERE batch_record_id=5),(SELECT count(*) FROM r2_joined_writer_retirement_items)`).Scan(&pending, &paused, &rows); err != nil || pending != 0 || !paused || rows != 5 {
		t.Fatalf("incomplete atomic drain pending=%d paused=%v audit=%d err=%v", pending, paused, rows, err)
	}
	if _, err := conn.Exec(ctx, `DELETE FROM r2_joined_writer_retirement_items`); err == nil {
		t.Fatal("audit mutable")
	}
	if _, err := attempt(snapshot, proof("stoarama-joined-test"), false); err == nil {
		t.Fatal("stale cancellation replay accepted")
	}
}
