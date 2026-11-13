package api

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestNASCleanupPreparerExecutesScopedQuery(t *testing.T) {
	f := newCleanupFixture(t, 24)
	f.admit("reserved", syntheticFiles(901, 3, 10))
	retired, _, _ := f.admit("retired", syntheticFiles(904, 4, 10))
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='retired' WHERE id=$1`, retired); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `
 CREATE TABLE nas_inventory_files(connection_id bigint,clip_id bigint,state text,relative_path text,sha256 text,size_bytes bigint);
 CREATE TABLE recording_collation_output_acks(connection_id bigint,output_id bigint,sha256 text,size_bytes bigint);
 INSERT INTO nas_inventory_files SELECT 13,id,'present',display_path,sha256,size_bytes FROM recording_clips WHERE id BETWEEN 901 AND 907;
 INSERT INTO delivery_nas_source_holds(clip_id) VALUES(905);
 UPDATE nas_inventory_files SET size_bytes=1 WHERE clip_id=906;
 UPDATE recording_clips SET clip_start_at='2026-09-20',clip_end_at='2026-09-20 00:01' WHERE id=907`); err != nil {
		t.Fatal(err)
	}
	python := os.Getenv("STOARAMA_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	dir, err := filepath.Abs("../../scripts/nas-cleanup")
	if err != nil {
		t.Fatal(err)
	}
	// Generate the exact shipping Python query, then execute it against real
	// PostgreSQL with reserved, held, identity-mismatched and kept controls.
	query, err := exec.Command(python, "-c", `import sys
sys.path.insert(0,sys.argv[1])
import prepare_keep28_plans as k
k.query=lambda sql,psql: print(sql)
k.stream_candidates(k.load_policy(k.POLICY_PATH)[3], 'bucket', 'psql')`, dir).Output()
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := f.pool.QueryRow(f.ctx, string(query)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		ID int64 `json:"source_id"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 1 || rows[0].ID != 904 {
		t.Fatalf("unsafe/incomplete candidates: %s, %v", raw, err)
	}
}
