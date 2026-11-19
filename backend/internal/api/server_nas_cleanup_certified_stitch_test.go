package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"
	"testing"
)

func testCertifiedStitchNASCleanup(t *testing.T, s *Server, hourID int64, key string) {
	t.Helper()
	ctx, pool := context.Background(), s.pool
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	count := func(query string, want int) {
		t.Helper()
		var got int
		if err := pool.QueryRow(ctx, query, hourID).Scan(&got); err != nil || got != want {
			t.Fatalf("custody rows: got %d want %d, %v", got, want, err)
		}
	}
	count(`SELECT count(*) FROM nas_stitch_r2_certified_raw WHERE hour_id=$1`, 2)
	const ready = `SELECT count(*) FROM nas_cleanup_certified_stitch_raw WHERE hour_id=$1`
	count(ready, 0)
	exec(`INSERT INTO r2_keep28_intents(bucket,object_key,endpoint_sha256,stream_id,source_id,size_bytes,etag,last_modified,
 manifest_sha256,policy_sha256,picks_sha256,drain_evidence_sha256,choice,custody)
 SELECT c.bucket,c.object_key,c.endpoint_sha256,c.stream_id,c.clip_id,c.size_bytes,c.etag,now(),
  repeat('a',64),$2,repeat('b',64),repeat('b',64),'st',jsonb_build_object(
   'ConnectionID',13,'SourceID',c.clip_id,'Path',c.relative_path,'SizeBytes',c.size_bytes,'SHA',c.sha256)
 FROM nas_stitch_r2_certified_raw c WHERE c.hour_id=$1 AND c.clip_id=901`, hourID, cleanupPolicySHA)
	exec(`INSERT INTO r2_keep28_attempts(bucket,object_key) VALUES('stoarama',$1)`, key)
	count(ready, 0)
	exec(`INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256)
 SELECT 'raw',c.clip_id,c.stream_id,c.bucket,c.object_key,c.size_bytes,c.sha256,'keep28_exact_nas','st',repeat('a',64),$2
 FROM nas_stitch_r2_certified_raw c WHERE c.hour_id=$1 AND c.clip_id=901`, hourID, cleanupPolicySHA)
	exec(`UPDATE recording_clips SET purged_at=clock_timestamp() WHERE id=901`)
	count(ready, 0)
	exec(`INSERT INTO r2_keep28_events(bucket,object_key,event) VALUES('stoarama',$1,'absent')`, key)
	count(ready, 1)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	item, err := cleanupCatalogItem(ctx, tx, "raw", 901, "stoarama")
	if err != nil {
		t.Fatal(err)
	}
	if err := loadNASCleanupStitchCertificate(ctx, tx, &item); err != nil || item.StitchCertificate.HourID != hourID {
		t.Fatalf("load completed replacement: %v", err)
	}
	item.Frozen = [5]int64{100, 1, 2, 90, item.StitchCertificate.Output.Identity[4]}
	item.FrozenSidecar = [5]int64{50, 1, 2, 91, item.Frozen[4]}
	if err := cleanupCheckCertifiedStitch(ctx, tx, item, true); err != nil {
		t.Fatal(err)
	}
	forged := item
	changed := *item.StitchCertificate
	changed.Output.Path += ".wrong"
	forged.StitchCertificate = &changed
	save, err := tx.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := persistNASCleanupPlan(ctx, save, "forged-certified-raw", []nasCleanupItem{forged}); err == nil ||
		!strings.Contains(err.Error(), "without exact stitch custody") {
		t.Fatalf("database accepted a different replacement: %v", err)
	}
	if err := save.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	id, digest, err := persistNASCleanupPlan(ctx, tx, "certified-raw", []nasCleanupItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	proof := map[string]any{}
	if err := json.Unmarshal(fixtureProof(item), &proof); err != nil {
		t.Fatal(err)
	}
	proof["root"] = [2]int64{item.Frozen[4], 1}
	proof["stitch_replacement"] = item.StitchCertificate.Output
	proofBytes, _ := json.Marshal(proof)
	if !validCleanupProof(proofBytes, item) {
		t.Fatal("exact NAS replacement and sidecar proof rejected")
	}
	delete(proof, "stitch_replacement")
	unproven, _ := json.Marshal(proof)
	if validCleanupProof(unproven, item) {
		t.Fatal("no physical replacement proof accepted")
	}
	lease := func(version string) []cleanupTask {
		t.Helper()
		exec(`UPDATE connections SET client_version=$1,client_boot_id='fixture-boot',last_seen_at=now() WHERE id=13`, version)
		w := cleanupCall(t, s.handleAccountNASCleanupLease, 47, 47, "", "",
			map[string]any{"client_version": version, "protocol": 2, "limit": 1, "execution_files": 1, "execution_bytes": 1000},
			"fixture-boot "+version)
		var response struct{ Tasks []cleanupTask }
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &response) != nil {
			t.Fatalf("cleanup lease: %d %s", w.Code, w.Body.String())
		}
		return response.Tasks
	}
	if tasks := lease(NASR2CopyPagedClientVersion); len(tasks) != 0 {
		t.Fatal("old client leased a certified stitch cleanup task")
	}
	tasks := lease(nasCleanupCertifiedStitchVersion)
	if len(tasks) != 1 {
		t.Fatalf("certified cleanup task not leased: %v", tasks)
	}
	task := tasks[0]
	s.nasCleanupReportCache = &nasCleanupReport{}
	report := fmt.Sprintf(`{"complete_node_actions":[{"path":%q,"owner_stream_ids":[3]}]}`, path.Dir(item.RelativePath))
	if err := json.Unmarshal([]byte(report), s.nasCleanupReportCache); err != nil {
		t.Fatal(err)
	}
	session := "fixture-boot " + nasCleanupCertifiedStitchVersion
	observe := func(req cleanupObserve, action string, deleting bool) {
		t.Helper()
		w := cleanupCall(t, s.handleAccountNASCleanupObserve, 47, 47, "itemId", fmt.Sprint(task.ItemID), req, session)
		var decision struct {
			Action   string
			Recovery string
			Delete   bool
		}
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &decision) != nil || decision.Action != action ||
			decision.Delete != deleting || (action != "reconcile" && decision.Recovery != nasCleanupCertifiedStitchRecovery) {
			t.Fatalf("observe wanted %s: %d %s", action, w.Code, w.Body.String())
		}
	}
	request := cleanupObserve{
		Attempt: task.Attempt, PlanSHA: digest, Outcome: "match", Proof: proofBytes,
		RequestID: strings.Repeat("1", 32), ClientVersion: nasCleanupCertifiedStitchVersion,
	}
	observe(request, "would_delete", false)
	control := func(mode string) int {
		t.Helper()
		w := cleanupCall(t, s.handleAdminNASCleanupControl, 47, 47, "id", fmt.Sprint(id),
			map[string]any{"plan_sha256": digest, "mode": mode, "reason": "certified fixture"})
		if w.Code != http.StatusOK {
			t.Log(w.Body.String())
		}
		return w.Code
	}
	if code := control("enabled"); code != http.StatusConflict {
		t.Fatalf("master OFF did not block stage cleanup: %d", code)
	}
	s.cfg.NASCleanupDeletionEnabled = true
	defer func() { s.cfg.NASCleanupDeletionEnabled = false }()
	if code := control("enabled"); code != http.StatusOK {
		t.Fatalf("exact dry run could not be enabled: %d", code)
	}
	tasks = lease(nasCleanupCertifiedStitchVersion)
	if len(tasks) != 1 || tasks[0].ApprovedProof == nil {
		t.Fatal("enabled cleanup task did not carry its approved proof")
	}
	task = tasks[0]
	issue := `UPDATE nas_cleanup_items SET state='delete_issued',delete_ever_issued=true,proof=$2,recovery='nas_stitch_certified',
 lease_client_version=$3 WHERE id=$1`
	for _, rows := range []struct{ proof, version string }{
		{string(unproven), nasCleanupCertifiedStitchVersion},
		{string(proofBytes), NASR2CopyPagedClientVersion},
	} {
		if _, err := pool.Exec(ctx, issue, task.ItemID, rows.proof, rows.version); err == nil {
			t.Fatal("DB issued permit without the exact replacement proof or client version")
		}
	}
	request.Attempt = task.Attempt
	request.RequestID = strings.Repeat("2", 32)
	observe(request, "delete_issued", true)
	observe(request, "delete_issued", true)
	s.cfg.NASCleanupDeletionEnabled = false
	observe(request, "reconcile", false)
	receipt := cleanupReceipt{Attempt: task.Attempt, PlanSHA: digest, ProofSHA: cleanupSHA(proofBytes), Outcome: "deleted"}
	for range 2 {
		w := cleanupCall(t, s.handleAccountNASCleanupResult, 47, 47, "itemId", fmt.Sprint(task.ItemID), receipt, session)
		if w.Code != http.StatusOK {
			t.Fatalf("durable cleanup receipt or exact replay: %d %s", w.Code, w.Body.String())
		}
	}
}
