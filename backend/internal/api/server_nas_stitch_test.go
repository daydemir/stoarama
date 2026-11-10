package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func stitchUnitFixture() (nasStitchPlan, nasStitchResult) {
	a := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	p := nasStitchPlan{Policy: nasStitchPolicy, Revision: nasStitchRevision, CorpusSHA: nasStitchCorpusSHA, KeepSHA: cleanupPolicySHA, StreamID: 3, RecordingID: 2, OutputFolder: "stream_V85_keep28/2/2026-09-20T00-00-00Z", Joins: []bool{true, false}, Eligible: []bool{true, true, false}}
	for n := int64(1); n <= 3; n++ {
		p.Clips = append(p.Clips, nasCleanupItem{Kind: "raw", SourceID: n, SourceIDs: []int64{n}, RecordingID: 2, StreamID: 3, RelativePath: fmt.Sprintf("stream/day/%d.mp4", n), SizeBytes: 3, SHA256: strings.Repeat("a", 64), SidecarRequired: true, Frozen: [5]int64{3, 1, 2, n, 77}, FrozenSidecar: [5]int64{300, 1, 2, n + 100, 77}, StartAt: a.Add(time.Duration(n-1) * time.Minute), EndAt: a.Add(time.Duration(n) * time.Minute)})
	}
	o := nasStitchOutput{Path: p.OutputFolder + "/run-000.mp4", SHA: strings.Repeat("b", 64), ReadbackSHA: strings.Repeat("b", 64), Size: 5, FullDecode: true, SeamDecode: true, PacketChain: true, NativeAudit: true, NoAudio: true, Fsynced: true}
	for _, c := range p.Clips[:2] {
		o.Inputs = append(o.Inputs, nasStitchInputProof{ID: c.SourceID, SHA: c.SHA256, Size: c.SizeBytes, PacketSHA: strings.Repeat("c", 64), Packets: 10, Frames: 10, RawProof: fixtureProof(c)})
	}
	return p, nasStitchResult{Token: strings.Repeat("f", 64), PlanSHA: strings.Repeat("e", 64), Outputs: []nasStitchOutput{o}, LeftOut: []int64{3}}
}
func TestNASStitchResultRequiresCompleteExactInclusion(t *testing.T) {
	p, q := stitchUnitFixture()
	items, err := validateNASStitchResult(p, q)
	if err != nil || len(items) != 2 {
		t.Fatalf("valid result: %v", err)
	}
	tests := map[string]func(*nasStitchPlan, *nasStitchResult){
		"full decode":     func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].FullDecode = false },
		"seam decode":     func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].SeamDecode = false },
		"packet proof":    func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].Inputs[0].PacketSHA = "" },
		"native proof":    func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].NativeAudit = false },
		"partial repair":  func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].Inputs[0].Repaired = true },
		"frame loss":      func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].Inputs[0].Frames-- },
		"fsync":           func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].Fsynced = false },
		"readback":        func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].ReadbackSHA = strings.Repeat("d", 64) },
		"audio missing":   func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].NoAudio = false },
		"source mismatch": func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].Inputs[0].SHA = strings.Repeat("d", 64) },
		"no frozen seam":  func(p *nasStitchPlan, q *nasStitchResult) { p.Joins[0] = false },
		"outside corpus":  func(p *nasStitchPlan, q *nasStitchResult) { p.Eligible[0] = false },
		"wrong order": func(p *nasStitchPlan, q *nasStitchResult) {
			q.Outputs[0].Inputs[0], q.Outputs[0].Inputs[1] = q.Outputs[0].Inputs[1], q.Outputs[0].Inputs[0]
		},
		"left out included":   func(p *nasStitchPlan, q *nasStitchResult) { q.LeftOut = append(q.LeftOut, 1) },
		"missing disposition": func(p *nasStitchPlan, q *nasStitchResult) { q.LeftOut = nil },
		"path escape":         func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].Path = "../raw.mp4" },
		"physical proof":      func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].Inputs[0].RawProof = []byte(`{}`) },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			p, q := stitchUnitFixture()
			change(&p, &q)
			if _, err := validateNASStitchResult(p, q); err == nil {
				t.Fatal("incomplete inclusion admitted")
			}
		})
	}
	s := &Server{}
	if s.stitchDeletionMaster(items[0]) {
		t.Fatal("default deletion enabled")
	}
	s.cfg.NASCleanupDeletionEnabled = true
	if !s.stitchDeletionMaster(nasCleanupItem{}) || s.stitchDeletionMaster(items[0]) {
		t.Fatal("independent switches not enforced")
	}
	s.cfg.NASStitchEnabled = true
	s.cfg.NASStitchDeletionEnabled = true
	if !s.stitchDeletionMaster(items[0]) {
		t.Fatal("explicit switches refused")
	}
}
func stitchMigration(t *testing.T, f *cleanupFixture) {
	t.Helper()
	b, err := os.ReadFile("../../../infra/sql/migrations/0178_nas_v85_stitch.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(f.ctx, string(b)); err != nil {
		t.Fatal(err)
	}
}
func TestNASStitchRegisteredOutputRetiresOnlyIncludedRawWithSeparateGates(t *testing.T) {
	f := newCleanupFixture(t, 1)
	stitchMigration(t, f)
	p, q := stitchUnitFixture()
	for _, c := range p.Clips {
		f.bucket.put(fmt.Sprintf("stitch/%d", c.SourceID), []byte("abc"))
		c.SHA256 = cleanupSHA([]byte("abc"))
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket) VALUES($1,2,$2,$3,3,$4,$5,$6,'bucket')`, c.SourceID, fmt.Sprintf("stitch/%d", c.SourceID), c.SHA256, c.RelativePath, c.StartAt, c.EndAt); err != nil {
			t.Fatal(err)
		}
		item, err := cleanupCatalogItem(f.ctx, f.pool, "raw", c.SourceID, "bucket")
		if err != nil {
			t.Fatal(err)
		}
		item.Frozen = c.Frozen
		item.FrozenSidecar = c.FrozenSidecar
		p.Clips[c.SourceID-1] = item
	}
	for n, c := range p.Clips[:2] {
		q.Outputs[0].Inputs[n].SHA = c.SHA256
		q.Outputs[0].Inputs[n].RawProof = fixtureProof(c)
	}
	plan, _ := json.Marshal(p)
	q.PlanSHA = cleanupSHA(plan)
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until,deletion_approved) VALUES(1,3,'2026-09-20','2026-09-21','test owner',now()+interval '1 hour',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_tasks(id,scope_id,recording_id,hour_at,plan,plan_sha256,state,lease_token,lease_expires_at) VALUES(7,1,2,'2026-09-20',$1,$2,'leased',$3,now()+interval '2 hours')`, string(plan), q.PlanSHA, q.Token); err != nil {
		t.Fatal(err)
	}
	result := func() int {
		w := cleanupCall(t, f.s.handleAccountNASStitchResult, 47, 5, "id", "7", q)
		if w.Code != 200 {
			t.Log(w.Body.String())
		}
		return w.Code
	}
	if result() != 409 {
		t.Fatal("master-off result accepted")
	}
	f.s.cfg.NASStitchEnabled = true
	if result() != 200 || result() != 200 {
		t.Fatal("registration or lost ACK retry failed")
	}
	var premature int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items`).Scan(&premature); err != nil || premature != 0 {
		t.Fatal("cleanup work visible before durable local receipts")
	}
	for n := 0; n < 2; n++ {
		w := cleanupCall(t, f.s.handleAccountNASStitchReady, 47, 5, "id", "7", map[string]any{"lease_token": q.Token, "plan_sha256": q.PlanSHA, "outputs_ready": true})
		if w.Code != 200 {
			t.Fatal(w.Body.String())
		}
	}
	var leaves int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items i JOIN nas_stitch_tasks t ON t.cleanup_plan_id=i.plan_id WHERE t.id=7`).Scan(&leaves); err != nil || leaves != 2 {
		t.Fatalf("excluded leaf retired: %d %v", leaves, err)
	}
	for _, sql := range []string{`UPDATE nas_stitch_outputs SET sha256=repeat('a',64)`, `DELETE FROM nas_stitch_outputs`, `UPDATE nas_stitch_tasks SET plan='{}'`, `UPDATE nas_stitch_scopes SET deletion_approved=false`} {
		if _, err := f.pool.Exec(f.ctx, sql); err == nil {
			t.Fatal("mutable stitch evidence", sql)
		}
	}
	f.dryRun()
	var pid int64
	if err := f.pool.QueryRow(f.ctx, `SELECT cleanup_plan_id FROM nas_stitch_tasks WHERE id=7`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	advance := func() {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		if err = f.s.stitchAdvanceRetirement(f.ctx, tx); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	f.s.cfg.NASCleanupDeletionEnabled = true
	advance()
	if mode, _ := f.planMode(pid); mode != "dry_run" {
		t.Fatal("stitch deletion gate bypassed")
	}
	f.s.cfg.NASStitchDeletionEnabled = true
	advance()
	if mode, _ := f.planMode(pid); mode != "enabled" {
		t.Fatal("scoped plan not enabled")
	}
	tasks := f.leaseV2(64, 64, 10000, "")
	if len(tasks) != 2 {
		t.Fatalf("execution leaves %d", len(tasks))
	}
	issued := f.observe(tasks[:1], nil)
	if len(issued) != 1 || !issued[0].Delete {
		t.Fatalf("verified kept clip not permitted: %+v", issued)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_stitch_scopes SET paused=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	for _, r := range f.observe(tasks[1:], nil) {
		if r.Delete {
			t.Fatal("paused scope issued deletion")
		}
	}
	if mode, _ := f.planMode(pid); mode != "paused" {
		t.Fatal("anomaly did not auto-pause")
	}
}
func TestNASStitchRejectsInventedCorpusAndOffLeavesQueueUntouched(t *testing.T) {
	f := newCleanupFixture(t, 1)
	stitchMigration(t, f)
	w := httptest.NewRecorder()
	f.s.handleAdminNASStitchCorpus(w, httptest.NewRequest("POST", "/", bytes.NewReader([]byte(`{"nomination":true}`))))
	if w.Code != 400 {
		t.Fatal("invented corpus admitted")
	}
	w = cleanupCall(t, f.s.handleAccountNASStitchLease, 47, 5, "", "", map[string]any{"client_version": "test"})
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"task":null`) {
		t.Fatal(w.Body.String())
	}
	var count int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_stitch_tasks`).Scan(&count); err != nil || count != 0 {
		t.Fatal("master off changed queue")
	}
}
