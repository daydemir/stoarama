package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func stitchUnitFixture() (nasStitchPlan, nasStitchResult) {
	a := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	p := nasStitchPlan{Policy: nasStitchPolicy, Revision: nasStitchRevision, CorpusSHA: nasStitchCorpusSHA, KeepSHA: cleanupPolicySHA, StreamID: 3, RecordingID: 2, OutputFolder: "stream_V85r_keep28/2/2026-09-20T00-00-00Z", Frozen: []json.RawMessage{nil, nil}, Joins: []bool{true, false}, Eligible: []bool{true, true, false}}
	for n := int64(1); n <= 3; n++ {
		p.Clips = append(p.Clips, nasCleanupItem{Kind: "raw", SourceID: n, SourceIDs: []int64{n}, RecordingID: 2, StreamID: 3, RelativePath: fmt.Sprintf("stream/day/%d.mp4", n), SizeBytes: 3, SHA256: strings.Repeat("a", 64), SidecarRequired: true, Frozen: [5]int64{3, 1, 2, n, 77}, FrozenSidecar: [5]int64{300, 1, 2, n + 100, 77}, StartAt: a.Add(time.Duration(n-1) * time.Minute), EndAt: a.Add(time.Duration(n) * time.Minute)})
	}
	provenance := nasStitchProvenance{OctoberRecovered: true, Policy: nasStitchPolicy, ManifestSHA: nasStitchManifestSHA, FFmpegVersion: "N-126826-gc0e8b139fd-20260924", FFprobeVersion: "N-126826-gc0e8b139fd-20260924", FFmpegSHA: "2388de8e05ad71c913ee1a9c4504a1aa5de09ac100a1de92858dd9ec08e9f98b", FFprobeSHA: "9b2bac862606a48f5bc36091d38e5334cf241538a6f4cc47c60f68e7973a365c"}
	o := nasStitchOutput{Policy: nasStitchPolicy, Provenance: provenance, Path: p.OutputFolder + "/run-000.mp4", SHA: strings.Repeat("b", 64), ReadbackSHA: strings.Repeat("b", 64), Size: 5, FullDecode: true, SeamDecode: true, PacketChain: true, NativeAudit: true, NoAudio: true, Fsynced: true}
	for _, c := range p.Clips[:2] {
		o.Inputs = append(o.Inputs, nasStitchInputProof{ID: c.SourceID, SHA: c.SHA256, Size: c.SizeBytes, PacketSHA: strings.Repeat("c", 64), Packets: 10, Frames: 10, RawProof: fixtureProof(c)})
	}
	return p, nasStitchResult{Policy: nasStitchPolicy, Seams: []nasStitchDecision{
		{Policy: nasStitchPolicy, LeftID: 1, RightID: 2, Join: true, Nomination: true, Status: "APPROVED85_nomination_requires_runtime_native_join_checks", Decision: "join", DiagnosticSHA: strings.Repeat("d", 64)},
		{Policy: nasStitchPolicy, LeftID: 2, RightID: 3, Status: "SPLIT_frozen_score_reject", Decision: "split"}}, Token: strings.Repeat("f", 64), PlanSHA: strings.Repeat("e", 64), Outputs: []nasStitchOutput{o}, LeftOut: []int64{3}}
}
func TestNASStitchResultRequiresCompleteExactInclusion(t *testing.T) {
	p, q := stitchUnitFixture()
	items, err := validateNASStitchResult(p, q)
	if err != nil || len(items) != 2 {
		t.Fatalf("valid result: %v", err)
	}
	tests := map[string]func(*nasStitchPlan, *nasStitchResult){
		"policy label": func(p *nasStitchPlan, q *nasStitchResult) { q.Policy = "v85" },
		"untrusted package": func(p *nasStitchPlan, q *nasStitchResult) {
			q.Outputs[0].Provenance.ManifestSHA = strings.Repeat("0", 64)
		},
		"unnominated join": func(p *nasStitchPlan, q *nasStitchResult) { q.Seams[0].Nomination = false },
		"ambiguous join":   func(p *nasStitchPlan, q *nasStitchResult) { q.Seams[0].Status = "UNKNOWN_status" },
		"held raw":         func(p *nasStitchPlan, q *nasStitchResult) { q.Seams[1].Decision = "hold" },
		"full decode":      func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].FullDecode = false },
		"seam decode":      func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].SeamDecode = false },
		"packet proof":     func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].Inputs[0].PacketSHA = "" },
		"native proof":     func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].NativeAudit = false },
		"partial repair":   func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].Inputs[0].Repaired = true },
		"frame loss":       func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].Inputs[0].Frames-- },
		"fsync":            func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].Fsynced = false },
		"readback":         func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].ReadbackSHA = strings.Repeat("d", 64) },
		"audio missing":    func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].NoAudio = false },
		"source mismatch":  func(p *nasStitchPlan, q *nasStitchResult) { q.Outputs[0].Inputs[0].SHA = strings.Repeat("d", 64) },
		"no frozen seam":   func(p *nasStitchPlan, q *nasStitchResult) { p.Joins[0] = false },
		"outside corpus":   func(p *nasStitchPlan, q *nasStitchResult) { p.Eligible[0] = false },
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
	b, err := os.ReadFile("../../../infra/sql/migrations/0179_nas_v85r_stitch.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(f.ctx, string(b)); err != nil {
		t.Fatal(err)
	}
}
func newNASStitchTaskFixture(t *testing.T) (*cleanupFixture, nasStitchPlan, nasStitchResult) {
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
	return f, p, q
}
func advanceNASStitchRetirement(t *testing.T, f *cleanupFixture) {
	t.Helper()
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var release func()
	defer func() {
		tx.Rollback(f.ctx)
		if release != nil {
			release()
		}
	}()
	if release, err = f.s.stitchAdvanceRetirement(f.ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func enabledNASStitchFixture(t *testing.T) (*cleanupFixture, int64) {
	t.Helper()
	f, _, q := newNASStitchTaskFixture(t)
	f.s.cfg.NASStitchEnabled = true
	f.s.cfg.NASStitchDeletionEnabled = true
	f.s.cfg.NASCleanupDeletionEnabled = true
	for _, call := range []struct {
		h    http.HandlerFunc
		body any
	}{
		{f.s.handleAccountNASStitchResult, q},
		{f.s.handleAccountNASStitchReady, map[string]any{"lease_token": q.Token, "plan_sha256": q.PlanSHA, "outputs_ready": true}},
	} {
		if w := cleanupCall(t, call.h, 47, 5, "id", "7", call.body); w.Code != 200 {
			t.Fatal(w.Body.String())
		}
	}
	f.dryRun()
	advanceNASStitchRetirement(t, f)
	var id int64
	if err := f.pool.QueryRow(f.ctx, `SELECT cleanup_plan_id FROM nas_stitch_tasks WHERE id=7`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return f, id
}

func TestNASStitchRetirementUsesExactCleanupFreezeGeneration(t *testing.T) {
	for _, change := range []string{"", `UPDATE nas_cleanup_frozen_streams SET generation=generation+1 WHERE plan_id=$1`, `DELETE FROM nas_cleanup_frozen_streams WHERE plan_id=$1`, `UPDATE nas_cleanup_rankings_cache SET freeze_generation=freeze_generation+1 WHERE plan_id=$1`} {
		t.Run(change, func(t *testing.T) {
			f, id := enabledNASStitchFixture(t)
			var bound bool
			if err := f.pool.QueryRow(f.ctx, `SELECT f.validated AND f.generation=c.freeze_generation AND f.generation=(p.execution_rankings->'3'->>'freeze_generation')::bigint FROM nas_cleanup_frozen_streams f JOIN nas_cleanup_rankings_cache c USING(plan_id,stream_id) JOIN nas_cleanup_plans p ON p.id=f.plan_id WHERE p.id=$1`, id).Scan(&bound); err != nil || !bound {
				t.Fatalf("missing shared activation: %t %v", bound, err)
			}
			_, err := f.pool.Exec(f.ctx, `UPDATE recording_clips SET size_bytes=size_bytes+1 WHERE id=1`)
			requireCleanupFrozen(t, err)
			tasks := f.leaseV2(64, 64, 10000, "")
			if len(tasks) != 1 {
				t.Fatalf("serial execution lease: %+v", tasks)
			}
			if change != "" {
				if _, err := f.pool.Exec(f.ctx, change, id); err != nil {
					t.Fatal(err)
				}
			}
			results := f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("d", 32)})
			if len(results) != 1 || results[0].Delete != (change == "") {
				t.Fatalf("freeze change=%q: %+v", change, results)
			}
		})
	}
}

func TestNASStitchCheckpointAndLeasePauseUnderCleanupFreeze(t *testing.T) {
	f, _, q := newNASStitchTaskFixture(t)
	f.s.cfg.NASStitchEnabled = true
	checkpoint := func(token string, want bool) {
		t.Helper()
		w := cleanupCall(t, f.s.handleAccountNASStitchCheckpoint, 47, 5, "id", "7", map[string]any{"lease_token": token, "plan_sha256": q.PlanSHA})
		var response struct {
			Continue bool `json:"continue"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Continue != want {
			t.Fatalf("checkpoint: %d %s", w.Code, w.Body.String())
		}
	}
	checkpoint(q.Token, true)
	if w := cleanupCall(t, f.s.handleAccountNASStitchCheckpoint, 47, 5, "id", "7", map[string]any{"lease_token": "wrong", "plan_sha256": q.PlanSHA}); w.Code != 409 {
		t.Fatal("foreign lease token accepted")
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_frozen_streams(plan_id,stream_id) VALUES(999,4)`); err != nil {
		t.Fatal(err)
	}
	checkpoint(q.Token, true) // running job's unrelated stream remains independent
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_frozen_streams SET stream_id=3; UPDATE nas_stitch_tasks SET lease_expires_at=now()-interval '1 second' WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	checkpoint(q.Token, false)
	if w := cleanupCall(t, f.s.handleAccountNASStitchLease, 47, 5, "", "", map[string]any{}); w.Code != 200 || !strings.Contains(w.Body.String(), `"task":null`) {
		t.Fatalf("frozen expired job re-leased: %d %s", w.Code, w.Body.String())
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_stitch_tasks SET lease_expires_at=now()+interval '1 hour' WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	checkpoint(q.Token, false)
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM nas_cleanup_frozen_streams`); err != nil {
		t.Fatal(err)
	}
	checkpoint(q.Token, true)
	f.s.cfg.NASStitchEnabled = false
	checkpoint(q.Token, false)
}

func TestNASStitchRegisteredOutputRetiresOnlyIncludedRawWithSeparateGates(t *testing.T) {
	f, _, q := newNASStitchTaskFixture(t)
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
	advance := func() { advanceNASStitchRetirement(t, f) }
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
	if len(tasks) != 1 {
		t.Fatalf("execution must serialize included leaves: %d", len(tasks))
	}
	issued := f.observe(tasks[:1], nil)
	if len(issued) != 1 || !issued[0].Delete {
		t.Fatalf("verified kept clip not permitted: %+v", issued)
	}
	f.receipts(tasks, issued, "deleted")
	tasks = f.leaseV2(64, 64, 10000, "")
	if len(tasks) != 1 {
		t.Fatalf("second included leaf not serialized: %d", len(tasks))
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_stitch_scopes SET paused=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	for _, r := range f.observe(tasks, nil) {
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

func TestNASStitchHeldEndpointsRemainRawDespiteVerifiedOutput(t *testing.T) {
	p, q := stitchUnitFixture()
	q.Seams[1].Decision = "hold"
	q.Outputs[0].Inputs[1].RetirementHeld = true
	items, err := validateNASStitchResult(p, q)
	if err != nil || len(items) != 1 || items[0].SourceID != 1 {
		t.Fatalf("held leaf retired: %v %v", items, err)
	}
}
func TestNASStitchFrozenDecisionDifferenceCannotJoin(t *testing.T) {
	p, q := stitchUnitFixture()
	f := map[string]any{"recording_id": 2, "source_key": [][3]any{{int64(1), strings.Repeat("a", 64), int64(3)}, {int64(2), strings.Repeat("a", 64), int64(3)}}, "nomination": true, "status": "UNKNOWN_october_status", "frozen_diagnostic_sha256": strings.Repeat("d", 64)}
	raw, _ := json.Marshal(f)
	p.Frozen[0] = raw
	q.Seams[0].Frozen = true
	if _, err := validateNASStitchResult(p, q); err == nil {
		t.Fatal("status difference admitted")
	}
}

func TestNASStitchToolCapabilityIsDefaultOffScopedAndExact(t *testing.T) {
	f := newCleanupFixture(t, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "HEAD" || !strings.HasSuffix(r.URL.Path, "/experimental/v85-retired-WORK-custody/20261013/2388de8e05ad71c913ee1a9c4504a1aa5de09ac100a1de92858dd9ec08e9f98b.bin") {
			t.Errorf("unexpected artifact request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Length", "174165384")
		w.Header().Set("ETag", "\"immutable-tool\"")
	}))
	defer server.Close()
	store, err := r2.New(context.Background(), r2.Config{AccessKey: "fake-key", SecretKey: "fake-secret", Region: "auto", Bucket: "bucket", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	f.s.r2 = store
	call := func(name string) *httptest.ResponseRecorder {
		return cleanupCall(t, f.s.handleAccountNASStitchTool, 47, 5, "name", name, nil)
	}
	if w := call("ffmpeg"); w.Code != 403 {
		t.Fatalf("off capability %d", w.Code)
	}
	f.s.cfg.NASStitchEnabled = true
	if w := call("unknown"); w.Code != 404 {
		t.Fatalf("arbitrary tool capability %d", w.Code)
	}
	w := call("ffmpeg")
	if w.Code != 200 {
		t.Fatalf("pinned tool capability %d", w.Code)
	}
	var cap struct {
		SHA    string `json:"sha256"`
		Size   int64  `json:"size_bytes"`
		Match  string `json:"if_match"`
		Policy string `json:"policy"`
	}
	if json.Unmarshal(w.Body.Bytes(), &cap) != nil || cap.SHA != "2388de8e05ad71c913ee1a9c4504a1aa5de09ac100a1de92858dd9ec08e9f98b" || cap.Size != 174165384 || cap.Match != "\"immutable-tool\"" || cap.Policy != "v85r" {
		t.Fatal("capability pin differs")
	}
}

func TestNASStitchMissingFrozenFeaturesNeverCreateNomination(t *testing.T) {
	f := map[string]any{"recording_id": 2, "source_key": [][3]any{{int64(1), strings.Repeat("a", 64), int64(3)}, {int64(2), strings.Repeat("b", 64), int64(4)}}, "nomination": false, "status": "UNKNOWN_frozen_features", "frozen_diagnostic_sha256": nil}
	raw, _ := json.Marshal(f)
	if _, err := parseNASStitchEdge(raw); err != nil {
		t.Fatal("frozen negative/unknown must remain importable", err)
	}
	f["nomination"] = true
	raw, _ = json.Marshal(f)
	if _, err := parseNASStitchEdge(raw); err == nil {
		t.Fatal("missing features nominated a frozen join")
	}
}
func TestNASStitchRecoveredExactCorpusImports(t *testing.T) {
	filename := os.Getenv("V85R_FROZEN_CORPUS")
	if filename == "" {
		t.Skip("set V85R_FROZEN_CORPUS to the exact recovered JSONL")
	}
	f := newCleanupFixture(t, 1)
	stitchMigration(t, f)
	file, err := os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	w := httptest.NewRecorder()
	f.s.handleAdminNASStitchCorpus(w, httptest.NewRequest("POST", "/", file))
	if w.Code != 200 {
		t.Fatalf("exact recovered corpus import: %d %s", w.Code, w.Body.String())
	}
	var count, nominees int
	if err = f.pool.QueryRow(f.ctx, `SELECT count(*),count(*) FILTER(WHERE nomination) FROM nas_stitch_edges`).Scan(&count, &nominees); err != nil || count != 73843 || nominees != 52462 {
		t.Fatalf("corpus census %d/%d: %v", count, nominees, err)
	}
}

func TestNASStitchNoRetirementReadinessReleasesLaneAndPreservesRaw(t *testing.T) {
	for _, pause := range []bool{false, true} {
		t.Run(fmt.Sprintf("paused_%t", pause), func(t *testing.T) {
			f, p, q := newNASStitchTaskFixture(t)
			f.s.cfg.NASStitchEnabled = true
			for n := range q.Outputs[0].Inputs {
				q.Outputs[0].Inputs[n].RetirementHeld = true
			}
			register := func() *httptest.ResponseRecorder {
				return cleanupCall(t, f.s.handleAccountNASStitchResult, 47, 5, "id", "7", q)
			}
			if w := register(); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			// A second exact task proves progress through the shared lease handler.
			plan, _ := json.Marshal(p)
			if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until) VALUES(2,3,'2026-09-21','2026-09-22','next scope',now()+interval '1 hour')`); err != nil {
				t.Fatal(err)
			}
			if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_tasks(id,scope_id,recording_id,hour_at,plan,plan_sha256) VALUES(8,2,2,'2026-09-21',$1,$2)`, string(plan), q.PlanSHA); err != nil {
				t.Fatal(err)
			}
			lease := func() *httptest.ResponseRecorder {
				return cleanupCall(t, f.s.handleAccountNASStitchLease, 47, 5, "", "", nil)
			}
			if w := lease(); w.Code != 200 || !strings.Contains(w.Body.String(), `"task":null`) {
				t.Fatalf("lane advanced before readiness: %s", w.Body.String())
			}
			if pause {
				w := cleanupCall(t, f.s.handleAdminNASStitchPause, 47, 5, "id", "1", nil)
				if w.Code != 200 {
					t.Fatal(w.Body.String())
				}
			}
			for n := 0; n < 2; n++ {
				w := cleanupCall(t, f.s.handleAccountNASStitchReady, 47, 5, "id", "7", map[string]any{"lease_token": q.Token, "plan_sha256": q.PlanSHA, "outputs_ready": true})
				if w.Code != 200 {
					t.Fatal(w.Body.String())
				}
			}
			if w := register(); w.Code != 200 {
				t.Fatalf("lost registration ACK: %s", w.Body.String())
			}
			var state string
			var cleanup, raw, outputs int
			if err := f.pool.QueryRow(f.ctx, `SELECT state,(SELECT count(*) FROM nas_cleanup_plans),(SELECT count(*) FROM recording_clips WHERE id BETWEEN 1 AND 3),(SELECT count(*) FROM nas_stitch_outputs WHERE task_id=7) FROM nas_stitch_tasks WHERE id=7 AND cleanup_plan_id IS NULL`).Scan(&state, &cleanup, &raw, &outputs); err != nil || state != "completed_no_retirement" || cleanup != 0 || raw != 3 || outputs != 1 {
				t.Fatalf("non-retiring disposition %s plans=%d raw=%d outputs=%d: %v", state, cleanup, raw, outputs, err)
			}
			if w := lease(); w.Code != 200 || !strings.Contains(w.Body.String(), `"id":8`) {
				t.Fatalf("successor not leased: %s", w.Body.String())
			}
			for _, sql := range []string{`UPDATE nas_stitch_tasks SET state='verified' WHERE id=7`, `UPDATE nas_stitch_tasks SET result='{}' WHERE id=7`} {
				if _, err := f.pool.Exec(f.ctx, sql); err == nil {
					t.Fatal("terminal evidence changed", sql)
				}
			}
		})
	}
}
