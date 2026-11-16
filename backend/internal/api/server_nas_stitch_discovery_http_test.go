package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Exercise real authentication, request-version binding, read-only source
// observation and response encoding. Only fake metadata in a disposable schema
// is used; the real compiled registry is never patched or replaced.
func TestNASStitchDiscoveryAuthenticatedAdmissionAndGuards(t *testing.T) {
	f, server := authenticatedCopyFixture(t)
	pair := reviewedNASStitchDiscovery(t)
	reader := pair.ReaderIdentity()
	const run = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	const activation = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	const path = "/api/v1/account/connections/stitch/evidence/admission"
	nativePath := path + "?run_id=" + run + "&activation_id=" + activation
	sample, facts, _ := syntheticNASStitchReadback(t)
	status := nasStitchReaderStatus{Schema: "stoarama.nas-stitch-reader-status.v1", Integrity: "clear", History: []nasStitchReadbackSummary{},
		Reader: nasStitchReaderExecution{ConnectionID: 13, AccountID: 47, BootID: sample.Reader.BootID,
			Version: string(reader.Version()), ArtifactSHA: reader.ArtifactSHA256().String(), ArtifactSize: reader.SizeBytes(),
			Host: sample.Reader.Host}}
	encode := func(value any) []byte {
		t.Helper()
		b, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	stitchMigration(t, f)
	if _, err := f.pool.Exec(f.ctx, `ALTER TABLE connections ADD COLUMN nas_host jsonb,ADD COLUMN nas_host_reported_at timestamptz,
 ADD COLUMN inventory_mismatches bigint DEFAULT 0;
 INSERT INTO streams VALUES(17208,'Europe/Prague');INSERT INTO recordings VALUES(444,47,17208,'Europe/Prague');
 INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until)
 VALUES(1,17208,'2026-08-09T09:00:52.52Z','2026-08-09T09:02:52.52Z','SYNTHETIC admission fixture',clock_timestamp()+interval '10 minutes')`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_tasks(id,scope_id,recording_id,hour_at,plan,plan_sha256)
 VALUES(1,1,444,'2026-08-09T09:00Z',$1,$2)`, encode(proofPlanFixture()), nasStitchProofSourceSHA); err != nil {
		t.Fatal(err)
	}
	reset := func() {
		t.Helper()
		f.s.cfg.NASStitchProofTaskID, f.s.cfg.NASStitchProofActivationID = 1, activation
		f.s.cfg.NASStitchEnabled, f.s.cfg.NASStitchDeletionEnabled = false, false
		if _, err := f.pool.Exec(f.ctx, `DELETE FROM nas_cleanup_frozen_streams;UPDATE nas_stitch_scopes SET paused=false`); err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version=$1,client_boot_id=$2,last_seen_at=clock_timestamp(),
 nas_host=$3,nas_host_reported_at=clock_timestamp(),nas_stitch_status=$4,nas_stitch_status_at=clock_timestamp(),inventory_mismatches=0 WHERE id=13`,
			string(reader.Version()), status.Reader.BootID, encode(facts.Host), encode(map[string]any{"stitch_reader": status})); err != nil {
			t.Fatal(err)
		}
	}
	call := func(target, version string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, server.URL+target, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer fixture-key")
		req.Header.Set("User-Agent", "stoarama-nas-pull/"+version)
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024))
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, body
	}
	reset()
	var approved time.Time
	if err := f.pool.QueryRow(f.ctx, `SELECT approved_until FROM nas_stitch_scopes WHERE id=1`).Scan(&approved); err != nil {
		t.Fatal(err)
	}
	code, body := call(nativePath, string(reader.Version()))
	if code != http.StatusOK {
		t.Fatalf("reviewed native metadata admission: %d %s", code, body)
	}
	var wire struct {
		Schema       string                     `json:"schema"`
		Mode         int                        `json:"mode"`
		Profiles     nasStitchProfileComponents `json:"profiles"`
		Task         json.RawMessage            `json:"task"`
		B64          string                     `json:"task_b64"`
		SHA          string                     `json:"task_sha256"`
		Expiry       string                     `json:"expires_at"`
		Run          string                     `json:"native_run_id"`
		ActivationID string                     `json:"activation_id"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(wire.B64)
	components, componentErr := nasStitchPairComponents(pair)
	if err != nil || componentErr != nil || !bytes.Equal(raw, wire.Task) || cleanupSHA(raw) != wire.SHA || wire.Profiles != components ||
		wire.Schema != "stoarama.nas-stitch-admission.v1" || wire.Mode != 1 || wire.Run != run || wire.ActivationID != activation {
		t.Fatal("actual admission lost raw-task/profile/operation binding")
	}
	var task nasStitchProof
	if json.Unmarshal(raw, &task) != nil || task.SourcePlanSHA != nasStitchProofSourceSHA || task.ActivationID != activation || !task.Expires.Equal(approved) ||
		wire.Expiry != approved.UTC().Format("2006-01-02T15:04:05.000000Z") {
		t.Fatal("actual DB scope clamp/UTC6/task identity changed")
	}
	var taskFields map[string]json.RawMessage
	json.Unmarshal(raw, &taskFields)
	if string(taskFields["expires_at"]) != `"`+wire.Expiry+`"` {
		t.Fatal("raw and outer expiry differ")
	}
	f.s.cfg.NASStitchProofTaskID = 2
	code, body = call(path+"?producer_profile_sha256="+pair.Key().Producer.String(), string(reader.Version()))
	var renewal map[string]json.RawMessage
	if code != http.StatusOK || json.Unmarshal(body, &renewal) != nil || len(renewal) != 4 || string(renewal["mode"]) != "2" {
		t.Fatal("readback admission fell through to native")
	}
	for _, tc := range []struct {
		name    string
		prepare func()
		target  string
		version string
	}{
		{"unstamped-transport", func() {}, nativePath, "development"},
		{"mismatched-heartbeat", func() { f.useClientVersion(nasCleanupProofClientVersion) }, nativePath, string(reader.Version())},
		{"wrong-activation", func() {}, strings.Replace(nativePath, activation, run, 1), string(reader.Version())},
		{"ordinary-enabled", func() { f.s.cfg.NASStitchEnabled = true }, nativePath, string(reader.Version())},
		{"deletion-enabled", func() { f.s.cfg.NASStitchDeletionEnabled = true }, nativePath, string(reader.Version())},
		{"paused-scope", func() { mustDiscoverySQL(t, f, `UPDATE nas_stitch_scopes SET paused=true`) }, nativePath, string(reader.Version())},
		{"inventory-mismatch", func() { mustDiscoverySQL(t, f, `UPDATE connections SET inventory_mismatches=1 WHERE id=13`) }, nativePath, string(reader.Version())},
		{"different-boot", func() {
			mustDiscoverySQL(t, f, `UPDATE connections SET client_boot_id='cccccccc-cccc-4ccc-8ccc-cccccccccccc' WHERE id=13`)
		}, nativePath, string(reader.Version())},
		{"stale-heartbeat", func() {
			mustDiscoverySQL(t, f, `UPDATE connections SET last_seen_at=clock_timestamp()-interval '121 seconds' WHERE id=13`)
		}, nativePath, string(reader.Version())},
		{"candidate-reader", func() {
			host := facts.Host
			host.Release = &nasReleaseHealth{Stable: true, Candidate: true}
			mustDiscoverySQL(t, f, `UPDATE connections SET nas_host=$1 WHERE id=13`, encode(host))
		}, nativePath, string(reader.Version())},
		{"different-artifact", func() {
			changed := status
			changed.Reader.ArtifactSHA = strings.Repeat("b", 64)
			mustDiscoverySQL(t, f, `UPDATE connections SET nas_stitch_status=$1 WHERE id=13`, encode(map[string]any{"stitch_reader": changed}))
		}, nativePath, string(reader.Version())},
		{"any-cleanup-freeze", func() {
			mustDiscoverySQL(t, f, `INSERT INTO nas_cleanup_frozen_streams(plan_id,stream_id) VALUES(999,3)`)
		}, nativePath, string(reader.Version())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset()
			tc.prepare()
			code, body := call(tc.target, tc.version)
			if code != http.StatusForbidden && code != http.StatusConflict {
				t.Fatalf("guard admitted altered context: %d %s", code, body)
			}
			var refused map[string]json.RawMessage
			if json.Unmarshal(body, &refused) != nil || refused["error"] == nil || refused["task"] != nil || refused["profiles"] != nil {
				t.Fatal("refusal leaked a task or accepted components")
			}
		})
	}
	// Real source/scope immutability is retained, not disabled to manufacture
	// negative admission fixtures. All previous calls left the task unleased.
	for _, query := range []string{`UPDATE nas_stitch_tasks SET plan_sha256=repeat('d',64) WHERE id=1`,
		`UPDATE nas_stitch_scopes SET approved_until=clock_timestamp()-interval '1 second' WHERE id=1`} {
		if _, err := f.pool.Exec(f.ctx, query); err == nil {
			t.Fatal("original source/scope identity guard was bypassed")
		}
	}
	var state string
	var attempt int
	if err := f.pool.QueryRow(f.ctx, `SELECT state,attempt FROM nas_stitch_tasks WHERE id=1`).Scan(&state, &attempt); err != nil || state != "pending" || attempt != 0 {
		t.Fatal("metadata admission leased or changed the original task")
	}
	var attempts, changed int
	if err := f.pool.QueryRow(f.ctx, `SELECT coalesce(sum(attempts),0),count(*) FILTER(WHERE state<>'pending') FROM nas_r2_copy_items`).Scan(&attempts, &changed); err != nil {
		t.Fatal(err)
	}
	gets, heads := f.bucket.counts()
	if attempts != 0 || changed != 0 || gets != 0 || heads != 0 {
		t.Fatal("metadata admission started copy/media work")
	}
}

func mustDiscoverySQL(t *testing.T, f *cleanupFixture, query string, args ...any) {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}
