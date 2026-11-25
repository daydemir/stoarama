package main

import (
	"strings"
	"testing"
)

// TestR2DeliveryKeyGuardBranches pins every RAISE of guard_r2_delivery_keys_admitted
// (0211, re-planned by 0267) to its exact message, and pins the admitted rows,
// including near misses the indexed path/content lookups must not over-match.
func TestR2DeliveryKeyGuardBranches(t *testing.T) {
	f := newNonrawFixture(t, "hold_guard_branches")
	const (
		reserveRaw = `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
 VALUES('raw',7000001,'stoarama','managed/acct-47/nonraw/source.mp4',500,repeat('c',64),repeat('d',64))`
	)
	intent := `INSERT INTO r2_nonraw_intents(bucket,object_key,endpoint_sha256,kind,stream_id,source_id,source_ids,size_bytes,sha256,nas_relative_path,etag,last_modified,verification_at,manifest_sha256,policy_sha256,picks_sha256,drain_evidence_sha256,retirement_class,choice)
 VALUES('stoarama','joined/nightly-collation-v2/objects/'||repeat('b',64)||'.mp4',repeat('f',64),'collated',3,10000001,ARRAY[7000001]::bigint[],900,repeat('b',64),'nightly/video.mp4','e',now(),now(),repeat('d',64),'` + f.policy + `',repeat('e',64),repeat('a',64),'owner_delivery_scope_20261118_v1','none')`
	const (
		joinedClip = `UPDATE recording_clips SET display_path='joined/source/source.mp4' WHERE id=7000001`
		upperClip  = `UPDATE recording_clips SET sha256=upper(sha256) WHERE id=7000001`
	)
	const (
		adoption = "R2 retirement reservation forbids delivery adoption"
		content  = "R2 retirement reservation forbids delivery content alias"
		path     = "R2 retirement reservation forbids delivery path alias"
		closure  = "R2 retirement reservation forbids delivery collated-source closure"
	)
	c64, b64 := strings.Repeat("c", 64), strings.Repeat("b", 64)
	for _, tc := range []struct {
		name  string
		seed  []string
		key   string
		sha   string
		size  int64
		rel   string
		raise string
	}{
		{"adoption_reserved_key", []string{reserveRaw}, "managed/acct-47/nonraw/source.mp4", "", 5, "", adoption},
		{"adoption_nonraw_intent_key", []string{intent}, f.key, "", 5, "", adoption},
		{"content_reserved_clip_case_folded", []string{reserveRaw, upperClip}, "gss/a.json", c64, 500, "", content},
		{"content_nonraw_intent", []string{intent}, "gss/a.json", b64, 900, "", content},
		{"path_reserved_clip_hold_joined", []string{reserveRaw}, "gss/a.json", "", 5, "joined/source/source.mp4", path},
		{"path_reserved_clip_joined", []string{reserveRaw, joinedClip}, "gss/a.json", "", 5, "source/source.mp4", path},
		{"path_nonraw_intent", []string{intent}, "gss/a.json", "", 5, "joined/nightly/video.mp4", path},
		{"closure_output_key", []string{reserveRaw}, f.key, "", 5, "", closure},
		{"closure_output_path", []string{reserveRaw}, "gss/a.json", "", 5, "joined/nightly/video.mp4", closure},
		{"closure_output_content", []string{reserveRaw}, "gss/a.json", b64, 900, "", closure},
		{"admitted_unrelated", []string{reserveRaw, intent}, "gss/a.json", strings.Repeat("a", 64), 5, "gss/a.json", ""},
		{"admitted_content_size_mismatch", []string{reserveRaw, intent}, "gss/a.json", c64, 501, "", ""},
		// regexp_replace strips one leading joined/: joined/joined/x normalizes to
		// joined/x, which the reserved joined/source/source.mp4 (-> source/...) is not.
		{"admitted_double_joined_prefix", []string{reserveRaw, joinedClip}, "gss/a.json", "", 5, "joined/joined/source/source.mp4", ""},
		{"admitted_output_without_retirement", nil, f.key, b64, 900, "nightly/video.mp4", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(f.ctx)
			for _, q := range append([]string{`SET LOCAL session_replication_role=replica`}, tc.seed...) {
				if _, err = tx.Exec(f.ctx, q); err != nil {
					t.Fatalf("seed %q: %v", q, err)
				}
			}
			if _, err = tx.Exec(f.ctx, `SET LOCAL session_replication_role=origin`); err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(f.ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256,relative_path) VALUES('stoarama',$1,'hold',$2,'metadata',$3,$4)`,
				tc.key, tc.size, tc.sha, tc.rel)
			switch {
			case tc.raise == "" && err != nil:
				t.Fatalf("admissible hold refused: %v", err)
			case tc.raise != "" && (err == nil || !strings.Contains(err.Error(), tc.raise)):
				t.Fatalf("want %q, got %v", tc.raise, err)
			}
		})
	}
}
