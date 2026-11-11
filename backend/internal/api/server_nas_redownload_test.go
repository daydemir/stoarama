package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestNASRedownloadPolicyAuthHoldsAndReadOnly(t *testing.T) {
	f := newCleanupFixture(t, 1)
	for _, name := range []string{"0174_r2_object_retirements.sql", "0182_r2_retirement_reservations.sql"} {
		body, err := os.ReadFile("../../../infra/sql/migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(f.ctx, string(body)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.pool.Exec(f.ctx, `ALTER TABLE recording_clips ADD COLUMN released_at timestamptz, ADD COLUMN capture_lease_token uuid`); err != nil {
		t.Fatal(err)
	}
	body := []byte("exact kept copy")
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	policy, windows, _ := Keep28Policy()
	w := windows[3]
	key := "managed/acct-47/copy.mp4"
	f.bucket.put(key, body)
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket,released_at,capture_lease_token)
 VALUES(901,2,$1,$2,$3,'p/copy.mp4',$4,$4::timestamptz+interval '1 minute','bucket',now(),'11111111-1111-1111-1111-111111111111')`, key, sha, len(body), w.Start); err != nil {
		t.Fatal(err)
	}
	call := func(account int64, keyID *int64, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/redownload", strings.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: account, APIKeyID: keyID, KeyScopes: []string{accountScopePull}}))
		out := httptest.NewRecorder()
		f.s.confineAccountScope(http.HandlerFunc(f.s.handleAccountConnectionRedownload)).ServeHTTP(out, req)
		return out
	}
	req := fmt.Sprintf(`{"clip_id":901,"sha256":%q,"size_bytes":%d,"policy_sha256":%q}`, sha, len(body), policy)
	for _, tt := range []struct {
		account int64
		key     *int64
		want    int
	}{{47, nil, 403}, {48, ptrInt64(6), 403}, {48, ptrInt64(5), 403}} {
		if out := call(tt.account, tt.key, req); out.Code != tt.want {
			t.Fatalf("auth code=%d body=%s", out.Code, out.Body)
		}
	}
	for _, bad := range []string{strings.Replace(req, policy, strings.Repeat("0", 64), 1), strings.Replace(req, sha, strings.Repeat("a", 64), 1), strings.TrimSuffix(req, "}") + `,"object_key":"evil"}`} {
		if out := call(47, ptrInt64(5), bad); out.Code == 200 {
			t.Fatal("bad identity/key/policy accepted")
		}
	}
	out := call(47, ptrInt64(5), req)
	if out.Code != 200 {
		t.Fatalf("copy code=%d body=%s", out.Code, out.Body)
	}
	if strings.Contains(out.Body.String(), "11111111-1111-1111-1111-111111111111") {
		t.Fatal("lease token leaked")
	}
	var response struct {
		Policy  string         `json:"policy_sha256"`
		Clip    map[string]any `json:"clip"`
		URL     string         `json:"url"`
		Headers http.Header    `json:"headers"`
	}
	if err := json.Unmarshal(out.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Policy != policy || response.Clip["sha256"] != sha || response.Headers.Get("If-Match") == "" || response.URL == "" {
		t.Fatalf("bad copy response %+v", response)
	}
	var unchanged bool
	if err := f.pool.QueryRow(f.ctx, `SELECT released_at IS NOT NULL AND purged_at IS NULL FROM recording_clips WHERE id=901`).Scan(&unchanged); err != nil || !unchanged {
		t.Fatal("presign changed lifecycle")
	}
	// Changed window/timezone and active cleanup reservations all refuse signing.
	for _, sql := range []string{
		`UPDATE streams SET local_timezone='UTC' WHERE id=3`,
		`UPDATE recording_clips SET clip_start_at='2026-08-01',clip_end_at='2026-08-01 00:01:00' WHERE id=901`,
	} {
		if _, err := f.pool.Exec(f.ctx, sql); err != nil {
			t.Fatal(err)
		}
		if out := call(47, ptrInt64(5), req); out.Code != 409 {
			t.Fatalf("changed source accepted: %d", out.Code)
		}
		if _, err := f.pool.Exec(f.ctx, `UPDATE streams SET local_timezone='Asia/Tokyo' WHERE id=3;UPDATE recording_clips SET clip_start_at='2026-09-12 00:00:00+09',clip_end_at='2026-09-12 00:01:00+09' WHERE id=901`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket)
 SELECT 902,recording_id,'managed/acct-47/held.mp4',sha256,size_bytes,'p/held.mp4',clip_start_at,clip_end_at,bucket FROM recording_clips WHERE id=901;
 INSERT INTO nas_cleanup_plans(account_id,connection_id,label,report_sha256,manifest,plan_sha256)
 VALUES(47,13,'redownload-held',repeat('a',64),'[{"kind":"raw","source_id":902}]',repeat('b',64))`); err != nil {
		t.Fatal(err)
	}
	heldReq := strings.Replace(req, `"clip_id":901`, `"clip_id":902`, 1)
	if out := call(47, ptrInt64(5), heldReq); out.Code != 409 {
		t.Fatalf("cleanup-held source accepted: %d", out.Code)
	}

	// Complete exact cleanup reservations, including a plan that has ever issued,
	// remain a veto. New R2 intent reservations independently veto redownload.
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
 VALUES('raw',901,'bucket',$1,$2,$3,repeat('a',64))`, key, len(body), sha); err != nil {
		t.Fatal(err)
	}
	if out := call(47, ptrInt64(5), req); out.Code != 409 {
		t.Fatalf("pending R2 delete accepted: %d", out.Code)
	}
}
