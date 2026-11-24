package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fakeCollatedStore struct{ heads map[string]r2.ObjectHead }

func (f fakeCollatedStore) Head(_ context.Context, key string) (r2.ObjectHead, error) {
	h, ok := f.heads[key]
	if !ok {
		return r2.ObjectHead{}, fmt.Errorf("missing %s", key)
	}
	return h, nil
}
func (fakeCollatedStore) OpenExact(context.Context, string, string, string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("unused")
}
func (fakeCollatedStore) PresignPutCreateOnlyRequest(context.Context, string, string, int64, string, time.Duration) (r2.PresignedRequest, error) {
	return r2.PresignedRequest{}, fmt.Errorf("unused")
}
func (fakeCollatedStore) PresignGetExactRequest(_ context.Context, key, etag, _ string, _ time.Duration) (r2.PresignedRequest, error) {
	return r2.PresignedRequest{URL: "https://r2.example/" + key + "?etag=" + etag}, nil
}
func (fakeCollatedStore) PresignHeadExactRequest(context.Context, string, string, string, time.Duration) (r2.PresignedRequest, error) {
	return r2.PresignedRequest{}, fmt.Errorf("unused")
}

func TestValidCollatedNASPathPinsContract(t *testing.T) {
	good := []string{
		"100000_North_America_US_Key_West_Duval_Street/July/26-Sunday/100000_Duval_Street_2026_July_W4_Sunday_hour_13_part_04_130027-140016.mp4",
		"f/July/26-Sunday/01_P_2026_July_W4_Sunday_hour_00_000001-005959.mp4",
		"f/November/01-Sunday/01_P_2026_November_W1_Sunday_hour_01_dst2_010001-015959.mp4",
		"f/July/26-Sunday/01_P_2026_July_W4_Sunday_hour_23.manifest.json",
		"recordings/339/joined/2026-09-24/339_2026-09-24_hour_16_part_02_161446-170012.mp4",
	}
	bad := []string{
		"joined/f/July/26-Sunday/01_P_2026_July_W4_Sunday_hour_13_130027-140016.mp4",
		"managed/acct-47/f/July/26-Sunday/01_P_2026_July_W4_Sunday_hour_13_130027-140016.mp4",
		"f/July/Sunday/01_P_2026_July_W4_Sunday_hour_13_130027-140016.mp4",
		"f/July/26-Sunday/01_P_2026_July_W4_Sunday_hour_24_130027-140016.mp4",
		"f/July/26-Sunday/../x_hour_13_130027-140016.mp4",
		"/f/July/26-Sunday/01_P_hour_13_130027-140016.mp4",
		"f/July/26-Sunday/.01_P_hour_13_130027-140016.mp4",
		"f/July/26-Sunday/01_P_hour_13_part_4_130027-140016.mp4",
		"recordings/339/joined/2026-09-24/338_2026-09-24_hour_16_161446-170012.mp4",
		"recordings/339/joined/2026-09-24/339_2026-09-25_hour_16_161446-170012.mp4",
	}
	for _, p := range good {
		if !ValidCollatedNASPath(p) {
			t.Fatalf("rejected %s", p)
		}
	}
	for _, p := range bad {
		if ValidCollatedNASPath(p) {
			t.Fatalf("accepted %s", p)
		}
	}
}

func TestCollatedDeliveryFeedDownloadAckAndPolicy(t *testing.T) {
	// Shared CI runners are far slower than production for this 20k-row scan;
	// the production budgets are exercised by the timing in production, not here.
	budget, timeout := collatedFeedBudget, collatedFeedStatementTimeout
	collatedFeedBudget, collatedFeedStatementTimeout = 60*time.Second, "60s"
	t.Cleanup(func() { collatedFeedBudget, collatedFeedStatementTimeout = budget, timeout })
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("collated_delivery_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	cfg, _ := pgxpool.ParseConfig(databaseURL)
	cfg.ConnConfig.RuntimeParams = map[string]string{"search_path": schema}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// Subset of 0158_recording_collation_v2 plus the connection columns used here.
	if _, err := pool.Exec(ctx, `
		CREATE TABLE connections(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,kind TEXT NOT NULL,api_key_id BIGINT,updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		  nas_storage_free_bytes BIGINT,nas_storage_reported_at TIMESTAMPTZ);
		CREATE TABLE recordings(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,delivery TEXT NOT NULL,stream_id BIGINT);
		CREATE TABLE nas_media_keep20_windows(stream_id BIGINT PRIMARY KEY,timezone TEXT NOT NULL,start_at TIMESTAMPTZ NOT NULL,end_at TIMESTAMPTZ NOT NULL,keep_everything BOOLEAN NOT NULL);
		CREATE TABLE recording_collation_hours(id BIGINT PRIMARY KEY,recording_id BIGINT NOT NULL,local_date DATE NOT NULL,delivery_hour SMALLINT NOT NULL,generation INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'collated');
		CREATE TABLE recording_collation_outputs(id BIGINT PRIMARY KEY,collation_hour_id BIGINT NOT NULL REFERENCES recording_collation_hours(id),part INTEGER NOT NULL,
		  object_key TEXT NOT NULL,nas_relative_path TEXT NOT NULL UNIQUE,size_bytes BIGINT NOT NULL,sha256 TEXT NOT NULL,
		  source_clip_ids BIGINT[] NOT NULL,start_at TIMESTAMPTZ,end_at TIMESTAMPTZ);
		CREATE TABLE recording_clips(id bigint PRIMARY KEY,display_path text);
		CREATE TABLE delivery_keep_holds(bucket text,object_key text,kind text,etag text,size_bytes bigint,sha256 text,PRIMARY KEY(bucket,object_key));
		CREATE TABLE clip_storage_billing_contracts(clip_id BIGINT PRIMARY KEY,mode TEXT NOT NULL,authoritative BOOLEAN NOT NULL DEFAULT true);
		INSERT INTO clip_storage_billing_contracts(clip_id,mode) SELECT g,'nas_collated_hold' FROM generate_series(1,1000) g;
		INSERT INTO clip_storage_billing_contracts(clip_id,mode) VALUES(5000,'nas_pull_monthly');
		INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',5),(14,48,'nas_pull',6);
		INSERT INTO recordings VALUES(1,47,'nas_pull'),(2,47,'managed'),(3,48,'nas_pull');
		INSERT INTO recording_collation_hours(id,recording_id,local_date,delivery_hour,generation) VALUES(10,1,'2026-07-26',6,2),(11,1,'2026-07-26',6,3),(12,1,'2026-07-26',7,2),(13,2,'2026-07-26',6,2),(14,3,'2026-07-26',6,2);
	`); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile("../../../infra/sql/migrations/0160_nas_collated_delivery.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(body)); err != nil {
		t.Fatalf("apply 0160: %v", err)
	}
	if body, err = os.ReadFile("../../../infra/sql/migrations/0174_r2_object_retirements.sql"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(body)); err != nil {
		t.Fatalf("apply 0174: %v", err)
	}
	body, err = os.ReadFile("../../../infra/sql/migrations/0224_delivery_nas_handoff.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(body)); err != nil {
		t.Fatalf("apply 0224: %v", err)
	}
	installDeliveryFenceTestSchema(t, pool)
	// The connection-wide gate starts open; closing it is exercised below.
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE OR REPLACE FUNCTION nas_delivery_work_epoch(connection bigint) RETURNS boolean LANGUAGE sql STABLE AS $$SELECT true$$;
 UPDATE connections SET nas_storage_free_bytes=%d,nas_storage_reported_at=now()`, collatedKeep20FreeBytes)); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	dir := "f/July/26-Sunday/01_P_2026_July_W4_Sunday_hour_"
	for _, o := range []struct {
		id, hour int64
		rel      string
	}{
		{100, 10, dir + "13_130001-140001.mp4"},         // superseded by generation 3
		{101, 11, dir + "13_part_01_130001-133001.mp4"}, // newest generation
		{102, 11, dir + "13_part_02_133001-140001.mp4"},
		{103, 12, dir + "14_140001-150001.mp4"},
		{104, 13, dir + "15_150001-160001.mp4"}, // managed recording
		{105, 14, dir + "16_160001-170001.mp4"}, // other account
		{106, 12, dir + "17_170001-180001.mp4"}, // backfill: raw already on the NAS
	} {
		sources := "{" + fmt.Sprint(o.id) + "}"
		if o.id == 106 {
			sources = "{5000}"
		}
		if _, err := pool.Exec(ctx, `INSERT INTO recording_collation_outputs VALUES($1,$2,1,$3,$4,1000,$5,$6)`, o.id, o.hour, fmt.Sprintf("joined/b/objects/%d.mp4", o.id), o.rel, sha, sources); err != nil {
			t.Fatal(err)
		}
	}
	keyID := int64(5)
	principal := accountPrincipal{AccountID: 47, APIKeyID: &keyID}
	s := &Server{pool: pool, joinedOutputStorage: fakeCollatedStore{heads: map[string]r2.ObjectHead{
		"joined/b/objects/101.mp4": {ETag: "e101", SizeBytes: 1000}, "joined/b/objects/102.mp4": {ETag: "e102", SizeBytes: 999},
	}}}
	do := func(h http.HandlerFunc, method, target string, payload any, params map[string]string) (int, map[string]any) {
		t.Helper()
		var reader io.Reader
		if payload != nil {
			raw, _ := json.Marshal(payload)
			reader = bytes.NewReader(raw)
		}
		req := httptest.NewRequest(method, target, reader)
		rctx := chi.NewRouteContext()
		for k, v := range params {
			rctx.URLParams.Add(k, v)
		}
		req = req.WithContext(context.WithValue(context.WithValue(req.Context(), chi.RouteCtxKey, rctx), accountPrincipalContextKey, principal))
		rec := httptest.NewRecorder()
		h(rec, req)
		var out map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}
	feedIDs := func() []int64 {
		t.Helper()
		code, out := do(s.handleAccountCollated, http.MethodGet, "/api/v1/account/collated?limit=10&handoff_readback=1", nil, nil)
		if code != 200 {
			t.Fatalf("feed code=%d out=%v", code, out)
		}
		var ids []int64
		for _, it := range out["items"].([]any) {
			ids = append(ids, int64(it.(map[string]any)["output_id"].(float64)))
		}
		return ids
	}
	if ids := feedIDs(); len(ids) != 0 {
		t.Fatalf("disabled feed returned %v", ids)
	}
	if code, _ := do(s.handleAccountCollatedDownload, http.MethodGet, "/x", nil, map[string]string{"outputId": "101"}); code != 404 {
		t.Fatalf("disabled download code=%d", code)
	}
	adminDo := func(method string, payload any) (int, map[string]any) {
		return do(s.handleAdminConnectionCollatedDelivery, method, "/x", payload, map[string]string{"id": "13"})
	}
	if code, out := adminDo(http.MethodGet, nil); code != 200 || out["pending_files"].(float64) != 3 {
		t.Fatalf("status code=%d out=%v", code, out)
	}
	if code, _ := adminDo(http.MethodPost, map[string]any{"download_parallel": 33}); code != 400 {
		t.Fatalf("parallel bound code=%d", code)
	}
	if code, out := adminDo(http.MethodPost, map[string]any{"enabled": true, "download_bytes_per_sec": 64 << 20, "download_parallel": 8}); code != 200 ||
		out["policy"].(map[string]any)["download_parallel"].(float64) != 8 {
		t.Fatalf("enable code=%d out=%v", code, out)
	}
	if ids := feedIDs(); fmt.Sprint(ids) != "[101 102 103]" {
		t.Fatalf("feed=%v, want newest generation of account 47 nas_pull hours only", ids)
	}
	code, out := do(s.handleAccountCollatedDownload, http.MethodGet, "/x", nil, map[string]string{"outputId": "101"})
	if code != 200 || out["if_match"] != `"e101"` || out["sha256"] != sha {
		t.Fatalf("download code=%d out=%v", code, out)
	}
	if code, _ := do(s.handleAccountCollatedDownload, http.MethodGet, "/x", nil, map[string]string{"outputId": "102"}); code != 409 {
		t.Fatalf("size drift download code=%d", code)
	}
	for _, id := range []string{"100", "104", "105", "106"} {
		if code, _ := do(s.handleAccountCollatedDownload, http.MethodGet, "/x", nil, map[string]string{"outputId": id}); code != 404 {
			t.Fatalf("ineligible %s download code=%d", id, code)
		}
	}
	ack := map[string]any{"output_id": 101, "nas_relative_path": dir + "13_part_01_130001-133001.mp4", "size_bytes": 1000, "sha256": sha}
	if code, out := do(s.handleAccountCollatedAck, http.MethodPost, "/x", ack, nil); code != 200 || out["already_verified"] != false {
		t.Fatalf("ack code=%d out=%v", code, out)
	}
	if code, out := do(s.handleAccountCollatedAck, http.MethodPost, "/x", ack, nil); code != 200 || out["already_verified"] != true {
		t.Fatalf("repeat ack code=%d out=%v", code, out)
	}
	ack["sha256"] = strings.Repeat("b", 64)
	if code, _ := do(s.handleAccountCollatedAck, http.MethodPost, "/x", ack, nil); code != 409 {
		t.Fatalf("mismatched ack code=%d", code)
	}
	if code, _ := do(s.handleAccountCollatedAck, http.MethodPost, "/x", map[string]any{"output_id": 105, "nas_relative_path": dir + "16_160001-170001.mp4", "size_bytes": 1000, "sha256": sha}, nil); code != 404 {
		t.Fatalf("foreign ack code=%d", code)
	}
	if ids := feedIDs(); fmt.Sprint(ids) != "[102 103]" {
		t.Fatalf("feed after ack=%v", ids)
	}
	if code, _ := do(s.handleAccountCollatedError, http.MethodPost, "/x", map[string]any{"output_id": 105, "error": "foreign"}, nil); code != 404 {
		t.Fatalf("foreign error report code=%d", code)
	}
	if code, _ := do(s.handleAccountCollatedError, http.MethodPost, "/x", map[string]any{"output_id": 102, "error": "boom"}, nil); code != 200 {
		t.Fatalf("error report code=%d", code)
	}
	if _, out := adminDo(http.MethodGet, nil); out["last_error"] != "boom" || out["files_pulled"].(float64) != 1 ||
		out["pending_files"].(float64) != 2 || out["backed_off_files"].(float64) != 1 {
		t.Fatalf("status after ack=%v", out)
	}
	if ids := feedIDs(); fmt.Sprint(ids) != "[103]" {
		t.Fatalf("failed output 102 was not backed off: feed=%v", ids)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_collation_output_errors SET next_attempt_at=now()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if ids := feedIDs(); fmt.Sprint(ids) != "[102 103]" {
		t.Fatalf("expired backoff not retried: feed=%v", ids)
	}
	// An output retired from R2 (stoaramactl r2-retire) leaves the feed and
	// can no longer be downloaded.
	if _, err := pool.Exec(ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256)
		VALUES ('collated',103,1,'stoarama','joined/nightly-collation-v2/objects/103.mp4',1000,$1,'outside_keep28_no_nas_copy','none',$1,$1)`, sha); err != nil {
		t.Fatal(err)
	}
	if ids := feedIDs(); fmt.Sprint(ids) != "[102]" {
		t.Fatalf("retired output still in feed=%v", ids)
	}
	if code, _ := do(s.handleAccountCollatedDownload, http.MethodGet, "/x", nil, map[string]string{"outputId": "103"}); code != 404 {
		t.Fatalf("retired output download code=%d", code)
	}

	// The handoff admits just the approved output and binds its R2 ETag before
	// presigning. An assembly hold or a divergent live hold cannot authorize it.
	const handoffKey = "joined/goodplus-20260821-generation-1/objects/106.mp4"
	if _, err := pool.Exec(ctx, `UPDATE recording_collation_outputs SET object_key=$1 WHERE id=106`, handoffKey); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO delivery_keep_holds VALUES ('stoarama',$1,'video_assembly','e106',1000,$2)`, handoffKey, sha); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO delivery_nas_handoff_mounts VALUES($1,5,99)`, sha); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO delivery_nas_handoff_outputs
	  (output_id,recording_id,roster_sha256,picks_sha256,bucket,object_key,etag,size_bytes,sha256,nas_relative_path)
	  SELECT id,1,$1,$1,'stoarama',object_key,'e106',size_bytes,sha256,nas_relative_path
	  FROM recording_collation_outputs WHERE id=106`, sha); err != nil {
		t.Fatal(err)
	}
	if ids := feedIDs(); fmt.Sprint(ids) != "[102]" {
		t.Fatalf("assembly hold admitted to handoff: %v", ids)
	}
	if _, err := pool.Exec(ctx, `UPDATE delivery_keep_holds SET kind='video_collated' WHERE object_key=$1`, handoffKey); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET delivery_nas_handoff_enabled=true,delivery_nas_handoff_roster_sha256=$1,collated_delivery_enabled=false WHERE id=13`, sha); err != nil {
		t.Fatal(err)
	}
	if ids := feedIDs(); fmt.Sprint(ids) != "[106]" {
		t.Fatalf("handoff-only did not isolate its roster: %v", ids)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET collated_delivery_enabled=true WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	if ids := feedIDs(); fmt.Sprint(ids) != "[102 106]" {
		t.Fatalf("approved handoff missing: %v", ids)
	}
	heads := s.joinedOutputStorage.(fakeCollatedStore).heads
	heads[handoffKey] = r2.ObjectHead{ETag: "drifted", SizeBytes: 1000}
	params := map[string]string{"outputId": "106"}
	if code, _ := do(s.handleAccountCollatedDownload, http.MethodGet, "/x", nil, params); code != 404 {
		t.Fatalf("predecessor client can download handoff output: %d", code)
	}
	const handoffURL = "/x?handoff_readback=1"
	if code, _ := do(s.handleAccountCollatedDownload, http.MethodGet, handoffURL, nil, params); code != 409 {
		t.Fatalf("drifted handoff ETag download code=%d", code)
	}
	heads[handoffKey] = r2.ObjectHead{ETag: "e106", SizeBytes: 1000}
	if code, out := do(s.handleAccountCollatedDownload, http.MethodGet, handoffURL, nil, params); code != 200 || out["if_match"] != `"e106"` {
		t.Fatalf("approved handoff download code=%d out=%v", code, out)
	}
	if _, err := pool.Exec(ctx, `UPDATE delivery_keep_holds SET etag='drifted' WHERE object_key=$1`, handoffKey); err != nil {
		t.Fatal(err)
	}
	if ids := feedIDs(); fmt.Sprint(ids) != "[102]" {
		t.Fatalf("drifted handoff hold still in feed: %v", ids)
	}
	if _, err := pool.Exec(ctx, `UPDATE delivery_keep_holds SET etag='e106' WHERE object_key=$1`, handoffKey); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE clip_storage_billing_contracts SET mode='nas_collated_hold' WHERE clip_id=5000`); err != nil {
		t.Fatal(err)
	}
	if code, _ := do(s.handleAccountCollatedError, http.MethodPost, handoffURL, map[string]any{"output_id": 106, "error": "test handoff backoff"}, nil); code != 200 {
		t.Fatalf("report handoff error code=%d", code)
	}
	if code, _ := do(s.handleAccountCollatedDownload, http.MethodGet, "/x", nil, params); code != 200 {
		t.Fatalf("handoff backoff withheld independent ordinary copy: %d", code)
	}
	if code, _ := do(s.handleAccountCollatedError, http.MethodPost, "/x", map[string]any{"output_id": 106, "error": "test ordinary copy backoff"}, nil); code != 200 {
		t.Fatalf("report ordinary error code=%d", code)
	}
	if _, err := pool.Exec(ctx, `UPDATE delivery_nas_handoff_errors SET next_attempt_at=now()-interval '1 second' WHERE output_id=106`); err != nil {
		t.Fatal(err)
	}
	if ids := feedIDs(); fmt.Sprint(ids) != "[102 106]" {
		t.Fatalf("ordinary error suppressed approved handoff receipt: %v", ids)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM recording_collation_output_errors WHERE output_id=106`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE clip_storage_billing_contracts SET mode='nas_pull_monthly' WHERE clip_id=5000;
 UPDATE delivery_nas_handoff_errors SET next_attempt_at=now()-interval '1 second' WHERE output_id=106`); err != nil {
		t.Fatal(err)
	}
	handoffAck := map[string]any{"output_id": 106, "nas_relative_path": dir + "17_170001-180001.mp4", "size_bytes": 1000, "sha256": sha}
	if code, _ := do(s.handleAccountCollatedAck, http.MethodPost, "/x", handoffAck, nil); code != 200 {
		t.Fatalf("ordinary ACK code=%d", code)
	}
	if ids := feedIDs(); fmt.Sprint(ids) != "[102 106]" {
		t.Fatalf("ordinary ACK suppressed fresh handoff readback: %v", ids)
	}
	handoffAck["handoff_readback"] = []int64{1000, 2, 3, 4, 5}
	handoffAck["handoff_readback_round"] = 0
	handoffAck["handoff_root_inode"] = 99
	if code, _ := do(s.handleAccountCollatedAck, http.MethodPost, "/x", handoffAck, nil); code != 400 {
		t.Fatalf("unnegotiated handoff ACK code=%d", code)
	}
	for range 2 {
		if code, _ := do(s.handleAccountCollatedAck, http.MethodPost, handoffURL, handoffAck, nil); code != 200 {
			t.Fatalf("valid handoff ACK code=%d", code)
		}
	}
	handoffAck["handoff_readback"] = []int64{1000, 2, 3, 44, 5}
	if code, _ := do(s.handleAccountCollatedAck, http.MethodPost, handoffURL, handoffAck, nil); code != 409 {
		t.Fatalf("different file identity replaced receipt: %d", code)
	}
	if ids := feedIDs(); fmt.Sprint(ids) != "[102]" {
		t.Fatalf("current handoff receipt left pending output: %v", ids)
	}

	if _, err := pool.Exec(ctx, `INSERT INTO delivery_nas_handoff_rounds(roster_sha256,round) VALUES($1,1)`, sha); err != nil {
		t.Fatal(err)
	}
	if code, _ := do(s.handleAccountCollatedAck, http.MethodPost, handoffURL, handoffAck, nil); code != 409 {
		t.Fatalf("preceding readback round accepted after recheck starts: %d", code)
	}
	handoffAck["handoff_readback_round"] = 1
	if ids := feedIDs(); fmt.Sprint(ids) != "[102 106]" {
		t.Fatalf("fresh round did not offer the approved output: %v", ids)
	}
	if code, _ := do(s.handleAccountCollatedAck, http.MethodPost, handoffURL, handoffAck, nil); code != 200 {
		t.Fatalf("fresh round readback code=%d", code)
	}

	if _, err := pool.Exec(ctx, `
 WITH m(body) AS (VALUES ('[[106,"fixture",[1000,2,3,44,5]]]'::jsonb))
 INSERT INTO delivery_nas_handoff_checks(roster_sha256,round,manifest,manifest_sha256)
 SELECT $1,1,body,encode(sha256(convert_to(body::text,'UTF8')),'hex') FROM m`, sha); err != nil {
		t.Fatal(err)
	}
	code, pathCheck := do(s.handleAccountDeliveryNASHandoffCheck, http.MethodGet, deliveryNASHandoffCheckPath, nil, nil)
	if code != 200 || pathCheck["check"] == nil {
		t.Fatalf("handoff check challenge code=%d result=%v", code, pathCheck)
	}
	challenge := pathCheck["check"].(map[string]any)
	checkStatusPath := fmt.Sprintf("%s?check_id=%d", deliveryNASHandoffCheckPath, int64(challenge["id"].(float64)))
	expectCheckStatus := func(want string) {
		t.Helper()
		code, result := do(s.handleAccountDeliveryNASHandoffCheck, http.MethodGet, checkStatusPath, nil, nil)
		if code != http.StatusOK || result["status"] != want {
			t.Fatalf("handoff check status code=%d result=%v want=%s", code, result, want)
		}
	}
	expectCheckStatus("pending")
	_, freezePage := do(s.handleAccountCollated, http.MethodGet, "/api/v1/account/collated?handoff_readback=1", nil, nil)
	if freezePage["policy"].(map[string]any)["handoff_check"] != true {
		t.Fatalf("active check did not request a client drain: %v", freezePage)
	}
	if code, _ := do(s.handleAccountDeliveryNASHandoffCheck, http.MethodPost, deliveryNASHandoffCheckPath,
		map[string]any{"id": challenge["id"], "manifest_sha256": challenge["manifest_sha256"], "mount": []int64{5, 100}}, nil); code != 409 {
		t.Fatalf("unapproved mount accepted at check completion: %d", code)
	}
	payload := map[string]any{"id": challenge["id"], "manifest_sha256": challenge["manifest_sha256"], "mount": []int64{5, 99}}
	for range 2 {
		if code, _ := do(s.handleAccountDeliveryNASHandoffCheck, http.MethodPost, deliveryNASHandoffCheckPath, payload, nil); code != 200 {
			t.Fatalf("path check completion or replay code=%d", code)
		}
	}
	expectCheckStatus("accepted")
	if code, result := do(s.handleAccountDeliveryNASHandoffCheck, http.MethodGet, deliveryNASHandoffCheckPath, nil, nil); code != 200 || result["check"] != nil {
		t.Fatalf("completed handoff check reissued code=%d result=%v", code, result)
	}

	if _, err := pool.Exec(ctx, "INSERT INTO delivery_nas_handoff_rounds(roster_sha256,round) VALUES($1,2)", sha); err != nil {
		t.Fatal(err)
	}
	expectCheckStatus("superseded")

	// Under the 20-day epoch an unheld output overlapping its stream's pinned
	// window is offered while fresh NAS free space clears the copy floor.
	baseline := fmt.Sprint(feedIDs())
	if _, err := pool.Exec(ctx, `UPDATE recordings SET stream_id=7 WHERE id=1;
 INSERT INTO nas_media_keep20_windows VALUES(7,'UTC','2026-07-26T00:00:00Z','2026-07-27T00:00:00Z',false)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_collation_outputs VALUES
  (107,12,1,'joined/b/objects/107.mp4',$1,1000,$3,'{5000}','2026-07-26T20:00:01Z','2026-07-26T21:00:01Z'),
  (108,12,1,'joined/b/objects/108.mp4',$2,1000,$3,'{5000}','2026-07-27T00:00:00Z','2026-07-27T01:00:00Z')`,
		dir+"20_200001-210001.mp4", dir+"21_000000-010000.mp4", sha); err != nil {
		t.Fatal(err)
	}
	withWindow := strings.Replace(baseline, "]", " 107]", 1)
	for _, step := range []struct{ sql, want string }{
		// A closed connection-wide gate offers nothing (not even holds) and
		// resets the cursor, so withheld window outputs are never skipped.
		{`CREATE OR REPLACE FUNCTION nas_delivery_work_epoch(connection bigint) RETURNS boolean LANGUAGE sql STABLE AS $$SELECT false$$`, "[]"},
		{`CREATE OR REPLACE FUNCTION nas_delivery_work_epoch(connection bigint) RETURNS boolean LANGUAGE sql STABLE AS $$SELECT true$$`, withWindow},
		{`UPDATE connections SET nas_storage_free_bytes=$1::bigint-1 WHERE id=13`, "[]"},
		{`UPDATE connections SET nas_storage_free_bytes=$1::bigint,nas_storage_reported_at=now()-interval '6 minutes' WHERE id=13`, "[]"},
		{`UPDATE connections SET nas_storage_reported_at=now() WHERE id=13`, withWindow},
	} {
		args := []any{}
		if strings.Contains(step.sql, "$1") {
			args = append(args, collatedKeep20FreeBytes)
		}
		if _, err := pool.Exec(ctx, step.sql, args...); err != nil {
			t.Fatal(err)
		}
		if ids := fmt.Sprint(feedIDs()); ids != step.want {
			t.Fatalf("after %q feed=%s want %s", step.sql, ids, step.want)
		}
		if step.want == "[]" {
			if code, page := do(s.handleAccountCollated, http.MethodGet, "/api/v1/account/collated?after_id=50&handoff_readback=1", nil, nil); code != 200 || page["next_after_id"] != float64(0) {
				t.Fatalf("closed gate advanced the cursor: %d %v", code, page)
			}
		}
	}
	// The ACK is the R2 lane's NAS evidence: exact contract path and size.
	windowAck := map[string]any{"output_id": 107, "nas_relative_path": dir + "20_200001-210001.mp4", "size_bytes": 1000, "sha256": sha}
	if code, _ := do(s.handleAccountCollatedAck, http.MethodPost, "/x", windowAck, nil); code != 200 {
		t.Fatalf("window ACK code=%d", code)
	}
	var acked bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_collation_output_acks a JOIN recording_collation_outputs o ON o.id=a.output_id
 WHERE a.output_id=107 AND a.connection_id=13 AND a.nas_relative_path=o.nas_relative_path AND a.size_bytes=o.size_bytes)`).Scan(&acked); err != nil || !acked {
		t.Fatalf("window ACK evidence=%t: %v", acked, err)
	}
	if ids := fmt.Sprint(feedIDs()); ids != baseline {
		t.Fatalf("acknowledged window output still offered: %s", ids)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM recording_collation_outputs WHERE id=108`); err != nil {
		t.Fatal(err)
	}

	// A prefix longer than the scan page is withheld, but the cursor must reach
	// the later deliverable output. Duplicated sources cannot duplicate outputs.
	prefixEnd := int64(1000 + collatedFeedScanLimit)
	if _, err := pool.Exec(ctx, `INSERT INTO recording_collation_outputs
 SELECT g,12,1,'object-'||g,'invalid-'||g,1000,repeat('a',64),ARRAY[5000::bigint]
 FROM generate_series(1000,$1::bigint)g`, prefixEnd); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_collation_outputs VALUES($2,12,1,'object-deliverable',$1,1000,repeat('a',64),ARRAY[101,101]::bigint[])`, dir+"18_180001-190001.mp4", prefixEnd+1); err != nil {
		t.Fatal(err)
	}
	// Production statistics are current; a fresh 20k-row insert without them
	// lets the planner pick a scan that overruns the feed's 2 s budget on slow CI.
	if _, err := pool.Exec(ctx, `ANALYZE recording_collation_outputs`); err != nil {
		t.Fatal(err)
	}
	// Preserve the pre-rewrite ANY predicate as a result-set oracle; compare all
	// pages at limit=1 so advancing to the candidate maximum would lose items.
	oracleSQL := strings.Replace(collatedEligibleSQL,
		"JOIN unnest(o.source_clip_ids) source_id ON source_id=hold.clip_id WHERE hold.mode=",
		"WHERE hold.clip_id=ANY(o.source_clip_ids) AND hold.mode=", 1)
	rows, err := pool.Query(ctx, `SELECT o.id `+oracleSQL+collatedNotBackedOffSQL+` ORDER BY o.id`, int64(13), false)
	if err != nil {
		t.Fatal(err)
	}
	var want []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		want = append(want, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	var got []int64
	after := int64(0)
	emptyAdvanced := false
	for n := 0; n < 20; n++ {
		code, page := do(s.handleAccountCollated, http.MethodGet, fmt.Sprintf("/api/v1/account/collated?limit=1&after_id=%d", after), nil, nil)
		if code != 200 {
			t.Fatalf("page %d: %v", code, page)
		}
		next := int64(page["next_after_id"].(float64))
		for _, raw := range page["items"].([]any) {
			got = append(got, int64(raw.(map[string]any)["output_id"].(float64)))
		}
		if next == 0 {
			break
		}
		if next <= after {
			t.Fatalf("non-forward cursor %d -> %d", after, next)
		}
		if len(page["items"].([]any)) == 0 {
			emptyAdvanced = true
		}
		after = next
	}
	if fmt.Sprint(got) != fmt.Sprint(want) || !emptyAdvanced {
		t.Fatalf("paged=%v oracle=%v advanced empty=%t", got, want, emptyAdvanced)
	}

	// A complete expensive-stage batch rejected by the fence must advance only
	// to that batch's end; later held outputs within the candidate page survive.
	fenceStart, fenceLimit := prefixEnd+1000, int64(collatedFeedFenceLimit)
	lastHeld := fenceStart + 2*fenceLimit
	if _, err := pool.Exec(ctx, `INSERT INTO recording_collation_outputs
 SELECT g,12,1,'held-object-'||g,'held-'||g,1000,repeat('a',64),ARRAY[101::bigint]
 FROM generate_series($1::bigint,$2::bigint)g`, fenceStart, lastHeld); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE OR REPLACE FUNCTION nas_delivery_allowed(connection bigint,kind text,source bigint) RETURNS boolean LANGUAGE sql STABLE AS $$SELECT source=%d$$`, lastHeld)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_collation_outputs SET nas_relative_path=$1 WHERE id=$2`, dir+"19_190001-200001.mp4", lastHeld); err != nil {
		t.Fatal(err)
	}
	after = fenceStart - 1
	for _, wantNext := range []int64{fenceStart - 1 + fenceLimit, fenceStart - 1 + 2*fenceLimit, lastHeld} {
		code, page := do(s.handleAccountCollated, http.MethodGet, fmt.Sprintf("/api/v1/account/collated?after_id=%d", after), nil, nil)
		if code != 200 || int64(page["next_after_id"].(float64)) != wantNext {
			t.Fatalf("fence page=%v status=%d", page, code)
		}
		if wantNext == lastHeld && len(page["items"].([]any)) != 1 {
			t.Fatalf("later fenced-page output lost: %v", page)
		}
		after = wantNext
	}
	// PostgreSQL, not only the Go context, cancels slow eligibility work. The
	// connection remains usable and the outer 3s budget has time to return 503.
	if _, err := pool.Exec(ctx, `CREATE OR REPLACE FUNCTION nas_delivery_allowed(connection bigint,kind text,source bigint) RETURNS boolean LANGUAGE plpgsql STABLE AS $$BEGIN PERFORM pg_sleep(4);RETURN true;END$$`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if code, _ := do(s.handleAccountCollated, http.MethodGet, "/api/v1/account/collated?after_id=0", nil, nil); code != 503 {
		t.Fatalf("slow feed status %d", code)
	}
	if elapsed := time.Since(started); elapsed >= collatedFeedBudget {
		t.Fatalf("feed exhausted HTTP rather than statement budget: %v", elapsed)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM recording_collation_output_acks`); err == nil {
		t.Fatal("ack rows are not append-only")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM connections WHERE id=13`); err != nil {
		t.Fatalf("connection removal blocked by its acks: %v", err)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_collation_output_acks`).Scan(&left); err != nil || left != 0 {
		t.Fatalf("acks left after connection removal=%d err=%v", left, err)
	}
}
