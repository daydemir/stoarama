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
	}
	for _, p := range good {
		if !validCollatedNASPath(p) {
			t.Fatalf("rejected %s", p)
		}
	}
	for _, p := range bad {
		if validCollatedNASPath(p) {
			t.Fatalf("accepted %s", p)
		}
	}
}

func TestCollatedDeliveryFeedDownloadAckAndPolicy(t *testing.T) {
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
		CREATE TABLE connections(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,kind TEXT NOT NULL,api_key_id BIGINT,updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
		CREATE TABLE recordings(id BIGINT PRIMARY KEY,account_id BIGINT NOT NULL,delivery TEXT NOT NULL);
		CREATE TABLE recording_collation_hours(id BIGINT PRIMARY KEY,recording_id BIGINT NOT NULL,local_date DATE NOT NULL,delivery_hour SMALLINT NOT NULL,generation INTEGER NOT NULL);
		CREATE TABLE recording_collation_outputs(id BIGINT PRIMARY KEY,collation_hour_id BIGINT NOT NULL REFERENCES recording_collation_hours(id),part INTEGER NOT NULL,
		  object_key TEXT NOT NULL,nas_relative_path TEXT NOT NULL UNIQUE,size_bytes BIGINT NOT NULL,sha256 TEXT NOT NULL);
		INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',5),(14,48,'nas_pull',6);
		INSERT INTO recordings VALUES(1,47,'nas_pull'),(2,47,'managed'),(3,48,'nas_pull');
		INSERT INTO recording_collation_hours VALUES(10,1,'2026-07-26',6,2),(11,1,'2026-07-26',6,3),(12,1,'2026-07-26',7,2),(13,2,'2026-07-26',6,2),(14,3,'2026-07-26',6,2);
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
	} {
		if _, err := pool.Exec(ctx, `INSERT INTO recording_collation_outputs VALUES($1,$2,1,$3,$4,1000,$5)`, o.id, o.hour, fmt.Sprintf("joined/b/objects/%d.mp4", o.id), o.rel, sha); err != nil {
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
		code, out := do(s.handleAccountCollated, http.MethodGet, "/api/v1/account/collated?limit=10", nil, nil)
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
	for _, id := range []string{"100", "104", "105"} {
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
	if code, _ := do(s.handleAccountCollatedError, http.MethodPost, "/x", map[string]any{"output_id": 102, "error": "boom"}, nil); code != 200 {
		t.Fatalf("error report code=%d", code)
	}
	if _, out := adminDo(http.MethodGet, nil); out["last_error"] != "boom" || out["files_pulled"].(float64) != 1 || out["pending_files"].(float64) != 2 {
		t.Fatalf("status after ack=%v", out)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM recording_collation_output_acks`); err == nil {
		t.Fatal("ack rows are not append-only")
	}
}
