package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/nasdelivery"
)

// TestNASCollatedHoldSkipsRawFeedAndBacklog proves a held clip never reaches the
// NAS raw feed, never counts as NAS pending work, and does not block the
// cursor from advancing past it to later deliverable clips.
func TestNASCollatedHoldSkipsRawFeedAndBacklog(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	const accountID = int64(47)
	var recordingID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,name,delivery) VALUES($1,'plaza','nas_pull') RETURNING id`, accountID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	insert := func(held bool) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,size_bytes,clip_start_at,clip_end_at) VALUES($1,100,$2,$3) RETURNING id`,
			recordingID, start, start.Add(time.Minute)).Scan(&id); err != nil {
			t.Fatal(err)
		}
		mode := "nas_pull_monthly"
		if held {
			mode = nasdelivery.HoldContractMode
		}
		if _, err := pool.Exec(ctx, `INSERT INTO clip_storage_billing_contracts(clip_id,mode) VALUES($1,$2)`, id, mode); err != nil {
			t.Fatal(err)
		}
		return id
	}
	raw1 := insert(false)
	held1 := insert(true)
	held2 := insert(true)
	raw2 := insert(false)

	page := getAccountClips(t, pool, accountID, 0, 100)
	if got := clipIDs(page.Clips); !equalInt64(got, []int64{raw1, raw2}) {
		t.Fatalf("raw feed = %v, want %v (held %d,%d excluded)", got, []int64{raw1, raw2}, held1, held2)
	}
	page = getAccountClips(t, pool, accountID, raw1, 1)
	if got := clipIDs(page.Clips); !equalInt64(got, []int64{raw2}) {
		t.Fatalf("cursor did not skip held clips: %v", got)
	}

	var connectionID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind) VALUES($1,'nas_pull') RETURNING id`, accountID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	var pending int64
	if err := pool.QueryRow(ctx, `SELECT pending.clips FROM connections conn `+connectionPendingLateralSQL+` WHERE conn.id=$1`, connectionID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 2 {
		t.Fatalf("pending clips = %d, want 2 raw clips (held clips are not NAS backlog)", pending)
	}
}

func TestAdminRecordingNASDeliveryModeFencesAndReportsHolds(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `ALTER TABLE recordings
		ADD COLUMN IF NOT EXISTS storage_retention_tier TEXT NOT NULL DEFAULT 'monthly',
		ADD COLUMN IF NOT EXISTS nas_delivery_mode TEXT NOT NULL DEFAULT 'raw',
		ADD COLUMN IF NOT EXISTS nas_delivery_mode_updated_at TIMESTAMPTZ,
		ADD COLUMN IF NOT EXISTS nas_delivery_mode_reason TEXT NOT NULL DEFAULT '',
		ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`); err != nil {
		t.Fatal(err)
	}
	ids := map[string]int64{}
	for _, row := range []struct {
		key, delivery, tier string
		account             int64
	}{
		{"a", "nas_pull", "monthly", 47}, {"b", "nas_pull", "monthly", 47}, {"managed", "managed", "monthly", 47},
		{"yearly", "nas_pull", "yearly_prepaid", 47}, {"foreign", "nas_pull", "monthly", 48},
	} {
		var id int64
		if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,name,delivery,storage_retention_tier) VALUES($1,$2,$3,$4) RETURNING id`,
			row.account, row.key, row.delivery, row.tier).Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids[row.key] = id
	}
	s := &Server{pool: pool}
	call := func(body map[string]any) (int, map[string]any) {
		t.Helper()
		raw, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		s.handleAdminRecordingNASDeliveryMode(rec, httptest.NewRequest(http.MethodPost, "/api/v1/recordings/nas-delivery-mode", bytes.NewReader(raw)))
		var out map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}
	for name, ids := range map[string][]int64{
		"managed": {ids["a"], ids["managed"]}, "yearly": {ids["yearly"]}, "foreign": {ids["foreign"]}, "missing": {ids["a"], 999999},
	} {
		if code, out := call(map[string]any{"account_id": 47, "recording_ids": ids, "mode": "collated_only", "reason": "test"}); code != http.StatusConflict {
			t.Fatalf("%s: code=%d out=%v, want 409", name, code, out)
		}
	}
	if code, _ := call(map[string]any{"account_id": 47, "recording_ids": []int64{ids["a"]}, "mode": "collated_only"}); code != http.StatusBadRequest {
		t.Fatalf("missing reason code=%d", code)
	}
	body := map[string]any{"account_id": 47, "recording_ids": []int64{ids["b"], ids["a"]}, "mode": "collated_only", "reason": "60-stream collated rollout", "dry_run": true}
	if code, out := call(body); code != http.StatusOK || out["dry_run"] != true {
		t.Fatalf("dry run code=%d out=%v", code, out)
	}
	var changed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recordings WHERE nas_delivery_mode<>'raw'`).Scan(&changed); err != nil || changed != 0 {
		t.Fatalf("dry run changed %d recordings err=%v", changed, err)
	}
	body["dry_run"] = false
	code, out := call(body)
	if code != http.StatusOK {
		t.Fatalf("apply code=%d out=%v", code, out)
	}
	items := out["items"].([]any)
	if len(items) != 2 || items[0].(map[string]any)["previous_mode"] != "raw" || items[0].(map[string]any)["mode"] != "collated_only" {
		t.Fatalf("apply items=%v", items)
	}
	var clipID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,size_bytes,clip_start_at,clip_end_at) VALUES($1,700,now(),now()) RETURNING id`, ids["a"]).Scan(&clipID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO clip_storage_billing_contracts(clip_id,mode) VALUES($1,$2)`, clipID, nasdelivery.HoldContractMode); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	s.handleAdminRecordingNASDeliveryStatus(rec, httptest.NewRequest(http.MethodGet, "/api/v1/recordings/nas-delivery-mode?account_id=47", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"held_clips":1`) || !strings.Contains(rec.Body.String(), `"held_bytes":700`) {
		t.Fatalf("status code=%d body=%s", rec.Code, rec.Body.String())
	}
}
