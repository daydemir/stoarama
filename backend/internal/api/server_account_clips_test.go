package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestAccountClipsCursorSQLShape pins the cursor query's load-bearing predicates
// (account scope, unpurged-only, forward cursor, ascending monotonic order) so a
// refactor cannot silently widen the scope or break the NAS pull client's
// drain-once-and-resume contract.
func TestAccountClipsCursorSQLShape(t *testing.T) {
	for _, want := range []string{
		"r.account_id = $1",
		"c.purged_at IS NULL",
		"c.id > $2",
		"ORDER BY c.id ASC",
		"LIMIT $3",
	} {
		if !strings.Contains(accountClipsCursorSQL, want) {
			t.Fatalf("account clips cursor SQL missing %q", want)
		}
	}
}

// TestAccountClipsCursorEndpoint exercises the live handler against Postgres:
// (a) a foreign account sees zero of this account's clips, (b) purged clips are
// excluded, and (c) the after_id cursor filters to id>after_id in ascending id
// order with next_after_id set to the page's max id.
func TestAccountClipsCursorEndpoint(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()

	ctx := context.Background()

	const (
		ownerAccountID   = int64(42)
		foreignAccountID = int64(99)
	)

	// One recording per account.
	var ownerRecID, foreignRecID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recordings (account_id, name) VALUES ($1, 'owner-rec') RETURNING id
	`, ownerAccountID).Scan(&ownerRecID); err != nil {
		t.Fatalf("insert owner recording: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO recordings (account_id, name) VALUES ($1, 'foreign-rec') RETURNING id
	`, foreignAccountID).Scan(&foreignRecID); err != nil {
		t.Fatalf("insert foreign recording: %v", err)
	}

	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	insertClip := func(recordingID int64, sizeBytes int64, purged bool) int64 {
		t.Helper()
		var id int64
		var purgedAt any
		if purged {
			purgedAt = start
		}
		if err := pool.QueryRow(ctx, `
			INSERT INTO recording_clips (recording_id, size_bytes, clip_start_at, clip_end_at, purged_at)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id
		`, recordingID, sizeBytes, start, start.Add(90*time.Second), purgedAt).Scan(&id); err != nil {
			t.Fatalf("insert clip: %v", err)
		}
		return id
	}

	// Owner: three live clips and one purged clip (ascending ids by insert order).
	live1 := insertClip(ownerRecID, 111, false)
	live2 := insertClip(ownerRecID, 222, false)
	purged := insertClip(ownerRecID, 333, true)
	live3 := insertClip(ownerRecID, 444, false)
	// Foreign account clip (must never appear for the owner).
	insertClip(foreignRecID, 555, false)

	// (a)+(b) Owner draining from cursor 0 sees only its own live clips, ascending,
	// purged excluded, foreign excluded.
	page := getAccountClips(t, pool, ownerAccountID, 0, 100)
	gotIDs := clipIDs(page.Clips)
	wantIDs := []int64{live1, live2, live3}
	if !equalInt64(gotIDs, wantIDs) {
		t.Fatalf("page1 clip ids = %v, want %v (purged %d and foreign account must be excluded)", gotIDs, wantIDs, purged)
	}
	if page.NextAfterID == nil || *page.NextAfterID != live3 {
		t.Fatalf("page1 next_after_id = %v, want %d", page.NextAfterID, live3)
	}
	// object_key must never leak.
	for _, c := range page.Clips {
		if _, leaked := c["object_key"]; leaked {
			t.Fatalf("clip exposed object_key: %v", c)
		}
		if c["download_path"] != fmt.Sprintf("/api/v1/account/recordings/%v/clips/%v/download", c["recording_id"], c["clip_id"]) {
			t.Fatalf("unexpected download_path: %v", c["download_path"])
		}
	}

	// (c) Cursor monotonic: after_id=live1 filters to id>live1 (drops live1, keeps
	// live2 and live3; purged still excluded).
	page2 := getAccountClips(t, pool, ownerAccountID, live1, 100)
	if got := clipIDs(page2.Clips); !equalInt64(got, []int64{live2, live3}) {
		t.Fatalf("page2 (after_id=%d) clip ids = %v, want %v", live1, got, []int64{live2, live3})
	}

	// (a) Foreign account draining from 0 sees ONLY its own clip, none of owner's.
	foreignPage := getAccountClips(t, pool, foreignAccountID, 0, 100)
	for _, c := range foreignPage.Clips {
		if int64(c["recording_id"].(float64)) == ownerRecID {
			t.Fatalf("foreign account saw an owner-account clip: %v", c)
		}
	}
	if len(foreignPage.Clips) != 1 {
		t.Fatalf("foreign account page = %d clips, want exactly its own 1", len(foreignPage.Clips))
	}

	// Empty page (cursor past the last id) yields zero clips and null next_after_id.
	empty := getAccountClips(t, pool, ownerAccountID, live3, 100)
	if len(empty.Clips) != 0 {
		t.Fatalf("page past last id = %d clips, want 0", len(empty.Clips))
	}
	if empty.NextAfterID != nil {
		t.Fatalf("empty page next_after_id = %v, want null", *empty.NextAfterID)
	}
}

type accountClipsPage struct {
	Clips       []map[string]any `json:"clips"`
	NextAfterID *int64           `json:"next_after_id"`
}

func getAccountClips(t *testing.T, pool *pgxpool.Pool, accountID, afterID int64, limit int) accountClipsPage {
	t.Helper()

	s := &Server{pool: pool}
	url := fmt.Sprintf("/api/v1/account/clips?after_id=%d&limit=%d", afterID, limit)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: accountID}))
	rec := httptest.NewRecorder()

	s.handleAccountClips(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("account clips status=%d body=%s", rec.Code, rec.Body.String())
	}
	var page accountClipsPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode account clips response: %v", err)
	}
	return page
}

func clipIDs(clips []map[string]any) []int64 {
	ids := make([]int64, 0, len(clips))
	for _, c := range clips {
		ids = append(ids, int64(c["clip_id"].(float64)))
	}
	return ids
}

func equalInt64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func testAccountClipsPool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()

	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed account clips regression")
	}

	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}

	schema := fmt.Sprintf("api_account_clips_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema)); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("parse db url: %v", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("open test pool: %v", err)
	}

	for _, stmt := range []string{
		`CREATE TABLE recordings (
			id BIGSERIAL PRIMARY KEY,
			account_id BIGINT NOT NULL,
			name TEXT NOT NULL
		)`,
		`CREATE TABLE recording_clips (
			id BIGSERIAL PRIMARY KEY,
			recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
			size_bytes BIGINT NOT NULL,
			clip_start_at TIMESTAMPTZ NOT NULL,
			clip_end_at TIMESTAMPTZ NOT NULL,
			purged_at TIMESTAMPTZ
		)`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			pool.Close()
			_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
			admin.Close()
			t.Fatalf("create test table: %v", err)
		}
	}

	return pool, func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
	}
}
