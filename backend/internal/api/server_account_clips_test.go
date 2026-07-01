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
		"c.released_at IS NULL",
		"r.delivery = 'nas_pull'",
		"c.created_at < now() - ",
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

	// One recording per account. Both are nas_pull: the pull feed only hands out
	// nas_pull recordings' clips (a managed recording's clips are never released).
	var ownerRecID, foreignRecID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recordings (account_id, name, delivery) VALUES ($1, 'owner-rec', 'nas_pull') RETURNING id
	`, ownerAccountID).Scan(&ownerRecID); err != nil {
		t.Fatalf("insert owner recording: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO recordings (account_id, name, delivery) VALUES ($1, 'foreign-rec', 'nas_pull') RETURNING id
	`, foreignAccountID).Scan(&foreignRecID); err != nil {
		t.Fatalf("insert foreign recording: %v", err)
	}

	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	insertClip := func(recordingID int64, sizeBytes int64, purged, released bool) int64 {
		t.Helper()
		var id int64
		var purgedAt, releasedAt any
		if purged {
			purgedAt = start
		}
		if released {
			releasedAt = start
		}
		if err := pool.QueryRow(ctx, `
			INSERT INTO recording_clips (recording_id, size_bytes, clip_start_at, clip_end_at, purged_at, released_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING id
		`, recordingID, sizeBytes, start, start.Add(90*time.Second), purgedAt, releasedAt).Scan(&id); err != nil {
			t.Fatalf("insert clip: %v", err)
		}
		return id
	}

	// Owner: three live clips, one purged clip, and one released clip (ascending ids
	// by insert order). Both purged and released must be excluded from the pull feed.
	live1 := insertClip(ownerRecID, 111, false, false)
	live2 := insertClip(ownerRecID, 222, false, false)
	purged := insertClip(ownerRecID, 333, true, false)
	released := insertClip(ownerRecID, 666, false, true)
	live3 := insertClip(ownerRecID, 444, false, false)
	// Foreign account clip (must never appear for the owner).
	insertClip(foreignRecID, 555, false, false)

	// (a)+(b) Owner draining from cursor 0 sees only its own live clips, ascending,
	// purged + released excluded, foreign excluded.
	page := getAccountClips(t, pool, ownerAccountID, 0, 100)
	gotIDs := clipIDs(page.Clips)
	wantIDs := []int64{live1, live2, live3}
	if !equalInt64(gotIDs, wantIDs) {
		t.Fatalf("page1 clip ids = %v, want %v (purged %d, released %d and foreign account must be excluded)", gotIDs, wantIDs, purged, released)
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

// TestReleaseClipKeepsRowAndDetaches exercises releaseClip against Postgres: a
// release stamps released_at, KEEPS the row (never deletes it), leaves purged_at
// NULL (the R2 object is retained, not purged), and removes the clip from the org
// pull feed. A second release is idempotent. A foreign account cannot release
// another account's clip. No r2 client is constructed anywhere in the path.
func TestReleaseClipKeepsRowAndDetaches(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()

	ctx := context.Background()
	const (
		ownerAccountID   = int64(42)
		foreignAccountID = int64(99)
	)

	var recID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recordings (account_id, name, delivery) VALUES ($1, 'owner-rec', 'nas_pull') RETURNING id
	`, ownerAccountID).Scan(&recID); err != nil {
		t.Fatalf("insert recording: %v", err)
	}
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	var clipID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recording_clips (recording_id, size_bytes, clip_start_at, clip_end_at)
		VALUES ($1, 999, $2, $3) RETURNING id
	`, recID, start, start.Add(90*time.Second)).Scan(&clipID); err != nil {
		t.Fatalf("insert clip: %v", err)
	}

	s := &Server{pool: pool}

	// A foreign account cannot release the owner's clip (found=false, no mutation).
	if found, _, err := s.releaseClip(ctx, foreignAccountID, recID, clipID, true); err != nil || found {
		t.Fatalf("foreign release: found=%v err=%v, want found=false err=nil", found, err)
	}

	// Owner release: found, not-already-released, row kept, released_at set, purged_at NULL.
	found, already, err := s.releaseClip(ctx, ownerAccountID, recID, clipID, true)
	if err != nil || !found || already {
		t.Fatalf("owner release: found=%v already=%v err=%v, want found=true already=false err=nil", found, already, err)
	}
	var (
		exists     bool
		releasedAt *time.Time
		purgedAt   *time.Time
	)
	if err := pool.QueryRow(ctx, `
		SELECT true, released_at, purged_at FROM recording_clips WHERE id=$1
	`, clipID).Scan(&exists, &releasedAt, &purgedAt); err != nil {
		t.Fatalf("row must still exist after release: %v", err)
	}
	if !exists || releasedAt == nil {
		t.Fatalf("after release: exists=%v released_at=%v, want row kept with released_at set", exists, releasedAt)
	}
	if purgedAt != nil {
		t.Fatalf("release must NOT purge: purged_at=%v, want NULL (R2 object retained)", purgedAt)
	}

	// The released clip disappears from the org pull feed.
	page := getAccountClips(t, pool, ownerAccountID, 0, 100)
	if len(page.Clips) != 0 {
		t.Fatalf("released clip still in pull feed: %d clips, want 0", len(page.Clips))
	}

	// Idempotent: a second release reports already-released and does not error.
	found2, already2, err := s.releaseClip(ctx, ownerAccountID, recID, clipID, true)
	if err != nil || !found2 || !already2 {
		t.Fatalf("second release: found=%v already=%v err=%v, want found=true already=true err=nil", found2, already2, err)
	}
}

// TestReleaseClipRequireNASPullSkipsManaged pins that releaseClip with
// requireNASPull=true never touches a delivery='managed' clip: the delivery
// predicate filters the row out so found=false and released_at stays NULL. Only a
// nas_pull recording's clips are ever release-eligible via the NAS-pull path, so a
// managed recording's clip can never be silently released through it.
func TestReleaseClipRequireNASPullSkipsManaged(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()

	ctx := context.Background()
	const ownerAccountID = int64(42)

	// A MANAGED recording (delivery defaults to 'managed'): its clip must never be
	// release-eligible via the NAS-pull path.
	var recID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recordings (account_id, name) VALUES ($1, 'managed-rec') RETURNING id
	`, ownerAccountID).Scan(&recID); err != nil {
		t.Fatalf("insert recording: %v", err)
	}
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	var clipID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recording_clips (recording_id, size_bytes, clip_start_at, clip_end_at)
		VALUES ($1, 999, $2, $3) RETURNING id
	`, recID, start, start.Add(90*time.Second)).Scan(&clipID); err != nil {
		t.Fatalf("insert clip: %v", err)
	}

	s := &Server{pool: pool}

	// requireNASPull=true against a managed clip: the delivery predicate excludes the
	// row, so found=false and nothing is mutated.
	found, already, err := s.releaseClip(ctx, ownerAccountID, recID, clipID, true)
	if err != nil || found || already {
		t.Fatalf("nas-pull release of managed clip: found=%v already=%v err=%v, want found=false already=false err=nil", found, already, err)
	}
	var releasedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT released_at FROM recording_clips WHERE id=$1`, clipID).Scan(&releasedAt); err != nil {
		t.Fatalf("read released_at: %v", err)
	}
	if releasedAt != nil {
		t.Fatalf("managed clip released_at=%v after nas-pull release, want NULL (never released)", releasedAt)
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
			name TEXT NOT NULL,
			delivery TEXT NOT NULL DEFAULT 'managed'
		)`,
		`CREATE TABLE recording_clips (
			id BIGSERIAL PRIMARY KEY,
			recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
			size_bytes BIGINT NOT NULL,
			clip_start_at TIMESTAMPTZ NOT NULL,
			clip_end_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now() - interval '10 minutes',
			purged_at TIMESTAMPTZ,
			released_at TIMESTAMPTZ
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
