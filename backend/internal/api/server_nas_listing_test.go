package api

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestNASListingCompleteBoundedAndResumable(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `ALTER TABLE recording_clips ADD COLUMN released_at timestamptz;CREATE TABLE nas_inventory_files(id bigint);CREATE TABLE nas_inventory_unmatched_files(id bigint);CREATE TABLE recording_collation_output_acks(id bigint)`)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../../infra/sql/migrations/0183_nas_listing_proof.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	g := "abcdefabcdefabcdefabcdefabcdefab"
	send := func(action, folder string, page int, files []nasListingFile, count, size int64) int {
		t.Helper()
		w := cleanupCall(t, s.handleAccountConnectionListing, 47, 5, "", "", nasListingRequest{Action: nasListingAction(action), Generation: g, Folders: []string{".", "_V85r_keep28", "joined"}, Folder: folder, Page: page, Files: files, Count: count, Bytes: size})
		return w.Code
	}
	if code := send("start", "", 0, nil, 0, 0); code != 200 {
		t.Fatalf("start %d", code)
	}
	if code := send("complete", "", 0, nil, 0, 0); code != 409 {
		t.Fatal("partial generation completed")
	}
	f := []nasListingFile{{Path: "joined/a.mp4", Size: 1, MtimeNS: 2, Inode: 3, Device: 4}}
	if code := send("page", "joined", 1, f, 0, 0); code != 409 {
		t.Fatal("page gap accepted")
	}
	if code := send("page", "joined", 0, f, 0, 0); code != 200 {
		t.Fatalf("page %d", code)
	}
	if code := send("page", "joined", 0, f, 0, 0); code != 200 {
		t.Fatal("identical page replay rejected")
	}
	f[0].Size = 2
	if code := send("page", "joined", 0, f, 0, 0); code != 409 {
		t.Fatal("changed replay accepted")
	}
	if code := send("folder_complete", "joined", 1, nil, 1, 2); code != 409 {
		t.Fatal("wrong count accepted")
	}
	if code := send("folder_complete", "joined", 1, nil, 1, 1); code != 200 {
		t.Fatal("folder completion failed")
	}
	for _, folder := range []string{".", "_V85r_keep28"} {
		if send("folder_complete", folder, 0, nil, 0, 0) != 200 {
			t.Fatal("empty folder incomplete")
		}
	}
	if send("complete", "", 0, nil, 0, 0) != 200 || send("complete", "", 0, nil, 0, 0) != 200 {
		t.Fatal("completion/retry failed")
	}
	// A publisher waiting on the public fence must not already hold the
	// inventory row that retirement is about to lock (the former inversion).
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files VALUES(1)`); err != nil {
		t.Fatal(err)
	}
	retire, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer retire.Rollback(ctx)
	if err := LockNASCleanupR2Retirement(ctx, retire); err != nil {
		t.Fatal(err)
	}
	publish, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer publish.Rollback(ctx)
	var publisher int
	if err := publish.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&publisher); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := publish.Exec(ctx, `UPDATE nas_inventory_files SET id=id WHERE id=1`); done <- err }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var waiting bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND NOT granted)`, publisher).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("publisher did not reach public fence")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := retire.Exec(ctx, `SET LOCAL lock_timeout='100ms';SELECT id FROM nas_inventory_files WHERE id=1 FOR UPDATE`); err != nil {
		t.Fatal("publisher inverted row/public-fence lock order", err)
	}
	if err := retire.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := publish.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var enabled bool
	if err := pool.QueryRow(ctx, `SELECT nas_listing_enabled FROM connections WHERE id=13`).Scan(&enabled); err != nil || enabled {
		t.Fatal("completed listing did not disarm", err)
	}
	if send("start", "", 0, nil, 0, 0) != 200 {
		t.Fatal("lost completion response cannot resume")
	}
	var complete bool
	if err := pool.QueryRow(ctx, `SELECT completed_at>=started_at FROM nas_listing_generations WHERE connection_id=13 AND generation=$1`, g).Scan(&complete); err != nil || !complete {
		t.Fatal("server completion marker missing", err)
	}
	w := cleanupCall(t, s.handleAccountConnectionListing, 48, 6, "", "", nasListingRequest{Action: nasListingStart, Generation: g, Folders: []string{"."}})
	if w.Code != 403 {
		t.Fatal("unreviewed connection enabled")
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_listing_enabled=false WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	g = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if send("start", "", 0, nil, 0, 0) != 403 {
		t.Fatal("server OFF ignored")
	}
}
func TestNASListingPathValidation(t *testing.T) {
	for _, p := range []string{"../a", "/a", "a/../b", "a//b", "a\\b", "a\x00b"} {
		if validateNASListing(nasListingRequest{Action: nasListingPage, Generation: "abcdefabcdefabcdefabcdefabcdefab", Folder: "a", Files: []nasListingFile{{Path: p}}}) == nil {
			t.Fatalf("unsafe path %q", p)
		}
	}
	if !pullPathAllowed("POST", "/api/v1/account/connections/listing") || pullPathAllowed("GET", "/api/v1/account/connections/listing") {
		t.Fatal("listing scope wrong")
	}
}
