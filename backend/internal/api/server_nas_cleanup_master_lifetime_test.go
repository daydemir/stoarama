package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// cleanupMasterRowShareHeld reports whether this backend currently holds the
// master's FOR SHARE row lock. Savepoint rollback releases it, which is why a
// cached ON decision must never outlive the savepoint that produced it.
func cleanupMasterRowShareHeld(t *testing.T, tx pgx.Tx, ctx context.Context) bool {
	t.Helper()
	var held bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks
 WHERE locktype='relation' AND relation=to_regclass('nas_cleanup_master')
 AND mode='RowShareLock' AND granted AND pid=pg_backend_pid())`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	return held
}

// A batched observation runs each item in its own savepoint, and rolling that
// savepoint back releases every row lock taken after it — including the master's
// FOR SHARE lock. A replayed permit returns write=false, so its savepoint IS
// rolled back. The cached ON must therefore be bound to the lock's real
// lifetime: this test drives a real replay (an exact repeat of an observation
// that already issued a permit), flips the switch OFF in a separate committed
// transaction, and then requires the next item in the same transaction to see
// OFF. A cached ON that survived the rollback would mint a permit after OFF.
func TestNASCleanupBatchReplayCannotResurrectCachedMaster(t *testing.T) {
	f := newCleanupFixture(t, 24)
	f.useClientVersion(nasCleanupSerialGroupClientVersion)
	plan, sha, _ := f.admit("replay-cache", syntheticFiles(100, 4, 10))
	f.dryRun()
	setCleanupMaster(t, f.s, true)
	if code, body := f.enableBatch([]int64{plan}, []string{sha}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 120*time.Second)
	defer cancel()

	// One execution lease hands out a bounded group of this plan's items.
	requestID := strings.Repeat("d", 32)
	w := cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{
		"client_version": f.version, "protocol": 2, "reconciliation_protocol": nasCleanupReconciliationProtocol,
		"limit": 8, "execution_files": 2, "execution_bytes": int64(1) << 40,
		"request_id": requestID, "execution_batch": true}, "fixture-boot "+f.version)
	if w.Code != 200 {
		t.Fatalf("lease: %d %s", w.Code, w.Body.String())
	}
	var leased struct {
		Tasks []cleanupTask `json:"tasks"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &leased); err != nil || len(leased.Tasks) != 2 {
		t.Fatalf("group lease %v %s", err, w.Body.String())
	}
	first, second := leased.Tasks[0], leased.Tasks[1]
	if first.ApprovedProof == nil || second.ApprovedProof == nil {
		t.Fatalf("group lease withheld authority: %+v", leased.Tasks)
	}
	roster := []int64{first.ItemID, second.ItemID}

	// The exact observation the client sends, including its request id: a
	// repeat of these bytes is what the replay path matches.
	retry := cleanupObserve{
		ReconciliationProtocol: nasCleanupReconciliationProtocol,
		Attempt:                first.Attempt,
		PlanSHA:                first.PlanSHA,
		Outcome:                "match",
		Proof:                  fixtureProof(first.Item),
		ClientVersion:          f.version,
		RequestID:              requestID,
	}
	prefetched := f.s.cleanupPrefetchRecovery(ctx, 13, map[int64]string{first.ItemID: "", second.ItemID: ""})
	env := &cleanupObserveEnv{
		executionGroup: roster,
		session:        cleanupClientSession{boot: "fixture-boot", version: f.version},
		freshRequests:  map[int64]bool{first.ItemID: true, second.ItemID: true},
		prefetched:     prefetched,
	}
	// The first permit is issued and committed exactly as the client's first
	// observation does.
	firstTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = firstTx.Exec(ctx, `SELECT 1`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.cleanupRegisterObservation(ctx, requestID, first.ItemID); err != nil {
		t.Fatal(err)
	}
	issued := f.s.cleanupObserveOne(ctx, firstTx, 13, first.ItemID, retry, env)
	if issued.status != 200 || issued.body["delete"] != true || !issued.write {
		firstTx.Rollback(ctx)
		t.Fatalf("first observation did not issue a permit: %+v", issued)
	}
	if err = firstTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// The batch transaction: the replay's savepoint is rolled back (write=false),
	// so the FOR SHARE lock its decision rested on is released.
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	replaySP, err := tx.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replayed := f.s.cleanupObserveOne(ctx, replaySP, 13, first.ItemID, retry, env)
	if replayed.status != 200 || replayed.body["replayed"] != true || replayed.write {
		t.Fatalf("replay did not take the non-writing path: %+v", replayed)
	}
	if err = replaySP.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if cleanupMasterRowShareHeld(t, tx, ctx) {
		t.Fatal("fixture assumption broken: the replay's savepoint rollback kept the row lock")
	}
	// An independent, committed OFF.
	setCleanupMaster(t, f.s, false)
	if cleanupMasterOn(ctx, f.pool) {
		t.Fatal("independent OFF did not commit")
	}

	// The next item of the same batch must not inherit the cached ON.
	next := retry
	next.Attempt = second.Attempt
	next.PlanSHA = second.PlanSHA
	next.Proof = fixtureProof(second.Item)
	next.RequestID = ""
	nextSP, err := tx.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer nextSP.Rollback(context.Background())
	result := f.s.cleanupObserveOne(ctx, nextSP, 13, second.ItemID, next, env)
	if result.body["delete"] == true {
		t.Fatalf("permit minted after OFF from a cached master decision: %+v", result)
	}
	// The fresh item must have *seen* the OFF, not merely failed somewhere else:
	// a non-permit observation during execution pauses the plan in the same
	// transaction, which is the only path that records this state.
	if result.status != 200 || result.body["plan_paused"] != true {
		t.Fatalf("the item did not observe the committed OFF: %+v", result)
	}
}

// A single-item observation holds its FOR SHARE to commit, but no decision may
// claim ON without a verified live row lock: an unreadable or missing control is
// OFF, and a cached ON never survives the loss of the lock that justified it.
func TestNASCleanupDeletionMasterNeedsTheLiveRowLock(t *testing.T) {
	f := newCleanupFixture(t, 24)
	_, _, _ = f.admit("lock-lifetime", syntheticFiles(100, 2, 10))
	setCleanupMaster(t, f.s, true)
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	// A savepoint takes the decision exactly as a batched observation does, so
	// its rollback releases the row lock the decision rests on.
	sp, err := tx.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	env := &cleanupObserveEnv{}
	if !env.deletionMaster(ctx, sp) {
		t.Fatal("live FOR SHARE did not permit authority")
	}
	if !cleanupMasterRowShareHeld(t, sp, ctx) {
		t.Fatal("an ON decision did not hold the master row lock")
	}
	if err = sp.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if cleanupMasterRowShareHeld(t, tx, ctx) {
		t.Fatal("fixture assumption broken: the savepoint rollback kept the row lock")
	}
	// An independent committed OFF, then the cached decision must not answer.
	setCleanupMaster(t, f.s, false)
	if env.deletionMaster(ctx, tx) {
		t.Fatal("a committed OFF was masked by a cached decision whose lock was released")
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// With the control unavailable, an ON decision can never be produced: the
	// read cannot be shown to hold a live row lock.
	if _, err = f.pool.Exec(ctx, `ALTER TABLE nas_cleanup_master RENAME TO nas_cleanup_master_gone`); err != nil {
		t.Fatal(err)
	}
	setCleanupMasterGone := &cleanupObserveEnv{}
	missingTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer missingTx.Rollback(context.Background())
	if setCleanupMasterGone.deletionMaster(ctx, missingTx) {
		t.Fatal("missing control reported ON")
	}
}
