package main

import (
	"context"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/db"
)

// The reservation guard's indexed functions must agree with the views they
// replace for every kind, source and issuance shape, and the guard must still
// refuse a reserved source.
func TestR2ReservationGuardIndexedLookupMatchesViews(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "reservation_guard_index")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO accounts(id,email) VALUES(47,'guard@test.invalid'),(48,'guard-foreign@test.invalid');
 INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(1,47,'test-guard-a','not-a-real-credential-a'),(2,48,'test-guard-b','not-a-real-credential-b');
 INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',1),(14,48,'nas_pull',2);
 INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode) VALUES
 (101,47,13,'guard-dry',repeat('a',64),'[{"kind":"raw","source_id":1001}]',repeat('b',64),'dry_run'),
 (102,47,13,'guard-paused',repeat('a',64),'[{"kind":"raw","source_id":"001002"}]',repeat('b',64),'paused'),
 (103,47,13,'guard-retired',repeat('a',64),'[{"kind":"raw","source_id":1003},{"kind":"raw","source_id":"001004"},{"kind":"raw","source_id":1005},{"kind":"raw","source_id":1006},{"kind":"raw","source_id":1006},{"kind":"collated","source_id":1008},{"kind":"raw","source_id":1009},{"kind":"raw","source_id":1010}]',repeat('b',64),'paused'),
 (104,48,14,'guard-foreign',repeat('a',64),'[{"kind":"raw","source_id":1007}]',repeat('b',64),'dry_run');
 INSERT INTO nas_cleanup_items(plan_id,ordinal,delete_ever_issued,state) VALUES(103,1,true,'deleted'),(103,2,false,'keep'),(103,4,false,'keep'),(103,5,true,'deleted'),(103,7,false,'unverified'),(103,8,false,'media_deleted_sidecar_failed');
 UPDATE nas_cleanup_plans SET mode='retired' WHERE id=103;`); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"raw", "collated", "joined"} {
		for _, id := range []int64{1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 9999} {
			var oldR2, newR2, oldOwner, newOwner bool
			if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_r2_reserved_files WHERE kind=$1 AND source_id=$2),nas_cleanup_r2_file_reserved($1,$2),
 EXISTS(SELECT 1 FROM nas_cleanup_owner_reserved_files WHERE kind=$1 AND source_id=$2),nas_cleanup_owner_file_reserved($1,$2)`, kind, id).Scan(&oldR2, &newR2, &oldOwner, &newOwner); err != nil {
				t.Fatal(err)
			}
			if oldR2 != newR2 || oldOwner != newOwner {
				t.Fatalf("%s/%d r2 view=%t fn=%t owner view=%t fn=%t", kind, id, oldR2, newR2, oldOwner, newOwner)
			}
		}
	}
	// The owner rule keeps uncertain states that the ordinary rule releases.
	var r2, owner bool
	if err := pool.QueryRow(ctx, `SELECT nas_cleanup_r2_file_reserved('raw',1009),nas_cleanup_owner_file_reserved('raw',1009)`).Scan(&r2, &owner); err != nil || r2 || !owner {
		t.Fatalf("unverified state: r2=%t owner=%t err=%v", r2, owner, err)
	}
	// Effective releases are class-specific: an owner-only release clears only the
	// owner rule, while an ordinary release on a retired plan clears both.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode) VALUES
 (105,47,13,'guard-released',repeat('a',64),'[{"kind":"raw","source_id":2001},{"kind":"raw","source_id":2002}]',repeat('b',64),'paused');
 INSERT INTO nas_cleanup_items(id,plan_id,ordinal,state,delete_ever_issued,recovery,outcome) VALUES
 (5001,105,1,'deleted',true,'r2_sha_verified','{"outcome":"deleted"}'),(5002,105,2,'deleted',true,'r2_sha_verified','{"outcome":"deleted"}');
 UPDATE nas_cleanup_plans SET mode='retired' WHERE id=105;
 SET LOCAL session_replication_role=replica;
 INSERT INTO nas_cleanup_r2_releases(item_id,manifest_sha256,picks_sha256,policy_sha256,reason,authority)
 SELECT i.id,repeat('a',64),repeat('b',64),repeat('c',64),'test',jsonb_build_object('recovery',i.recovery,
  'receipt_sha256',encode(sha256(convert_to(i.outcome::text,'UTF8')),'hex'),
  'recovery_sha256',encode(sha256(convert_to(coalesce(i.recovery_evidence::text,''),'UTF8')),'hex'),
  'candidate',jsonb_build_object('reason',CASE i.id WHEN 5001 THEN 'owner_accepted_outside_window' ELSE 'ordinary' END))
 FROM nas_cleanup_items i WHERE i.id IN (5001,5002);
 SET LOCAL session_replication_role=origin;`); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		id                int64
		wantR2, wantOwner bool
	}{{2001, true, false}, {2002, false, false}} {
		var oldR2, newR2, oldOwner, newOwner bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_r2_reserved_files WHERE kind='raw' AND source_id=$1),nas_cleanup_r2_file_reserved('raw',$1),
 EXISTS(SELECT 1 FROM nas_cleanup_owner_reserved_files WHERE kind='raw' AND source_id=$1),nas_cleanup_owner_file_reserved('raw',$1)`, c.id).Scan(&oldR2, &newR2, &oldOwner, &newOwner); err != nil {
			t.Fatal(err)
		}
		if oldR2 != c.wantR2 || newR2 != c.wantR2 || oldOwner != c.wantOwner || newOwner != c.wantOwner {
			t.Fatalf("released %d: r2 view=%t fn=%t owner view=%t fn=%t", c.id, oldR2, newR2, oldOwner, newOwner)
		}
	}
	// The owner-only release admits only an owner-class reservation.
	reserve := func(class string) error {
		_, err := tx.Exec(ctx, `SAVEPOINT r; INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class) VALUES('raw',2001,'stoarama','managed/guard/2001.mp4',1,repeat('a',64),repeat('b',64),'`+class+`'); ROLLBACK TO SAVEPOINT r`)
		if err != nil {
			_, _ = tx.Exec(ctx, `ROLLBACK TO SAVEPOINT r`)
		}
		return err
	}
	if err = reserve(""); err == nil || !strings.Contains(err.Error(), "NAS cleanup already reserves") {
		t.Fatalf("ordinary reservation after owner-only release: %v", err)
	}
	if err = reserve("owner_accepted_outside_window"); err != nil {
		t.Fatalf("owner reservation after owner-only release: %v", err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// The guard itself still refuses a reserved source in each class.
	for _, c := range []struct{ class, want string }{{"", "NAS cleanup already reserves"}, {"owner_accepted_outside_window", "owner class recovery hold"}} {
		_, err := pool.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class) VALUES('raw',1001,'stoarama','managed/guard/1001.mp4',1,repeat('a',64),repeat('b',64),$1)`, c.class)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("class %q reservation: %v", c.class, err)
		}
	}
}
