package api

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// cleanupMasterRoleMu serializes the 0229 tests: nas_cleanup_master_writer is a
// cluster-wide role, so one test creating, inspecting or dropping it must not
// interleave with another. Other packages' fixtures create it too, which is why
// the fresh-creation case only runs when it can prove the role was absent.
var cleanupMasterRoleMu sync.Mutex

func lockCleanupMasterRole(t *testing.T) func() {
	t.Helper()
	cleanupMasterRoleMu.Lock()
	return cleanupMasterRoleMu.Unlock
}

// applyCleanupMasterMigration runs migration 0229 inside a fresh, isolated
// schema owned by role, in one transaction, exactly as the migrator does.
func applyCleanupMasterMigration(t *testing.T, admin *pgx.Conn, ctx context.Context, schema, role string) error {
	t.Helper()
	body, err := os.ReadFile("../../../infra/sql/migrations/0229_nas_cleanup_master.sql")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL ROLE `+pgx.Identifier{role}.Sanitize()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SET LOCAL search_path = `+pgx.Identifier{schema}.Sanitize()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, string(body)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Migration 0229 must work for a non-superuser schema owner (it only needs
// CREATE on the schema for the transfer), and must fail rather than fall back
// to the migrator as the history recorder owner.
func TestNASCleanupMasterMigrationRequiresDedicatedWriterOwner(t *testing.T) {
	defer lockCleanupMasterRole(t)()
	dsn := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	owner := "nas_mig_owner_" + suffix
	unsettable := "nas_mig_unsettable_" + suffix
	schema := "nas_mig_" + suffix
	for _, sql := range []string{
		`CREATE ROLE ` + owner + ` NOLOGIN`,
		`CREATE ROLE ` + unsettable + ` NOLOGIN`,
		`CREATE SCHEMA ` + schema + ` AUTHORIZATION ` + owner,
	} {
		if _, err = admin.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+owner)
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+unsettable)
	}()
	// The migrator is also a member of the writer role: the runtime role never
	// needs it, only the migration does. Create the role here when this cluster
	// has not run the migration yet.
	var exists int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname='nas_cleanup_master_writer'`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists == 0 {
		if _, err = admin.Exec(ctx, `CREATE ROLE nas_cleanup_master_writer NOLOGIN`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = admin.Exec(ctx, `GRANT nas_cleanup_master_writer TO `+owner+` WITH SET TRUE`); err != nil {
		t.Fatal(err)
	}
	// A non-superuser schema owner must be able to run the migration and must
	// end up with the recorder owned by the dedicated insert-only role.
	if err = applyCleanupMasterMigration(t, admin, ctx, schema, owner); err != nil {
		t.Fatalf("non-superuser schema owner could not apply 0229: %v", err)
	}
	var recorderOwner string
	if err = admin.QueryRow(ctx, `SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE proname='record_nas_cleanup_master' AND pronamespace=$1::regnamespace`, schema).Scan(&recorderOwner); err != nil {
		t.Fatal(err)
	}
	if recorderOwner != "nas_cleanup_master_writer" {
		t.Fatalf("recorder owned by %q, want the dedicated writer role", recorderOwner)
	}
	// The temporary CREATE grant must be gone: the insert-only role can no
	// longer create objects in the schema.
	var canCreate bool
	if err = admin.QueryRow(ctx, `SELECT has_schema_privilege('nas_cleanup_master_writer',$1,'CREATE')`, schema).Scan(&canCreate); err != nil {
		t.Fatal(err)
	}
	if canCreate {
		t.Fatal("migration left CREATE on the schema with the writer role")
	}
	// A migrator that is a member but cannot SET ROLE must fail loudly instead
	// of silently making itself the recorder owner.
	other := schema + "_unsettable"
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+other+` AUTHORIZATION `+unsettable); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, `GRANT nas_cleanup_master_writer TO `+unsettable+` WITH SET FALSE`); err != nil {
		t.Fatal(err)
	}
	err = applyCleanupMasterMigration(t, admin, ctx, other, unsettable)
	if err == nil {
		t.Fatal("migration silently fell back to the migrator as the recorder owner")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "member") && !strings.Contains(strings.ToLower(err.Error()), "role") {
		t.Fatalf("unexpected refusal: %v", err)
	}
	// Nothing half-applied: the migration transaction rolled back.
	var tables int
	if err = admin.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname IN ('nas_cleanup_master','nas_cleanup_master_history')`, other).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatalf("failed migration left %d tables behind", tables)
	}
}

// Ordinary CREATE on the schema is not schema ownership: it cannot hand CREATE
// or object ownership to another role, so 0229 must refuse such a migrator
// instead of failing somewhere in the transfer.
func TestNASCleanupMasterMigrationRequiresSchemaOwnership(t *testing.T) {
	defer lockCleanupMasterRole(t)()
	dsn := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	provisioner := "nas_mig_prov_" + suffix
	creator := "nas_mig_creator_" + suffix
	schema := "nas_mig_creator_" + suffix
	for _, sql := range []string{
		`CREATE ROLE ` + provisioner + ` NOLOGIN`,
		`CREATE ROLE ` + creator + ` NOLOGIN`,
		// The schema belongs to the provisioner; the migrator only gets CREATE.
		`CREATE SCHEMA ` + schema + ` AUTHORIZATION ` + provisioner,
		`GRANT CREATE, USAGE ON SCHEMA ` + schema + ` TO ` + creator,
	} {
		if _, err = admin.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+creator)
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+provisioner)
	}()
	var exists int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname='nas_cleanup_master_writer'`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists == 0 {
		if _, err = admin.Exec(ctx, `CREATE ROLE nas_cleanup_master_writer NOLOGIN`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = admin.Exec(ctx, `GRANT nas_cleanup_master_writer TO `+creator+` WITH SET TRUE`); err != nil {
		t.Fatal(err)
	}
	err = applyCleanupMasterMigration(t, admin, ctx, schema, creator)
	if err == nil {
		t.Fatal("a CREATE-only migrator applied 0229; schema authority is required")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "transfer schema authority") {
		t.Fatalf("wrong refusal for a non-owner migrator: %v", err)
	}
	var tables int
	if err = admin.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname IN ('nas_cleanup_master','nas_cleanup_master_history')`, schema).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatalf("failed migration left %d tables behind", tables)
	}
}

// The recorder must end up with no effective CREATE on the schema. Revoking the
// direct grant is not enough when CREATE is inherited through another role, so
// the migration verifies the effective privilege and fails closed rather than
// silently leaving the insert-only recorder able to create objects.
func TestNASCleanupMasterMigrationRefusesInheritedCreateForTheWriter(t *testing.T) {
	defer lockCleanupMasterRole(t)()
	dsn := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	owner := "nas_mig_inherit_owner_" + suffix
	grantor := "nas_mig_inherit_grantor_" + suffix
	schema := "nas_mig_inherit_" + suffix
	for _, sql := range []string{
		`CREATE ROLE ` + owner + ` NOLOGIN`,
		`CREATE ROLE ` + grantor + ` NOLOGIN`,
		`CREATE SCHEMA ` + schema + ` AUTHORIZATION ` + owner,
	} {
		if _, err = admin.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+grantor)
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+owner)
	}()
	var exists int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname='nas_cleanup_master_writer'`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists == 0 {
		if _, err = admin.Exec(ctx, `CREATE ROLE nas_cleanup_master_writer NOLOGIN`); err != nil {
			t.Fatal(err)
		}
	}
	// The writer inherits CREATE from a role created for this test only, so the
	// migration's own REVOKE cannot take it away.
	if _, err = admin.Exec(ctx, `GRANT nas_cleanup_master_writer TO `+owner+` WITH SET TRUE;
		GRANT CREATE ON SCHEMA `+schema+` TO `+grantor+`;
		GRANT `+grantor+` TO nas_cleanup_master_writer`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `REVOKE `+grantor+` FROM nas_cleanup_master_writer`)
	}()
	err = applyCleanupMasterMigration(t, admin, ctx, schema, owner)
	if err == nil {
		t.Fatal("migration kept an inherited CREATE for the insert-only recorder")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "create") {
		t.Fatalf("wrong refusal for an inherited CREATE: %v", err)
	}
	var tables int
	if err = admin.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname IN ('nas_cleanup_master','nas_cleanup_master_history')`, schema).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatalf("failed migration left %d tables behind", tables)
	}
}

// The migration is documented for PostgreSQL 16+, where pg_auth_members
// carries set_option. Verifying the module-level requirement keeps the doc and
// the migration from drifting apart.
func TestNASCleanupMasterMigrationStatesItsPostgresFloor(t *testing.T) {
	body, err := os.ReadFile("../../../infra/sql/migrations/0229_nas_cleanup_master.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{"server_version_num", "160000", "PostgreSQL 16", "transfer schema authority", "has_schema_privilege"} {
		if !strings.Contains(text, want) {
			t.Fatalf("0229 no longer asserts %q", want)
		}
	}
}

// The production-shaped path on a fresh database: the roles do not exist yet,
// the migrator is a non-superuser schema owner with CREATEROLE, and the API's
// runtime role is a separate non-superuser login that owns nothing. This proves
// both halves the reviewers asked for:
//
//   - the migration creates the dedicated writer itself (the CREATE ROLE branch),
//     leaves the recorder owned by it and revokes its effective CREATE;
//   - a non-superuser post-migration *operating* role can flip the switch and gets
//     exactly one history row appended by the recorder on its behalf, while it
//     cannot append history directly.
//
// It needs the cluster-wide writer role to be creatable, so it skips with the
// exact reason when a sibling fixture already owns it.
func TestNASCleanupMasterMigrationCreatesFreshWriterAndServesARuntimeRole(t *testing.T) {
	defer lockCleanupMasterRole(t)()
	dsn := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	// If the writer role exists and owns objects anywhere, dropping it would
	// disturb another package's fixtures. State that precisely and skip; the
	// runtime-role half would not be affected but cannot run without this one.
	var owners int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_shdepend d JOIN pg_roles r ON r.oid=d.refobjid WHERE r.rolname='nas_cleanup_master_writer' AND d.deptype='o'`).Scan(&owners); err != nil {
		t.Fatal(err)
	}
	if owners != 0 {
		t.Skipf("skipped fresh CREATE ROLE verification: nas_cleanup_master_writer already owns %d object(s) from another test package, so it cannot be dropped here", owners)
	}
	if _, err := admin.Exec(ctx, `DROP ROLE IF EXISTS nas_cleanup_master_writer`); err != nil {
		t.Fatalf("cannot isolate the writer role: %v", err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	owner := "nas_mig_fresh_owner_" + suffix
	runtime := "nas_mig_fresh_runtime_" + suffix
	if _, err := admin.Exec(ctx, `CREATE ROLE `+owner+` NOLOGIN CREATEROLE`); err != nil {
		t.Fatal(err)
	}
	// The operating role: ordinary login, no superuser, no CREATEROLE, owns
	// nothing. This is what the API runs as.
	if _, err := admin.Exec(ctx, `CREATE ROLE `+runtime+` LOGIN`); err != nil {
		t.Fatal(err)
	}
	dbName := fmt.Sprintf("stoarama_fresh_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS `+dbName); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+dbName); err != nil {
		t.Fatalf("create isolated database: %v", err)
	}
	defer func() {
		// Dropping the database first removes the schema and its dependent
		// recorder, which is what lets the roles go.
		_, _ = admin.Exec(context.Background(), `DROP DATABASE IF EXISTS `+dbName)
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS nas_cleanup_master_writer`)
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+runtime)
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+owner)
	}()
	schema := "fresh"
	dbCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	dbCfg.Database = dbName
	dbCfg.RuntimeParams["search_path"] = schema
	conn, err := pgx.ConnectConfig(ctx, dbCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `CREATE SCHEMA `+schema+` AUTHORIZATION `+owner); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `GRANT CONNECT ON DATABASE `+dbName+` TO `+runtime); err != nil {
		t.Fatal(err)
	}
	if err := applyCleanupMasterMigration(t, conn, ctx, schema, owner); err != nil {
		t.Fatalf("fresh-writer migration as schema owner: %v", err)
	}
	var recorderOwner string
	if err := conn.QueryRow(ctx, `SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE proname='record_nas_cleanup_master' AND pronamespace=$1::regnamespace`, schema).Scan(&recorderOwner); err != nil {
		t.Fatal(err)
	}
	if recorderOwner != "nas_cleanup_master_writer" {
		t.Fatalf("fresh recorder owned by %q", recorderOwner)
	}
	var canCreate bool
	if err := conn.QueryRow(ctx, `SELECT has_schema_privilege('nas_cleanup_master_writer',$1,'CREATE')`, schema).Scan(&canCreate); err != nil {
		t.Fatal(err)
	}
	if canCreate {
		t.Fatal("freshly created writer retained CREATE")
	}
	// The minimal runtime grants: exactly what the API needs to flip the switch.
	if _, err := conn.Exec(ctx, `GRANT USAGE ON SCHEMA `+schema+` TO `+runtime+`;
		GRANT SELECT, INSERT, UPDATE ON `+schema+`.nas_cleanup_master TO `+runtime+`;
		GRANT SELECT ON `+schema+`.nas_cleanup_master_history TO `+runtime+`;`); err != nil {
		t.Fatal(err)
	}
	// Run as the operating role for the rest of the test.
	runtimeCfg := dbCfg.Copy()
	runtimeCfg.User = runtime
	runtimeConn, err := pgx.ConnectConfig(ctx, runtimeCfg)
	if err != nil {
		t.Fatalf("operating role could not connect: %v", err)
	}
	defer runtimeConn.Close(context.Background())
	var super bool
	if err := runtimeConn.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname=current_user`).Scan(&super); err != nil {
		t.Fatal(err)
	}
	if super {
		t.Fatal("the operating role is a superuser; this would not prove the real path")
	}
	// A non-superuser operating role flips the switch; the recorder appends
	// exactly one history row on its behalf.
	if _, err := runtimeConn.Exec(ctx, `UPDATE nas_cleanup_master SET enabled=true,actor='nas-cleanup-controller',claimed_by='ops@host',reason='runtime flip'`); err != nil {
		t.Fatalf("operating role could not flip the master: %v", err)
	}
	var rows int
	if err := runtimeConn.QueryRow(ctx, `SELECT count(*) FROM nas_cleanup_master_history`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("history rows after one flip: %d", rows)
	}
	var actor string
	if err := runtimeConn.QueryRow(ctx, `SELECT actor FROM nas_cleanup_master_history WHERE new_enabled`).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	if actor != "nas-cleanup-controller" {
		t.Fatalf("history actor %q", actor)
	}
	// And it still cannot append history off-path: the guard refuses it.
	if _, err := runtimeConn.Exec(ctx, `INSERT INTO nas_cleanup_master_history(changed_at,actor,claimed_by,old_enabled,new_enabled,reason) VALUES(now(),'forged','',false,true,'forged')`); err == nil {
		t.Fatal("the operating role appended history directly")
	}
}
