package api

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

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
