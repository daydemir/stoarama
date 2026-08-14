package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/db"
)

const campaignAuthorityRole = "stoarama_admission_authority"

func main() {
	ctx := context.Background()
	migrationURL := strings.TrimSpace(os.Getenv("MIGRATION_DATABASE_URL"))
	runtimeURL := strings.TrimSpace(os.Getenv("RUNTIME_DATABASE_URL"))
	executorURL := strings.TrimSpace(os.Getenv("ADMISSION_DATABASE_URL"))
	if migrationURL == "" || runtimeURL == "" || executorURL == "" || migrationURL == runtimeURL || migrationURL == executorURL || runtimeURL == executorURL {
		log.Fatal("distinct MIGRATION_DATABASE_URL, RUNTIME_DATABASE_URL, and ADMISSION_DATABASE_URL are required")
	}
	migratorRole, err := databaseURLUser(migrationURL)
	if err != nil {
		log.Fatalf("migration database identity: %v", err)
	}
	runtimeRole, err := databaseURLUser(runtimeURL)
	if err != nil {
		log.Fatalf("runtime database identity: %v", err)
	}
	executorRole, err := databaseURLUser(executorURL)
	if err != nil {
		log.Fatalf("admission executor database identity: %v", err)
	}
	if migratorRole == runtimeRole || migratorRole == executorRole || runtimeRole == executorRole {
		log.Fatal("migration, runtime, and admission executor database logins must be distinct")
	}
	migrationPool, err := db.Open(ctx, migrationURL)
	if err != nil {
		log.Fatalf("open migration database: %v", err)
	}
	defer migrationPool.Close()
	lockConn, err := migrationPool.Acquire(ctx)
	if err != nil {
		log.Fatalf("acquire migration lock connection: %v", err)
	}
	defer lockConn.Release()
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended('stoarama-schema-migrations-v1',0))`); err != nil {
		log.Fatalf("lock migrations: %v", err)
	}
	defer lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtextextended('stoarama-schema-migrations-v1',0))`)
	if err := db.BootstrapCampaignRoles(ctx, migrationPool, runtimeRole, executorRole, campaignAuthorityRole); err != nil {
		log.Fatalf("bootstrap campaign roles: %v", err)
	}
	if err := db.MigrateUpWithCampaignRoles(ctx, migrationPool, strings.TrimSpace(os.Getenv("MIGRATION_DIR")), runtimeRole, executorRole, campaignAuthorityRole); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	runtimePool, err := db.Open(ctx, runtimeURL)
	if err != nil {
		log.Fatalf("open runtime verification database: %v", err)
	}
	defer runtimePool.Close()
	if err := db.ValidateCampaignRuntimePrivileges(ctx, runtimePool, runtimeRole, campaignAuthorityRole); err != nil {
		log.Fatalf("verify campaign runtime split: %v", err)
	}
	executorPool, err := db.Open(ctx, executorURL)
	if err != nil {
		log.Fatalf("open admission executor verification database: %v", err)
	}
	defer executorPool.Close()
	if err := db.ValidateCampaignExecutorPrivileges(ctx, executorPool, executorRole, campaignAuthorityRole); err != nil {
		log.Fatalf("verify campaign executor split: %v", err)
	}
	log.Printf("migrations complete; runtime role %s and admission executor role %s are privilege-fenced", runtimeRole, executorRole)
}

func databaseURLUser(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil || strings.TrimSpace(u.User.Username()) == "" {
		return "", fmt.Errorf("database URL has no username")
	}
	return strings.TrimSpace(u.User.Username()), nil
}
