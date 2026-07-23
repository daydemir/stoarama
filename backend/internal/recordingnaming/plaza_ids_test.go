package recordingnaming

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestParsePlazaIDNormalizesZeroPadding(t *testing.T) {
	want, err := parsePlazaID("8")
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"08", "008"} {
		got, err := parsePlazaID(raw)
		if err != nil || got != want {
			t.Fatalf("parsePlazaID(%q)=(%d,%v), want (%d,nil)", raw, got, err, want)
		}
	}
}

func TestLockOrganizationPlazaIDsAcceptsIntegerAccountID(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed plaza ID regression")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := LockOrganizationPlazaIDs(ctx, tx, 47); err != nil {
		t.Fatalf("lock plaza ID sequence: %v", err)
	}
}
