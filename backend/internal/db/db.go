package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := openPool(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	runtimeRole := strings.TrimSpace(os.Getenv("STOARAMA_DATABASE_RUNTIME_ROLE"))
	authorityRole := strings.TrimSpace(os.Getenv("STOARAMA_ADMISSION_AUTHORITY_ROLE"))
	roleKind := strings.TrimSpace(os.Getenv("STOARAMA_DATABASE_ROLE_KIND"))
	if roleKind == "runtime" {
		if err := ValidateCampaignRuntimePrivileges(ctx, pool, runtimeRole, authorityRole); err != nil {
			pool.Close()
			return nil, fmt.Errorf("validate runtime database boundary: %w", err)
		}
	} else if roleKind != "" {
		pool.Close()
		return nil, fmt.Errorf("unsupported STOARAMA_DATABASE_ROLE_KIND %q", roleKind)
	}
	return pool, nil
}

func OpenCampaignExecutor(ctx context.Context, databaseURL, executorRole, authorityRole string) (*pgxpool.Pool, error) {
	pool, err := openPool(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := ValidateCampaignExecutorPrivileges(ctx, pool, executorRole, authorityRole); err != nil {
		pool.Close()
		return nil, fmt.Errorf("validate admission executor database boundary: %w", err)
	}
	return pool, nil
}

func openPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}
