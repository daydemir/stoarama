package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestParseRelayRoutingArgs(t *testing.T) {
	opts, err := parseRelayRoutingArgs([]string{
		"set-group-bandwidth",
		"--account-id", "47",
		"--group-id", "3",
		"--expected-name", "deniz-durham",
		"--bandwidth-mbps", "80.5",
		"--apply",
	})
	if err != nil {
		t.Fatal(err)
	}
	if opts.accountID != 47 || opts.groupID != 3 || opts.expectedName != "deniz-durham" || opts.bandwidthMbps != 80.5 || !opts.apply {
		t.Fatalf("unexpected options: %+v", opts)
	}
}

func TestParseRelayRoutingArgsRejectsUnsafeInput(t *testing.T) {
	tests := [][]string{
		{"set-group-bandwidth", "--account-id", "47", "--group-id", "3", "--expected-name", "", "--bandwidth-mbps", "80"},
		{"set-group-bandwidth", "--account-id", "47", "--group-id", "3", "--expected-name", " deniz-durham ", "--bandwidth-mbps", "80"},
		{"set-group-bandwidth", "--account-id", "47", "--group-id", "3", "--expected-name", "deniz-durham", "--bandwidth-mbps", "0"},
		{"set-group-bandwidth", "--account-id", "47", "--group-id", "3", "--expected-name", "deniz-durham", "--bandwidth-mbps", "NaN"},
		{"set-group-bandwidth", "--account-id", "47", "--group-id", "3", "--expected-name", "deniz-durham", "--bandwidth-mbps", "10001"},
	}
	for _, args := range tests {
		if _, err := parseRelayRoutingArgs(args); err == nil {
			t.Fatalf("expected error for %v", args)
		}
	}
}

func TestSetRelayGroupBandwidthPreviewsAndAppliesExactGroup(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed relay routing regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("relay_routing_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE relay_groups (
		  id BIGINT,
		  account_id BIGINT,
		  name TEXT NOT NULL,
		  bandwidth_capacity_bps BIGINT,
		  PRIMARY KEY (account_id,id)
		);
		INSERT INTO relay_groups VALUES
		  (2,47,'deniz-cambridge',NULL),
		  (3,47,'deniz-durham',NULL),
		  (3,99,'other-account',1000000);
	`); err != nil {
		t.Fatal(err)
	}
	opts := relayRoutingOptions{accountID: 47, groupID: 3, expectedName: "deniz-durham", bandwidthMbps: 80}
	if _, err := setRelayGroupBandwidth(ctx, pool, opts); err != nil {
		t.Fatal(err)
	}
	var got *int64
	if err := pool.QueryRow(ctx, `SELECT bandwidth_capacity_bps FROM relay_groups WHERE account_id=47 AND id=3`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("preview mutated bandwidth: %d", *got)
	}
	opts.apply = true
	if _, err := setRelayGroupBandwidth(ctx, pool, opts); err != nil {
		t.Fatal(err)
	}
	var applied int64
	if err := pool.QueryRow(ctx, `SELECT bandwidth_capacity_bps FROM relay_groups WHERE account_id=47 AND id=3`).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if applied != 80_000_000 {
		t.Fatalf("bandwidth=%d want 80000000", applied)
	}
	bad := opts
	bad.expectedName = "deniz-cambridge"
	if _, err := setRelayGroupBandwidth(ctx, pool, bad); err == nil {
		t.Fatal("expected exact-name safety failure")
	}
}
