package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
)

type relayRoutingOptions struct {
	accountID     int64
	groupID       int64
	expectedName  string
	bandwidthMbps float64
	apply         bool
}

func parseRelayRoutingArgs(args []string) (relayRoutingOptions, error) {
	if len(args) < 1 || args[0] != "set-group-bandwidth" {
		return relayRoutingOptions{}, fmt.Errorf("usage: stoaramactl relay-routing set-group-bandwidth --account-id ID --group-id ID --expected-name NAME --bandwidth-mbps N [--apply]")
	}
	fs := flag.NewFlagSet("relay-routing set-group-bandwidth", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	accountID := fs.Int64("account-id", 0, "account id")
	groupID := fs.Int64("group-id", 0, "relay group id")
	expectedName := fs.String("expected-name", "", "exact relay group name safety check")
	bandwidthMbps := fs.Float64("bandwidth-mbps", 0, "conservative native-bandwidth routing budget in Mbps")
	apply := fs.Bool("apply", false, "persist the change; otherwise preview only")
	if err := fs.Parse(args[1:]); err != nil {
		return relayRoutingOptions{}, err
	}
	if len(fs.Args()) != 0 {
		return relayRoutingOptions{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	name := strings.TrimSpace(*expectedName)
	if *accountID <= 0 || *groupID <= 0 || name == "" {
		return relayRoutingOptions{}, fmt.Errorf("--account-id, --group-id, and --expected-name are required")
	}
	if math.IsNaN(*bandwidthMbps) || math.IsInf(*bandwidthMbps, 0) || *bandwidthMbps < 1 || *bandwidthMbps > 10000 {
		return relayRoutingOptions{}, fmt.Errorf("--bandwidth-mbps must be between 1 and 10000")
	}
	return relayRoutingOptions{
		accountID:     *accountID,
		groupID:       *groupID,
		expectedName:  name,
		bandwidthMbps: *bandwidthMbps,
		apply:         *apply,
	}, nil
}

func runRelayRouting(ctx context.Context, cfg config.Config, args []string) {
	opts, err := parseRelayRoutingArgs(args)
	if err != nil {
		log.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer pool.Close()
	result, err := setRelayGroupBandwidth(ctx, pool, opts)
	if err != nil {
		log.Fatalf("set relay group bandwidth: %v", err)
	}
	printJSON(result)
}

func setRelayGroupBandwidth(ctx context.Context, pool *pgxpool.Pool, opts relayRoutingOptions) (map[string]any, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var name string
	var previousBPS *int64
	if err := tx.QueryRow(ctx, `
		SELECT name, bandwidth_capacity_bps
		FROM relay_groups
		WHERE id=$1 AND account_id=$2
		FOR UPDATE
	`, opts.groupID, opts.accountID).Scan(&name, &previousBPS); err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("relay group %d does not belong to account %d", opts.groupID, opts.accountID)
		}
		return nil, err
	}
	if name != opts.expectedName {
		return nil, fmt.Errorf("relay group %d name is %q, not expected %q", opts.groupID, name, opts.expectedName)
	}
	requestedBPS := int64(math.Round(opts.bandwidthMbps * 1_000_000))
	if opts.apply {
		if _, err := tx.Exec(ctx, `
			UPDATE relay_groups
			SET bandwidth_capacity_bps=$3
			WHERE id=$1 AND account_id=$2
		`, opts.groupID, opts.accountID, requestedBPS); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{
		"account_id":              opts.accountID,
		"group_id":                opts.groupID,
		"name":                    name,
		"previous_bandwidth_mbps": relayBandwidthMbps(previousBPS),
		"bandwidth_mbps":          opts.bandwidthMbps,
		"applied":                 opts.apply,
	}, nil
}

func relayBandwidthMbps(bps *int64) any {
	if bps == nil {
		return nil
	}
	return float64(*bps) / 1_000_000
}
