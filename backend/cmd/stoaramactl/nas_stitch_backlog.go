package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
)

// nasStitchBacklogStatementTimeout bounds this read-only report. Production's
// default statement_timeout is 10s, which a multi-day window scan can exceed;
// the report is a diagnostic, not a request path, so it takes a longer bounded
// budget and never writes.
const nasStitchBacklogStatementTimeout = "600s"

// backlogTxQuerier adapts a read-only transaction to the report's querier.
type backlogTxQuerier struct{ tx pgx.Tx }

func (q backlogTxQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return q.tx.Query(ctx, sql, args...)
}

// parseNASStitchBacklogArgs accepts no flags or positional arguments; a typo is
// an error rather than a silently ignored word.
func parseNASStitchBacklogArgs(args []string) error {
	fs := flag.NewFlagSet("nas-stitch backlog", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if rest := fs.Args(); len(rest) != 0 {
		return fmt.Errorf("unexpected arguments: %v", rest)
	}
	return nil
}

func runNASStitchBacklog(ctx context.Context, cfg config.Config, args []string) {
	if err := parseNASStitchBacklogArgs(args); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 12*time.Minute)
	defer cancel()
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatal("invalid database configuration")
	}
	// One bounded connection; the transaction below is READ ONLY and carries its
	// own extended statement_timeout.
	poolConfig.MaxConns = 1
	poolConfig.MinConns = 0
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "stoarama-nas-stitch-backlog"
	poolConfig.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}
	defer pool.Close()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		log.Fatalf("begin backlog transaction: %v", err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SET LOCAL statement_timeout = '"+nasStitchBacklogStatementTimeout+"'"); err != nil {
		log.Fatalf("set backlog statement_timeout: %v", err)
	}
	report, err := api.NASStitchBacklog(ctx, backlogTxQuerier{tx: tx})
	if err != nil {
		log.Fatalf("load stitch backlog: %v", err)
	}
	if err = tx.Commit(ctx); err != nil {
		log.Fatalf("commit read-only backlog observation: %v", err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err = enc.Encode(report); err != nil {
		log.Fatal(err)
	}
	fmt.Fprintln(os.Stderr, "read-only: no task, scope, inventory or media row was written")
}
