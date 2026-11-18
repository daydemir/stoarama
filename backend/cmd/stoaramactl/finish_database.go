package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
)

type finishStatement string

const (
	finishPoolOpen             finishStatement = "pool_open"
	finishPoolAcquire          finishStatement = "pool_acquire"
	finishTickLock             finishStatement = "tick_lock"
	finishOwnerQuery           finishStatement = "cleanup_owner_binding"
	finishRecoveryQuery        finishStatement = "recovery_binding"
	finishEnabledQuery         finishStatement = "enabled_cleanup"
	finishQueueDepth           finishStatement = "queue_depth"
	finishStorageQuery         finishStatement = "storage_budget"
	finishRefreshDepth         finishStatement = "refresh_depth"
	finishDebitQuery           finishStatement = "late_debits"
	finishCandidateBegin       finishStatement = "candidate_begin"
	finishCandidateQuery       finishStatement = "candidate_select"
	finishCandidateScan        finishStatement = "candidate_scan"
	finishCandidateCommit      finishStatement = "candidate_commit"
	finishRefreshControl       finishStatement = "refresh_control"
	finishRefreshInitialize    finishStatement = "refresh_initialize"
	finishRefreshCatalogBegin  finishStatement = "refresh_catalog_begin"
	finishRefreshCatalogSelect finishStatement = "refresh_catalog_select"
	finishRefreshCatalogCommit finishStatement = "refresh_catalog_commit"
	finishRefreshCursor        finishStatement = "refresh_cursor"
	finishRefreshAdmission     finishStatement = "refresh_admission"
	finishImportBegin          finishStatement = "import_begin"
	finishImportSource         finishStatement = "import_source"
	finishImportInsert         finishStatement = "import_insert"
	finishImportCommit         finishStatement = "import_commit"
)

type finishDBCause string

const (
	finishDBStatementTimeout finishDBCause = "statement_timeout"
	finishDBQueryCanceled    finishDBCause = "query_canceled"
	finishDBSQL              finishDBCause = "sql_error"
	finishDBDeadline         finishDBCause = "context_deadline"
	finishDBCanceled         finishDBCause = "context_canceled"
	finishDBDNS              finishDBCause = "dns_error"
	finishDBRefused          finishDBCause = "connection_refused"
	finishDBNetworkTimeout   finishDBCause = "network_timeout"
	finishDBConnection       finishDBCause = "connection_error"
	finishDBClosed           finishDBCause = "connection_closed"
	finishDBOther            finishDBCause = "database_error"
	finishDBConfig           finishDBCause = "invalid_database_config"
	finishDBPoolClosed       finishDBCause = "pool_closed"
)

// Only typed, non-secret diagnostics cross the status/stderr boundary. Driver
// errors may contain credentials, SQL parameter values or connection strings.
// The original error remains available to errors.Is/As inside the process.
type finishDatabaseFailure struct {
	Statement finishStatement `json:"statement"`
	Tier      finishTier      `json:"tier,omitempty"`
	SQLState  string          `json:"sqlstate,omitempty"`
	Cause     finishDBCause   `json:"cause"`
	err       error
}

func (e *finishDatabaseFailure) Error() string {
	return fmt.Sprintf("%s: statement=%s tier=%d sqlstate=%s cause=%s", finishDatabase, e.Statement, e.Tier, e.SQLState, e.Cause)
}
func (e *finishDatabaseFailure) Unwrap() []error { return []error{finishDatabase, e.err} }
func finishDBError(statement finishStatement, tier finishTier, err error) error {
	if err == nil {
		return nil
	}
	var existing *finishDatabaseFailure
	if errors.As(err, &existing) {
		return err
	}
	e := &finishDatabaseFailure{Statement: statement, Tier: tier, Cause: finishDBOther, err: err}
	var pg *pgconn.PgError
	var dns *net.DNSError
	var network net.Error
	var connect *pgconn.ConnectError
	var parse *pgconn.ParseConfigError
	switch {
	case errors.As(err, &parse):
		e.Cause = finishDBConfig
	case errors.Is(err, puddle.ErrClosedPool):
		e.Cause = finishDBPoolClosed
	case errors.As(err, &pg):
		e.SQLState = pg.Code
		e.Cause = finishDBSQL
		if pg.Code == "57014" {
			e.Cause = finishDBQueryCanceled
			if pg.Message == "canceling statement due to statement timeout" {
				e.Cause = finishDBStatementTimeout
			}
		}
	case errors.Is(err, context.DeadlineExceeded):
		e.Cause = finishDBDeadline
	case errors.Is(err, context.Canceled):
		e.Cause = finishDBCanceled
	case errors.As(err, &dns):
		e.Cause = finishDBDNS
	case errors.Is(err, syscall.ECONNREFUSED):
		e.Cause = finishDBRefused
	case errors.As(err, &network) && network.Timeout():
		e.Cause = finishDBNetworkTimeout
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed):
		e.Cause = finishDBClosed
	case errors.As(err, &connect), errors.As(err, &network), pgconn.SafeToRetry(err):
		e.Cause = finishDBConnection
	}
	return e
}

func finishImportFailure(tier finishTier, err error) error {
	var failure *finishDatabaseFailure
	if errors.As(err, &failure) {
		annotated := *failure
		annotated.Tier = tier
		err = &annotated
	}
	// Imports can commit a subset before a DB failure. Retain import-failed
	// disposition, the exact manifest, and the underlying DB diagnostic together.
	return fmt.Errorf("%w: %w", finishImport, err)
}
