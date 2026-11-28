package main

import (
	"context"
	"errors"
)

var errDeleteHeld = errors.New("legacy R2 deletion is disabled: exact surviving copy verification and permanent cross-store protection are required")

func runDelete(context.Context, []string) error {
	return errDeleteHeld
}

// Reject at the former wire boundary as well: no DeleteObjects request or
// ledger acknowledgement is possible, even for a caller bypassing the CLI.
func deleteBatchOnce(context.Context, store, []manifestRow, func([]ledgerRow) error) error {
	return errDeleteHeld
}

// Historical outcome row shape; the disabled wire boundary never emits one.
type ledgerRow struct {
	Key     string  `json:"key"`
	Size    int64   `json:"size"`
	Outcome outcome `json:"outcome"`
	Detail  string  `json:"detail,omitempty"`
	At      string  `json:"at"`
}

type outcome string
