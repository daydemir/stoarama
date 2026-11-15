package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/stitchproof"
)

func r2RetireRequireIndexes(ctx context.Context, q r2RetireQuerier) error {
	var ready bool
	if err := q.QueryRow(ctx, `SELECT r2_retirement_delivery_indexes_ready()`).Scan(&ready); err != nil {
		return fmt.Errorf("check R2 retirement indexes: %w", err)
	}
	if !ready {
		return errors.New("R2 retirement indexes are not ready")
	}
	return nil
}

func r2RetireDeliveryClosureUnchanged(ctx context.Context, store api.DeliveryKeepStore, expected api.DeliveryKeepSnapshot) error {
	current, err := api.ReadPublicDeliveryKeep(ctx, store)
	if err != nil {
		return err
	}
	if !r2RetireLowerHex64(expected.SHA256) || current.SHA256 != expected.SHA256 {
		return errors.New("full public delivery dependency closure changed; re-plan")
	}
	return nil
}

func runR2RetireVerifyStitch(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-retire verify-stitch-proof", flag.ContinueOnError)
	file := fs.String("stitch-proof", "", "real NAS attestation")
	sha := fs.String("expected-stitch-proof-sha256", "", "outer digest pinned by independent postrun review")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *file == "" || !r2RetireLowerHex64(*sha) {
		log.Fatal("verify-stitch-proof requires --stitch-proof FILE --expected-stitch-proof-sha256 SHA")
	}
	pool, err := r2RetireOpenPool(ctx, cfg, true)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	observed := time.Now()
	var now time.Time
	if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		log.Fatal(err)
	}
	authority, err := stitchproof.Verify(ctx, *file, *sha, now, observed)
	if err != nil {
		log.Fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(authority.Provenance()); err != nil {
		log.Fatal(err)
	}
}

func r2RetireOperatorRisk(opts r2RetireApplyOptions) error {
	if opts.ProviderUnconditionalHeadGuard && strings.TrimSpace(opts.AcceptOperatorCredentialRisk) == "" {
		return errors.New("HEAD-guard mode requires --accept-operator-credential-risk with an owner decision reference")
	}
	if !opts.ProviderUnconditionalHeadGuard && opts.AcceptOperatorCredentialRisk != "" {
		return errors.New("--accept-operator-credential-risk requires HEAD-guard mode")
	}
	return nil
}
