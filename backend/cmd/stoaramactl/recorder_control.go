package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/billing"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/dropletpool"
	"github.com/daydemir/stoarama/backend/internal/recsched"
)

// runRecorderControl is the entrypoint for the dedicated single-instance control
// service. It runs the recorder cron scheduler and the droplet-pool autoscaler
// under restart-with-backoff loops in the same process. There is no leader
// election because this service runs exactly one replica.
func runRecorderControl(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || args[0] != "run" {
		log.Fatalf("usage: stoaramactl recorder-control run")
	}
	fs := flag.NewFlagSet("recorder-control run", flag.ExitOnError)
	_ = fs.Parse(args[1:])

	if !cfg.RecSchedEnabled && !cfg.DropletPoolEnabled {
		log.Printf("recorder-control: both REC_SCHED_ENABLED and DROPLET_POOL_ENABLED are false; nothing to run.")
		return
	}

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()

	// Billing gates capture on the billable predicate only when Stripe is wired.
	billingEnabled := cfg.StripeSecretKey != "" && cfg.StripeWebhookSecret != "" && cfg.StripePriceID != "" && cfg.StripeGBMonthPriceID != ""

	const restartDelay = 3 * time.Second
	var wg sync.WaitGroup

	if cfg.RecSchedEnabled {
		if cfg.RecSchedTickSec <= 0 || cfg.RecSchedCatchupSec <= 0 || cfg.RecSchedMinIntervalSec <= 0 || cfg.RecSchedMaxJobsPerTick <= 0 {
			log.Fatalf("invalid recorder scheduler config (tick/catchup/min-interval/max-jobs must all be > 0)")
		}
		scheduler := recsched.New(pool, recsched.Config{
			TickInterval:   time.Duration(cfg.RecSchedTickSec) * time.Second,
			CatchupWindow:  time.Duration(cfg.RecSchedCatchupSec) * time.Second,
			MinIntervalSec: cfg.RecSchedMinIntervalSec,
			MaxJobsPerTick: cfg.RecSchedMaxJobsPerTick,
			BillingEnabled: billingEnabled,
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWithBackoff(ctx, "recorder scheduler", restartDelay, scheduler.Run)
		}()
	} else {
		log.Printf("recorder-control: REC_SCHED_ENABLED is false; scheduler not started.")
	}

	if cfg.DropletPoolEnabled {
		controller := mustBuildDropletPool(ctx, cfg, pool, billingEnabled)
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWithBackoff(ctx, "droplet pool", restartDelay, controller.Run)
		}()
	} else {
		log.Printf("recorder-control: DROPLET_POOL_ENABLED is false; autoscaler not started.")
	}

	// Monthly usage metering: the only place recording-days are reported to Stripe.
	// Gated on billingEnabled (same secret+webhook+price gate as capture), so free
	// mode never charges. Runs under the same restart-with-backoff loop.
	if billingEnabled {
		reporter, err := billing.New(cfg.StripeSecretKey, cfg.StripePriceID, cfg.StripeGBMonthPriceID, cfg.AppBaseURL, cfg.StripeLivemode)
		if err != nil {
			log.Fatalf("init stripe billing client for metering: %v", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWithBackoff(ctx, "recording metering", restartDelay, func(ctx context.Context) error {
				return runRecordingMetering(ctx, pool, reporter)
			})
		}()
	} else {
		log.Printf("recorder-control: billing disabled; usage metering not started.")
	}

	// Managed-storage retention/purge: deletes stopped-payers' managed R2 objects
	// after the grace period. Gated on billing AND a valid operator R2 config (the
	// same creds managed-provision seals into each managed destination row). Runs
	// under the same restart-with-backoff loop. Never touches BYO objects.
	if billingEnabled && cfg.ValidateR2() == nil {
		purgeR2 := mustOperatorR2Client(ctx, cfg)
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWithBackoff(ctx, "managed storage purge", restartDelay, func(ctx context.Context) error {
				return runManagedPurge(ctx, pool, purgeR2)
			})
		}()
	} else {
		log.Printf("recorder-control: managed-storage purge not started (billing disabled or operator R2 unconfigured).")
	}

	wg.Wait()
}

// mustBuildDropletPool validates the pool config, resolves the operator account
// that owns the per-droplet node tokens, and constructs the autoscaler with a
// real godo client. The autoscaler runs expired-lease reclaim itself only when
// the scheduler is not co-running on this service (C8).
func mustBuildDropletPool(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, billingEnabled bool) *dropletpool.Controller {
	if err := cfg.ValidatePool(); err != nil {
		log.Fatalf("invalid droplet pool config: %v", err)
	}
	// A stock distribution slug boots a bare image that must apt-install + clone +
	// build the worker via cloud-init (minutes), which typically exceeds
	// DROPLET_POOL_PROVISION_LEAD_SEC; a prebuilt snapshot (numeric id) boots
	// worker-ready in well under the lead. Warn the operator on a stock slug so the
	// best-effort lead is a conscious choice (promotion still waits for the worker
	// to report in, it does not flip active on power-on).
	if !isNumericImageID(cfg.DropletPoolImage) {
		log.Printf("droplet pool: WARNING DROPLET_POOL_IMAGE=%q is a stock distribution slug, not a prebuilt snapshot id; cloud-init worker build will likely exceed DROPLET_POOL_PROVISION_LEAD_SEC=%d, so the provision lead is best-effort",
			cfg.DropletPoolImage, cfg.DropletPoolProvisionLeadSec)
	}
	doClient, err := dropletpool.NewGodoClient(cfg.DOAPIToken)
	if err != nil {
		log.Fatalf("init DO client: %v", err)
	}
	operatorAccountID, err := dropletpool.NewStore(pool).ResolveOperatorAccount(ctx, cfg.DropletPoolOperatorEmail)
	if err != nil {
		log.Fatalf("resolve droplet pool operator account: %v", err)
	}
	return dropletpool.NewController(pool, doClient, dropletpool.Config{
		OperatorAccountID: operatorAccountID,
		BillingEnabled:    billingEnabled,
		TickInterval:      time.Duration(cfg.DropletPoolTickSec) * time.Second,
		Lookahead:         time.Duration(cfg.DropletPoolLookaheadSec) * time.Second,
		Capacity:          cfg.DropletPoolCapacity,
		ProvisionLead:     time.Duration(cfg.DropletPoolProvisionLeadSec) * time.Second,
		ProvisionTimeout:  time.Duration(cfg.DropletPoolProvisionTimeoutSec) * time.Second,
		IdleGrace:         time.Duration(cfg.DropletPoolIdleGraceSec) * time.Second,
		DrainTimeout:      time.Duration(cfg.DropletPoolDrainTimeoutSec) * time.Second,
		ScaleUpCooldown:   time.Duration(cfg.DropletPoolScaleUpCooldownSec) * time.Second,
		ScaleDownCooldown: time.Duration(cfg.DropletPoolScaleDownCooldownSec) * time.Second,
		Min:               cfg.DropletPoolMin,
		Max:               cfg.DropletPoolMax,
		Region:            cfg.DropletPoolRegion,
		Size:              cfg.DropletPoolSize,
		Image:             cfg.DropletPoolImage,
		SSHKey:            cfg.DropletPoolSSHKey,
		ProjectID:         cfg.DropletPoolProjectID,
		FirewallID:        cfg.DropletPoolFirewallID,
		BackendAPIURL:     cfg.DropletPoolBackendAPIURL,
		HeartbeatSec:      cfg.RecordingWorkerHeartbeatSec,
		PollSec:           cfg.RecordingWorkerPollSec,
		RepoURL:           cfg.DropletPoolRepoURL,
		RepoRef:           cfg.DropletPoolRepoRef,
		RepoCloneToken:    cfg.DropletPoolRepoCloneToken,
		ReclaimLeases:     !cfg.RecSchedEnabled,
	})
}

// isNumericImageID reports whether the configured DO image is a numeric snapshot
// / image id (worker-ready prebuilt image) rather than a stock distribution slug
// such as "ubuntu-24-04-x64".
func isNumericImageID(image string) bool {
	image = strings.TrimSpace(image)
	if image == "" {
		return false
	}
	_, err := strconv.ParseInt(image, 10, 64)
	return err == nil
}

// runWithBackoff runs fn, restarting it after restartDelay on any non-cancel
// error until ctx is canceled.
func runWithBackoff(ctx context.Context, name string, restartDelay time.Duration, fn func(context.Context) error) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("%s exited: %v; restarting in %s", name, err, restartDelay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(restartDelay):
			}
			continue
		}
		return
	}
}
