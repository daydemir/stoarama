package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/billing"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/dropletpool"
	"github.com/daydemir/stoarama/backend/internal/recsched"
	"github.com/daydemir/stoarama/backend/internal/secretbox"
)

// runRecorderControl is the entrypoint for the dedicated single-instance control
// service. It runs the recorder cron scheduler and the droplet-pool autoscaler
// under restart-with-backoff loops in the same process. There is no leader
// election because this service runs exactly one replica.
func runRecorderControl(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl recorder-control run|drain|dedicated-canary")
	}
	switch args[0] {
	case "run":
		// Falls through to the shared flag parsing and service startup below.
	case "drain":
		runRecorderControlDrain(ctx, cfg, args[1:])
		return
	case "dedicated-canary":
		runRecorderControlDedicatedCanary(ctx, cfg, args[1:])
		return
	default:
		log.Fatalf("unknown recorder-control subcommand: %s (want run|drain|dedicated-canary)", args[0])
	}
	fs := flag.NewFlagSet("recorder-control run", flag.ExitOnError)
	_ = fs.Parse(args[1:])
	if err := cfg.ValidateStripe(); err != nil {
		log.Fatalf("invalid recorder-control Stripe configuration: %v", err)
	}

	if !cfg.RecSchedEnabled && !cfg.DropletPoolEnabled {
		log.Printf("recorder-control: both REC_SCHED_ENABLED and DROPLET_POOL_ENABLED are false; nothing to run.")
		return
	}

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()

	// Billing gates capture on the billable predicate only when Stripe is wired.
	billingEnabled := cfg.StripeBillingEnabled()
	var reporter *billing.Client
	if billingEnabled {
		var err error
		reporter, err = billing.New(cfg.StripeSecretKey, cfg.StripePriceID, cfg.StripeGBMonthPriceID, cfg.AppBaseURL, cfg.StripeLivemode)
		if err != nil {
			log.Fatalf("init stripe billing client for metering: %v", err)
		}
		validateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = reporter.ValidateConfiguration(validateCtx, cfg.StripeMeterID, cfg.StripeGBMonthMeterID)
		cancel()
		if err != nil {
			log.Fatalf("validate stripe billing configuration for metering: %v", err)
		}
	}

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

	// Monthly usage metering: the only place recording-hours are reported to Stripe.
	// Gated on billingEnabled (same secret+webhook+price gate as capture), so free
	// mode never charges. Runs under the same restart-with-backoff loop.
	if billingEnabled {
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

	// Managed-storage retention/release: after the grace period, stopped-payers'
	// managed clips are RELEASED (billing stops + org no longer sees them) while
	// their R2 objects are RETAINED (DENIZ policy: recorded content is never
	// hard-deleted). Gated on billing. Runs under the same restart-with-backoff
	// loop. Never touches BYO clips and never deletes any R2 object.
	if billingEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWithBackoff(ctx, "managed storage release", restartDelay, func(ctx context.Context) error {
				return runManagedRelease(ctx, pool)
			})
		}()
	} else {
		log.Printf("recorder-control: managed-storage release not started (billing disabled).")
	}

	// Clip transfer: async "send my recorded clip to my own S3 bucket". A
	// background COPY (streamed GET source -> PUT target) leased from
	// clip_transfer_jobs. It needs the storage credential cipher to decrypt both
	// the source and target destination secrets; if STORAGE_CRED_KEY is unset the
	// loop logs one warning and idles (runClipTransfer handles the nil cipher), so
	// the control process never crashes for lack of a key. Runs under the same
	// restart-with-backoff loop.
	transferCipher := mustBuildStorageCipher(cfg)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runWithBackoff(ctx, "clip transfer", restartDelay, func(ctx context.Context) error {
			return runClipTransfer(ctx, pool, transferCipher)
		})
	}()

	wg.Wait()
}

// runRecorderControlDedicatedCanary is an explicit operator command, not part
// of the long-running autoscaler. It provisions only after a typed DB fence is
// created and never changes the shared pool defaults.
func runRecorderControlDedicatedCanary(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl recorder-control dedicated-canary provision|status|release")
	}
	switch args[0] {
	case "provision":
		fs := flag.NewFlagSet("recorder-control dedicated-canary provision", flag.ExitOnError)
		recordingID := fs.Int64("recording-id", 0, "explicit disposable active cloud recording id")
		owner := fs.String("owner", "", "owner fence value")
		ttl := fs.Duration("ttl", 8*time.Hour, "reservation lifetime")
		ack := fs.Bool("ack-disposable", false, "acknowledge that this recording is disposable and not Fine-potential")
		_ = fs.Parse(args[1:])
		if !*ack {
			log.Fatalf("--ack-disposable is required; protected recordings are not admitted")
		}
		pool := mustOpenPool(ctx, cfg)
		defer pool.Close()
		doClient, err := dropletpool.NewGodoClient(cfg.DOAPIToken)
		if err != nil {
			log.Fatalf("init DigitalOcean client: %v", err)
		}
		operatorID, err := dropletpool.NewStore(pool).ResolveOperatorAccount(ctx, cfg.DropletPoolOperatorEmail)
		if err != nil {
			log.Fatalf("resolve recorder operator: %v", err)
		}
		res, err := dropletpool.ProvisionDedicatedCanary(ctx, pool, doClient, dropletpool.DedicatedCanarySpec{
			RecordingID: *recordingID, Owner: *owner, TTL: *ttl, OperatorAccountID: operatorID,
			Region: cfg.DropletPoolRegion, Size: cfg.DropletPoolSize, Image: cfg.DropletPoolImage,
			SSHKey: cfg.DropletPoolSSHKey, ProjectID: cfg.DropletPoolProjectID, FirewallID: cfg.DropletPoolFirewallID,
			BackendAPIURL: cfg.DropletPoolBackendAPIURL, HeartbeatSec: cfg.RecordingWorkerHeartbeatSec,
			PollSec: cfg.RecordingWorkerPollSec, RepoURL: cfg.DropletPoolRepoURL, RepoRef: cfg.DropletPoolRepoRef,
			BuildSHA: cfg.DropletPoolBuildSHA, RepoCloneToken: cfg.DropletPoolRepoCloneToken,
		})
		if err != nil {
			log.Fatalf("provision dedicated canary: %v", err)
		}
		fmt.Printf("reservation_id=%s recording_id=%d worker_name=%s expires_at=%s state=%s\n", res.ID, res.RecordingID, res.WorkerName, res.ExpiresAt.UTC().Format(time.RFC3339), res.State)
	case "status":
		fs := flag.NewFlagSet("recorder-control dedicated-canary status", flag.ExitOnError)
		reservationID := fs.String("reservation-id", "", "reservation UUID")
		_ = fs.Parse(args[1:])
		id, err := uuid.Parse(strings.TrimSpace(*reservationID))
		if err != nil {
			log.Fatalf("invalid --reservation-id: %v", err)
		}
		pool := mustOpenPool(ctx, cfg)
		defer pool.Close()
		res, err := dropletpool.NewStore(pool).DedicatedCanaryStatus(ctx, id)
		if err != nil {
			log.Fatalf("dedicated canary status: %v", err)
		}
		fmt.Printf("reservation_id=%s recording_id=%d worker_name=%s expires_at=%s state=%s worker_state=%s worker_ready=%t\n", res.ID, res.RecordingID, res.WorkerName, res.ExpiresAt.UTC().Format(time.RFC3339), res.State, res.WorkerState, res.WorkerReady)
	case "release":
		fs := flag.NewFlagSet("recorder-control dedicated-canary release", flag.ExitOnError)
		reservationID := fs.String("reservation-id", "", "reservation UUID")
		owner := fs.String("owner", "", "owner fence value")
		failed := fs.Bool("failed", false, "record failed setup")
		_ = fs.Parse(args[1:])
		if strings.TrimSpace(*owner) == "" {
			log.Fatalf("--owner is required")
		}
		id, err := uuid.Parse(strings.TrimSpace(*reservationID))
		if err != nil {
			log.Fatalf("invalid --reservation-id: %v", err)
		}
		pool := mustOpenPool(ctx, cfg)
		defer pool.Close()
		var res dropletpool.DedicatedCanaryReservation
		if *failed {
			if err := dropletpool.NewStore(pool).FailDedicatedCanaryReservation(ctx, id, *owner); err != nil {
				log.Fatalf("fail dedicated canary reservation: %v", err)
			}
			fmt.Printf("reservation_id=%s state=failed\n", id)
			return
		}
		res, err = dropletpool.NewStore(pool).ReleaseDedicatedCanary(ctx, id, *owner)
		if err != nil {
			log.Fatalf("release dedicated canary reservation: %v", err)
		}
		fmt.Printf("reservation_id=%s state=%s worker_name=%s\n", res.ID, res.State, res.WorkerName)
	default:
		log.Fatalf("unknown dedicated-canary subcommand %q", args[0])
	}
}

// mustBuildStorageCipher builds the storage credential cipher (AES-256-GCM) from
// STORAGE_CRED_KEY, mirroring how the API server constructs s.secrets. An unset
// key returns nil (not a fatal error): the clip-transfer worker treats a nil
// cipher as "idle", since it cannot decrypt any destination secret. A present
// but invalid key is fatal, because it means a misconfiguration the operator
// must fix.
func mustBuildStorageCipher(cfg config.Config) *secretbox.Cipher {
	key := strings.TrimSpace(cfg.StorageCredKey)
	if key == "" {
		return nil
	}
	cipher, err := secretbox.NewFromBase64Key(key)
	if err != nil {
		log.Fatalf("init storage credential cipher: %v", err)
	}
	return cipher
}

// runRecorderControlDrain flips one active droplet to draining so the running
// control service destroys it and the autoscaler replaces it from current main.
//
// This verb exists because a busy droplet may never recycle on its own. Scale-down
// is not "idle past the grace, therefore drained": Decide additionally requires the
// droplet to be SURPLUS to forecast demand, so standing demand that keeps
// required == live pins the existing droplets indefinitely. That matters because
// droplets clone DROPLET_POOL_REPO_REF and build at provision time and never update
// in place, so a merged capture fix reaches cloud ONLY as droplets recycle. Without
// a way to force a roll the operator is left waiting for something that will not
// happen, and the alternative -- hand-editing recorder_droplets.state -- puts a
// second writer on state the pool controller owns.
//
// So this deliberately destroys nothing itself. It only flips the one state
// transition the controller already acts on: the cloud lease query refuses new jobs
// to a draining droplet (it locks on state IN ('provisioning','active')),
// progressDrains destroys the droplet once it holds no live lease or once
// DROPLET_POOL_DRAIN_TIMEOUT_SEC elapses, and Decide provisions the replacement.
// The control service stays the only writer that talks to DigitalOcean.
func runRecorderControlDrain(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recorder-control drain", flag.ExitOnError)
	force := fs.Bool("force", false, "drain even while the droplet holds live leases, interrupting those capture windows")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		log.Fatalf("usage: stoaramactl recorder-control drain [-force] <droplet-id>")
	}
	id, err := strconv.ParseInt(strings.TrimSpace(fs.Arg(0)), 10, 64)
	if err != nil {
		log.Fatalf("droplet id must be an integer: %v", err)
	}

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()

	// Read the droplet, count its leases, and flip the state in ONE transaction,
	// holding a row lock on the droplet throughout. Otherwise a worker can lease a
	// job in the gap between "does it hold live leases?" and the state flip, and a
	// drain the operator was told was free would interrupt that window once the
	// drain timeout expired. The cloud lease path takes the same row lock and
	// re-reads state IN ('provisioning','active') under it, so a lease racing this
	// transaction blocks here and then correctly finds the droplet draining.
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatalf("begin drain tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		name      string
		state     string
		createdAt time.Time
	)
	if err := tx.QueryRow(ctx, `
		SELECT name, state, created_at FROM recorder_droplets WHERE id=$1 FOR UPDATE
	`, id).Scan(&name, &state, &createdAt); err != nil {
		log.Fatalf("load droplet %d: %v", id, err)
	}
	// Only an active droplet has a drain to begin. Re-draining one that is already
	// draining would restamp drain_started_at and push its forced-destroy deadline
	// out, which is the opposite of what an operator forcing a roll wants.
	if state != "active" {
		log.Fatalf("droplet %d (%s) is %s, not active; only an active droplet can be drained", id, name, state)
	}

	// Report the cost before paying it. A draining droplet that still holds live
	// leases is destroyed anyway once the drain timeout elapses, dropping those
	// windows, so that has to be an explicit choice rather than a surprise.
	rows, err := tx.Query(ctx, `
		SELECT recording_id FROM recording_jobs
		WHERE lease_owner=$1 AND status='leased' AND lease_expires_at > now()
		ORDER BY recording_id
	`, name)
	if err != nil {
		log.Fatalf("list live leases for %s: %v", name, err)
	}
	var live []int64
	for rows.Next() {
		var recordingID int64
		if err := rows.Scan(&recordingID); err != nil {
			rows.Close()
			log.Fatalf("scan live lease: %v", err)
		}
		live = append(live, recordingID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Fatalf("list live leases for %s: %v", name, err)
	}

	if len(live) > 0 && !*force {
		log.Fatalf("droplet %d (%s) still holds %d live lease(s) for recordings %v; they are interrupted when the drain times out after %ds. Re-run with -force to accept that, or wait for those windows to close.",
			id, name, len(live), live, cfg.DropletPoolDrainTimeoutSec)
	}

	// This mirrors dropletpool.Store.MarkDraining, which cannot be reused here
	// because the store is constructed over a *pgxpool.Pool and so cannot join
	// this transaction -- and the row lock is the whole point of the transaction.
	if _, err := tx.Exec(ctx, `
		UPDATE recorder_droplets SET state='draining', drain_started_at=now(), updated_at=now() WHERE id=$1
	`, id); err != nil {
		log.Fatalf("mark droplet %d draining: %v", id, err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("commit drain for droplet %d: %v", id, err)
	}

	log.Printf("droplet %d (%s, provisioned %s) marked draining; it takes no new jobs from now on.",
		id, name, createdAt.UTC().Format(time.RFC3339))
	if len(live) > 0 {
		log.Printf("it still holds %d live lease(s) for recordings %v; the control service destroys it once they finish, or forcibly after %ds.",
			len(live), live, cfg.DropletPoolDrainTimeoutSec)
	} else {
		log.Printf("it holds no live lease, so the control service destroys it on its next tick.")
	}
	log.Printf("the replacement provisions from DROPLET_POOL_REPO_REF at that point; confirm the new row's created_at is after the merge you are rolling out.")
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
		MaxScaleUpBatch:   cfg.DropletPoolMaxScaleUpBatch,
		Region:            cfg.DropletPoolRegion,
		Size:              cfg.DropletPoolSize,
		Image:             cfg.DropletPoolImage,
		SSHKey:            cfg.DropletPoolSSHKey,
		ProjectID:         cfg.DropletPoolProjectID,
		FirewallID:        cfg.DropletPoolFirewallID,
		BackendAPIURL:     cfg.DropletPoolBackendAPIURL,
		BuildSHA:          cfg.DropletPoolBuildSHA,
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
