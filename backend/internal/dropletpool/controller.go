package dropletpool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config configures the autoscaler. It runs on the dedicated single-instance
// control service, so there is no leader election.
type Config struct {
	OperatorAccountID int64
	BillingEnabled    bool

	TickInterval      time.Duration
	Lookahead         time.Duration
	Capacity          int
	ProvisionLead     time.Duration
	ProvisionTimeout  time.Duration
	IdleGrace         time.Duration
	DrainTimeout      time.Duration
	ScaleUpCooldown   time.Duration
	ScaleDownCooldown time.Duration
	Min               int
	Max               int
	MaxScaleUpBatch   int

	Region     string
	Size       string
	Image      string
	SSHKey     string
	ProjectID  string
	FirewallID string

	BackendAPIURL  string
	BuildSHA       string
	HeartbeatSec   int
	PollSec        int
	RepoURL        string
	RepoRef        string
	RepoCloneToken string

	// ReclaimLeases makes the controller run expired-lease reclaim at the top of
	// each tick. Set true only when the scheduler is NOT running on this service
	// (otherwise the scheduler owns reclaim, C8).
	ReclaimLeases bool
}

// Controller is the droplet-pool autoscaler.
type Controller struct {
	store *Store
	do    DOClient
	cfg   Config

	fleetReadFailureSince time.Time
	fleetReadLastFailure  time.Time
	fleetReadSuccessSince time.Time
	fleetReadFailures     int
	fleetReadAlerted      bool
}

const fleetReadSustainedFailureThreshold = 5 * time.Minute
const fleetReadRecoveryDwell = 5 * time.Minute

var errFleetRead = errors.New("fleet read failed")

// NewController builds the autoscaler. doClient is the real godo client in
// production (or a fake in tests).
func NewController(pool *pgxpool.Pool, doClient DOClient, cfg Config) *Controller {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 30 * time.Second
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = 1
	}
	if cfg.MaxScaleUpBatch <= 0 {
		cfg.MaxScaleUpBatch = 1
	}
	return &Controller{store: NewStore(pool), do: doClient, cfg: cfg}
}

// Run drives the autoscaler tick loop until ctx is canceled.
func (c *Controller) Run(ctx context.Context) error {
	log.Printf("droplet pool start tick=%s lookahead=%s capacity=%d min=%d max=%d lead=%s idle_grace=%s drain_timeout=%s reclaim=%t",
		c.cfg.TickInterval, c.cfg.Lookahead, c.cfg.Capacity, c.cfg.Min, c.cfg.Max,
		c.cfg.ProvisionLead, c.cfg.IdleGrace, c.cfg.DrainTimeout, c.cfg.ReclaimLeases)
	ticker := time.NewTicker(c.cfg.TickInterval)
	defer ticker.Stop()
	if err := c.tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("droplet pool first tick error: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.tick(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				log.Printf("droplet pool tick error: %v", err)
			}
		}
	}
}

// tick runs one reconcile + scale pass. Each phase is best-effort and logs its
// own errors; a single failed DO call must not wedge the loop.
func (c *Controller) tick(ctx context.Context) error {
	now := time.Now().UTC()

	// (0) reclaim expired leases when the scheduler is not co-running (C8).
	if c.cfg.ReclaimLeases {
		if err := c.store.ReclaimExpiredLeases(ctx); err != nil {
			log.Printf("droplet pool: reclaim leases: %v", err)
		}
	}

	// (1) reconcile against the live DO fleet + reap stuck provisioning rows.
	// No scale, drain, or destroy decision is safe without a current complete
	// fleet read. Return before any decision code when reconciliation fails; the
	// next ordinary tick retries from the read boundary.
	if err := c.reconcile(ctx, now); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		if !errors.Is(err, errFleetRead) {
			return err
		}
		if c.noteFleetReadFailure(now) {
			log.Printf("droplet pool: CRITICAL fleet reconciliation degraded for %s across %d failed ticks; provider mutations remain paused: %v",
				now.Sub(c.fleetReadFailureSince).Truncate(time.Second), c.fleetReadFailures, err)
		} else {
			log.Printf("droplet pool: reconcile: %v", err)
		}
		log.Printf("droplet pool: fleet truth is stale; skipping scale-up, drain, and destroy progression for this tick")
		return nil
	} else {
		alert, outageDuration, failures, recovered := c.noteFleetReadSuccess(now)
		if alert {
			log.Printf("droplet pool: CRITICAL fleet reconciliation intermittently degraded for %s across %d failed ticks; current fleet read is fresh but incident remains open",
				outageDuration.Truncate(time.Second), failures)
		}
		if recovered {
			log.Printf("droplet pool: fleet reconciliation stable for %s; incident recovered after %s and %d failed ticks",
				fleetReadRecoveryDwell, outageDuration.Truncate(time.Second), failures)
		}
	}

	// Refresh per-droplet idle tracking before deciding.
	if err := c.refreshIdle(ctx); err != nil {
		log.Printf("droplet pool: refresh idle: %v", err)
	}
	// Forecast demand.
	forecast, err := ForecastDemand(ctx, c.poolPool(), c.cfg.BillingEnabled, now, c.cfg.Lookahead)
	if err != nil {
		return err
	}

	live, err := c.store.CountLive(ctx)
	if err != nil {
		return err
	}
	// Spend tripwire (S-cap): the live count must never exceed the hard cap. The
	// write-ahead provisioning row + per-tick clamp make this impossible; if it ever
	// trips it means a reconcile/counting bug is leaking billable droplets past the
	// cap, so log it loudly. This is a pure invariant check, not a scale action.
	if live > c.cfg.Max {
		log.Printf("droplet pool: SPEND TRIPWIRE live=%d exceeds hard cap max=%d (reconcile/counting bug; investigate immediately)", live, c.cfg.Max)
	}
	active, err := c.store.ListByStates(ctx, "active")
	if err != nil {
		return err
	}
	idleEligible := IdleEligibleDroplets(active, now, c.cfg.IdleGrace)
	ps, err := c.store.LoadPoolState(ctx)
	if err != nil {
		return err
	}

	decision := Decide(DecisionParams{
		Now:               now,
		Forecast:          forecast,
		Live:              live,
		IdleEligible:      len(idleEligible),
		PoolState:         ps,
		Capacity:          c.cfg.Capacity,
		Min:               c.cfg.Min,
		Max:               c.cfg.Max,
		MaxScaleUpBatch:   c.cfg.MaxScaleUpBatch,
		ProvisionLead:     c.cfg.ProvisionLead,
		IdleGrace:         c.cfg.IdleGrace,
		ScaleUpCooldown:   c.cfg.ScaleUpCooldown,
		ScaleDownCooldown: c.cfg.ScaleDownCooldown,
	})

	// Cap-hit alert: demand wants more droplets than DROPLET_POOL_MAX allows. The
	// create-time capacity preflight already rejects an over-cap schedule up front,
	// so this fires only if standing demand drifts past the cap; surface it as a
	// loud persistent WARNING (the control worker's alerting surface; it has no
	// email sender) with the shortfall so the operator raises DROPLET_POOL_MAX
	// instead of clips silently missing their freshness deadline.
	if decision.ScaleUpBlockedByCap {
		log.Printf("droplet pool: CAP HIT scale-up wanted but at hard cap max=%d live=%d peak=%d need_droplets_over_cap=%d ceiling=%d; raise DROPLET_POOL_MAX",
			c.cfg.Max, live, forecast.PeakConcurrent, decision.CapShortfall, c.cfg.Max*c.cfg.Capacity)
	}
	for i := 0; i < decision.ScaleUpCount; i++ {
		if err := c.scaleUp(ctx, now, i); err != nil {
			log.Printf("droplet pool: scale up: %v", err)
			break // a failing DO create will likely fail again this tick; retry next tick
		}
	}
	requiredCapacity := forecast.PeakConcurrent
	if minCapacity := c.cfg.Min * c.cfg.Capacity; requiredCapacity < minCapacity {
		requiredCapacity = minCapacity
	}
	rollingBuild, err := c.rolloutBuild(ctx, now, live+decision.ScaleUpCount, decision.ScaleUpCount, requiredCapacity)
	if err != nil {
		log.Printf("droplet pool: build rollout: %v", err)
	}
	if !rollingBuild && decision.DrainCount > 0 {
		c.beginDrains(ctx, now, idleEligible, decision.DrainCount)
	}

	// (3b) progress any draining droplets toward destruction.
	c.progressDrains(ctx, now)
	return nil
}

// noteFleetReadFailure latches one provider-read incident across ticks. It
// returns true exactly once when the incident crosses the sustained threshold,
// keeping the ordinary per-tick error useful without emitting CRITICAL noise on
// every subsequent tick.
func (c *Controller) noteFleetReadFailure(now time.Time) bool {
	if c.fleetReadFailureSince.IsZero() {
		c.fleetReadFailureSince = now
	}
	c.fleetReadLastFailure = now
	c.fleetReadSuccessSince = time.Time{}
	c.fleetReadFailures++
	if c.fleetReadAlerted || now.Sub(c.fleetReadFailureSince) < fleetReadSustainedFailureThreshold {
		return false
	}
	c.fleetReadAlerted = true
	return true
}

// noteFleetReadSuccess keeps an intermittent incident open until a full healthy
// dwell has elapsed since its latest failure. alert is true exactly once if the
// incident crosses the sustained threshold on a successful tick between
// failures; recovered becomes true only after the healthy dwell.
func (c *Controller) noteFleetReadSuccess(now time.Time) (alert bool, duration time.Duration, failures int, recovered bool) {
	if c.fleetReadFailureSince.IsZero() {
		return false, 0, 0, false
	}
	duration = now.Sub(c.fleetReadFailureSince)
	failures = c.fleetReadFailures
	if c.fleetReadSuccessSince.IsZero() {
		c.fleetReadSuccessSince = now
	}
	if !c.fleetReadAlerted && duration >= fleetReadSustainedFailureThreshold && failures >= 2 {
		c.fleetReadAlerted = true
		alert = true
	}
	if now.Sub(c.fleetReadSuccessSince) >= fleetReadRecoveryDwell {
		c.fleetReadFailureSince = time.Time{}
		c.fleetReadLastFailure = time.Time{}
		c.fleetReadSuccessSince = time.Time{}
		c.fleetReadFailures = 0
		c.fleetReadAlerted = false
		return alert, duration, failures, true
	}
	return alert, duration, failures, false
}

// poolPool exposes the underlying pgx pool for the forecast query.
func (c *Controller) poolPool() *pgxpool.Pool {
	return c.store.pool
}

// reconcile diffs the DB against the live DO fleet and reaps orphans + stuck
// provisioning rows.
func (c *Controller) reconcile(ctx context.Context, now time.Time) error {
	fleet, err := c.do.ListDropletsByName(ctx, c.cfg.ProjectID, NamePrefix)
	if err != nil {
		return fmt.Errorf("%w: list DO fleet: %w", errFleetRead, err)
	}
	liveRows, err := c.store.ListByStates(ctx, "provisioning", "active", "draining", "destroying")
	if err != nil {
		return err
	}
	plan := ReconcileOrphans(fleet, liveRows, now, c.cfg.ProvisionTimeout)
	presentDropletIDs := make(map[int64]struct{}, len(fleet))
	for _, d := range fleet {
		presentDropletIDs[d.ID] = struct{}{}
	}

	// Adopt: bind a DO id to an existing write-ahead row that lost its id, or
	// (rarely) create a row for a prefixed droplet with no row. We only bind ids
	// to existing rows here; a prefixed droplet with no row at all and younger than
	// the timeout is left for a later tick (it will either gain its id via the
	// in-flight create or age into DestroyOrphan).
	for _, d := range plan.AdoptByName {
		row, lookupErr := c.findRowByName(ctx, d.Name)
		if lookupErr != nil {
			log.Printf("droplet pool: adopt lookup %s: %v", d.Name, lookupErr)
			continue
		}
		if row == nil {
			continue
		}
		if row.DODropletID == nil {
			if err := c.store.SetDropletID(ctx, row.ID, d.ID, d.IP); err != nil {
				log.Printf("droplet pool: adopt bind id %s: %v", d.Name, err)
			}
		}
	}

	// Destroy genuinely-leaked prefixed droplets (no DB row, past timeout).
	for _, d := range plan.DestroyOrphan {
		log.Printf("droplet pool: destroying orphan droplet name=%s do_id=%d", d.Name, d.ID)
		if err := c.do.DeleteDroplet(ctx, d.ID); err != nil {
			log.Printf("droplet pool: destroy orphan %s: %v", d.Name, err)
		}
	}

	// Reconcile DB rows whose DO droplet vanished: revoke their token and mark
	// destroyed so they stop counting against the cap.
	for _, r := range plan.MissingFromDO {
		retired, retireErr := c.store.BeginDestroyIfIdle(ctx, r.ID)
		if retireErr != nil {
			log.Printf("droplet pool: CRITICAL cannot atomically verify idle before retiring missing droplet id=%d name=%s; teardown skipped: %v", r.ID, r.Name, retireErr)
			continue
		}
		if !retired {
			log.Printf("droplet pool: missing droplet id=%d name=%s is leased or no longer retireable; teardown skipped", r.ID, r.Name)
			continue
		}
		log.Printf("droplet pool: DB droplet id=%d name=%s missing from DO; reconciling to destroyed", r.ID, r.Name)
		c.revokeAndDestroyRow(ctx, r.ID)
	}

	// A crash or provider error after the atomic idle->destroying transition must
	// not strand a billable droplet. Resume that durable state idempotently on
	// every reconcile tick.
	for _, r := range liveRows {
		if r.State != "destroying" || r.DODropletID == nil {
			continue
		}
		if _, present := presentDropletIDs[*r.DODropletID]; !present {
			continue // MissingFromDO finalized it above.
		}
		if err := c.do.DeleteDroplet(ctx, *r.DODropletID); err != nil {
			log.Printf("droplet pool: resume destroying droplet id=%d name=%s: %v", r.ID, r.Name, err)
			continue
		}
		c.revokeAndDestroyRow(ctx, r.ID)
	}

	// Reap stuck provisioning rows past the provision timeout with no DO id (the
	// CreateDroplet never landed) or whose DO droplet was found above and is older
	// than the timeout without ever going active (SRE-stuck).
	for _, r := range liveRows {
		if r.State != "provisioning" {
			continue
		}
		if now.Sub(r.CreatedAt) < c.cfg.ProvisionTimeout {
			continue
		}
		busy, busyErr := c.store.HasInflightJob(ctx, r.Name)
		if busyErr != nil {
			// Lease truth is required before teardown. A DB read failure must
			// retain the worker and its credential, then retry next tick.
			log.Printf("droplet pool: CRITICAL cannot verify leases for overdue provisioning droplet id=%d name=%s; teardown skipped: %v", r.ID, r.Name, busyErr)
			continue
		}
		if busy {
			// A live lease proves the worker is already doing production work,
			// even when a build mismatch kept the rollout readiness gate from
			// promoting its pool row. Never revoke or delete underneath it.
			providerPresent := false
			if r.DODropletID != nil {
				_, providerPresent = presentDropletIDs[*r.DODropletID]
			}
			if providerPresent && r.LastSeenAt != nil && now.Sub(*r.LastSeenAt) <= c.workerReadyWindow() {
				promoted, err := c.store.MarkActive(ctx, r.ID)
				if err != nil {
					log.Printf("droplet pool: CRITICAL preserve leased provisioning droplet id=%d name=%s but promote active failed: %v", r.ID, r.Name, err)
					continue
				}
				if promoted {
					log.Printf("droplet pool: preserving leased provisioning droplet id=%d name=%s as active; ordinary stale-build drain will wait for idle", r.ID, r.Name)
				}
			} else {
				log.Printf("droplet pool: CRITICAL overdue provisioning droplet id=%d name=%s holds a live lease but provider identity or heartbeat is unavailable; teardown skipped", r.ID, r.Name)
			}
			continue
		}
		retired, retireErr := c.store.BeginDestroyIfIdle(ctx, r.ID)
		if retireErr != nil {
			log.Printf("droplet pool: CRITICAL cannot atomically verify idle before destroying overdue provisioning droplet id=%d name=%s; teardown skipped: %v", r.ID, r.Name, retireErr)
			continue
		}
		if !retired {
			log.Printf("droplet pool: overdue provisioning droplet id=%d name=%s became leased or changed state; teardown skipped", r.ID, r.Name)
			continue
		}
		log.Printf("droplet pool: provisioning droplet id=%d name=%s stuck past timeout; failing + destroying", r.ID, r.Name)
		if r.DODropletID != nil {
			if err := c.do.DeleteDroplet(ctx, *r.DODropletID); err != nil {
				log.Printf("droplet pool: destroy stuck droplet %s: %v", r.Name, err)
			}
		}
		if err := c.store.MarkFailed(ctx, r.ID, "provision timed out"); err != nil {
			log.Printf("droplet pool: mark stuck droplet failed %s: %v", r.Name, err)
		}
		nodeID, nodeTokenID, _ := c.store.NodeBinding(ctx, r.ID)
		if err := c.store.RevokeNodeToken(ctx, nodeTokenID, nodeID); err != nil {
			log.Printf("droplet pool: revoke stuck droplet token %s: %v", r.Name, err)
		}
	}

	// Promote provisioning rows whose worker has reported in (alive), not merely
	// whose DO droplet powered on.
	c.promoteActive(ctx, now, fleet)
	return nil
}

// workerReadyWindow is how fresh a droplet's last_seen_at must be for its worker
// to count as alive. The worker heartbeats every HeartbeatSec; allow a few missed
// beats plus clock skew, with a sane floor for tiny intervals.
func (c *Controller) workerReadyWindow() time.Duration {
	w := time.Duration(c.cfg.HeartbeatSec) * time.Second * 3
	if w < 45*time.Second {
		w = 45 * time.Second
	}
	return w
}

// promoteActive flips a provisioning droplet to active only once its worker is
// proven alive (a fresh droplet-heartbeat last_seen_at), not merely once DO
// reports the instance powered on. Power-on (~30-60s) precedes worker readiness
// by the whole cloud-init build (apt + clone + build), which on a stock image can
// be minutes and exceed ProvisionLead; gating on DO status alone would mark a
// droplet active before its worker can serve a job. A provisioning row that ages
// past ProvisionLead without becoming worker-ready logs a best-effort lead-miss
// WARNING (the first fire after idle may land before the worker is up).
func (c *Controller) promoteActive(ctx context.Context, now time.Time, fleet []DODroplet) {
	byID := make(map[int64]DODroplet, len(fleet))
	for _, d := range fleet {
		byID[d.ID] = d
	}
	provisioning, err := c.store.ListByStates(ctx, "provisioning")
	if err != nil {
		log.Printf("droplet pool: list provisioning: %v", err)
		return
	}
	window := c.workerReadyWindow()
	for _, r := range provisioning {
		workerReady := r.LastSeenAt != nil && now.Sub(*r.LastSeenAt) <= window && workerBuildReady(c.cfg.BuildSHA, r.BuildSHA)
		if !workerReady {
			// Best-effort lead-miss warning: a provisioning row older than the
			// provision lead whose worker has not reported in yet means the first
			// fire after idle can land before the worker is alive (the job then waits
			// pending until a later poll). Reconcile still hard-reaps at the longer
			// ProvisionTimeout.
			if now.Sub(r.CreatedAt) > c.cfg.ProvisionLead {
				log.Printf("droplet pool: WARNING provision lead missed: droplet id=%d name=%s worker not ready after %s (lead=%s); jobs may wait until it reports in",
					r.ID, r.Name, now.Sub(r.CreatedAt).Truncate(time.Second), c.cfg.ProvisionLead)
			}
			continue
		}
		// Worker is alive. Refresh the recorded IP from the DO record if available,
		// then promote.
		if r.DODropletID != nil {
			if d, ok := byID[*r.DODropletID]; ok && d.IP != "" && d.IP != r.IPAddress {
				_ = c.store.SetDropletID(ctx, r.ID, *r.DODropletID, d.IP)
			}
		}
		if _, err := c.store.MarkActive(ctx, r.ID); err != nil {
			log.Printf("droplet pool: promote active %s: %v", r.Name, err)
			continue
		}
		log.Printf("droplet pool: droplet id=%d name=%s active (worker ready)", r.ID, r.Name)
	}
}

func workerBuildReady(desired, reported string) bool {
	desired = strings.ToLower(strings.TrimSpace(desired))
	return desired == "" || strings.ToLower(strings.TrimSpace(reported)) == desired
}

func shouldDrainStaleBuild(desired, reported string, busy bool) bool {
	return !busy && !workerBuildReady(desired, reported)
}

// drainIdleStaleBuilds retires old binaries without interrupting an active
// recording. Empty build_sha is intentionally stale when the controller has a
// desired build, so workers deployed before the handshake are rolled forward.
func (c *Controller) rolloutBuild(ctx context.Context, now time.Time, live, nextBatchIndex, requiredCapacity int) (bool, error) {
	if strings.TrimSpace(c.cfg.BuildSHA) == "" {
		return false, nil
	}
	workers, err := c.store.ListByStates(ctx, "active", "provisioning")
	if err != nil {
		return false, err
	}
	hasStale := false
	hasCurrentActive := false
	hasCurrentPending := false
	for _, d := range workers {
		// A provisioning row is already a write-ahead replacement reservation.
		// Legacy rows created before build_sha tracking may be blank; treating them
		// as absent would provision another droplet every tick until they heartbeat.
		// Reconcile independently reaps genuinely stuck rows at ProvisionTimeout.
		if d.State == "provisioning" {
			hasCurrentPending = true
			continue
		}
		if workerBuildReady(c.cfg.BuildSHA, d.BuildSHA) {
			hasCurrentPending = true
			hasCurrentActive = true
		} else {
			hasStale = true
		}
	}
	if !hasStale {
		return false, nil
	}
	// Surge a pinned replacement before retiring the last old binary. This avoids
	// a cold-capacity hole during rollout while respecting the hard spend cap.
	if !hasCurrentPending {
		if live >= c.cfg.Max {
			return true, fmt.Errorf("stale workers present but hard cap %d leaves no replacement slot", c.cfg.Max)
		}
		if err := c.scaleUp(ctx, now, nextBatchIndex); err != nil {
			return true, fmt.Errorf("provision build replacement: %w", err)
		}
		return true, nil
	}
	if !hasCurrentActive {
		return true, nil
	}
	draining, err := c.store.ListByStates(ctx, "draining")
	if err != nil {
		return true, err
	}
	active, err := c.store.ListByStates(ctx, "active")
	if err != nil {
		return true, err
	}
	activeCapacity := 0
	for _, d := range active {
		activeCapacity += d.Capacity
	}
	for _, d := range active {
		if workerBuildReady(c.cfg.BuildSHA, d.BuildSHA) {
			continue
		}
		if !canDrainStale(activeCapacity, d.Capacity, requiredCapacity, len(draining)) {
			continue
		}
		drained, err := c.store.MarkDrainingIfIdle(ctx, d.ID)
		if err != nil {
			return true, fmt.Errorf("mark stale droplet %s draining: %w", d.Name, err)
		}
		if drained {
			log.Printf("droplet pool: draining idle stale worker id=%d name=%s build=%q desired=%q", d.ID, d.Name, d.BuildSHA, c.cfg.BuildSHA)
			break // roll one at a time so current capacity remains stable
		}
	}
	return true, nil
}

func canDrainStale(activeCapacity, candidateCapacity, requiredCapacity, drainingCount int) bool {
	// Finish one replacement transition before starting another, and never
	// retire capacity the current demand forecast needs.
	return drainingCount == 0 && candidateCapacity > 0 && activeCapacity-candidateCapacity >= requiredCapacity
}

// refreshIdle stamps/clears idle_since on active droplets based on whether they
// currently hold a live leased job.
func (c *Controller) refreshIdle(ctx context.Context) error {
	active, err := c.store.ListByStates(ctx, "active")
	if err != nil {
		return err
	}
	for _, d := range active {
		busy, err := c.store.HasInflightJob(ctx, d.Name)
		if err != nil {
			log.Printf("droplet pool: inflight check %s: %v", d.Name, err)
			continue
		}
		if err := c.store.SetIdleSince(ctx, d.ID, !busy); err != nil {
			log.Printf("droplet pool: idle stamp %s: %v", d.Name, err)
		}
	}
	return nil
}

// dropletName builds a unique recorder-droplet name for one scale-up. The tick
// timestamp (UnixNano) makes it unique across ticks; the batch index makes it
// unique WITHIN a single tick's scale-up batch, where every iteration shares the
// same `now`. Without the index every droplet in a batch would collide on the
// recorder_droplets name UNIQUE index, so a batch of N could provision only 1.
func dropletName(now time.Time, batchIndex int) string {
	return fmt.Sprintf("%s%d-%d", NamePrefix, now.UnixNano(), batchIndex)
}

// scaleUp mints a per-droplet node token, writes the write-ahead provisioning row
// BEFORE the DO create (SRE-cap), creates+assigns+firewalls the droplet, then
// records its DO id. On any failure after the row is written it revokes the token
// and marks the row failed so the cap is not leaked. batchIndex is the droplet's
// position within this tick's scale-up batch; it keeps names unique within a
// single tick (where `now` is shared across the batch loop).
func (c *Controller) scaleUp(ctx context.Context, now time.Time, batchIndex int) error {
	name := dropletName(now, batchIndex)

	token, nodeID, nodeTokenID, err := c.store.MintNodeToken(ctx, c.cfg.OperatorAccountID, name)
	if err != nil {
		return fmt.Errorf("mint node token: %w", err)
	}

	rowID, err := c.store.InsertProvisioning(ctx, name, c.cfg.Region, c.cfg.Size, c.cfg.Capacity, nodeID, c.cfg.BuildSHA)
	if err != nil {
		// Roll back the token we just minted so it is not orphaned.
		_ = c.store.RevokeNodeToken(ctx, &nodeTokenID, &nodeID)
		return fmt.Errorf("write-ahead provisioning row: %w", err)
	}

	userData, err := BuildUserData(UserDataConfig{
		ServerID:       name,
		NodeToken:      token,
		BackendAPIURL:  c.cfg.BackendAPIURL,
		Capacity:       c.cfg.Capacity,
		HeartbeatSec:   c.cfg.HeartbeatSec,
		PollSec:        c.cfg.PollSec,
		RepoURL:        c.cfg.RepoURL,
		RepoRef:        c.cfg.RepoRef,
		BuildSHA:       c.cfg.BuildSHA,
		RepoCloneToken: c.cfg.RepoCloneToken,
	})
	if err != nil {
		c.failProvision(ctx, rowID, nodeID, nodeTokenID, fmt.Sprintf("build user data: %v", err))
		return err
	}

	droplet, err := c.do.CreateDroplet(ctx, CreateDropletInput{
		Name:       name,
		Region:     c.cfg.Region,
		Size:       c.cfg.Size,
		Image:      c.cfg.Image,
		SSHKey:     c.cfg.SSHKey,
		UserData:   userData,
		ProjectID:  c.cfg.ProjectID,
		FirewallID: c.cfg.FirewallID,
	})
	if err != nil {
		c.failProvision(ctx, rowID, nodeID, nodeTokenID, fmt.Sprintf("create droplet: %v", err))
		return err
	}
	if err := c.store.SetDropletID(ctx, rowID, droplet.ID, droplet.IP); err != nil {
		// The droplet exists but we failed to record its id. Reconcile will adopt it
		// by name (the write-ahead row exists), so do not destroy here.
		return fmt.Errorf("set droplet id (will reconcile by name): %w", err)
	}
	if err := c.store.StampScaleUp(ctx, now); err != nil {
		log.Printf("droplet pool: stamp scale up: %v", err)
	}
	log.Printf("droplet pool: provisioned droplet name=%s do_id=%d", name, droplet.ID)
	return nil
}

// failProvision marks a provisioning row failed and revokes its node token after
// a provisioning error so the spend cap and credential are not leaked.
func (c *Controller) failProvision(ctx context.Context, rowID, nodeID, nodeTokenID int64, reason string) {
	if err := c.store.MarkFailed(ctx, rowID, reason); err != nil {
		log.Printf("droplet pool: mark provision failed: %v", err)
	}
	if err := c.store.RevokeNodeToken(ctx, &nodeTokenID, &nodeID); err != nil {
		log.Printf("droplet pool: revoke failed-provision token: %v", err)
	}
}

// beginDrains flips up to n idle-eligible droplets to draining. The lease query
// then refuses them new jobs; progressDrains destroys them once drained.
func (c *Controller) beginDrains(ctx context.Context, now time.Time, idleEligible []Droplet, n int) {
	drained := 0
	for _, d := range idleEligible {
		if drained >= n {
			break
		}
		if err := c.store.MarkDraining(ctx, d.ID); err != nil {
			log.Printf("droplet pool: mark draining %s: %v", d.Name, err)
			continue
		}
		log.Printf("droplet pool: draining droplet id=%d name=%s", d.ID, d.Name)
		drained++
	}
	if drained > 0 {
		if err := c.store.StampScaleDown(ctx, now); err != nil {
			log.Printf("droplet pool: stamp scale down: %v", err)
		}
	}
}

// progressDrains destroys draining droplets that are fully drained (no live
// leased job) or whose drain has exceeded the bounded drain timeout (forced
// destroy, SRE-2). It reclaims any expired lease first so a stranded job is
// requeued before its droplet is destroyed.
func (c *Controller) progressDrains(ctx context.Context, now time.Time) {
	draining, err := c.store.ListByStates(ctx, "draining")
	if err != nil {
		log.Printf("droplet pool: list draining: %v", err)
		return
	}
	if len(draining) == 0 {
		return
	}
	// Reclaim expired leases before destroying, so a stranded job goes back to
	// pending rather than being lost when its droplet disappears.
	if err := c.store.ReclaimExpiredLeases(ctx); err != nil {
		log.Printf("droplet pool: reclaim before destroy: %v", err)
	}
	for _, d := range draining {
		busy, err := c.store.HasInflightJob(ctx, d.Name)
		if err != nil {
			log.Printf("droplet pool: drained check %s: %v", d.Name, err)
			continue
		}
		forced := d.DrainStartedAt != nil && now.Sub(*d.DrainStartedAt) >= c.cfg.DrainTimeout
		if busy && !forced {
			continue
		}
		if busy && forced {
			log.Printf("droplet pool: drain timeout exceeded for id=%d name=%s; forcing destroy", d.ID, d.Name)
		}
		c.destroyDraining(ctx, d)
	}
}

// destroyDraining deletes a draining droplet's DO instance, revokes its node
// token, and marks the row destroyed.
func (c *Controller) destroyDraining(ctx context.Context, d Droplet) {
	if err := c.store.MarkDestroying(ctx, d.ID); err != nil {
		log.Printf("droplet pool: mark destroying %s: %v", d.Name, err)
		return
	}
	if d.DODropletID != nil {
		if err := c.do.DeleteDroplet(ctx, *d.DODropletID); err != nil {
			log.Printf("droplet pool: delete droplet %s: %v", d.Name, err)
			return
		}
	}
	nodeID, nodeTokenID, _ := c.store.NodeBinding(ctx, d.ID)
	if err := c.store.RevokeNodeToken(ctx, nodeTokenID, nodeID); err != nil {
		log.Printf("droplet pool: revoke token %s: %v", d.Name, err)
	}
	if err := c.store.MarkDestroyed(ctx, d.ID); err != nil {
		log.Printf("droplet pool: mark destroyed %s: %v", d.Name, err)
		return
	}
	log.Printf("droplet pool: destroyed droplet id=%d name=%s", d.ID, d.Name)
}

// revokeAndDestroyRow reconciles a DB row whose DO droplet has vanished: revoke
// the token and mark the row destroyed (the DO instance is already gone).
func (c *Controller) revokeAndDestroyRow(ctx context.Context, rowID int64) {
	nodeID, nodeTokenID, _ := c.store.NodeBinding(ctx, rowID)
	if err := c.store.RevokeNodeToken(ctx, nodeTokenID, nodeID); err != nil {
		log.Printf("droplet pool: revoke vanished droplet token: %v", err)
	}
	if err := c.store.MarkDestroyed(ctx, rowID); err != nil {
		log.Printf("droplet pool: mark vanished droplet destroyed: %v", err)
	}
}

// findRowByName loads a recorder_droplets row by name in any live state.
func (c *Controller) findRowByName(ctx context.Context, name string) (*Droplet, error) {
	rows, err := c.store.ListByStates(ctx, "provisioning", "active", "draining", "destroying")
	if err != nil {
		return nil, err
	}
	for i := range rows {
		if rows[i].Name == name {
			return &rows[i], nil
		}
	}
	return nil, nil
}
