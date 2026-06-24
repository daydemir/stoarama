package dropletpool

import (
	"context"
	"errors"
	"fmt"
	"log"
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

	Region     string
	Size       string
	Image      string
	SSHKey     string
	ProjectID  string
	FirewallID string

	BackendAPIURL  string
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
}

// NewController builds the autoscaler. doClient is the real godo client in
// production (or a fake in tests).
func NewController(pool *pgxpool.Pool, doClient DOClient, cfg Config) *Controller {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 30 * time.Second
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = 1
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
	if err := c.reconcile(ctx, now); err != nil {
		log.Printf("droplet pool: reconcile: %v", err)
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
		ProvisionLead:     c.cfg.ProvisionLead,
		IdleGrace:         c.cfg.IdleGrace,
		ScaleUpCooldown:   c.cfg.ScaleUpCooldown,
		ScaleDownCooldown: c.cfg.ScaleDownCooldown,
	})

	if decision.ScaleUpBlockedByCap {
		log.Printf("droplet pool: scale-up wanted but at hard cap max=%d live=%d peak=%d", c.cfg.Max, live, forecast.PeakConcurrent)
	}
	if decision.ScaleUp {
		if err := c.scaleUp(ctx, now); err != nil {
			log.Printf("droplet pool: scale up: %v", err)
		}
	}
	if decision.DrainCount > 0 {
		c.beginDrains(ctx, now, idleEligible, decision.DrainCount)
	}

	// (3b) progress any draining droplets toward destruction.
	c.progressDrains(ctx, now)
	return nil
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
		return fmt.Errorf("list DO fleet: %w", err)
	}
	liveRows, err := c.store.ListByStates(ctx, "provisioning", "active", "draining", "destroying")
	if err != nil {
		return err
	}
	plan := ReconcileOrphans(fleet, liveRows, now, c.cfg.ProvisionTimeout)

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
		log.Printf("droplet pool: DB droplet id=%d name=%s missing from DO; reconciling to destroyed", r.ID, r.Name)
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
		workerReady := r.LastSeenAt != nil && now.Sub(*r.LastSeenAt) <= window
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
		if err := c.store.MarkActive(ctx, r.ID); err != nil {
			log.Printf("droplet pool: promote active %s: %v", r.Name, err)
			continue
		}
		log.Printf("droplet pool: droplet id=%d name=%s active (worker ready)", r.ID, r.Name)
	}
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

// scaleUp mints a per-droplet node token, writes the write-ahead provisioning row
// BEFORE the DO create (SRE-cap), creates+assigns+firewalls the droplet, then
// records its DO id. On any failure after the row is written it revokes the token
// and marks the row failed so the cap is not leaked.
func (c *Controller) scaleUp(ctx context.Context, now time.Time) error {
	name := fmt.Sprintf("%s%d", NamePrefix, now.UnixNano())

	token, nodeID, nodeTokenID, err := c.store.MintNodeToken(ctx, c.cfg.OperatorAccountID, name)
	if err != nil {
		return fmt.Errorf("mint node token: %w", err)
	}

	rowID, err := c.store.InsertProvisioning(ctx, name, c.cfg.Region, c.cfg.Size, c.cfg.Capacity, nodeID)
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
