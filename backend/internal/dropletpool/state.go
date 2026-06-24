package dropletpool

import (
	"time"
)

// Decision is the pure scale decision for one tick, computed from the forecast,
// the current live count, the cooldown ledger, and the config. The controller
// applies exactly one of ScaleUp / drain candidates per tick.
type Decision struct {
	// ScaleUp is true when a new droplet should be provisioned this tick.
	ScaleUp bool
	// ScaleUpBlockedByCap is true when scale-up is wanted but the hard spend cap
	// (DROPLET_POOL_MAX) prevents it; surfaced for observability.
	ScaleUpBlockedByCap bool
	// DrainCount is how many idle droplets should begin draining this tick.
	DrainCount int
}

// DecisionParams are the pure inputs to the scale decision.
type DecisionParams struct {
	Now          time.Time
	Forecast     Forecast
	Live         int // current live droplet count (provisioning+active+draining+destroying)
	IdleEligible int // active droplets idle past the idle grace, eligible to drain
	PoolState    PoolState

	Capacity          int
	Min               int
	Max               int
	ProvisionLead     time.Duration
	IdleGrace         time.Duration
	ScaleUpCooldown   time.Duration
	ScaleDownCooldown time.Duration
}

// Decide computes the scale decision. Scale-up fires when demand within the
// provision-lead window needs more droplets than are live, the scale-up cooldown
// has elapsed, and the hard cap is not yet reached. Scale-down (drain) fires when
// the forecast peak stays below what (live-1) droplets can serve AND no fire is
// forecast within IdleGrace+ProvisionLead (so a droplet drained now would not be
// needed again before a fresh one could boot, SRE-thrash), with its own cooldown.
// Scale-up and scale-down are mutually exclusive in a tick (up wins).
func Decide(p DecisionParams) Decision {
	capacity := p.Capacity
	if capacity <= 0 {
		capacity = 1
	}
	// requiredUncapped is what demand wants ignoring the spend cap (capacity ceil,
	// floored at Min); required is that value clamped to the hard cap.
	requiredUncapped := 0
	if p.Forecast.PeakConcurrent > 0 {
		requiredUncapped = (p.Forecast.PeakConcurrent + capacity - 1) / capacity
	}
	if requiredUncapped < p.Min {
		requiredUncapped = p.Min
	}
	required := requiredUncapped
	if required > p.Max {
		required = p.Max
	}

	// Demand is "imminent" if the earliest forecast fire is within the provision
	// lead (so a droplet started now is up in time).
	imminent := p.Forecast.PeakConcurrent > 0 &&
		!p.Forecast.NextFireAt.IsZero() && p.Forecast.NextFireAt.Sub(p.Now) <= p.ProvisionLead
	// MIN floor is always wanted regardless of imminence.
	wantUp := required > p.Live && (imminent || required <= p.Min || p.Live < p.Min)

	var d Decision
	// Surface a cap-block whenever demand wants more droplets than the cap allows
	// and we are at the cap with that demand imminent (or a standing MIN over cap).
	if requiredUncapped > p.Max && p.Live >= p.Max && (imminent || p.Min > p.Max) {
		d.ScaleUpBlockedByCap = true
	}
	if wantUp {
		if p.Live >= p.Max {
			d.ScaleUpBlockedByCap = true
			return d
		}
		if cooldownElapsed(p.PoolState.LastScaleUpAt, p.Now, p.ScaleUpCooldown) {
			d.ScaleUp = true
		}
		// Wanted up but blocked (cooldown): never drain in the same tick.
		return d
	}

	// Scale down: only when we have more droplets than required, hysteresis holds
	// (forecast stays below (live-1)*capacity), and nothing fires within the
	// combined idle-grace + provision-lead horizon, and the cooldown has elapsed.
	if p.Live > p.Min && p.Live > required && p.IdleEligible > 0 {
		hysteresisOK := p.Forecast.PeakConcurrent <= (p.Live-1)*capacity
		noImminentFire := p.Forecast.NextFireAt.IsZero() ||
			p.Forecast.NextFireAt.Sub(p.Now) > (p.IdleGrace+p.ProvisionLead)
		if hysteresisOK && noImminentFire && cooldownElapsed(p.PoolState.LastScaleDownAt, p.Now, p.ScaleDownCooldown) {
			// Drain at most the surplus, bounded by how many are idle-eligible, and
			// never below Min.
			surplus := p.Live - max(required, p.Min)
			if surplus > p.IdleEligible {
				surplus = p.IdleEligible
			}
			if surplus < 0 {
				surplus = 0
			}
			d.DrainCount = surplus
		}
	}
	return d
}

// cooldownElapsed reports whether `cooldown` has passed since `last` (true when
// last is nil, i.e. never scaled).
func cooldownElapsed(last *time.Time, now time.Time, cooldown time.Duration) bool {
	if last == nil {
		return true
	}
	return now.Sub(*last) >= cooldown
}

// IdleEligibleDroplets returns the subset of active droplets whose idle_since is
// older than the idle grace (eligible to drain). A droplet with no idle stamp or
// a stamp newer than the grace is not eligible.
func IdleEligibleDroplets(droplets []Droplet, now time.Time, idleGrace time.Duration) []Droplet {
	out := make([]Droplet, 0, len(droplets))
	for _, d := range droplets {
		if d.State != "active" || d.IdleSince == nil {
			continue
		}
		if now.Sub(*d.IdleSince) >= idleGrace {
			out = append(out, d)
		}
	}
	return out
}

// OrphanPlan is the reconcile diff between the live DO fleet and the DB rows.
type OrphanPlan struct {
	// AdoptByName are prefixed DO droplets with no DB row at all: a crash between
	// CreateDroplet and SetDropletID can leave one. They are adopted (a DB row is
	// written) so they are counted and managed, not destroyed (the credential and
	// node already exist for them via the write-ahead row keyed by name).
	AdoptByName []DODroplet
	// DestroyOrphan are prefixed DO droplets with no live DB row that are older
	// than the provision timeout: genuinely leaked, destroy them (SRE-orphan).
	DestroyOrphan []DODroplet
	// MissingFromDO are live DB rows (active/draining) whose DO droplet has
	// vanished: the row should be reconciled to destroyed and its token revoked.
	MissingFromDO []Droplet
}

// ReconcileOrphans computes the orphan-reap diff. It matches a prefixed DO
// droplet to a DB row by DO droplet id first, then by name (so a row written
// write-ahead but missing its do_droplet_id is still matched to its DO droplet).
//
// A prefixed DO droplet is:
//   - matched (no action) if its id or name is known live;
//   - adopted if its name is unknown but it is younger than provisionTimeout
//     (likely a brand-new create whose DB row write lost the race — but with the
//     write-ahead row this is rare; treated as adopt to be counted);
//   - destroyed if its name is unknown AND it is older than provisionTimeout
//     (a genuine leak).
//
// liveRows are the DB rows in active/draining; any whose DO id is not present in
// the live DO fleet is flagged MissingFromDO.
func ReconcileOrphans(doFleet []DODroplet, liveRows []Droplet, now time.Time, provisionTimeout time.Duration) OrphanPlan {
	var plan OrphanPlan

	liveNames := make(map[string]Droplet, len(liveRows))
	liveDOIDs := make(map[int64]Droplet, len(liveRows))
	for _, r := range liveRows {
		liveNames[r.Name] = r
		if r.DODropletID != nil {
			liveDOIDs[*r.DODropletID] = r
		}
	}

	presentDOIDs := make(map[int64]struct{}, len(doFleet))
	for _, d := range doFleet {
		presentDOIDs[d.ID] = struct{}{}
		if _, ok := liveDOIDs[d.ID]; ok {
			continue
		}
		if _, ok := liveNames[d.Name]; ok {
			// The write-ahead row exists by name but its do_droplet_id was never
			// recorded (crash between Create and SetDropletID): adopt to bind the id.
			plan.AdoptByName = append(plan.AdoptByName, d)
			continue
		}
		// No DB row at all for this prefixed DO droplet.
		if dropletAge(d, now) >= provisionTimeout {
			plan.DestroyOrphan = append(plan.DestroyOrphan, d)
		} else {
			plan.AdoptByName = append(plan.AdoptByName, d)
		}
	}

	// DB rows whose DO droplet is gone.
	for _, r := range liveRows {
		if r.DODropletID == nil {
			continue
		}
		if _, ok := presentDOIDs[*r.DODropletID]; !ok {
			plan.MissingFromDO = append(plan.MissingFromDO, r)
		}
	}
	return plan
}

// dropletAge is the age of a DO droplet from its creation instant. An unknown
// (zero) creation time is treated as "very old" so a prefixed droplet with no DB
// row whose age cannot be read is still reaped rather than left running and
// billing forever.
func dropletAge(d DODroplet, now time.Time) time.Duration {
	if d.CreatedAt.IsZero() {
		return 1<<62 - 1 // effectively infinite age
	}
	return now.Sub(d.CreatedAt)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
