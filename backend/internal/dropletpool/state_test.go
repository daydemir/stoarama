package dropletpool

import (
	"context"
	"sync"
	"testing"
	"time"
)

func ptrTime(t time.Time) *time.Time { return &t }
func ptrInt64(v int64) *int64        { return &v }

func baseDecisionParams(now time.Time) DecisionParams {
	return DecisionParams{
		Now:               now,
		Capacity:          1,
		Min:               0,
		Max:               5,
		MaxScaleUpBatch:   4,
		ProvisionLead:     10 * time.Minute,
		IdleGrace:         10 * time.Minute,
		ScaleUpCooldown:   1 * time.Minute,
		ScaleDownCooldown: 5 * time.Minute,
	}
}

func TestDecide_ScaleUpAheadOfImminentDemand(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	p := baseDecisionParams(now)
	p.Live = 0
	p.Forecast = Forecast{PeakConcurrent: 1, NextFireAt: now.Add(5 * time.Minute)} // within lead
	d := Decide(p)
	if d.ScaleUpCount != 1 || d.DrainCount != 0 {
		t.Fatalf("expected scale up by 1, got %+v", d)
	}
}

func TestDecide_ScaleUpBatchesTheWholeGap(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	p := baseDecisionParams(now)
	p.Max = 20
	p.MaxScaleUpBatch = 4
	p.Live = 0
	// A 100-clip bundle on one cron => required 100/1=100, clamped to Max 20; the
	// batch cap means this tick provisions 4, not 1, and not all 20.
	p.Forecast = Forecast{PeakConcurrent: 100, NextFireAt: now.Add(1 * time.Minute)}
	d := Decide(p)
	if d.ScaleUpCount != 4 {
		t.Fatalf("expected batch of 4 (gap 20 capped to batch 4), got %+v", d)
	}
}

func TestDecide_ScaleUpBatchNeverOvershootsGap(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	p := baseDecisionParams(now)
	p.Max = 20
	p.MaxScaleUpBatch = 8
	p.Live = 4
	// Demand needs 6 droplets, 4 live => gap 2, below the batch cap of 8: provision
	// exactly the gap, never overshoot.
	p.Forecast = Forecast{PeakConcurrent: 6, NextFireAt: now.Add(1 * time.Minute)}
	d := Decide(p)
	if d.ScaleUpCount != 2 {
		t.Fatalf("expected exactly the gap of 2, got %+v", d)
	}
}

func TestDecide_NoScaleUpWhenDemandBeyondLead(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	p := baseDecisionParams(now)
	p.Live = 0
	// Demand exists but the earliest fire is 30m out, beyond the 10m lead.
	p.Forecast = Forecast{PeakConcurrent: 1, NextFireAt: now.Add(30 * time.Minute)}
	d := Decide(p)
	if d.ScaleUpCount != 0 {
		t.Fatalf("expected NO scale up (demand beyond provision lead), got %+v", d)
	}
}

func TestDecide_ScaleUpBlockedByHardCap(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	p := baseDecisionParams(now)
	p.Max = 2
	p.Live = 2 // already at cap
	p.Forecast = Forecast{PeakConcurrent: 10, NextFireAt: now.Add(1 * time.Minute)}
	d := Decide(p)
	if d.ScaleUpCount != 0 {
		t.Fatalf("must not scale past hard cap, got %+v", d)
	}
	if !d.ScaleUpBlockedByCap {
		t.Fatalf("expected ScaleUpBlockedByCap, got %+v", d)
	}
	// Cap shortfall = requiredUncapped(10) - Max(2) = 8.
	if d.CapShortfall != 8 {
		t.Fatalf("expected CapShortfall 8, got %+v", d)
	}
}

func TestDecide_ScaleUpRespectsCooldown(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	p := baseDecisionParams(now)
	p.Live = 1
	p.Forecast = Forecast{PeakConcurrent: 3, NextFireAt: now.Add(1 * time.Minute)}
	// Last scale up 30s ago, cooldown is 60s -> blocked.
	p.PoolState = PoolState{LastScaleUpAt: ptrTime(now.Add(-30 * time.Second))}
	if d := Decide(p); d.ScaleUpCount != 0 {
		t.Fatalf("scale up should be blocked by cooldown, got %+v", d)
	}
	// Last scale up 2m ago -> allowed.
	p.PoolState = PoolState{LastScaleUpAt: ptrTime(now.Add(-2 * time.Minute))}
	if d := Decide(p); d.ScaleUpCount == 0 {
		t.Fatalf("scale up should be allowed after cooldown, got %+v", d)
	}
}

func TestDecide_ScaleDownWithHysteresisAndIdleGrace(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	p := baseDecisionParams(now)
	p.Live = 2
	p.IdleEligible = 1
	// No demand at all -> required 0, hysteresis holds (0 <= (2-1)*1), no fire.
	p.Forecast = Forecast{PeakConcurrent: 0}
	d := Decide(p)
	if d.DrainCount != 1 {
		t.Fatalf("expected drain 1 idle droplet, got %+v", d)
	}
}

func TestDecide_NoScaleDownWhenFireWithinHorizon(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	p := baseDecisionParams(now)
	p.Live = 2
	p.IdleEligible = 1
	// No standing demand, but a fire is forecast within idleGrace+lead (20m) -> hold.
	p.Forecast = Forecast{PeakConcurrent: 0, NextFireAt: now.Add(15 * time.Minute)}
	if d := Decide(p); d.DrainCount != 0 {
		t.Fatalf("must not drain when a fire is within the idle-grace+lead horizon, got %+v", d)
	}
}

func TestDecide_NoScaleDownWhenHysteresisFails(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	p := baseDecisionParams(now)
	p.Live = 2
	p.IdleEligible = 1
	// Demand needs both droplets (peak 2, capacity 1): (2 <= (2-1)*1) is false -> hold.
	p.Forecast = Forecast{PeakConcurrent: 2, NextFireAt: now.Add(2 * time.Hour)}
	if d := Decide(p); d.DrainCount != 0 {
		t.Fatalf("must not drain when demand still needs current fleet, got %+v", d)
	}
}

func TestDecide_ScaleDownRespectsCooldown(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	p := baseDecisionParams(now)
	p.Live = 2
	p.IdleEligible = 1
	p.Forecast = Forecast{PeakConcurrent: 0}
	// Scaled down 1m ago, cooldown 5m -> blocked.
	p.PoolState = PoolState{LastScaleDownAt: ptrTime(now.Add(-1 * time.Minute))}
	if d := Decide(p); d.DrainCount != 0 {
		t.Fatalf("scale down should be blocked by cooldown, got %+v", d)
	}
}

func TestDecide_ScaleDownNeverBelowMin(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	p := baseDecisionParams(now)
	p.Min = 1
	p.Live = 1
	p.IdleEligible = 1
	p.Forecast = Forecast{PeakConcurrent: 0}
	if d := Decide(p); d.DrainCount != 0 {
		t.Fatalf("must not drain below Min, got %+v", d)
	}
}

func TestIdleEligibleDroplets(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	grace := 10 * time.Minute
	droplets := []Droplet{
		{ID: 1, Name: "a", State: "active", IdleSince: ptrTime(now.Add(-15 * time.Minute))}, // eligible
		{ID: 2, Name: "b", State: "active", IdleSince: ptrTime(now.Add(-5 * time.Minute))},  // too fresh
		{ID: 3, Name: "c", State: "active", IdleSince: nil},                                 // busy
		{ID: 4, Name: "d", State: "draining", IdleSince: ptrTime(now.Add(-1 * time.Hour))},  // not active
	}
	got := IdleEligibleDroplets(droplets, now, grace)
	if len(got) != 1 || got[0].ID != 1 {
		t.Fatalf("expected only droplet 1 eligible, got %+v", got)
	}
}

func TestReconcileOrphans_AdoptBindByName(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	// Write-ahead row exists by name but never got its do_droplet_id (crash between
	// Create and SetDropletID). The DO droplet must be adopted, not destroyed.
	fleet := []DODroplet{{ID: 555, Name: NamePrefix + "100", CreatedAt: now.Add(-1 * time.Minute)}}
	rows := []Droplet{{ID: 1, Name: NamePrefix + "100", State: "provisioning", DODropletID: nil, CreatedAt: now.Add(-1 * time.Minute)}}
	plan := ReconcileOrphans(fleet, rows, now, 15*time.Minute)
	if len(plan.AdoptByName) != 1 || plan.AdoptByName[0].ID != 555 {
		t.Fatalf("expected adopt-by-name of do id 555, got %+v", plan)
	}
	if len(plan.DestroyOrphan) != 0 {
		t.Fatalf("must not destroy a droplet with a write-ahead row, got %+v", plan)
	}
}

func TestReconcileOrphans_DestroyLeakedOldDroplet(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	// Prefixed DO droplet with NO DB row, older than the provision timeout -> leak.
	fleet := []DODroplet{{ID: 777, Name: NamePrefix + "old", CreatedAt: now.Add(-30 * time.Minute)}}
	plan := ReconcileOrphans(fleet, nil, now, 15*time.Minute)
	if len(plan.DestroyOrphan) != 1 || plan.DestroyOrphan[0].ID != 777 {
		t.Fatalf("expected destroy of leaked old orphan, got %+v", plan)
	}
}

func TestReconcileOrphans_YoungUnknownIsAdoptedNotDestroyed(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	// Prefixed DO droplet with no DB row but younger than timeout -> adopt (give it
	// a chance to bind), never destroy a fresh droplet.
	fleet := []DODroplet{{ID: 888, Name: NamePrefix + "young", CreatedAt: now.Add(-2 * time.Minute)}}
	plan := ReconcileOrphans(fleet, nil, now, 15*time.Minute)
	if len(plan.DestroyOrphan) != 0 {
		t.Fatalf("must not destroy a fresh unknown droplet, got %+v", plan)
	}
	if len(plan.AdoptByName) != 1 {
		t.Fatalf("expected young unknown droplet to be adopted, got %+v", plan)
	}
}

func TestReconcileOrphans_MissingFromDO(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	// DB row says active with do id 999, but the DO fleet no longer has it.
	rows := []Droplet{{ID: 2, Name: NamePrefix + "gone", State: "active", DODropletID: ptrInt64(999)}}
	plan := ReconcileOrphans(nil, rows, now, 15*time.Minute)
	if len(plan.MissingFromDO) != 1 || plan.MissingFromDO[0].ID != 2 {
		t.Fatalf("expected the vanished DB row flagged MissingFromDO, got %+v", plan)
	}
}

func TestReconcileOrphans_MatchedByIDNoAction(t *testing.T) {
	now := mustTime(t, "2026-06-24T12:00:00Z")
	fleet := []DODroplet{{ID: 1000, Name: NamePrefix + "ok", CreatedAt: now.Add(-1 * time.Minute)}}
	rows := []Droplet{{ID: 3, Name: NamePrefix + "ok", State: "active", DODropletID: ptrInt64(1000)}}
	plan := ReconcileOrphans(fleet, rows, now, 15*time.Minute)
	if len(plan.AdoptByName) != 0 || len(plan.DestroyOrphan) != 0 || len(plan.MissingFromDO) != 0 {
		t.Fatalf("a fully-matched droplet needs no action, got %+v", plan)
	}
}

// fakeDOClient is a test-only DOClient used to confirm the production interface is
// satisfiable without any live API call and to record CreateDroplet/DeleteDroplet
// invocations. The production path always uses the real godo client.
type fakeDOClient struct {
	mu      sync.Mutex
	fleet   []DODroplet
	created []CreateDropletInput
	deleted []int64
	nextID  int64
}

func (f *fakeDOClient) CreateDroplet(_ context.Context, in CreateDropletInput) (DODroplet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, in)
	f.nextID++
	d := DODroplet{ID: f.nextID, Name: in.Name, Status: "new", CreatedAt: time.Now().UTC()}
	f.fleet = append(f.fleet, d)
	return d, nil
}

func (f *fakeDOClient) DeleteDroplet(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, id)
	out := f.fleet[:0]
	for _, d := range f.fleet {
		if d.ID != id {
			out = append(out, d)
		}
	}
	f.fleet = out
	return nil
}

func (f *fakeDOClient) ListDropletsByName(_ context.Context, _, prefix string) ([]DODroplet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]DODroplet, 0, len(f.fleet))
	for _, d := range f.fleet {
		if len(prefix) == 0 || (len(d.Name) >= len(prefix) && d.Name[:len(prefix)] == prefix) {
			out = append(out, d)
		}
	}
	return out, nil
}

// Ensure the fake satisfies the production interface at compile time.
var _ DOClient = (*fakeDOClient)(nil)
