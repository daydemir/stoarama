package dospend

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func testConfig() Config {
	return Config{WarnUSDPerDay: 5, CriticalUSDPerDay: 10, MonthlyBudgetUSD: 150,
		Allowlist: ParseAllowlist("stoarama-survey-*, copresence-*,stoarama-collate-*=15"), UnmanagedMinAge: time.Hour}
}

func TestAnalyzeFlagsTheCollationFleetAndSparesThePool(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	old := now.Add(-12 * 24 * time.Hour)
	inv := Inventory{MonthToDateUSD: 1980.99, Resources: []Resource{
		{Kind: KindDroplet, ID: "1", Name: "stoarama-rec-1-0", Size: "s-2vcpu-4gb", CreatedAt: old, USDPerDay: DropletUSDPerDay(0.03571), Tags: []string{"fleet:recorder-pool-v1"}},
		{Kind: KindDroplet, ID: "2", Name: "stoarama-rec-2-0", Size: "s-2vcpu-4gb", CreatedAt: old, USDPerDay: DropletUSDPerDay(0.03571)}, // provisioning row, id not yet stored
		{Kind: KindDroplet, ID: "3", Name: "stoarama-survey-1", Size: "s-2vcpu-4gb", CreatedAt: old, USDPerDay: DropletUSDPerDay(0.03571)},
		{Kind: KindDroplet, ID: "4", Name: "c-2", Size: "c-32", CreatedAt: old, USDPerDay: DropletUSDPerDay(1.19)},
		{Kind: KindDroplet, ID: "5", Name: "c-32", Size: "c-32", CreatedAt: old, USDPerDay: DropletUSDPerDay(1.19)},
		{Kind: KindDroplet, ID: "6", Name: "scratch", Size: "s-1vcpu-1gb", CreatedAt: now.Add(-10 * time.Minute), USDPerDay: 0.2},
		{Kind: KindDroplet, ID: "7", Name: "stoarama-rec-9-0", Size: "s-2vcpu-4gb", CreatedAt: old, USDPerDay: DropletUSDPerDay(0.03571)}, // row destroyed: orphan
		{Kind: KindVolume, ID: "v1", Name: "rec-scratch", Size: "100GiB", CreatedAt: old, DropletIDs: []string{"1"}, USDPerDay: StorageUSDPerDay(100, VolumeUSDPerGiBMonth)},
		{Kind: KindVolume, ID: "v2", Name: "collate-vol-7", Size: "500GiB", CreatedAt: old, DropletIDs: []string{"4"}, USDPerDay: StorageUSDPerDay(500, VolumeUSDPerGiBMonth)},
		{Kind: KindVolume, ID: "v3", Name: "pgdata", Size: "10GiB", CreatedAt: old, DropletIDs: []string{"3"}, USDPerDay: StorageUSDPerDay(10, VolumeUSDPerGiBMonth)},
		{Kind: KindSnapshot, ID: "s1", Name: "stoarama-recorder-20260704", Size: "4.69GiB", CreatedAt: old, USDPerDay: StorageUSDPerDay(4.69, SnapshotUSDPerGiBMonth)},
	}}
	managed := ManagedSet{DropletIDs: map[string]bool{"1": true}, Names: map[string]bool{"stoarama-rec-2-0": true}}
	r := Analyze(inv, managed, testConfig(), now)

	owners := map[string]string{}
	for _, res := range r.Resources {
		owners[res.ID] = res.Owner
	}
	want := map[string]string{"1": OwnerPool, "2": OwnerPool, "3": OwnerAllowlist, "4": OwnerUnmanaged, "5": OwnerUnmanaged,
		"6": OwnerUnmanaged, "7": OwnerUnmanaged, "v1": OwnerPool, "v2": OwnerUnmanaged, "v3": OwnerAllowlist, "s1": OwnerNA}
	for id, o := range want {
		if owners[id] != o {
			t.Errorf("owner[%s]=%q want %q", id, owners[id], o)
		}
	}
	gotUnmanaged := []string{}
	for _, res := range r.Unmanaged {
		gotUnmanaged = append(gotUnmanaged, res.ID)
	}
	// The 10-minute-old scratch droplet is not yet due; largest $/day first.
	if len(gotUnmanaged) != 4 || gotUnmanaged[0] != "4" && gotUnmanaged[0] != "5" {
		t.Fatalf("unmanaged=%v", gotUnmanaged)
	}
	for _, id := range gotUnmanaged {
		if id == "6" {
			t.Fatalf("young resource reported: %v", gotUnmanaged)
		}
	}
	if r.Level != LevelCritical || !r.OverBudget {
		t.Fatalf("level=%s over=%t burn=%.2f", r.Level, r.OverBudget, r.BurnUSDPerDay)
	}
	sum := 0.0
	for _, res := range inv.Resources {
		sum += res.USDPerDay
	}
	if !near(r.BurnUSDPerDay, sum) {
		t.Fatalf("burn=%v want %v", r.BurnUSDPerDay, sum)
	}
	if r.Groups[0].Group != "droplet c-*" || r.Groups[0].Count != 2 {
		t.Fatalf("top group=%+v", r.Groups[0])
	}
	if r.ProjectedMonthUSD <= r.MonthToDateUSD {
		t.Fatalf("projection %v not above MTD %v", r.ProjectedMonthUSD, r.MonthToDateUSD)
	}
}

func TestBurnLevelThresholdsAreExclusive(t *testing.T) {
	cfg := testConfig()
	for burn, want := range map[float64]string{0: LevelOK, 5: LevelOK, 5.01: LevelWarn, 10: LevelWarn, 10.01: LevelCritical} {
		if got := BurnLevel(burn, cfg); got != want {
			t.Errorf("BurnLevel(%v)=%s want %s", burn, got, want)
		}
	}
}

func TestNamePrefix(t *testing.T) {
	for in, want := range map[string]string{"c-2": "c-*", "c-32": "c-*", "copresence-api-01": "copresence-api-*", "scratch": "scratch", "42": "42",
		"stoarama-recorder-20260630-daf3035": "stoarama-recorder-20260630-daf3035", "stoarama-recorder-20260627": "stoarama-recorder-*", "vol_7": "vol_*", "x-": "x-"} {
		if got := NamePrefix(in); got != want {
			t.Errorf("NamePrefix(%q)=%q want %q", in, got, want)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	if err := testConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	bad := testConfig()
	bad.WarnUSDPerDay = 20
	if bad.Validate() == nil {
		t.Fatal("warn above critical accepted")
	}
	bad = testConfig()
	bad.CriticalUSDPerDay = 0
	if bad.Validate() == nil {
		t.Fatal("zero critical accepted")
	}
	for _, v := range []float64{math.NaN(), math.Inf(1)} {
		bad = testConfig()
		bad.CriticalUSDPerDay = v
		if bad.Validate() == nil {
			t.Fatalf("non-finite critical %v accepted", v)
		}
	}
	bad = testConfig()
	bad.Allowlist = ParseAllowlist("x-*=fifteen")
	if bad.Validate() == nil {
		t.Fatal("malformed cap accepted")
	}
	bad = testConfig()
	bad.Allowlist = ParseAllowlist("x-*=-1")
	if bad.Validate() == nil {
		t.Fatal("negative cap accepted")
	}
	bad = testConfig()
	bad.Allowlist = []AllowEntry{{Pattern: "["}}
	if bad.Validate() == nil {
		t.Fatal("malformed glob accepted")
	}
}

type fakeClient struct {
	inv      Inventory
	sizeUSD  float64
	sizeErr  error
	sizeHits int
}

func (f *fakeClient) Inventory(context.Context, bool) (Inventory, error) { return f.inv, nil }
func (f *fakeClient) SizeUSDPerDay(context.Context, string) (float64, error) {
	f.sizeHits++
	return f.sizeUSD, f.sizeErr
}

func TestGuardQuotePricesFromInventoryThenSizes(t *testing.T) {
	inv := Inventory{Resources: []Resource{
		{Kind: KindDroplet, Size: "s-2vcpu-4gb", USDPerDay: 0.857},
		{Kind: KindVolume, Size: "100GiB", USDPerDay: 0.33},
	}}
	fc := &fakeClient{inv: inv}
	burn, size, err := Guard{Client: fc, CriticalUSDPerDay: 10}.Quote(context.Background(), "s-2vcpu-4gb")
	if err != nil || !near(burn, 1.187) || !near(size, 0.857) || fc.sizeHits != 0 {
		t.Fatalf("burn=%v size=%v err=%v sizeHits=%d", burn, size, err, fc.sizeHits)
	}
	fc = &fakeClient{inv: inv, sizeUSD: 2.4}
	if _, size, err = (Guard{Client: fc}).Quote(context.Background(), "c-4"); err != nil || size != 2.4 || fc.sizeHits != 1 {
		t.Fatalf("fallback size=%v err=%v hits=%d", size, err, fc.sizeHits)
	}
	fc = &fakeClient{inv: inv, sizeErr: errors.New("unknown size")}
	if _, _, err = (Guard{Client: fc}).Quote(context.Background(), "nope"); err == nil {
		t.Fatal("unknown size priced")
	}
}

func TestAllowlistCapCountsDropletAndAttachedVolumes(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	old := now.Add(-2 * time.Hour)
	cfg := testConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	collate := func(dropletPerHour float64, volGiB float64) Inventory {
		return Inventory{Resources: []Resource{
			{Kind: KindDroplet, ID: "10", Name: "stoarama-collate-01", Size: "c-16", CreatedAt: old, USDPerDay: DropletUSDPerDay(dropletPerHour)},
			{Kind: KindVolume, ID: "v10", Name: "scratch-200", Size: "200GiB", CreatedAt: old, DropletIDs: []string{"10"}, USDPerDay: StorageUSDPerDay(volGiB, VolumeUSDPerGiBMonth)},
			{Kind: KindDroplet, ID: "11", Name: "copresence-api-01", CreatedAt: old, USDPerDay: 0.21},
			// Named like another entry but attached to the collation droplet: charged to collation.
			{Kind: KindVolume, ID: "v11", Name: "copresence-data", Size: "10GiB", CreatedAt: old, DropletIDs: []string{"10"}, USDPerDay: StorageUSDPerDay(10, VolumeUSDPerGiBMonth)},
		}}
	}
	// c-8 ($0.25/h = $6/day) + 200 GiB ($0.66/day) stays under $15.
	r := Analyze(collate(0.25, 200), ManagedSet{}, cfg, now)
	if len(r.Unmanaged) != 0 || len(r.OverCap()) != 0 {
		t.Fatalf("authorized collation flagged: unmanaged=%v over=%v", r.Unmanaged, r.OverCap())
	}
	var got AllowSpend
	for _, a := range r.Allowlisted {
		if a.Pattern == "stoarama-collate-*" {
			got = a
		}
	}
	if got.Count != 3 || got.CapUSDPerDay != 15 || !near(got.USDPerDay, 6+StorageUSDPerDay(210, VolumeUSDPerGiBMonth)) {
		t.Fatalf("collate spend=%+v", got)
	}
	// c-32 ($1.19/h = $28.56/day) breaches the $15 cap; burn counts it either way.
	r = Analyze(collate(1.19, 200), ManagedSet{}, cfg, now)
	over := r.OverCap()
	if len(over) != 1 || over[0].Pattern != "stoarama-collate-*" {
		t.Fatalf("over cap=%+v", over)
	}
	if r.BurnUSDPerDay < 28.56 {
		t.Fatalf("burn %v excludes allowlisted spend", r.BurnUSDPerDay)
	}
}
