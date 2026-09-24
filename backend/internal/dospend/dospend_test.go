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
		Allowlist: ParseAllowlist("stoarama-survey-*, copresence-*"), UnmanagedMinAge: time.Hour}
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
		{Kind: KindSnapshot, ID: "s1", Name: "stoarama-recorder-20260704", Size: "4.69GiB", CreatedAt: old, USDPerDay: StorageUSDPerDay(4.69, SnapshotUSDPerGiBMonth)},
	}}
	managed := ManagedSet{DropletIDs: map[string]bool{"1": true}, Names: map[string]bool{"stoarama-rec-2-0": true}}
	r := Analyze(inv, managed, testConfig(), now)

	owners := map[string]string{}
	for _, res := range r.Resources {
		owners[res.ID] = res.Owner
	}
	want := map[string]string{"1": OwnerPool, "2": OwnerPool, "3": OwnerAllowlist, "4": OwnerUnmanaged, "5": OwnerUnmanaged,
		"6": OwnerUnmanaged, "7": OwnerUnmanaged, "v1": OwnerPool, "v2": OwnerUnmanaged, "s1": OwnerNA}
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
	bad = testConfig()
	bad.Allowlist = []string{"["}
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
