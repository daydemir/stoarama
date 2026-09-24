package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/dospend"
	"github.com/daydemir/stoarama/backend/internal/dropletpool"
)

func TestDOSpendAlertKeys(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	r := dospend.Report{GeneratedAt: now, Level: dospend.LevelCritical, OverBudget: true,
		Unmanaged: []dospend.Resource{{Kind: dospend.KindDroplet, ID: "4"}, {Kind: dospend.KindVolume, ID: "v2"}}}
	keys := doSpendAlertKeys(r)
	if got := keys[signalDOSpendBurn]; len(got) != 1 || got[0] != "do_spend_burn:critical" {
		t.Fatalf("burn keys=%v", got)
	}
	if got := keys[signalDOSpendMTD]; len(got) != 1 || got[0] != "do_spend_mtd:2026-09" {
		t.Fatalf("mtd keys=%v", got)
	}
	if got := keys[signalDOUnmanaged]; len(got) != 2 || got[0] != "do_unmanaged:droplet:4" || got[1] != "do_unmanaged:volume:v2" {
		t.Fatalf("unmanaged keys=%v", got)
	}
	capped := doSpendAlertKeys(dospend.Report{GeneratedAt: now, Level: dospend.LevelOK, Allowlisted: []dospend.AllowSpend{
		{AllowEntry: dospend.AllowEntry{Pattern: "stoarama-collate-*", CapUSDPerDay: 15}, USDPerDay: 28.6, OverCap: true},
		{AllowEntry: dospend.AllowEntry{Pattern: "copresence-*"}, USDPerDay: 0.2}}})
	if got := capped[signalDOAllowCap]; len(got) != 1 || got[0] != "do_allowlist_over_cap:stoarama-collate-*" {
		t.Fatalf("allowlist cap keys=%v", got)
	}
	// A healthy report still names every signal so open episodes resolve.
	ok := doSpendAlertKeys(dospend.Report{GeneratedAt: now, Level: dospend.LevelOK})
	for _, s := range []string{signalDOSpendBurn, signalDOSpendMTD, signalDOUnmanaged, signalDOAllowCap} {
		if keys, present := ok[s]; !present || len(keys) != 0 {
			t.Fatalf("healthy report signal %s keys=%v present=%t", s, keys, present)
		}
	}
}

func TestComposeDOSpendEmailNamesEveryCondition(t *testing.T) {
	r := dospend.Report{GeneratedAt: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC), Level: dospend.LevelCritical,
		BurnUSDPerDay: 133.4, MonthToDateUSD: 1980.99, MonthlyBudgetUSD: 150, OverBudget: true, WarnUSDPerDay: 5, CriticalUSDPerDay: 10,
		Groups:             []dospend.GroupTotal{{Group: "droplet c-*", Count: 19, USDPerDay: 110}},
		Unmanaged:          []dospend.Resource{{Kind: dospend.KindDroplet, ID: "4", Name: "c-2", Size: "c-32", USDPerDay: 28.56}},
		UnmanagedUSDPerDay: 28.56}
	subject, body := composeDOSpendEmail(r, true)
	for _, want := range []string{"CRITICAL $133.40/day", "1 unmanaged", "MTD $1980.99 over $150", "scale-up blocked"} {
		if !strings.Contains(subject, want) {
			t.Errorf("subject %q missing %q", subject, want)
		}
	}
	for _, want := range []string{"BLOCKED a scale-up", "droplet c-2 (id 4) c-32 $28.56/day", "droplet c-*", "OVER BUDGET", "do-spend report"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q:\n%s", want, body)
		}
	}
	if !strings.Contains(composeDigestDOSpend(nil, os.ErrPermission), "blind") {
		t.Fatal("digest hides a failed DO read")
	}
}

func TestDOSpendBlockedEpisodeLifecycle(t *testing.T) {
	pool, cleanup := testStabilityMonitorPool(t)
	defer cleanup()
	ctx := context.Background()
	migration, err := os.ReadFile("../../../infra/sql/migrations/0153_recording_stability_guards.sql")
	if err != nil {
		t.Fatal(err)
	}
	ddl := string(migration)
	if _, err := pool.Exec(ctx, ddl[strings.Index(ddl, "CREATE TABLE ops_alert_episodes"):]); err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 9, 24, 7, 0, 0, 0, time.UTC)
	if open, due, err := doSpendBlockedEpisode(ctx, pool, t0, false); err != nil || open || due {
		t.Fatalf("no episode open=%t due=%t err=%v", open, due, err)
	}
	if err := dropletpool.NewStore(pool).NoteOpsAlert(ctx, dropletpool.SpendBlockedSignal, dropletpool.SpendBlockedSignal, t0); err != nil {
		t.Fatal(err)
	}
	if open, due, err := doSpendBlockedEpisode(ctx, pool, t0.Add(time.Minute), false); err != nil || !open || !due {
		t.Fatalf("fresh episode open=%t due=%t err=%v", open, due, err)
	}
	if err := markOpsAlertsDelivered(ctx, pool, []string{dropletpool.SpendBlockedSignal}, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if open, due, err := doSpendBlockedEpisode(ctx, pool, t0.Add(time.Hour), false); err != nil || !open || due {
		t.Fatalf("delivered episode open=%t due=%t err=%v", open, due, err)
	}
	// Dry-run never resolves; a real sweep resolves once the controller is quiet.
	if open, _, err := doSpendBlockedEpisode(ctx, pool, t0.Add(3*time.Hour), true); err != nil || open {
		t.Fatalf("stale dry-run open=%t err=%v", open, err)
	}
	var resolved bool
	_ = pool.QueryRow(ctx, `SELECT resolved_at IS NOT NULL FROM ops_alert_episodes`).Scan(&resolved)
	if resolved {
		t.Fatal("dry-run resolved the episode")
	}
	if _, _, err := doSpendBlockedEpisode(ctx, pool, t0.Add(3*time.Hour), false); err != nil {
		t.Fatal(err)
	}
	_ = pool.QueryRow(ctx, `SELECT resolved_at IS NOT NULL FROM ops_alert_episodes`).Scan(&resolved)
	if !resolved {
		t.Fatal("quiet episode not resolved")
	}
}
