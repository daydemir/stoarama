// Package dospend measures DigitalOcean account spend so a runaway fleet cannot
// bill silently. It lists the account inventory (droplets, volumes, snapshots)
// and month-to-date usage, prices the inventory at DO list prices into a
// current burn rate, and classifies every droplet and volume as owned by the
// recorder pool, allowlisted, or unmanaged.
//
// The Sep 11-23 2026 collation fleet (19 hand-created droplets + 56 volumes,
// about $133/day) ran for 12 days because nothing in Stoarama looked at the
// account as a whole; the pool controller only ever sees its own droplets.
package dospend

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
)

// DigitalOcean list prices for storage, in USD per GiB per month.
const (
	VolumeUSDPerGiBMonth   = 0.10
	SnapshotUSDPerGiBMonth = 0.06
	daysPerMonth           = 365.0 / 12.0
)

// Resource kinds.
const (
	KindDroplet  = "droplet"
	KindVolume   = "volume"
	KindSnapshot = "snapshot"
)

// Ownership classes.
const (
	OwnerPool      = "pool"      // a live recorder_droplets row (or a volume attached to one)
	OwnerAllowlist = "allowlist" // matches DO_SPEND_ALLOWLIST
	OwnerUnmanaged = "unmanaged" // nobody in Stoarama claims it
	OwnerNA        = "-"         // snapshots: priced, never classified
)

// Alert levels for the burn rate.
const (
	LevelOK       = "ok"
	LevelWarn     = "warn"
	LevelCritical = "critical"
)

// Resource is one billable DO object priced at list price.
type Resource struct {
	Kind       string    `json:"kind"`
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Size       string    `json:"size"` // droplet size slug, or "<n>GiB"
	Status     string    `json:"status,omitempty"`
	Tags       []string  `json:"tags,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	DropletIDs []string  `json:"droplet_ids,omitempty"` // volumes: attachments
	USDPerDay  float64   `json:"usd_per_day"`
	Owner      string    `json:"owner"`
	Group      string    `json:"group"`
}

// Inventory is one complete read of the account.
type Inventory struct {
	MonthToDateUSD float64    `json:"month_to_date_usd"`
	BalanceAt      time.Time  `json:"balance_generated_at"`
	Resources      []Resource `json:"resources"`
}

// Config holds the spend thresholds and the unmanaged-resource allowlist.
type Config struct {
	WarnUSDPerDay     float64
	CriticalUSDPerDay float64
	MonthlyBudgetUSD  float64
	// Allowlist holds path.Match globs of droplet/volume names that are known
	// and intentionally outside the recorder pool (e.g. "copresence-*").
	Allowlist []string
	// UnmanagedMinAge keeps a freshly created resource out of the tripwire so a
	// short-lived operator action does not page.
	UnmanagedMinAge time.Duration
}

// ParseAllowlist splits a comma-separated glob list.
func ParseAllowlist(raw string) []string {
	out := []string{}
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Validate rejects nonsensical thresholds so a typo cannot disable the guard.
func (c Config) Validate() error {
	if c.WarnUSDPerDay <= 0 || c.CriticalUSDPerDay <= 0 || c.MonthlyBudgetUSD <= 0 {
		return fmt.Errorf("DO spend thresholds must be > 0 (warn=%v critical=%v budget=%v)", c.WarnUSDPerDay, c.CriticalUSDPerDay, c.MonthlyBudgetUSD)
	}
	if c.WarnUSDPerDay > c.CriticalUSDPerDay {
		return fmt.Errorf("DO spend warn threshold %v exceeds critical %v", c.WarnUSDPerDay, c.CriticalUSDPerDay)
	}
	for _, p := range c.Allowlist {
		if _, err := path.Match(p, ""); err != nil {
			return fmt.Errorf("DO spend allowlist pattern %q: %w", p, err)
		}
	}
	return nil
}

// Allowlisted reports whether name matches any allowlist glob.
func (c Config) Allowlisted(name string) bool {
	for _, p := range c.Allowlist {
		if ok, _ := path.Match(p, name); ok {
			return true
		}
	}
	return false
}

// ManagedSet is what the recorder pool claims: DO droplet ids with a live
// recorder_droplets row, and names of live rows whose DO id is not yet known.
type ManagedSet struct {
	DropletIDs map[string]bool
	Names      map[string]bool
}

// GroupTotal is one line of the burn breakdown.
type GroupTotal struct {
	Group     string  `json:"group"`
	Count     int     `json:"count"`
	USDPerDay float64 `json:"usd_per_day"`
}

// Report is the analysed spend picture.
type Report struct {
	GeneratedAt        time.Time    `json:"generated_at"`
	MonthToDateUSD     float64      `json:"month_to_date_usd"`
	MonthlyBudgetUSD   float64      `json:"monthly_budget_usd"`
	OverBudget         bool         `json:"over_budget"`
	BurnUSDPerDay      float64      `json:"burn_usd_per_day"`
	ProjectedMonthUSD  float64      `json:"projected_month_usd"`
	WarnUSDPerDay      float64      `json:"warn_usd_per_day"`
	CriticalUSDPerDay  float64      `json:"critical_usd_per_day"`
	Level              string       `json:"level"`
	Groups             []GroupTotal `json:"groups"`
	Unmanaged          []Resource   `json:"unmanaged"`
	UnmanagedUSDPerDay float64      `json:"unmanaged_usd_per_day"`
	Resources          []Resource   `json:"resources"`
}

// Analyze classifies and totals an inventory. It is pure so every branch is
// unit-testable without the DO API.
func Analyze(inv Inventory, managed ManagedSet, cfg Config, now time.Time) Report {
	r := Report{
		GeneratedAt:       now.UTC(),
		MonthToDateUSD:    inv.MonthToDateUSD,
		MonthlyBudgetUSD:  cfg.MonthlyBudgetUSD,
		WarnUSDPerDay:     cfg.WarnUSDPerDay,
		CriticalUSDPerDay: cfg.CriticalUSDPerDay,
	}
	// A volume inherits its droplet's ownership: attached to a pool droplet it
	// belongs to the pool, attached to an allowlisted droplet it is allowlisted.
	poolDroplets, allowedDroplets := map[string]bool{}, map[string]bool{}
	for _, res := range inv.Resources {
		if res.Kind != KindDroplet {
			continue
		}
		if managed.DropletIDs[res.ID] || managed.Names[res.Name] {
			poolDroplets[res.ID] = true
		} else if cfg.Allowlisted(res.Name) {
			allowedDroplets[res.ID] = true
		}
	}
	groups := map[string]*GroupTotal{}
	for _, res := range inv.Resources {
		res.Group = GroupOf(res)
		switch res.Kind {
		case KindSnapshot:
			res.Owner = OwnerNA
		case KindDroplet:
			res.Owner = classify(poolDroplets[res.ID], res.Name, cfg)
		case KindVolume:
			res.Owner = classify(false, res.Name, cfg)
			for _, id := range res.DropletIDs {
				switch {
				case poolDroplets[id]:
					res.Owner = OwnerPool
				case allowedDroplets[id] && res.Owner == OwnerUnmanaged:
					res.Owner = OwnerAllowlist
				}
			}
		}
		r.BurnUSDPerDay += res.USDPerDay
		g := groups[res.Group]
		if g == nil {
			g = &GroupTotal{Group: res.Group}
			groups[res.Group] = g
		}
		g.Count++
		g.USDPerDay += res.USDPerDay
		if res.Owner == OwnerUnmanaged && !res.CreatedAt.IsZero() && now.Sub(res.CreatedAt) >= cfg.UnmanagedMinAge {
			r.Unmanaged = append(r.Unmanaged, res)
			r.UnmanagedUSDPerDay += res.USDPerDay
		}
		r.Resources = append(r.Resources, res)
	}
	for _, g := range groups {
		r.Groups = append(r.Groups, *g)
	}
	sort.Slice(r.Groups, func(i, j int) bool {
		if r.Groups[i].USDPerDay != r.Groups[j].USDPerDay {
			return r.Groups[i].USDPerDay > r.Groups[j].USDPerDay
		}
		return r.Groups[i].Group < r.Groups[j].Group
	})
	sort.Slice(r.Unmanaged, func(i, j int) bool {
		if r.Unmanaged[i].USDPerDay != r.Unmanaged[j].USDPerDay {
			return r.Unmanaged[i].USDPerDay > r.Unmanaged[j].USDPerDay
		}
		return r.Unmanaged[i].Name < r.Unmanaged[j].Name
	})
	r.Level = BurnLevel(r.BurnUSDPerDay, cfg)
	r.OverBudget = r.MonthToDateUSD > cfg.MonthlyBudgetUSD
	r.ProjectedMonthUSD = r.MonthToDateUSD + r.BurnUSDPerDay*daysLeftInMonth(now)
	return r
}

func classify(pool bool, name string, cfg Config) string {
	switch {
	case pool:
		return OwnerPool
	case cfg.Allowlisted(name):
		return OwnerAllowlist
	default:
		return OwnerUnmanaged
	}
}

// BurnLevel maps a $/day burn onto the configured thresholds.
func BurnLevel(usdPerDay float64, cfg Config) string {
	switch {
	case usdPerDay > cfg.CriticalUSDPerDay:
		return LevelCritical
	case usdPerDay > cfg.WarnUSDPerDay:
		return LevelWarn
	default:
		return LevelOK
	}
}

func daysLeftInMonth(now time.Time) float64 {
	now = now.UTC()
	next := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
	return next.Sub(now).Hours() / 24
}

// GroupOf buckets a resource for the breakdown: its fleet:/role: tag when it
// has one, otherwise its name with the trailing instance number stripped, so
// "c-2".."c-32" collapse into "c-*".
func GroupOf(res Resource) string {
	for _, prefix := range []string{"fleet:", "role:"} {
		for _, t := range res.Tags {
			if strings.HasPrefix(t, prefix) {
				return res.Kind + " " + t
			}
		}
	}
	return res.Kind + " " + NamePrefix(res.Name)
}

// NamePrefix replaces a trailing all-digit name segment with "*", so
// "c-2".."c-32" collapse into "c-*" while "recorder-20260630-daf3035" stays whole.
func NamePrefix(name string) string {
	i := strings.LastIndexAny(name, "-_.")
	if i <= 0 || i == len(name)-1 {
		return name
	}
	for _, r := range name[i+1:] {
		if r < '0' || r > '9' {
			return name
		}
	}
	return name[:i+1] + "*"
}

// DropletUSDPerDay prices a droplet from its size's hourly list price.
func DropletUSDPerDay(priceHourly float64) float64 { return priceHourly * 24 }

// StorageUSDPerDay prices GiB of volume or snapshot storage.
func StorageUSDPerDay(gib, usdPerGiBMonth float64) float64 {
	return gib * usdPerGiBMonth / daysPerMonth
}
