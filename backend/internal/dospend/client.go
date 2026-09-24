package dospend

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/digitalocean/godo"
)

// Client reads the DO account. The production implementation wraps godo;
// tests inject a fake.
type Client interface {
	// Inventory lists every droplet, volume, and snapshot, priced at list
	// price. withBalance also reads month-to-date usage (one more call).
	Inventory(ctx context.Context, withBalance bool) (Inventory, error)
	// SizeUSDPerDay returns the list price of one droplet of size slug.
	SizeUSDPerDay(ctx context.Context, slug string) (float64, error)
}

type godoClient struct{ c *godo.Client }

// NewGodoClient builds a read-only spend client from a DO API token. It needs
// read access to droplet, block_storage, snapshot, sizes, and billing.
func NewGodoClient(token string) (Client, error) {
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("DO API token is empty")
	}
	return &godoClient{c: godo.NewFromToken(strings.TrimSpace(token))}, nil
}

const perPage = 200

func (g *godoClient) Inventory(ctx context.Context, withBalance bool) (Inventory, error) {
	inv := Inventory{}
	if withBalance {
		bal, _, err := g.c.Balance.Get(ctx)
		if err != nil {
			return inv, fmt.Errorf("read DO balance: %w", err)
		}
		mtd, err := strconv.ParseFloat(strings.TrimSpace(bal.MonthToDateUsage), 64)
		if err != nil {
			return inv, fmt.Errorf("parse DO month_to_date_usage %q: %w", bal.MonthToDateUsage, err)
		}
		inv.MonthToDateUSD, inv.BalanceAt = mtd, bal.GeneratedAt
	}
	for page := 1; ; page++ {
		droplets, resp, err := g.c.Droplets.List(ctx, &godo.ListOptions{Page: page, PerPage: perPage})
		if err != nil {
			return inv, fmt.Errorf("list DO droplets: %w", err)
		}
		for _, d := range droplets {
			inv.Resources = append(inv.Resources, dropletResource(d))
		}
		if lastPage(resp) {
			break
		}
	}
	for page := 1; ; page++ {
		volumes, resp, err := g.c.Storage.ListVolumes(ctx, &godo.ListVolumeParams{ListOptions: &godo.ListOptions{Page: page, PerPage: perPage}})
		if err != nil {
			return inv, fmt.Errorf("list DO volumes: %w", err)
		}
		for _, v := range volumes {
			inv.Resources = append(inv.Resources, volumeResource(v))
		}
		if lastPage(resp) {
			break
		}
	}
	for page := 1; ; page++ {
		snaps, resp, err := g.c.Snapshots.List(ctx, &godo.ListOptions{Page: page, PerPage: perPage})
		if err != nil {
			return inv, fmt.Errorf("list DO snapshots: %w", err)
		}
		for _, s := range snaps {
			inv.Resources = append(inv.Resources, snapshotResource(s))
		}
		if lastPage(resp) {
			break
		}
	}
	return inv, nil
}

func (g *godoClient) SizeUSDPerDay(ctx context.Context, slug string) (float64, error) {
	for page := 1; ; page++ {
		sizes, resp, err := g.c.Sizes.List(ctx, &godo.ListOptions{Page: page, PerPage: perPage})
		if err != nil {
			return 0, fmt.Errorf("list DO sizes: %w", err)
		}
		for _, s := range sizes {
			if s.Slug == slug {
				return DropletUSDPerDay(s.PriceHourly), nil
			}
		}
		if lastPage(resp) {
			return 0, fmt.Errorf("DO size %q not found", slug)
		}
	}
}

func lastPage(resp *godo.Response) bool {
	return resp == nil || resp.Links == nil || resp.Links.IsLastPage()
}

func dropletResource(d godo.Droplet) Resource {
	created, _ := time.Parse(time.RFC3339, d.Created)
	price := 0.0
	if d.Size != nil {
		price = d.Size.PriceHourly
	}
	return Resource{
		Kind: KindDroplet, ID: strconv.Itoa(d.ID), Name: d.Name, Size: d.SizeSlug, Status: d.Status,
		Tags: d.Tags, CreatedAt: created, USDPerDay: DropletUSDPerDay(price),
	}
}

func volumeResource(v godo.Volume) Resource {
	ids := make([]string, 0, len(v.DropletIDs))
	for _, id := range v.DropletIDs {
		ids = append(ids, strconv.Itoa(id))
	}
	return Resource{
		Kind: KindVolume, ID: v.ID, Name: v.Name, Size: fmt.Sprintf("%dGiB", v.SizeGigaBytes), Tags: v.Tags,
		CreatedAt: v.CreatedAt, DropletIDs: ids, USDPerDay: StorageUSDPerDay(float64(v.SizeGigaBytes), VolumeUSDPerGiBMonth),
	}
}

func snapshotResource(s godo.Snapshot) Resource {
	created, _ := time.Parse(time.RFC3339, s.Created)
	return Resource{
		Kind: KindSnapshot, ID: s.ID, Name: s.Name, Size: fmt.Sprintf("%.2fGiB", s.SizeGigaBytes), Status: s.ResourceType,
		Tags: s.Tags, CreatedAt: created, USDPerDay: StorageUSDPerDay(s.SizeGigaBytes, SnapshotUSDPerGiBMonth),
	}
}

// Guard answers the pool controller's pre-scale-up question: what is the
// account burning now, and what would one more droplet of this size add.
type Guard struct {
	Client            Client
	CriticalUSDPerDay float64
}

// Quote returns the account-wide burn and the $/day price of one droplet of
// size. It skips the balance call; the burn alone decides scale-up.
func (g Guard) Quote(ctx context.Context, size string) (burnUSDPerDay, sizeUSDPerDay float64, err error) {
	inv, err := g.Client.Inventory(ctx, false)
	if err != nil {
		return 0, 0, err
	}
	for _, r := range inv.Resources {
		burnUSDPerDay += r.USDPerDay
	}
	// Prefer the price of an existing droplet of the same size (no extra call).
	for _, r := range inv.Resources {
		if r.Kind == KindDroplet && r.Size == size && r.USDPerDay > 0 {
			return burnUSDPerDay, r.USDPerDay, nil
		}
	}
	sizeUSDPerDay, err = g.Client.SizeUSDPerDay(ctx, size)
	return burnUSDPerDay, sizeUSDPerDay, err
}

// Critical returns the $/day ceiling a scale-up must stay under.
func (g Guard) Critical() float64 { return g.CriticalUSDPerDay }
