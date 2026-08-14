package dropletpool

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/digitalocean/godo"
)

// ProviderAttestation is the minimum server-observed DigitalOcean state needed
// to prove a targeted probe runs on the configured physical pool object. It is
// intentionally path-, IP-, and credential-free.
type ProviderAttestation struct {
	DropletID int64
	Name      string
	Region    string
	Status    string
}

// AttestManagedDroplet reads the current physical droplet, project membership,
// and exact egress firewall from DigitalOcean. Heartbeat JSON alone is never
// sufficient for campaign admission.
func AttestManagedDroplet(ctx context.Context, token, projectID, firewallID string, dropletID int64, expectedName string) (ProviderAttestation, error) {
	if strings.TrimSpace(token) == "" || strings.TrimSpace(projectID) == "" || strings.TrimSpace(firewallID) == "" || dropletID <= 0 || strings.TrimSpace(expectedName) == "" {
		return ProviderAttestation{}, fmt.Errorf("managed droplet provider attestation is not configured")
	}
	c := godo.NewFromToken(strings.TrimSpace(token))
	d, _, err := c.Droplets.Get(ctx, int(dropletID))
	if err != nil {
		return ProviderAttestation{}, fmt.Errorf("get DigitalOcean droplet: %w", err)
	}
	if d.ID != int(dropletID) || d.Name != strings.TrimSpace(expectedName) || d.Status != "active" || d.Region == nil || strings.TrimSpace(d.Region.Slug) == "" {
		return ProviderAttestation{}, fmt.Errorf("DigitalOcean droplet identity/status mismatch")
	}
	resources, _, err := c.Projects.ListResources(ctx, strings.TrimSpace(projectID), &godo.ListOptions{PerPage: 200})
	if err != nil {
		return ProviderAttestation{}, fmt.Errorf("list DigitalOcean project resources: %w", err)
	}
	wantURN := "do:droplet:" + strconv.FormatInt(dropletID, 10)
	inProject := false
	for _, resource := range resources {
		if resource.URN == wantURN {
			inProject = true
			break
		}
	}
	if !inProject {
		return ProviderAttestation{}, fmt.Errorf("DigitalOcean droplet is outside the configured recorder project")
	}
	firewalls, _, err := c.Firewalls.ListByDroplet(ctx, int(dropletID), &godo.ListOptions{PerPage: 200})
	if err != nil {
		return ProviderAttestation{}, fmt.Errorf("list DigitalOcean droplet firewalls: %w", err)
	}
	hasFirewall := false
	for _, firewall := range firewalls {
		if firewall.ID == strings.TrimSpace(firewallID) && firewall.Status == "succeeded" {
			hasFirewall = true
			break
		}
	}
	if !hasFirewall {
		return ProviderAttestation{}, fmt.Errorf("DigitalOcean droplet lacks the exact succeeded recorder firewall")
	}
	return ProviderAttestation{DropletID: dropletID, Name: d.Name, Region: d.Region.Slug, Status: d.Status}, nil
}
