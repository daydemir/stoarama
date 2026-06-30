// Package dropletpool is the demand-based DigitalOcean droplet autoscaler for the
// standalone stream recorder. It runs on the dedicated single-instance control
// service alongside the cron scheduler: it forecasts cron demand, provisions
// recorder droplets ahead of it (each with its own scoped local_recorder node
// token), drains and destroys idle ones with hysteresis, reconciles the DB
// against the live DO fleet, and is bounded by a hard spend cap.
package dropletpool

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/digitalocean/godo"
)

// NamePrefix is the droplet-name prefix the autoscaler owns. Reconciliation
// adopts/destroys droplets by this prefix, independent of tags (SRE-orphan).
const NamePrefix = "stoarama-rec-"

// Tags applied to every autoscaled recorder droplet, mirroring the do-capture
// fleet conventions so the droplets show up in the same spend audits.
var dropletTags = []string{
	"project:stoarama",
	"role:recorder",
	"fleet:recorder-pool-v1",
	"env:prod",
}

// CreateDropletInput is the provider-agnostic create request the controller
// hands to the DO client. The node token is injected via cloud-init user_data;
// it is never logged or stored in plaintext.
type CreateDropletInput struct {
	Name       string
	Region     string
	Size       string
	Image      string // snapshot id (numeric) or image slug
	SSHKey     string // optional fingerprint or numeric id
	UserData   string
	ProjectID  string
	FirewallID string
}

// DODroplet is the minimal view of a DigitalOcean droplet the controller needs.
type DODroplet struct {
	ID        int64
	Name      string
	IP        string
	Status    string
	CreatedAt time.Time // DO droplet creation instant (zero if unknown)
}

// DOClient is the narrow DigitalOcean surface the controller depends on. The
// production implementation wraps the godo SDK; tests inject a fake to exercise
// the reconcile/orphan-reap diff without any live API calls.
type DOClient interface {
	// CreateDroplet provisions a droplet, assigns it to the project, and applies
	// the egress firewall before returning. It returns the created droplet id.
	CreateDroplet(ctx context.Context, in CreateDropletInput) (DODroplet, error)
	// DeleteDroplet destroys a droplet by its DO id. Deleting an already-absent
	// droplet must not error (idempotent teardown).
	DeleteDroplet(ctx context.Context, doDropletID int64) error
	// ListDropletsByName returns every droplet in the configured project whose
	// name starts with prefix.
	ListDropletsByName(ctx context.Context, projectID, prefix string) ([]DODroplet, error)
}

// godoClient is the production DOClient backed by the godo SDK.
type godoClient struct {
	c *godo.Client
}

// NewGodoClient builds a production DO client from an operator-scoped API token.
func NewGodoClient(token string) (DOClient, error) {
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("DO API token is empty")
	}
	return &godoClient{c: godo.NewFromToken(strings.TrimSpace(token))}, nil
}

func (g *godoClient) CreateDroplet(ctx context.Context, in CreateDropletInput) (DODroplet, error) {
	req := &godo.DropletCreateRequest{
		Name:       in.Name,
		Region:     in.Region,
		Size:       in.Size,
		Image:      parseImage(in.Image),
		UserData:   in.UserData,
		Tags:       append([]string(nil), dropletTags...),
		Monitoring: true,
		IPv6:       false,
	}
	if key := strings.TrimSpace(in.SSHKey); key != "" {
		if id, err := strconv.Atoi(key); err == nil {
			req.SSHKeys = []godo.DropletCreateSSHKey{{ID: id}}
		} else {
			req.SSHKeys = []godo.DropletCreateSSHKey{{Fingerprint: key}}
		}
	}
	droplet, _, err := g.c.Droplets.Create(ctx, req)
	if err != nil {
		return DODroplet{}, fmt.Errorf("create droplet: %w", err)
	}
	out := DODroplet{ID: int64(droplet.ID), Name: droplet.Name, Status: droplet.Status}

	// A droplet must never survive without its project assignment + egress firewall
	// (S-1): if either post-create step fails, destroy the droplet so it cannot run
	// ffmpeg without the egress block. If the teardown itself fails the droplet is
	// left for the reconcile orphan-reaper (matched by name prefix).
	if pid := strings.TrimSpace(in.ProjectID); pid != "" {
		if _, _, err := g.c.Projects.AssignResources(ctx, pid, droplet.URN()); err != nil {
			return DODroplet{}, g.destroyAfterFailedSetup(ctx, droplet.ID, fmt.Errorf("assign droplet %d to project: %w", droplet.ID, err))
		}
	}
	// Apply the egress firewall that blocks the metadata IP / RFC1918 (S-1).
	if fid := strings.TrimSpace(in.FirewallID); fid != "" {
		if _, err := g.c.Firewalls.AddDroplets(ctx, fid, droplet.ID); err != nil {
			return DODroplet{}, g.destroyAfterFailedSetup(ctx, droplet.ID, fmt.Errorf("apply firewall to droplet %d: %w", droplet.ID, err))
		}
	}
	return out, nil
}

// destroyAfterFailedSetup tears down a droplet whose post-create setup (project
// assignment or egress firewall) failed, so it never runs without the SSRF egress
// block. If the teardown itself fails the droplet is left for the reconcile
// orphan-reaper (matched by name prefix) and the error says so.
func (g *godoClient) destroyAfterFailedSetup(ctx context.Context, dropletID int, cause error) error {
	if _, err := g.c.Droplets.Delete(ctx, dropletID); err != nil {
		return fmt.Errorf("%w; AND failed to destroy unconfigured droplet %d (left for reconcile): %v", cause, dropletID, err)
	}
	return fmt.Errorf("%w (droplet destroyed)", cause)
}

func (g *godoClient) DeleteDroplet(ctx context.Context, doDropletID int64) error {
	resp, err := g.c.Droplets.Delete(ctx, int(doDropletID))
	if err != nil {
		// A 404 means the droplet is already gone; teardown is idempotent.
		if resp != nil && resp.StatusCode == 404 {
			return nil
		}
		return fmt.Errorf("delete droplet %d: %w", doDropletID, err)
	}
	return nil
}

func (g *godoClient) ListDropletsByName(ctx context.Context, projectID, prefix string) ([]DODroplet, error) {
	out := make([]DODroplet, 0, 16)
	opt := &godo.ListOptions{Page: 1, PerPage: 200}
	for {
		droplets, resp, err := g.c.Droplets.List(ctx, opt)
		if err != nil {
			return nil, fmt.Errorf("list droplets: %w", err)
		}
		for _, d := range droplets {
			if !strings.HasPrefix(d.Name, prefix) {
				continue
			}
			ip, _ := d.PublicIPv4()
			created, _ := time.Parse(time.RFC3339, d.Created)
			out = append(out, DODroplet{ID: int64(d.ID), Name: d.Name, IP: ip, Status: d.Status, CreatedAt: created})
		}
		if resp == nil || resp.Links == nil || resp.Links.IsLastPage() {
			break
		}
		page, err := resp.Links.CurrentPage()
		if err != nil {
			return nil, fmt.Errorf("paginate droplets: %w", err)
		}
		opt.Page = page + 1
	}
	return out, nil
}

// parseImage interprets the image config as a numeric snapshot id when it parses
// as an int, otherwise as an image slug (e.g. "ubuntu-24-04-x64").
func parseImage(image string) godo.DropletCreateImage {
	trimmed := strings.TrimSpace(image)
	if id, err := strconv.Atoi(trimmed); err == nil {
		return godo.DropletCreateImage{ID: id}
	}
	return godo.DropletCreateImage{Slug: trimmed}
}

// UserDataConfig is the set of values rendered into a droplet's cloud-init.
type UserDataConfig struct {
	ServerID       string // = droplet name = recorder_droplets.name = worker_id
	NodeToken      string // per-droplet local_recorder node token (RECORDER_NODE_TOKEN)
	BackendAPIURL  string
	Capacity       int // RECORDING_WORKER_CONCURRENCY; must equal DROPLET_POOL_CAPACITY
	HeartbeatSec   int
	PollSec        int
	RepoURL        string
	RepoRef        string
	RepoCloneToken string
}

// BuildUserData renders the recorder droplet cloud-init. It installs an outbound
// egress firewall (iptables) that DROPS traffic to the metadata IP and every
// private range BEFORE the worker starts, then boots the recording worker. It
// reuses the baked binary when the image's recorded build sha matches the
// freshly-reset HEAD (the fast path that keeps a cold MIN=0 boot inside
// ProvisionLead) and rebuilds only when the image lags HEAD. RECORDER_SERVER_ID
// is passed via env so the worker never needs the (now-blocked) metadata service.
func BuildUserData(cfg UserDataConfig) (string, error) {
	if strings.TrimSpace(cfg.ServerID) == "" {
		return "", fmt.Errorf("user data: server id is required")
	}
	if strings.TrimSpace(cfg.NodeToken) == "" {
		return "", fmt.Errorf("user data: node token is required")
	}
	if strings.TrimSpace(cfg.BackendAPIURL) == "" {
		return "", fmt.Errorf("user data: backend api url is required")
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = 1
	}
	if cfg.HeartbeatSec <= 0 {
		cfg.HeartbeatSec = 15
	}
	if cfg.PollSec <= 0 {
		cfg.PollSec = 5
	}
	if strings.TrimSpace(cfg.RepoRef) == "" {
		cfg.RepoRef = "main"
	}
	var buf bytes.Buffer
	if err := userDataTemplate.Execute(&buf, cfg); err != nil {
		return "", fmt.Errorf("render recorder cloud-init: %w", err)
	}
	return buf.String(), nil
}

// userDataTemplate is the recorder droplet cloud-init. The egress firewall is the
// remaining SSRF mitigation for HLS child-segment/redirect URLs that the in-app
// guard cannot re-validate (the in-app guard pins the top-level URL); see S-1.
var userDataTemplate = template.Must(template.New("recorder-cloud-init").Parse(`#cloud-config
package_update: true
package_upgrade: false

packages:
  - ca-certificates
  - curl
  - ffmpeg
  - git
  - golang-go
  - iptables
  - iptables-persistent
  - jq

write_files:
  - path: /etc/stoarama/recorder.env
    permissions: "0600"
    owner: root:root
    content: |
      export BACKEND_API_URL='{{.BackendAPIURL}}'
      export RECORDER_NODE_TOKEN='{{.NodeToken}}'
      export RECORDER_SERVER_ID='{{.ServerID}}'
      export RECORDING_WORKER_CONCURRENCY='{{.Capacity}}'
      export RECORDING_WORKER_HEARTBEAT_SEC='{{.HeartbeatSec}}'
      export RECORDING_WORKER_POLL_SEC='{{.PollSec}}'

  - path: /usr/local/sbin/stoarama-egress-firewall.sh
    permissions: "0755"
    owner: root:root
    content: |
      #!/usr/bin/env bash
      # Outbound egress firewall (S-1): DROP traffic from this droplet to the
      # cloud metadata service and every private/internal range so ffmpeg cannot
      # be redirected (via an HLS child segment or HTTP redirect) at a private or
      # metadata target the in-app guard never sees. Public egress (DO API, the
      # user's S3, public streams) and DNS stay allowed. RECORDER_SERVER_ID is
      # passed via cloud-init env, so the worker never needs the metadata service.
      set -euo pipefail
      BLOCKED4=(
        169.254.0.0/16
        10.0.0.0/8
        172.16.0.0/12
        192.168.0.0/16
        127.0.0.0/8
        100.64.0.0/10
      )
      BLOCKED6=(
        fc00::/7
        fe80::/10
        ::1/128
      )
      # Allow established/related and loopback (stub-resolver) DNS first, then drop
      # the private ranges; public DNS and everything else fall through to the
      # final RETURN. DNS is scoped to loopback so a query cannot be aimed at the
      # metadata IP or an internal resolver (the earlier blanket dport-53 RETURN
      # let DNS reach any address before the REJECT rules, S-1).
      iptables -F STOARAMA_EGRESS 2>/dev/null || true
      iptables -N STOARAMA_EGRESS 2>/dev/null || true
      iptables -F STOARAMA_EGRESS
      iptables -A STOARAMA_EGRESS -m state --state ESTABLISHED,RELATED -j RETURN
      iptables -A STOARAMA_EGRESS -p udp --dport 53 -d 127.0.0.0/8 -j RETURN
      iptables -A STOARAMA_EGRESS -p tcp --dport 53 -d 127.0.0.0/8 -j RETURN
      for cidr in "${BLOCKED4[@]}"; do
        iptables -A STOARAMA_EGRESS -d "$cidr" -j REJECT
      done
      iptables -A STOARAMA_EGRESS -j RETURN
      iptables -C OUTPUT -j STOARAMA_EGRESS 2>/dev/null || iptables -I OUTPUT 1 -j STOARAMA_EGRESS

      ip6tables -F STOARAMA_EGRESS 2>/dev/null || true
      ip6tables -N STOARAMA_EGRESS 2>/dev/null || true
      ip6tables -F STOARAMA_EGRESS
      ip6tables -A STOARAMA_EGRESS -m state --state ESTABLISHED,RELATED -j RETURN
      ip6tables -A STOARAMA_EGRESS -p udp --dport 53 -d ::1/128 -j RETURN
      ip6tables -A STOARAMA_EGRESS -p tcp --dport 53 -d ::1/128 -j RETURN
      for cidr in "${BLOCKED6[@]}"; do
        ip6tables -A STOARAMA_EGRESS -d "$cidr" -j REJECT
      done
      ip6tables -A STOARAMA_EGRESS -j RETURN
      ip6tables -C OUTPUT -j STOARAMA_EGRESS 2>/dev/null || ip6tables -I OUTPUT 1 -j STOARAMA_EGRESS

      netfilter-persistent save || true

  - path: /etc/systemd/system/stoarama-egress-firewall.service
    permissions: "0644"
    owner: root:root
    content: |
      [Unit]
      Description=Stoarama Recorder Egress Firewall
      Wants=network-pre.target
      Before=network-pre.target stoarama-recording.service
      DefaultDependencies=no

      [Service]
      Type=oneshot
      RemainAfterExit=yes
      ExecStart=/usr/local/sbin/stoarama-egress-firewall.sh

      [Install]
      WantedBy=multi-user.target

  - path: /etc/systemd/system/stoarama-recording.service
    permissions: "0644"
    owner: root:root
    content: |
      [Unit]
      Description=Stoarama Recording Worker
      Wants=network-online.target
      After=network-online.target stoarama-egress-firewall.service
      Requires=stoarama-egress-firewall.service

      [Service]
      Type=simple
      User=root
      Group=root
      WorkingDirectory=/opt/stoarama
      Environment=RECORDER_ENV_FILE=/etc/stoarama/recorder.env
      Environment=STOARAMACTL_BIN=/opt/stoarama/bin/stoaramactl
      ExecStart=/opt/stoarama/backend/scripts/start-recording-worker.sh
      Restart=always
      RestartSec=5
      TimeoutStartSec=120
      TimeoutStopSec=20

      [Install]
      WantedBy=multi-user.target

runcmd:
  - mkdir -p /opt /opt/stoarama/bin
  - /usr/local/sbin/stoarama-egress-firewall.sh
  - |
    clone_url='{{.RepoURL}}'
    if [ -n '{{.RepoCloneToken}}' ]; then
      clone_url="$(printf '%s' '{{.RepoURL}}' | sed 's#^https://#https://x-access-token:{{.RepoCloneToken}}@#')"
    fi
    if [ ! -d /opt/stoarama/.git ]; then
      git clone --depth 1 --branch {{.RepoRef}} "$clone_url" /opt/stoarama
    else
      git -C /opt/stoarama remote set-url origin "$clone_url"
      git -C /opt/stoarama fetch --depth 1 origin {{.RepoRef}}
      git -C /opt/stoarama checkout {{.RepoRef}}
      git -C /opt/stoarama reset --hard origin/{{.RepoRef}}
    fi
  - |
    # Prepare the worker binary. With DROPLET_POOL_MIN=0 the pool is cold between
    # fires, so the whole cold-start (snapshot boot + this step + worker register)
    # must fit inside ProvisionLead. A from-scratch go build on the 2-vcpu size
    # measured ~13-15 min, far past the 600s lead, so a cold fire missed its
    # freshness deadline. The fix is a snapshot rebaked from HEAD whose binary is
    # already current: when the baked binary's recorded HEAD sha matches the
    # freshly-reset HEAD, skip the build and boot in snapshot time (~1-2 min).
    #
    # The previous .sha fast-path was removed because the sha could be written
    # separately from the build, leaving a stale binary while .sha falsely claimed
    # it was current. This restores the fast-path SAFELY: the sha is written ONLY
    # by build_worker below, atomically, after go build produced a fresh binary
    # at the final path, so .sha can never name a binary that step did not just
    # build. On a sha MISS (image lags HEAD, e.g. a push since the last rebake) it
    # rebuilds; that rebuild is the slow path and, with MIN=0, requires a rebaked
    # snapshot to stay inside the lead (rebake the image after recorder pushes).
    # A build failure exits non-zero so provisioning fails fast rather than running
    # stale. cloud-init runcmd has no HOME/PATH for the Go 1.24 toolchain or its
    # caches, so set them here.
    set -e
    export HOME=/root
    export PATH=/usr/local/go/bin:$PATH
    export GOPATH=/root/go
    export GOCACHE=/root/.cache/go-build
    HEAD_SHA="$(git -C /opt/stoarama rev-parse HEAD)"
    SHA_FILE=/opt/stoarama/bin/.stoaramactl.sha
    BIN=/opt/stoarama/bin/stoaramactl
    build_worker() {
      # Build to a temp path, then atomically move it into place and record the
      # source HEAD only on success, so .stoaramactl.sha never names a binary this
      # function did not just produce (the staleness bug that removed the old
      # fast-path).
      tmp="$(mktemp /opt/stoarama/bin/.stoaramactl.XXXXXX)"
      (cd /opt/stoarama/backend && go build -o "$tmp" ./cmd/stoaramactl)
      chmod +x "$tmp"
      mv -f "$tmp" "$BIN"
      printf '%s' "$HEAD_SHA" > "$SHA_FILE"
    }
    BUILT_SHA="$(cat "$SHA_FILE" 2>/dev/null || true)"
    if [ -x "$BIN" ] && [ "$HEAD_SHA" = "$BUILT_SHA" ]; then
      echo "stoarama: baked worker binary is current ($HEAD_SHA); skipping build"
    else
      echo "stoarama: baked worker binary missing or stale (have '$BUILT_SHA' want '$HEAD_SHA'); rebuilding"
      build_worker
    fi
  - chmod +x /opt/stoarama/backend/scripts/start-recording-worker.sh
  - systemctl daemon-reload
  - systemctl enable --now stoarama-egress-firewall.service
  - systemctl enable --now stoarama-recording.service
`))

// generateNodeSecret mirrors the API's generateSecret: base64url random with no
// padding. Used to mint the per-droplet local_recorder node token.
func generateNodeSecret(numBytes int) (string, error) {
	b := make([]byte, numBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return strings.TrimRight(base64.RawURLEncoding.EncodeToString(b), "="), nil
}

// hashNodeSecret mirrors the API's hashSecret: SHA-256 hex of the trimmed token.
// The DB stores only this hash; the plaintext token lives only in cloud-init.
func hashNodeSecret(raw string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(raw)))
	return hex.EncodeToString(sum[:])
}
