package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/dropletpool"
	"github.com/digitalocean/godo"
)

// The unified survey+detection droplet is a STANDALONE persistent box, fully
// isolated from the recorder pool. PROD-ISOLATION MUST-FIXES enforced here:
//
//   - Distinct name prefix (NOT dropletpool.NamePrefix "stoarama-rec-"), so the
//     recorder pool's reconcile() orphan-reaper (which destroys prefix-matched
//     droplets with no pool DB row) can NEVER match and destroy this droplet.
//   - A SEPARATE DO project (must differ from DROPLET_POOL_PROJECT_ID), so it is
//     not grouped with or managed alongside recorder resources.
//   - A DEDICATED DO firewall created just for this droplet; the recorder pool's
//     DROPLET_POOL_FIREWALL_ID is NEVER read or mutated. The metadata-IP / RFC1918
//     egress block is enforced in-droplet by the same iptables script the recorder
//     uses (rendered into cloud-init below), so isolation is identical without
//     touching any recorder-shared resource.
//   - Survey-specific tags (role:survey), never the recorder-pool dropletTags, so
//     spend audits and any tag-based tooling never see it as a recorder.
const (
	surveyDropletNamePrefix = "stoarama-survey-"
	surveyFirewallName      = "stoarama-survey-egress"

	// Pinned onnxruntime CPU build for the droplet (linux x64). Verified sha256 of
	// the GitHub release tarball; cloud-init fails fast on mismatch.
	surveyORTVersion = "1.26.0"
	surveyORTSHA256  = "1254da24fb389cf39dc0ff3451ab48301740ffbfcbaf646849df92f80ee92c57"

	// macbook-pro DO SSH key id (this Mac) added alongside the pool key so the
	// operator can SSH in to verify. (Memory: DO key 'macbook-pro' id 42737010.)
	macbookSSHKeyID = "42737010"
)

var surveyDropletTags = []string{
	"project:stoarama",
	"role:survey",
	"fleet:survey-detect-v1",
	"env:prod",
}

func runSurveyDroplet(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl survey-droplet provision ...")
	}
	switch args[0] {
	case "provision":
		runSurveyDropletProvision(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown survey-droplet subcommand: %s", args[0])
	}
}

func runSurveyDropletProvision(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("survey-droplet provision", flag.ExitOnError)
	name := fs.String("name", surveyDropletNamePrefix+"1", "droplet name (must start with "+surveyDropletNamePrefix+", must NOT start with the recorder prefix)")
	region := fs.String("region", cfg.DropletPoolRegion, "DO region (default = recorder pool region for matching egress reputation)")
	size := fs.String("size", "s-2vcpu-4gb", "droplet size")
	image := fs.String("image", "ubuntu-24-04-x64", "base image slug")
	projectID := fs.String("project-id", "", "SEPARATE DO project id for the survey droplet (REQUIRED; must NOT be the recorder DROPLET_POOL_PROJECT_ID)")
	sshKey := fs.String("ssh-key", cfg.DropletPoolSSHKey, "DO ssh key id/fingerprint for access (macbook-pro key is added automatically)")
	repoURL := fs.String("repo-url", cfg.DropletPoolRepoURL, "git repo url to clone/build on the droplet")
	repoRef := fs.String("repo-ref", cfg.DropletPoolRepoRef, "git ref to build")
	repoCloneToken := fs.String("repo-clone-token", cfg.DropletPoolRepoCloneToken, "git clone token for a private repo")
	surveyConcurrency := fs.Int("survey-concurrency", 3, "survey sweep concurrency on the droplet (CPU-bound at 2 vCPU; keep small)")
	apply := fs.Bool("apply", false, "actually create the firewall + droplet (default: dry-run, render cloud-init only)")
	_ = fs.Parse(args)

	// MUST-FIX #1: name prefix isolation.
	if !strings.HasPrefix(*name, surveyDropletNamePrefix) {
		log.Fatalf("--name must start with %q (isolation from the recorder pool)", surveyDropletNamePrefix)
	}
	if strings.HasPrefix(*name, dropletpool.NamePrefix) {
		log.Fatalf("--name must NOT start with the recorder pool prefix %q (it would be reaped as an orphan)", dropletpool.NamePrefix)
	}
	// MUST-FIX #1: separate project, never the recorder pool project.
	if strings.TrimSpace(*projectID) == "" {
		log.Fatalf("--project-id is required (a SEPARATE DO project from the recorder pool)")
	}
	if strings.TrimSpace(*projectID) == strings.TrimSpace(cfg.DropletPoolProjectID) {
		log.Fatalf("--project-id must NOT equal the recorder pool DROPLET_POOL_PROJECT_ID (%q): keep the survey droplet in its own project", cfg.DropletPoolProjectID)
	}
	if strings.TrimSpace(cfg.DOAPIToken) == "" {
		log.Fatalf("DO_API_TOKEN is required")
	}

	// Secrets injected into cloud-init survey.env. Fail fast if missing so we never
	// provision a box that cannot capture or detect.
	if strings.TrimSpace(cfg.DatabaseURL) == "" {
		log.Fatalf("DATABASE_URL is required (written to the droplet survey.env)")
	}
	if err := cfg.ValidateR2(); err != nil {
		log.Fatalf("R2 config required for the droplet survey.env: %v", err)
	}
	if strings.TrimSpace(*repoURL) == "" {
		log.Fatalf("--repo-url (or DROPLET_POOL_REPO_URL) is required to build on the droplet")
	}

	userData, err := renderSurveyCloudInit(surveyCloudInitConfig{
		RepoURL:         *repoURL,
		RepoRef:         firstNonBlank(*repoRef, "main"),
		RepoCloneToken:  *repoCloneToken,
		DatabaseURL:     cfg.DatabaseURL,
		R2AccountID:     cfg.R2AccountID,
		R2AccessKeyID:   cfg.R2AccessKeyID,
		R2SecretKey:     cfg.R2SecretAccessKey,
		R2Bucket:        cfg.R2Bucket,
		R2Region:        firstNonBlank(cfg.R2Region, "auto"),
		R2Endpoint:      cfg.R2Endpoint,
		ModelKey:        cfg.SurveyModelKey,
		ModelSHA256:     cfg.SurveyModelSHA256,
		ModelPath:       cfg.SurveyModelPath,
		PipelineVersion: cfg.SurveyDetectPipelineVersion,
		Conf:            cfg.SurveyDetectConf,
		IoU:             cfg.SurveyDetectIoU,
		Imgsz:           cfg.SurveyDetectImgsz,
		IntraOpThreads:  cfg.SurveyDetectIntraOpThreads,
		SampleRate:      cfg.SurveyDetectSampleRate,
		Concurrency:     *surveyConcurrency,
		ORTVersion:      surveyORTVersion,
		ORTSHA256:       surveyORTSHA256,
		ProbeHost:       *name,
	})
	if err != nil {
		log.Fatalf("render survey cloud-init: %v", err)
	}

	sshKeys := strings.TrimSpace(*sshKey)
	if sshKeys != "" {
		sshKeys += ","
	}
	sshKeys += macbookSSHKeyID

	fmt.Printf("survey-droplet provision plan:\n")
	fmt.Printf("  name=%s region=%s size=%s image=%s\n", *name, *region, *size, *image)
	fmt.Printf("  project=%s (separate from recorder pool project=%s)\n", *projectID, orNone(cfg.DropletPoolProjectID))
	fmt.Printf("  firewall=%s (dedicated; recorder DROPLET_POOL_FIREWALL_ID untouched)\n", surveyFirewallName)
	fmt.Printf("  ssh_keys=%s tags=%s\n", sshKeys, strings.Join(surveyDropletTags, ","))
	fmt.Printf("  cloud-init bytes=%d\n", len(userData))

	if !*apply {
		fmt.Printf("dry-run (no --apply): nothing created. Re-run with --apply to provision.\n")
		return
	}

	godoClient := godo.NewFromToken(strings.TrimSpace(cfg.DOAPIToken))

	// MUST-FIX #2: create a DEDICATED firewall (never touch the recorder firewall).
	fwID, err := ensureSurveyFirewall(ctx, godoClient)
	if err != nil {
		log.Fatalf("create survey firewall: %v", err)
	}
	fmt.Printf("survey firewall id=%s\n", fwID)

	client, err := dropletpool.NewGodoClient(strings.TrimSpace(cfg.DOAPIToken))
	if err != nil {
		log.Fatalf("do client: %v", err)
	}
	d, err := client.CreateDroplet(ctx, dropletpool.CreateDropletInput{
		Name:       *name,
		Region:     *region,
		Size:       *size,
		Image:      *image,
		SSHKey:     sshKeys,
		UserData:   userData,
		ProjectID:  *projectID,
		FirewallID: fwID,
		Tags:       surveyDropletTags,
	})
	if err != nil {
		log.Fatalf("create survey droplet: %v", err)
	}
	fmt.Printf("survey droplet created: id=%d name=%s status=%s\n", d.ID, d.Name, d.Status)
	fmt.Printf("SSH once it boots: ssh root@<ip> (get ip: doctl compute droplet get %d --format PublicIPv4)\n", d.ID)
}

// ensureSurveyFirewall creates (or finds) the dedicated survey firewall: inbound
// SSH only, all outbound allowed. The metadata/RFC1918 egress block is enforced
// in-droplet by iptables (cloud-init), identical to the recorder. The recorder's
// firewall is never referenced.
func ensureSurveyFirewall(ctx context.Context, c *godo.Client) (string, error) {
	fws, _, err := c.Firewalls.List(ctx, &godo.ListOptions{PerPage: 200})
	if err != nil {
		return "", fmt.Errorf("list firewalls: %w", err)
	}
	for _, f := range fws {
		if f.Name == surveyFirewallName {
			return f.ID, nil
		}
	}
	req := &godo.FirewallRequest{
		Name: surveyFirewallName,
		InboundRules: []godo.InboundRule{
			{Protocol: "tcp", PortRange: "22", Sources: &godo.Sources{Addresses: []string{"0.0.0.0/0", "::/0"}}},
		},
		OutboundRules: []godo.OutboundRule{
			{Protocol: "tcp", PortRange: "all", Destinations: &godo.Destinations{Addresses: []string{"0.0.0.0/0", "::/0"}}},
			{Protocol: "udp", PortRange: "all", Destinations: &godo.Destinations{Addresses: []string{"0.0.0.0/0", "::/0"}}},
			{Protocol: "icmp", Destinations: &godo.Destinations{Addresses: []string{"0.0.0.0/0", "::/0"}}},
		},
	}
	fw, _, err := c.Firewalls.Create(ctx, req)
	if err != nil {
		return "", fmt.Errorf("create firewall: %w", err)
	}
	return fw.ID, nil
}

func firstNonBlank(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func orNone(v string) string {
	if strings.TrimSpace(v) == "" {
		return "(unset)"
	}
	return v
}

type surveyCloudInitConfig struct {
	RepoURL         string
	RepoRef         string
	RepoCloneToken  string
	DatabaseURL     string
	R2AccountID     string
	R2AccessKeyID   string
	R2SecretKey     string
	R2Bucket        string
	R2Region        string
	R2Endpoint      string
	ModelKey        string
	ModelSHA256     string
	ModelPath       string
	PipelineVersion string
	Conf            float64
	IoU             float64
	Imgsz           int
	IntraOpThreads  int
	SampleRate      float64
	Concurrency     int
	ORTVersion      string
	ORTSHA256       string
	ProbeHost       string
}

func renderSurveyCloudInit(c surveyCloudInitConfig) (string, error) {
	var buf bytes.Buffer
	if err := surveyCloudInitTemplate.Execute(&buf, c); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// surveyCloudInitTemplate provisions the ONE unified survey+detection+probe box,
// fail-fast at each step. It mirrors the recorder's egress-firewall iptables so
// SSRF egress is blocked identically, installs the pinned onnxruntime .so and
// yolo11x model (sha256-verified), CGO-builds stoaramactl (with the ONNX binding),
// and installs three systemd timer/oneshot workloads: hourly survey --detect,
// and a staggered recordability probe.
var surveyCloudInitTemplate = template.Must(template.New("survey-cloud-init").Parse(`#cloud-config
package_update: true
package_upgrade: false

packages:
  - ca-certificates
  - curl
  - git
  - gcc
  - golang-go
  - iptables
  - iptables-persistent
  - jq
  - xz-utils

write_files:
  - path: /etc/stoarama/survey.env
    permissions: "0600"
    owner: root:root
    content: |
      export DATABASE_URL='{{.DatabaseURL}}'
      export R2_ACCOUNT_ID='{{.R2AccountID}}'
      export R2_ACCESS_KEY_ID='{{.R2AccessKeyID}}'
      export R2_SECRET_ACCESS_KEY='{{.R2SecretKey}}'
      export R2_BUCKET='{{.R2Bucket}}'
      export R2_REGION='{{.R2Region}}'
      export R2_ENDPOINT='{{.R2Endpoint}}'
      export FFMPEG_BIN=/usr/local/bin/ffmpeg
      export YT_DLP_BIN=/usr/local/bin/yt-dlp
      export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
      export STREAM_RECORDABILITY_PROBE_ENABLED=true
      export ONNXRUNTIME_LIB_PATH=/usr/local/lib/onnxruntime/libonnxruntime.so
      export SURVEY_MODEL_PATH='{{.ModelPath}}'
      export SURVEY_MODEL_KEY='{{.ModelKey}}'
      export SURVEY_MODEL_SHA256='{{.ModelSHA256}}'
      export SURVEY_DETECT_CONF='{{.Conf}}'
      export SURVEY_DETECT_IOU='{{.IoU}}'
      export SURVEY_DETECT_IMGSZ='{{.Imgsz}}'
      export SURVEY_DETECT_INTRA_OP_THREADS='{{.IntraOpThreads}}'
      export SURVEY_DETECT_SAMPLE_RATE='{{.SampleRate}}'
      export SURVEY_DETECT_PIPELINE_VERSION='{{.PipelineVersion}}'

  - path: /usr/local/sbin/stoarama-egress-firewall.sh
    permissions: "0755"
    owner: root:root
    content: |
      #!/usr/bin/env bash
      # Outbound egress firewall (S-1), identical intent to the recorder pool: DROP
      # traffic to the cloud metadata service and every private/internal range so
      # ffmpeg cannot be redirected at a private/metadata target. Public egress
      # (R2, DB over public, public streams) and loopback DNS stay allowed.
      set -euo pipefail
      BLOCKED4=(169.254.0.0/16 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 127.0.0.0/8 100.64.0.0/10)
      BLOCKED6=(fc00::/7 fe80::/10 ::1/128)
      iptables -F STOARAMA_EGRESS 2>/dev/null || true
      iptables -N STOARAMA_EGRESS 2>/dev/null || true
      iptables -F STOARAMA_EGRESS
      iptables -A STOARAMA_EGRESS -m state --state ESTABLISHED,RELATED -j RETURN
      iptables -A STOARAMA_EGRESS -p udp --dport 53 -d 127.0.0.0/8 -j RETURN
      iptables -A STOARAMA_EGRESS -p tcp --dport 53 -d 127.0.0.0/8 -j RETURN
      for cidr in "${BLOCKED4[@]}"; do iptables -A STOARAMA_EGRESS -d "$cidr" -j REJECT; done
      iptables -A STOARAMA_EGRESS -j RETURN
      iptables -C OUTPUT -j STOARAMA_EGRESS 2>/dev/null || iptables -I OUTPUT 1 -j STOARAMA_EGRESS
      ip6tables -F STOARAMA_EGRESS 2>/dev/null || true
      ip6tables -N STOARAMA_EGRESS 2>/dev/null || true
      ip6tables -F STOARAMA_EGRESS
      ip6tables -A STOARAMA_EGRESS -m state --state ESTABLISHED,RELATED -j RETURN
      ip6tables -A STOARAMA_EGRESS -p udp --dport 53 -d ::1/128 -j RETURN
      ip6tables -A STOARAMA_EGRESS -p tcp --dport 53 -d ::1/128 -j RETURN
      for cidr in "${BLOCKED6[@]}"; do ip6tables -A STOARAMA_EGRESS -d "$cidr" -j REJECT; done
      ip6tables -A STOARAMA_EGRESS -j RETURN
      ip6tables -C OUTPUT -j STOARAMA_EGRESS 2>/dev/null || ip6tables -I OUTPUT 1 -j STOARAMA_EGRESS
      netfilter-persistent save || true

  - path: /etc/systemd/system/stoarama-egress-firewall.service
    permissions: "0644"
    owner: root:root
    content: |
      [Unit]
      Description=Stoarama Survey Egress Firewall
      Wants=network-pre.target
      Before=network-pre.target
      DefaultDependencies=no
      [Service]
      Type=oneshot
      RemainAfterExit=yes
      ExecStart=/usr/local/sbin/stoarama-egress-firewall.sh
      [Install]
      WantedBy=multi-user.target

  - path: /etc/systemd/system/stoarama-survey.service
    permissions: "0644"
    owner: root:root
    content: |
      [Unit]
      Description=Stoarama Survey + Detection sweep
      Wants=network-online.target
      After=network-online.target stoarama-egress-firewall.service
      Requires=stoarama-egress-firewall.service
      [Service]
      Type=oneshot
      EnvironmentFile=/etc/stoarama/survey.env
      WorkingDirectory=/opt/stoarama/backend
      ExecStart=/opt/stoarama/bin/stoaramactl survey run-once --detect --concurrency {{.Concurrency}} --json

  - path: /etc/systemd/system/stoarama-survey.timer
    permissions: "0644"
    owner: root:root
    content: |
      [Unit]
      Description=Hourly Stoarama Survey + Detection
      [Timer]
      OnCalendar=*-*-* *:00:00
      Persistent=true
      [Install]
      WantedBy=timers.target

  - path: /etc/systemd/system/stoarama-recordability.service
    permissions: "0644"
    owner: root:root
    content: |
      [Unit]
      Description=Stoarama Recordability probe
      Wants=network-online.target
      After=network-online.target stoarama-egress-firewall.service
      Requires=stoarama-egress-firewall.service
      [Service]
      Type=oneshot
      EnvironmentFile=/etc/stoarama/survey.env
      WorkingDirectory=/opt/stoarama/backend
      ExecStart=/opt/stoarama/bin/stoaramactl recordability run-once --window-sec 600 --segment-sec 60 --probe-host {{.ProbeHost}} --json

  - path: /etc/systemd/system/stoarama-recordability.timer
    permissions: "0644"
    owner: root:root
    content: |
      [Unit]
      Description=Staggered Stoarama Recordability probe
      [Timer]
      OnCalendar=*-*-* *:30:00
      Persistent=true
      [Install]
      WantedBy=timers.target

runcmd:
  - set -e
  - mkdir -p /opt /usr/local/lib/onnxruntime
  - /usr/local/sbin/stoarama-egress-firewall.sh
  # ffmpeg (BtbN gpl) + yt-dlp, same rationale as the Render survey cron.
  - |
    set -e
    curl -fsSL -o /tmp/ffmpeg.tar.xz https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz
    mkdir -p /tmp/ffmpeg-extract
    tar -xJf /tmp/ffmpeg.tar.xz -C /tmp/ffmpeg-extract --strip-components=1
    cp /tmp/ffmpeg-extract/bin/ffmpeg /usr/local/bin/ffmpeg
    cp /tmp/ffmpeg-extract/bin/ffprobe /usr/local/bin/ffprobe
    chmod +x /usr/local/bin/ffmpeg /usr/local/bin/ffprobe
    curl -fsSL -o /usr/local/bin/yt-dlp https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux
    chmod +x /usr/local/bin/yt-dlp
  # onnxruntime CPU shared library (pinned version + sha256 verify, fail-fast).
  - |
    set -e
    curl -fsSL -o /tmp/ort.tgz https://github.com/microsoft/onnxruntime/releases/download/v{{.ORTVersion}}/onnxruntime-linux-x64-{{.ORTVersion}}.tgz
    echo "{{.ORTSHA256}}  /tmp/ort.tgz" | sha256sum -c -
    tar -xzf /tmp/ort.tgz -C /tmp
    cp -a /tmp/onnxruntime-linux-x64-{{.ORTVersion}}/lib/. /usr/local/lib/onnxruntime/
    ldconfig
  # clone + CGO build stoaramactl (WITH the onnxruntime binding; CGO needs gcc).
  - |
    set -e
    export HOME=/root PATH=/usr/local/go/bin:$PATH GOPATH=/root/go GOCACHE=/root/.cache/go-build CGO_ENABLED=1
    clone_url='{{.RepoURL}}'
    if [ -n '{{.RepoCloneToken}}' ]; then
      clone_url="$(printf '%s' '{{.RepoURL}}' | sed 's#^https://#https://x-access-token:{{.RepoCloneToken}}@#')"
    fi
    if [ ! -d /opt/stoarama/.git ]; then
      rm -rf /opt/stoarama
      git clone --depth 1 --branch {{.RepoRef}} "$clone_url" /opt/stoarama
    else
      git -C /opt/stoarama fetch --depth 1 origin {{.RepoRef}}
      git -C /opt/stoarama reset --hard origin/{{.RepoRef}}
    fi
    mkdir -p /opt/stoarama/bin
    (cd /opt/stoarama/backend && CGO_ENABLED=1 go build -o /opt/stoarama/bin/stoaramactl ./cmd/stoaramactl)
  # fetch + sha256-verify the yolo11x model (fail-fast on mismatch, no start).
  - |
    set -a; . /etc/stoarama/survey.env; set +a
    /opt/stoarama/bin/stoaramactl survey download-model
  - systemctl daemon-reload
  - systemctl enable --now stoarama-egress-firewall.service
  - systemctl enable --now stoarama-survey.timer
  - systemctl enable --now stoarama-recordability.timer
`))
