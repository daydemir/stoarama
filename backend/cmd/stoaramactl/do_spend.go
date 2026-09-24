package main

// DigitalOcean spend guard. The hourly recording-health sweep reads the whole
// DO account, prices it into a $/day burn, and emails operators (deduped
// through ops_alert_episodes) when burn or month-to-date spend crosses its
// budget, when a droplet or volume nobody in Stoarama owns has been running
// for over an hour, or when the pool controller's spend guard blocked a
// scale-up. `stoaramactl do-spend report` shows the same picture on demand.

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/dospend"
	"github.com/daydemir/stoarama/backend/internal/dropletpool"
	"github.com/daydemir/stoarama/backend/internal/email"
)

const (
	signalDOSpendBurn = "do_spend_burn"
	signalDOSpendMTD  = "do_spend_mtd"
	signalDOUnmanaged = "do_unmanaged"
)

// doUnmanagedMinAge keeps a resource an operator just created out of the
// tripwire; anything left running past it pages.
const doUnmanagedMinAge = time.Hour

// doSpendBlockedStaleAfter resolves the controller's scale-up-blocked episode
// once the controller has not re-noted it for this long.
const doSpendBlockedStaleAfter = 2 * time.Hour

func doSpendConfig(cfg config.Config) dospend.Config {
	return dospend.Config{
		WarnUSDPerDay:     cfg.DOSpendWarnUSDPerDay,
		CriticalUSDPerDay: cfg.DOSpendCriticalUSDPerDay,
		MonthlyBudgetUSD:  cfg.DOSpendMonthlyBudgetUSD,
		Allowlist:         dospend.ParseAllowlist(cfg.DOSpendAllowlist),
		UnmanagedMinAge:   doUnmanagedMinAge,
	}
}

// loadDOManagedSet returns the droplets the recorder pool owns: every
// recorder_droplets row that is not terminal. A DO droplet whose row is
// destroyed or failed is an orphan and belongs in the tripwire.
func loadDOManagedSet(ctx context.Context, pool *pgxpool.Pool) (dospend.ManagedSet, error) {
	set := dospend.ManagedSet{DropletIDs: map[string]bool{}, Names: map[string]bool{}}
	rows, err := pool.Query(ctx, `
		SELECT do_droplet_id, name FROM recorder_droplets
		WHERE state NOT IN ('destroyed','failed')`)
	if err != nil {
		return set, fmt.Errorf("load recorder pool droplets: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id *int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return set, fmt.Errorf("scan recorder pool droplet: %w", err)
		}
		if id != nil {
			set.DropletIDs[strconv.FormatInt(*id, 10)] = true
		}
		set.Names[name] = true
	}
	return set, rows.Err()
}

func buildDOSpendReport(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, now time.Time) (dospend.Report, error) {
	spendCfg := doSpendConfig(cfg)
	if err := spendCfg.Validate(); err != nil {
		return dospend.Report{}, err
	}
	client, err := dospend.NewGodoClient(cfg.DOAPIToken)
	if err != nil {
		return dospend.Report{}, fmt.Errorf("DO_API_TOKEN: %w", err)
	}
	inv, err := client.Inventory(ctx, true)
	if err != nil {
		return dospend.Report{}, err
	}
	managed, err := loadDOManagedSet(ctx, pool)
	if err != nil {
		return dospend.Report{}, err
	}
	return dospend.Analyze(inv, managed, spendCfg, now), nil
}

func runDOSpend(ctx context.Context, cfg config.Config, args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: stoaramactl do-spend report [--json] | check [--dry-run]")
		os.Exit(2)
	}
	switch args[0] {
	case "report":
		fs := flag.NewFlagSet("do-spend report", flag.ExitOnError)
		asJSON := fs.Bool("json", false, "print the full report as JSON")
		_ = fs.Parse(args[1:])
		pool := mustOpenPool(ctx, cfg)
		defer pool.Close()
		report, err := buildDOSpendReport(ctx, cfg, pool, time.Now().UTC())
		if err != nil {
			log.Fatalf("do-spend report: %v", err)
		}
		if *asJSON {
			printJSON(report)
			return
		}
		fmt.Print(composeDOSpendReport(report, true))
	case "check":
		fs := flag.NewFlagSet("do-spend check", flag.ExitOnError)
		dryRun := fs.Bool("dry-run", false, "detect and print only; do not write episodes or email")
		_ = fs.Parse(args[1:])
		pool := mustOpenPool(ctx, cfg)
		defer pool.Close()
		printJSON(runDOSpendAlerts(ctx, pool, cfg, *dryRun))
	default:
		fmt.Fprintf(os.Stderr, "unknown do-spend subcommand %q\n", args[0])
		os.Exit(2)
	}
}

type doSpendAlertResult struct {
	Checked        bool    `json:"checked"`
	Skipped        string  `json:"skipped,omitempty"`
	BurnUSDPerDay  float64 `json:"burn_usd_per_day"`
	MonthToDateUSD float64 `json:"month_to_date_usd"`
	Level          string  `json:"level,omitempty"`
	OverBudget     bool    `json:"over_budget"`
	Unmanaged      int     `json:"unmanaged"`
	ScaleUpBlocked bool    `json:"scaleup_blocked"`
	Due            int     `json:"due"`
	Emailed        int     `json:"emailed"`
}

// doSpendAlertKeys maps a report onto the episode keys of each signal. An
// empty slice for a signal resolves its open episodes.
func doSpendAlertKeys(r dospend.Report) map[string][]string {
	keys := map[string][]string{signalDOSpendBurn: {}, signalDOSpendMTD: {}, signalDOUnmanaged: {}}
	if r.Level != dospend.LevelOK {
		keys[signalDOSpendBurn] = append(keys[signalDOSpendBurn], signalDOSpendBurn+":"+r.Level)
	}
	if r.OverBudget {
		keys[signalDOSpendMTD] = append(keys[signalDOSpendMTD], signalDOSpendMTD+":"+r.GeneratedAt.Format("2006-01"))
	}
	for _, res := range r.Unmanaged {
		keys[signalDOUnmanaged] = append(keys[signalDOUnmanaged], signalDOUnmanaged+":"+res.Kind+":"+res.ID)
	}
	return keys
}

// runDOSpendAlerts is one sweep of the spend guard. It is isolated from the
// recording-health run: any failure is logged and never suppresses recording
// alerts, and a failed DO read neither raises nor resolves spend episodes.
func runDOSpendAlerts(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, dryRun bool) doSpendAlertResult {
	out := doSpendAlertResult{}
	if strings.TrimSpace(cfg.DOAPIToken) == "" {
		out.Skipped = "DO_API_TOKEN unset"
		log.Printf("recording health: DO spend check skipped: DO_API_TOKEN unset")
		return out
	}
	now := time.Now().UTC()
	report, err := buildDOSpendReport(ctx, cfg, pool, now)
	if err != nil {
		out.Skipped = err.Error()
		log.Printf("recording health: DO spend check skipped: %v", err)
		return out
	}
	out.Checked, out.BurnUSDPerDay, out.MonthToDateUSD = true, report.BurnUSDPerDay, report.MonthToDateUSD
	out.Level, out.OverBudget, out.Unmanaged = report.Level, report.OverBudget, len(report.Unmanaged)
	blocked, blockedDue, err := doSpendBlockedEpisode(ctx, pool, now, dryRun)
	if err != nil {
		log.Printf("recording health: DO spend scale-up-blocked episode: %v", err)
	}
	out.ScaleUpBlocked = blocked
	if dryRun {
		fmt.Print(composeDOSpendReport(report, false))
		return out
	}
	due := []string{}
	if blockedDue {
		due = append(due, dropletpool.SpendBlockedSignal)
	}
	for signal, keys := range doSpendAlertKeys(report) {
		signalDue, err := recordOpsAlertEpisodes(ctx, pool, signal, keys, now)
		if err != nil {
			log.Printf("recording health: DO spend episodes %s skipped: %v", signal, err)
			continue
		}
		due = append(due, signalDue...)
	}
	out.Due = len(due)
	if len(due) == 0 {
		return out
	}
	sent, err := deliverDOSpendEmail(ctx, pool, cfg, report, blocked)
	out.Emailed = sent
	if err != nil {
		log.Printf("recording health: DO spend alert delivery failed (retries next sweep): %v", err)
		return out
	}
	if err := markOpsAlertsDelivered(ctx, pool, due, now); err != nil {
		log.Printf("recording health: mark DO spend alerts delivered: %v", err)
	}
	return out
}

// doSpendBlockedEpisode reads the controller-owned scale-up-blocked episode.
// It is open while the controller keeps re-noting it; once quiet for
// doSpendBlockedStaleAfter it is resolved here.
func doSpendBlockedEpisode(ctx context.Context, pool *pgxpool.Pool, now time.Time, dryRun bool) (open, due bool, err error) {
	var lastDetected time.Time
	var lastAlerted *time.Time
	err = pool.QueryRow(ctx, `
		SELECT last_detected_at, last_alerted_at FROM ops_alert_episodes
		WHERE alert_key=$1 AND resolved_at IS NULL`, dropletpool.SpendBlockedSignal).Scan(&lastDetected, &lastAlerted)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, false, nil
		}
		return false, false, err
	}
	if now.Sub(lastDetected) > doSpendBlockedStaleAfter {
		if !dryRun {
			_, err = pool.Exec(ctx, `UPDATE ops_alert_episodes SET resolved_at=$2 WHERE alert_key=$1 AND resolved_at IS NULL`, dropletpool.SpendBlockedSignal, now)
		}
		return false, false, err
	}
	return true, lastAlerted == nil || now.Sub(*lastAlerted) >= opsAlertReminder, nil
}

func deliverDOSpendEmail(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, r dospend.Report, blocked bool) (int, error) {
	if strings.ToLower(strings.TrimSpace(cfg.EmailProvider)) != "resend" {
		return 0, fmt.Errorf("EMAIL_PROVIDER=%q is not resend", cfg.EmailProvider)
	}
	recipients := operatorRecipients(ctx, pool)
	if len(recipients) == 0 {
		return 0, fmt.Errorf("no operator recipients")
	}
	mailer, err := email.NewSender(email.Config{Provider: cfg.EmailProvider, From: cfg.EmailFrom, ReplyTo: cfg.EmailReplyTo, ResendKey: cfg.EmailResendAPIKey})
	if err != nil {
		return 0, fmt.Errorf("init email sender: %w", err)
	}
	subject, body := composeDOSpendEmail(r, blocked)
	idem := sha256.Sum256([]byte(subject + body))
	sent := 0
	for _, addr := range recipients {
		rcpt := sha256.Sum256([]byte(strings.ToLower(addr)))
		if _, err := mailer.Send(ctx, email.Message{To: addr, Subject: subject, PlainText: body, MessageType: "do_spend_alert",
			IdempotencyKey: fmt.Sprintf("do-spend:%x:%x", idem[:8], rcpt[:8])}); err != nil {
			return sent, fmt.Errorf("send to %s: %w", addr, err)
		}
		sent++
	}
	return sent, nil
}

func composeDOSpendEmail(r dospend.Report, blocked bool) (string, string) {
	parts := []string{}
	if r.Level != dospend.LevelOK {
		parts = append(parts, fmt.Sprintf("burn %s $%.2f/day", strings.ToUpper(r.Level), r.BurnUSDPerDay))
	}
	if len(r.Unmanaged) > 0 {
		parts = append(parts, fmt.Sprintf("%d unmanaged resource(s) $%.2f/day", len(r.Unmanaged), r.UnmanagedUSDPerDay))
	}
	if r.OverBudget {
		parts = append(parts, fmt.Sprintf("MTD $%.2f over $%.0f budget", r.MonthToDateUSD, r.MonthlyBudgetUSD))
	}
	if blocked {
		parts = append(parts, "pool scale-up blocked")
	}
	if len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("burn $%.2f/day", r.BurnUSDPerDay))
	}
	subject := "[Stoarama] DigitalOcean spend: " + strings.Join(parts, "; ")
	var b strings.Builder
	if blocked {
		b.WriteString("The recorder pool controller BLOCKED a scale-up because the account burn plus one more droplet would exceed the critical ceiling.\nScheduled recordings may be short of capacity. Remove the spend that is not the pool, or raise DO_SPEND_CRITICAL_USD_PER_DAY on stoarama-recorder-control and redeploy.\n\n")
	}
	if len(r.Unmanaged) > 0 {
		b.WriteString("Droplets/volumes below are not owned by the recorder pool and not in DO_SPEND_ALLOWLIST. Destroy them if nobody claims them, or add them to the allowlist.\n\n")
	}
	b.WriteString(composeDOSpendReport(r, false))
	b.WriteString("\nFull inventory: stoaramactl do-spend report\n")
	return subject, b.String()
}

// composeDOSpendReport renders the operator view; withInventory adds every
// priced resource.
func composeDOSpendReport(r dospend.Report, withInventory bool) string {
	var b strings.Builder
	over := ""
	if r.OverBudget {
		over = " OVER BUDGET"
	}
	fmt.Fprintf(&b, "DigitalOcean spend at %s\n", r.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "  Month to date: $%.2f of $%.2f budget%s; projected month end $%.2f\n", r.MonthToDateUSD, r.MonthlyBudgetUSD, over, r.ProjectedMonthUSD)
	fmt.Fprintf(&b, "  Burn (list price of current droplets, volumes, snapshots): $%.2f/day ($%.0f/month), level %s (warn > $%.2f, critical > $%.2f)\n",
		r.BurnUSDPerDay, r.BurnUSDPerDay*365/12, strings.ToUpper(r.Level), r.WarnUSDPerDay, r.CriticalUSDPerDay)
	b.WriteString("  Burn excludes managed databases, bandwidth, and other products; month to date includes everything.\n\n")
	b.WriteString("BREAKDOWN ($/day by tag or name prefix)\n")
	for _, g := range r.Groups {
		fmt.Fprintf(&b, "  %8.2f  %3d  %s\n", g.USDPerDay, g.Count, g.Group)
	}
	fmt.Fprintf(&b, "\nUNMANAGED (not pool, not allowlisted, older than %s): %d, $%.2f/day\n", doUnmanagedMinAge, len(r.Unmanaged), r.UnmanagedUSDPerDay)
	for _, res := range r.Unmanaged {
		fmt.Fprintf(&b, "  %s %s (id %s) %s $%.2f/day created %s\n", res.Kind, res.Name, res.ID, res.Size, res.USDPerDay, res.CreatedAt.UTC().Format(time.RFC3339))
	}
	if withInventory {
		b.WriteString("\nINVENTORY\n")
		for _, res := range r.Resources {
			fmt.Fprintf(&b, "  %-8s %-40s %-14s %-9s %7.2f/day  %s\n", res.Kind, res.Name, res.Size, res.Owner, res.USDPerDay, res.CreatedAt.UTC().Format("2006-01-02"))
		}
	}
	return b.String()
}

// composeDigestDOSpend is the 8-hour digest section. A failed read is shown,
// not hidden, so a broken token cannot silently disable the guard.
func composeDigestDOSpend(r *dospend.Report, readErr error) string {
	var b strings.Builder
	b.WriteString("\nDIGITALOCEAN SPEND\n")
	if r == nil {
		fmt.Fprintf(&b, "  Unavailable: %v. The spend guard is blind until this is fixed.\n", readErr)
		return b.String()
	}
	fmt.Fprintf(&b, "  Burn $%.2f/day (%s); month to date $%.2f of $%.0f budget; %d unmanaged ($%.2f/day).\n",
		r.BurnUSDPerDay, r.Level, r.MonthToDateUSD, r.MonthlyBudgetUSD, len(r.Unmanaged), r.UnmanagedUSDPerDay)
	for i, g := range r.Groups {
		if i == 5 {
			break
		}
		fmt.Fprintf(&b, "  %8.2f/day  %3d  %s\n", g.USDPerDay, g.Count, g.Group)
	}
	for _, res := range r.Unmanaged {
		fmt.Fprintf(&b, "  UNMANAGED %s %s %s $%.2f/day\n", res.Kind, res.Name, res.Size, res.USDPerDay)
	}
	return b.String()
}

func loadDigestDOSpend(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, now time.Time) string {
	if strings.TrimSpace(cfg.DOAPIToken) == "" {
		return composeDigestDOSpend(nil, fmt.Errorf("DO_API_TOKEN unset on this cron"))
	}
	r, err := buildDOSpendReport(ctx, cfg, pool, now)
	if err != nil {
		return composeDigestDOSpend(nil, err)
	}
	return composeDigestDOSpend(&r, nil)
}
