package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/qualitygrade"
)

type qualityGradesReport struct {
	Days       int                      `json:"days"`
	RunLength  int                      `json:"run_length"`
	Recordings []qualitygrade.Recording `json:"recordings"`
}

// runRecordingQualityGrades exercises GET /api/v1/account/recordings/quality-grades:
// daily A-F grades plus Fine+/Good+/Great+ 14-day progress per recording.
func runRecordingQualityGrades(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || args[0] != "report" {
		log.Fatal("quality-grades requires report")
	}
	fs := flag.NewFlagSet("recordings quality-grades report", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "account API token")
	days := fs.Int("days", 30, "completed-window history to grade (1-120 days)")
	asJSON := fs.Bool("json", false, "print the raw API response")
	_ = fs.Parse(args[1:])
	if len(fs.Args()) != 0 {
		log.Fatalf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	raw := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/account/recordings/quality-grades?days=%d", *days))
	if *asJSON {
		printJSON(raw)
		return
	}
	body, err := json.Marshal(raw)
	if err != nil {
		log.Fatalf("encode quality grades: %v", err)
	}
	var report qualityGradesReport
	if err := json.Unmarshal(body, &report); err != nil {
		log.Fatalf("decode quality grades: %v", err)
	}
	writeQualityGradesTable(os.Stdout, report.Recordings)
}

// writeQualityGradesTable prints one line per recording: the last 14 day
// grades (oldest first) and each tier's current run.
func writeQualityGradesTable(w io.Writer, recs []qualitygrade.Recording) {
	fmt.Fprintf(w, "%-6s %-14s %-9s %-15s %-15s %s\n", "id", "last 14 days", "latest", "good+ run", "great+ run", "name")
	for _, r := range recs {
		fmt.Fprintf(w, "%-6d %-14s %-9s %-15s %-15s %s\n", r.RecordingID, gradeStrip(r.Windows, qualitygrade.RunLength),
			latestGrade(r), runText(r.TierProgress(qualitygrade.TierGood)), runText(r.TierProgress(qualitygrade.TierGreat)), r.Name)
	}
}

func gradeStrip(windows []qualitygrade.Window, n int) string {
	if len(windows) > n {
		windows = windows[len(windows)-n:]
	}
	var b strings.Builder
	for _, w := range windows {
		if w.Grade == qualitygrade.GradeUnknown {
			b.WriteByte('?')
			continue
		}
		b.WriteString(string(w.Grade))
	}
	return b.String()
}

func latestGrade(r qualitygrade.Recording) string {
	w, ok := r.Latest()
	if !ok {
		return "-"
	}
	return w.LocalDate.Format("01-02") + ":" + string(w.Grade)
}

func runText(p qualitygrade.Progress) string {
	if p.Completed && p.RunDays == qualitygrade.RunLength {
		return fmt.Sprintf("14/14 E%d", p.ECount)
	}
	return fmt.Sprintf("%d/14 E%d F%d", p.RunDays, p.ECount, p.FCount)
}
