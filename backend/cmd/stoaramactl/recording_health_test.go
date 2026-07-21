package main

import (
	"strings"
	"testing"
	"time"
)

func TestShouldNotifyHealthIncident(t *testing.T) {
	runStart := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name          string
		newlyInserted bool
		lastAlertedAt time.Time
		want          bool
	}{
		{"newly inserted always notifies", true, runStart.Add(-48 * time.Hour), true},
		{"restamped this cycle notifies", false, runStart.Add(2 * time.Second), true},
		{"restamped exactly at runStart notifies", false, runStart, true},
		{"stale last_alerted stays quiet", false, runStart.Add(-3 * time.Hour), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldNotifyHealthIncident(tc.newlyInserted, tc.lastAlertedAt, runStart); got != tc.want {
				t.Fatalf("shouldNotifyHealthIncident=%v want %v", got, tc.want)
			}
		})
	}
}

func sampleIncidents() []healthIncident {
	return []healthIncident{
		{
			RecordingID: 101, StreamID: 11, OrgName: "MIT", OrgEmail: "ops@mit.edu",
			RecName: "Kresge Plaza", StreamURL: "https://cam/1.m3u8",
			Signal: signalContinuousSilentDeath, Severity: "CRITICAL",
			SinceText: "window opened 2026-07-04T13:00:00Z, last clip never",
			Diag:      "last_error=ffmpeg exited",
		},
		{
			RecordingID: 202, StreamID: 22, OrgName: "Lab", OrgEmail: "lab@stoarama.com",
			RecName: "Corner", StreamURL: "https://cam/2.m3u8",
			Signal: signalJobRetriesExhausted, Severity: "HIGH",
			SinceText: "2026-07-04T13:20:00Z",
			Diag:      "job_id=999 kind=clip attempts=3/3 error=timeout",
		},
	}
}

func TestComposeHealthEmailSubject(t *testing.T) {
	one := sampleIncidents()[:1]
	if got := composeHealthEmailSubject(one); got != "[Stoarama] Recording 101 unhealthy: "+healthSignalLabels[signalContinuousSilentDeath] {
		t.Fatalf("single subject unexpected: %q", got)
	}
	if got := composeHealthEmailSubject(sampleIncidents()); got != "[Stoarama] 2 recording health alert(s)" {
		t.Fatalf("multi subject unexpected: %q", got)
	}
}

func TestComposeHealthEmailBody(t *testing.T) {
	body := composeHealthEmailBody("https://stoarama.test/", sampleIncidents())
	for _, want := range []string{
		"2 recording health issue(s)",
		"Recording #101 \"Kresge Plaza\"",
		"MIT <ops@mit.edu>",
		"https://cam/1.m3u8",
		"Stoarama: https://stoarama.test/streams/11",
		"Recording: https://stoarama.test/recordings/101",
		healthSignalLabels[signalContinuousSilentDeath] + " [CRITICAL]",
		"last_error=ffmpeg exited",
		"Recording #202 \"Corner\"",
		"job_id=999 kind=clip attempts=3/3 error=timeout",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q\n---\n%s", want, body)
		}
	}
}

func TestDiagTextDropsBlanks(t *testing.T) {
	got := diagText("job_id", "5", "error", "  ", "kind", "clip")
	if got != "job_id=5 kind=clip" {
		t.Fatalf("diagText=%q", got)
	}
}
