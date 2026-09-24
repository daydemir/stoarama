package recordingworker

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

func TestDeliveryDiagnosticsAreBoundedAndAllowlisted(t *testing.T) {
	d := &RelayDiagnostics{}
	d.Start(recordingapi.RecordingJob{JobID: 42, RecordingID: 402})
	d.DeliveryPhase(42, "put", 2*time.Second)
	d.DeliveryPhase(42, "put", 5*time.Second)
	d.DeliveryPhase(42, "https://secret.example/token=abc", time.Hour)
	d.DeliveryQueue(42, 20000)
	d.DeliveryRetry(42)
	b, err := json.Marshal(d.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, secret := range []string{"secret.example", "token=abc"} {
		if strings.Contains(s, secret) {
			t.Fatalf("diagnostics leaked %q: %s", secret, s)
		}
	}
	if !strings.Contains(s, `"put":{"count":2,"total_ms":7000,"max_ms":5000}`) {
		t.Fatalf("missing aggregate: %s", s)
	}
	if !strings.Contains(s, `"delivery_queue_max":10000`) || !strings.Contains(s, `"delivery_retries":1`) {
		t.Fatalf("missing bounded queue/retry: %s", s)
	}
}

func TestRelayDiagnosticsReportsResolvesPerHour(t *testing.T) {
	d := &RelayDiagnostics{}
	d.Start(recordingapi.RecordingJob{JobID: 7, RecordingID: 70})
	d.Start(recordingapi.RecordingJob{JobID: 8, RecordingID: 80})
	now := time.Now().UTC()
	d.Resolve(7, now.Add(-2*time.Hour)) // outside the trailing window
	for i := 0; i < 3; i++ {
		d.Resolve(7, now.Add(-time.Duration(i)*time.Minute))
	}
	d.Resolve(8, now)
	d.Resolve(99, now) // unknown job is ignored

	snapshot := d.Snapshot()
	if got := snapshot["active_resolves_last_hour"]; got != 4 {
		t.Fatalf("relay resolves_last_hour=%v want 4", got)
	}
	active := snapshot["active"].([]map[string]any)
	if active[0]["job_id"] != int64(7) || active[0]["resolves_last_hour"] != 3 || active[0]["resolves_total"] != 4 {
		t.Fatalf("job 7 resolve diagnostics=%v", active[0])
	}
	if active[1]["resolves_last_hour"] != 1 || active[1]["resolves_total"] != 1 {
		t.Fatalf("job 8 resolve diagnostics=%v", active[1])
	}
	d.Finish(7, "done", nil)
	last := d.Snapshot()["last"].(map[string]any)
	if last["resolves_total"] != 4 {
		t.Fatalf("finished job lost resolve total: %v", last)
	}
}

func TestPruneResolveTimesBoundsHistory(t *testing.T) {
	now := time.Now()
	times := make([]time.Time, 0, relayResolveTimesLimit+10)
	for i := 0; i < relayResolveTimesLimit+10; i++ {
		times = append(times, now.Add(-time.Duration(relayResolveTimesLimit+10-i)*time.Millisecond))
	}
	if got := len(pruneResolveTimes(times, now)); got != relayResolveTimesLimit {
		t.Fatalf("pruned history=%d want %d", got, relayResolveTimesLimit)
	}
}
