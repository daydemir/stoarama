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
