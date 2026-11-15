package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNASHostClientMemoryOptional(t *testing.T) {
	var host nasHostFacts
	if err := json.Unmarshal([]byte(`{"machine":"x86_64","system":"Linux"}`), &host); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(host)
	if err != nil || strings.Contains(string(encoded), "client_rss_bytes") || strings.Contains(string(encoded), "client_peak_rss_bytes") {
		t.Fatalf("unobserved client memory must be omitted: %s %v", encoded, err)
	}
	if err = json.Unmarshal([]byte(`{"client_rss_bytes":123456,"client_peak_rss_bytes":234567}`), &host); err != nil {
		t.Fatal(err)
	}
	if err = validateNASHostFacts(&host); err != nil || host.ClientRSSBytes == nil || *host.ClientRSSBytes != 123456 || host.ClientPeakRSSBytes == nil || *host.ClientPeakRSSBytes != 234567 {
		t.Fatalf("numeric client memory: %+v %v", host, err)
	}
}
