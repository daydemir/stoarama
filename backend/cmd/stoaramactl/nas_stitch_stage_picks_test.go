package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/api"
)

// picksFixture writes a picks file and returns its path and digest.
func picksFixture(t *testing.T, body string) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "r2-picks.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	return path, hex.EncodeToString(sum[:])
}

func equalIDsForTest(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The decided and keep-everything sets must come from the real picks parser and
// the embedded policy window, never from a caller-supplied array. Each case
// here is a document an operator could hand the command.
func TestDurhamStagePicksDeriveFromReviewedPicks(t *testing.T) {
	_, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	for stream := range windows {
		id = stream
		break
	}
	if id == 0 {
		t.Skip("no embedded policy window")
	}
	w := windows[id]
	span := fmt.Sprintf(`"keep_start_utc":%q,"keep_end_utc":%q`,
		w.Start.Format("2006-01-02 15:04:05Z07:00"), w.End.Format("2006-01-02 15:04:05Z07:00"))

	for _, tc := range []struct {
		name    string
		body    string
		want    []int64
		wantKE  []int64
		wantErr bool
	}{
		{"decided st", fmt.Sprintf(`{"choices":{"%d":{"choice":"st",%s}}}`, id, span), []int64{id}, []int64{}, false},
		{"decided keep", fmt.Sprintf(`{"choices":{"%d":{"choice":"keep",%s}}}`, id, span), []int64{id}, []int64{}, false},
		{"decided all is keep-everything", fmt.Sprintf(`{"choices":{"%d":{"choice":"all",%s}}}`, id, span), []int64{id}, []int64{id}, false},
		// A pick made against a different window is undecided, exactly as
		// selection treats it, so it can never authorize a stage.
		{"window mismatch is undecided", fmt.Sprintf(`{"choices":{"%d":{"choice":"st","keep_start_utc":"2020-01-01 00:00:00Z","keep_end_utc":%q}}}`,
			id, w.End.Format("2006-01-02 15:04:05Z07:00")), nil, nil, false},
		// A pick without the window it was made against is undecided.
		{"missing window is undecided", fmt.Sprintf(`{"choices":{"%d":{"choice":"st"}}}`, id), nil, nil, false},
		// An undecided entry is skipped, never treated as a decision.
		{"empty choice is undecided", fmt.Sprintf(`{"choices":{"%d":{"choice":""}}}`, id), nil, nil, false},
		{"unknown choice is refused", fmt.Sprintf(`{"choices":{"%d":{"choice":"maybe",%s}}}`, id, span), nil, nil, true},
		{"no choices object", `{"saved_at":"x"}`, nil, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, digest := picksFixture(t, tc.body)
			got, err := durhamStagePicks(path, digest, windows)
			if tc.wantErr {
				if err == nil {
					t.Fatal("invalid picks were accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !equalIDsForTest(got.Decided, tc.want) || !equalIDsForTest(got.KE, tc.wantKE) {
				t.Fatalf("derivation = %v/%v, want %v/%v", got.Decided, got.KE, tc.want, tc.wantKE)
			}
			if got.SHA256 != digest {
				t.Fatal("derivation did not pin the picks digest it read")
			}
		})
	}
}

// A stream outside the embedded policy has no reviewed window, so its pick is
// unknown provenance and must not become a decision.
func TestDurhamStagePicksIgnoresStreamsOutsideThePolicy(t *testing.T) {
	_, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	const outside = 999999999
	if _, ok := windows[outside]; ok {
		t.Skip("fixture id is in the policy")
	}
	path, digest := picksFixture(t, fmt.Sprintf(`{"choices":{"%d":{"choice":"all","keep_start_utc":"2026-09-11 11:00:00-04:00","keep_end_utc":"2026-09-11 12:00:00-04:00"}}}`, outside))
	got, err := durhamStagePicks(path, digest, windows)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Decided) != 0 || len(got.KE) != 0 {
		t.Fatalf("stream outside the policy became a decision: %v/%v", got.Decided, got.KE)
	}
}

// The approved digest is the independent pin: a swapped file must not ride
// along on a digest approved for different bytes.
func TestDurhamStagePicksRefusesASwappedFile(t *testing.T) {
	_, windows, _ := api.Keep28Policy()
	path, _ := picksFixture(t, `{"choices":{}}`)
	approved := strings.Repeat("a", 64)
	if _, err := durhamStagePicks(path, approved, windows); err == nil {
		t.Fatal("picks whose digest differs from the approved one were accepted")
	}
	if _, err := durhamStagePicks(path, "not-a-digest", windows); err == nil {
		t.Fatal("a malformed approved digest was accepted")
	}
}

// Time parsing must accept both formats the page and select.py write.
func TestDurhamStagePickTimeFormats(t *testing.T) {
	for _, raw := range []string{"2026-09-11 11:00:00-04:00", "2026-09-11T15:00:00+00:00"} {
		if _, ok := r2RetireParsePickTime(raw); !ok {
			t.Fatalf("pick time %q was refused", raw)
		}
	}
	for _, raw := range []string{"", " 2026-09-11 11:00:00-04:00", "yesterday"} {
		if _, ok := r2RetireParsePickTime(raw); ok {
			t.Fatalf("pick time %q was accepted", raw)
		}
	}
}
