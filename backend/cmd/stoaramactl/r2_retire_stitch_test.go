package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/stitchproof"
	"github.com/daydemir/stoarama/backend/internal/survey"
	"github.com/google/uuid"
)

func TestR2StitchMissingProofRefusesBeforeDBOrLedger(t *testing.T) {
	dir := t.TempDir()
	for _, mode := range []bool{false, true} {
		opts := r2RetireApplyOptions{ProviderUnconditionalHeadGuard: mode, LedgerPath: filepath.Join(dir, "ledger")}
		if _, err := r2RetireApply(context.Background(), nil, nil, opts, "", nil); err == nil || !strings.Contains(err.Error(), "--stitch-proof") {
			t.Fatal(err)
		}
		if _, err := os.Stat(opts.LedgerPath); !os.IsNotExist(err) {
			t.Fatal("refusal created a ledger", err)
		}
	}
	if _, err := survey.DeleteStreamCaptures(context.Background(), nil, nil, 1); !errors.Is(err, r2.ErrLegacyDeleteRetired) {
		t.Fatal("empty-list catalog bypass", err)
	}
	if err := refuseLegacyR2Apply(true); !errors.Is(err, r2.ErrLegacyDeleteRetired) {
		t.Fatal(err)
	}
}

func TestR2StitchBest14ProtectedEvenWithExactNAS(t *testing.T) {
	w := r2tWindow(t, 3, "America/Los_Angeles", "2026-10-25", "2026-11-22")
	loc, err := time.LoadLocation(w.Timezone)
	if err != nil {
		t.Fatal(err)
	}
	start, err := time.ParseInLocation(time.DateOnly, w.BestStart, loc)
	if err != nil {
		t.Fatal(err)
	}
	end := start.AddDate(0, 0, 14)
	windows := map[int64]api.Keep28Window{3: w}
	picks := map[int64]r2RetirePick{3: {Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}
	for _, span := range [][2]time.Time{{start, start.Add(time.Minute)}, {end.Add(-time.Nanosecond), end.Add(time.Minute)}, {w.Start, end}} {
		c := r2tCandidate("raw", 3, w.Timezone, span[0], span[1])
		c.Facts.ExactNAS = true
		if reason, _, eligible := r2RetireDecideMode(c, "stoarama", windows, picks, true); eligible || reason != "delivery_keep_period" {
			t.Fatal(reason, eligible)
		}
	}
	c := r2tCandidate("raw", 3, w.Timezone, end, end.Add(time.Minute))
	c.Facts.ExactNAS = true
	if _, _, ok := r2RetireDecideMode(c, "stoarama", windows, picks, true); !ok {
		t.Fatal("non-delivery kept period with exact NAS was lost")
	}
	c.SourceStart = start
	c.SourceEnd = start.Add(time.Minute)
	if _, _, ok := r2RetireDecideMode(c, "stoarama", windows, picks, true); ok {
		t.Fatal("source span bypassed delivery keep")
	}
}

// These are historical ledger parser fixtures only. They cannot construct the
// private verified Authority, perform a wire delete or authorize a new run.
func ledgerFixture(t *testing.T) (*r2RetireLedger, r2RetireTarget, r2RetireLedgerRecord) {
	t.Helper()
	l := &r2RetireLedger{records: map[string]r2RetireLedgerRecord{}, intents: map[r2RetireTarget]r2RetireLedgerRecord{}}
	target := r2RetireTarget{r2tSHA, "stoarama", r2tSHA, r2RetireTargetRaw, 1, 2, 3, "managed/a", "a.mp4", r2tUTC("2026-08-01T00:00:00Z"), r2tUTC("2026-08-01T00:01:00Z"), 100, r2tSHA, "etag"}
	stamp := time.Now().UTC()
	id := uuid.NewString()
	e := r2RetireLedgerLine{Version: 3, RecordID: id, At: stamp, Event: r2LedgerAuthorized, ManifestSHA256: target.ManifestSHA, Authorization: &r2RetireAuthorization{id, target.ManifestSHA, target.Bucket, target.EndpointSHA, stitchproof.Provenance{OuterSHA: r2tSHA, PayloadSHA: r2tSHA, BundleSHA: r2tSHA, ReviewSHA: r2tSHA, ClientSHA: stitchproof.ClientSHA, VerifierSHA: stitchproof.VerifierSHA, CodeHead: "4e433a0f9203b579f8f7e55f2b3e25557367d3e4", NASRunID: uuid.NewString(), ExpiresAt: stamp.Add(time.Hour)}}}
	raw, _ := json.Marshal(e)
	if err := l.accept(e, ledgerSHA(raw), true); err != nil {
		t.Fatal(err)
	}
	l.current = l.records[id]
	intent := l.operation(target, r2LedgerIntent)
	intent.Version = 3
	intent.At = stamp
	intent.RecordID = uuid.NewString()
	raw, _ = json.Marshal(intent)
	if err := l.accept(intent, ledgerSHA(raw), true); err != nil {
		t.Fatal(err)
	}
	return l, target, l.records[intent.RecordID]
}

func TestR2StitchLedgerUsesCompleteIdentity(t *testing.T) {
	l, target, _ := ledgerFixture(t)
	for name, change := range map[string]func(*r2RetireTarget){
		"manifest":  func(x *r2RetireTarget) { x.ManifestSHA = strings.Repeat("a", 64) },
		"bucket":    func(x *r2RetireTarget) { x.Bucket = "other" },
		"endpoint":  func(x *r2RetireTarget) { x.EndpointSHA = strings.Repeat("b", 64) },
		"kind":      func(x *r2RetireTarget) { x.Kind = "collated" },
		"source":    func(x *r2RetireTarget) { x.SourceID++ },
		"recording": func(x *r2RetireTarget) { x.RecordingID++ },
		"stream":    func(x *r2RetireTarget) { x.StreamID++ },
		"path":      func(x *r2RetireTarget) { x.RelativePath = "other.mp4" },
		"span":      func(x *r2RetireTarget) { x.StartAt = x.StartAt.Add(time.Second) },
		"size":      func(x *r2RetireTarget) { x.SizeBytes++ },
		"sha":       func(x *r2RetireTarget) { x.SHA256 = strings.Repeat("c", 64) },
		"etag":      func(x *r2RetireTarget) { x.ETag = "replacement" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := target
			change(&changed)
			if _, ok := l.intents[changed]; ok {
				t.Fatal("same-key different target adopted old authority")
			}
		})
	}
}

func TestR2StitchResumeRequiresNewAuthorizationAndExactOriginal(t *testing.T) {
	l, target, old := ledgerFixture(t)
	e := l.operation(target, r2LedgerResume)
	e.Version = 3
	e.RecordID = uuid.NewString()
	e.At = time.Now().UTC()
	e.OriginalIntentID = old.line.RecordID
	e.OriginalIntentSHA = old.sha
	if err := l.accept(e, r2tSHA, false); err == nil {
		t.Fatal("same authorization renewed itself")
	}
	a := *l.current.line.Authorization
	a.RunID = uuid.NewString()
	next := r2RetireLedgerLine{Version: 3, RecordID: a.RunID, At: e.At, Event: r2LedgerAuthorized, ManifestSHA256: target.ManifestSHA, Authorization: &a}
	if err := l.accept(next, r2tSHA, true); err != nil {
		t.Fatal(err)
	}
	e.AuthorizationID = a.RunID
	e.AuthorizationSHA = r2tSHA
	if err := l.accept(e, r2tSHA, false); err != nil {
		t.Fatal(err)
	}
	e.OriginalIntentSHA = strings.Repeat("f", 64)
	if err := l.accept(e, r2tSHA, false); err == nil {
		t.Fatal("unbound historical intent accepted")
	}
	if !l.records[old.line.RecordID].line.At.Equal(old.line.At) {
		t.Fatal("old intent rewritten")
	}
}

func TestR2StitchLedgerPreservesAndRejectsLegacyOrTornBytes(t *testing.T) {
	for _, raw := range []string{`{"event":"intent","object_key":"managed/a"}` + "\n", `{"event":"intent"}`, "{bad}\n"} {
		file := filepath.Join(t.TempDir(), "ledger")
		if err := os.WriteFile(file, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if ledger, err := openR2RetireLedger(file); err == nil {
			ledger.Close()
			t.Fatal("unsupported history silently upgraded")
		}
		after, err := os.ReadFile(file)
		if err != nil || string(after) != raw {
			t.Fatal("old ledger bytes rewritten", err)
		}
	}
}
