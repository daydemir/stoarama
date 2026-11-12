package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/daydemir/stoarama/backend/internal/api"
	"os"
	"strings"
	"time"
)

func r2RetireDeliveryPreview(path, want string, live api.DeliveryKeepSnapshot) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	if !r2RetireLowerHex64(want) || hex.EncodeToString(sum[:]) != want {
		return errors.New("delivery preview digest differs")
	}
	seen := map[string]bool{}
	scan := bufio.NewScanner(bytes.NewReader(raw))
	for scan.Scan() {
		var row struct {
			Key, ETag string
			Size      int64
		}
		if json.Unmarshal(scan.Bytes(), &row) != nil {
			return errors.New("invalid delivery preview")
		}
		o, ok := live.Objects[row.Key]
		if !ok || seen[row.Key] || row.ETag != o.ETag || row.Size != o.Size {
			return errors.New("delivery preview and live registry disagree")
		}
		seen[row.Key] = true
	}
	if err := scan.Err(); err != nil {
		return err
	}
	if len(seen) != len(live.Objects) {
		return errors.New("delivery preview is incomplete")
	}
	return nil
}
func r2RetireDeliveryHeld(ctx context.Context, q r2RetireQuerier, bucket, key string) error {
	var held bool
	if err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM delivery_keep_holds WHERE bucket=$1 AND object_key=$2)`, bucket, key).Scan(&held); err != nil {
		return errors.New("delivery keep hold check unavailable")
	}
	if held {
		return errors.New("DELIVERY KEEP HOLD: selected key cannot retire")
	}
	return nil
}
func r2RetireDeliveryDecide(c r2RetireCandidate, scope r2RetireScope, windows map[int64]api.Keep28Window, picks map[int64]r2RetirePick) (string, string, bool) {
	if p, ok := picks[c.StreamID]; ok && p.Choice == "all" {
		return "pick_all", "all", false
	}
	w, ok := windows[c.StreamID]
	if !ok {
		return "no_keep_window", "", false
	}
	effective := map[int64]r2RetirePick{c.StreamID: {Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}
	outside := r2RetireOutside(w, c.StartAt, c.EndAt) && r2RetireOutside(w, c.SourceStart, c.SourceEnd)
	// The owner waives absence proof outside keep28. Broad NAS presence and
	// every other veto still win; only kept footage needs a verified copy.
	if outside {
		c.Facts.NASAbsent = true
	}
	reason, choice, eligible := r2RetireDecideMode(c, scope.Bucket, windows, effective, true)
	if !eligible && !outside && (reason == "kept_collated_no_exact_inventory" || reason == "nas_identity_unproven") {
		reason = "copy_to_nas"
	}
	return reason, choice, eligible
}
func (c r2RetireClass) owner() bool { return c == r2RetireOwnerAccepted || c == r2RetireDeliveryOnly }
func r2RetireDeliveryChoices(picks map[int64]r2RetirePick, windows map[int64]api.Keep28Window) map[string]string {
	out := map[string]string{}
	for id := range windows {
		choice := "none"
		if picks[id].Choice == "all" {
			choice = "all"
		}
		out[fmt.Sprint(id)] = choice
	}
	return out
}
func r2RetireDeliveryPolicy(windows map[int64]api.Keep28Window, picks map[int64]r2RetirePick) []byte {
	effective := map[int64]r2RetirePick{}
	for id, w := range windows {
		if picks[id].Choice == "all" {
			continue
		}
		effective[id] = r2RetirePick{Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}
	}
	return r2RetireOwnerPolicy(windows, effective)
}
func r2RetireDeliveryRegistryUnchanged(ctx context.Context, store r2RetireHeadStore, s api.DeliveryKeepSnapshot) error {
	for key, o := range s.Objects {
		if o.Kind != "registry" || !strings.HasSuffix(key, "public/current.json") {
			continue
		}
		h, err := store.Head(ctx, key)
		if err != nil || strings.Trim(h.ETag, `"`) != o.ETag || h.SizeBytes != o.Size {
			return errors.New("live delivery registry changed; re-plan")
		}
	}
	return nil
}

// The new owner choice covers both kept and outside windows. It does not
// waive complete source membership, timezone, foreign-account or all-stream
// references, which remain protected by the same owner source closure.
func r2RetireDeliverySourceSQL(sql string) string {
	sql = strings.ReplaceAll(sql, `(c.clip_end_at<=p.start OR c.clip_start_at>=p."end")`, "true")
	return strings.ReplaceAll(sql, `(o.end_at<=p.start OR o.start_at>=p."end")`, "true")
}

// Reuse is allowed only for a checksummed, recent whole-bucket enumeration.
// Catalog-prefix listings cannot establish the delivery-only end-state totals.
func r2RetireRequireWholeListing(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	if !scan.Scan() {
		return errors.New("whole listing header missing")
	}
	var h struct {
		Prefixes []string `json:"prefixes"`
	}
	if json.Unmarshal(scan.Bytes(), &h) != nil || len(h.Prefixes) != 1 || h.Prefixes[0] != "" {
		return errors.New("delivery-only requires a complete whole-bucket listing")
	}
	return nil
}

// A planning-only class cannot inherit hold-release authority.
func (c r2RetireClass) releaseValid() bool { return c == "" || c == r2RetireOwnerAccepted }
