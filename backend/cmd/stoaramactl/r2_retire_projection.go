package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/daydemir/stoarama/backend/internal/api"
	"os"
	"path/filepath"
	"time"
)

// These manifests are deliberately non-executable. They explain the delta
// after explicit releases, and the conditional upper bound if a future fresh
// complete listing confirms absence. They never manufacture production proof.
type r2RetireProjection struct {
	AfterRelease               r2RetireStat `json:"after_hold_release"`
	ListingPotential           r2RetireStat `json:"listing_absence_upper_bound"`
	AfterReleaseSHA            string       `json:"after_hold_release_sha256"`
	ListingSHA                 string       `json:"listing_absence_upper_bound_sha256"`
	release                    map[string]bool
	held                       map[string]bool
	delivery                   map[string]bool
	releaseItems, listingItems []r2RetireItem
}

func loadR2RetireProjection(ctx context.Context, q r2RetireQuerier, path, digest string, picks r2RetirePicks, policy string) (*r2RetireProjection, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !r2RetireLowerHex64(digest) || r2SHA(raw) != digest {
		return nil, errors.New("hold release preview digest differs")
	}
	var m r2HoldReleaseManifest
	if json.Unmarshal(raw, &m) != nil || m.Tool != "r2-retire release-holds" || m.Version != 1 || m.PolicySHA != policy || m.PicksSHA != picks.SHA256 || time.Since(m.CreatedAt) > 24*time.Hour {
		return nil, errors.New("hold release preview authority differs")
	}
	p := &r2RetireProjection{release: map[string]bool{}, held: map[string]bool{}, delivery: map[string]bool{}}
	itemIDs := []int64{}
	for _, i := range m.Items {
		p.release[retireIdentity(i.Candidate.Kind, i.Candidate.SourceID)] = true
		itemIDs = append(itemIDs, i.ItemID)
	}
	// Materialize protections after exactly these proposed item releases. Never
	// assume completion frees the rest of a plan or any incident identity.
	rows, err := q.Query(ctx, `SELECT e.value,i.id FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n) LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n WHERE p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL`)
	if err != nil {
		return nil, err
	}
	proposed := map[int64]bool{}
	for _, id := range itemIDs {
		proposed[id] = true
	}
	for rows.Next() {
		var raw []byte
		var itemID *int64
		if err := rows.Scan(&raw, &itemID); err != nil {
			rows.Close()
			return nil, err
		}
		var i struct {
			Kind    string  `json:"kind"`
			ID      int64   `json:"source_id"`
			Sources []int64 `json:"source_ids"`
		}
		if json.Unmarshal(raw, &i) != nil {
			rows.Close()
			return nil, errors.New("invalid held identity")
		}
		key := retireIdentity(i.Kind, i.ID)
		if itemID != nil && proposed[*itemID] {
			continue
		}
		p.held[key] = true
		for _, id := range i.Sources {
			p.held[retireIdentity("raw", id)] = true
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	rows, err = q.Query(ctx, `SELECT DISTINCT h.clip_id FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id WHERE (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued)) AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(p.manifest) e WHERE (e->'source_ids') @> to_jsonb(ARRAY[h.clip_id]) OR (e->>'kind'='raw' AND (e->>'source_id')::bigint=h.clip_id))`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		p.held[retireIdentity("raw", id)] = true
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	var reservations bool
	if err := q.QueryRow(ctx, `SELECT to_regclass('r2_retirement_reservations') IS NOT NULL`).Scan(&reservations); err != nil {
		return nil, err
	}
	if reservations {
		rows, err = q.Query(ctx, `SELECT kind,source_id FROM r2_retirement_reservations`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var kind string
			var id int64
			if err := rows.Scan(&kind, &id); err != nil {
				rows.Close()
				return nil, err
			}
			p.held[retireIdentity(kind, id)] = true
		}
		rows.Close()
		if rows.Err() != nil {
			return nil, rows.Err()
		}
	}
	for _, id := range r2RetireIncidentSources {
		p.held[retireIdentity("raw", id)] = true
	}
	rows, err = q.Query(ctx, `SELECT c.id FROM recording_clips c JOIN recordings r ON r.id=c.recording_id JOIN connections n ON n.id=13 AND n.account_id=r.account_id WHERE r.account_id=47 AND c.purged_at IS NULL AND (c.released_at IS NOT NULL OR c.id<=n.last_cursor_id)`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		p.delivery[retireIdentity("raw", id)] = true
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	rows, err = q.Query(ctx, `SELECT output_id FROM recording_collation_output_acks WHERE connection_id=13`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		p.delivery[retireIdentity("collated", id)] = true
	}
	rows.Close()
	return p, rows.Err()
}
func (p *r2RetireProjection) add(c r2RetireCandidate, scope r2RetireScope, picks r2RetirePicks, windows map[int64]api.Keep28Window, actualEligible bool) {
	if actualEligible || !api.NASListingPathValid(c.RelativePath) {
		return
	}
	key := retireIdentity(c.Kind, c.SourceID)
	if p.release[key] {
		c.Facts.Held = p.held[key]
		c.Facts.NASAbsent = true
		// The receipt supersedes prior inventory, subject to a live recheck when
		// actually applying retirement. The projection is not deletion authority.
		c.Facts.NASCopy = false
		c.Facts.ExactNAS = false
		reason, choice, ok := r2RetireDecideForScope(c, scope, windows, picks.Choices)
		if ok {
			p.AfterRelease.add(c.SizeBytes)
			p.releaseItems = append(p.releaseItems, r2RetireItem{r2RetireCandidate: c, Reason: reason, Choice: choice})
			return
		}
	}
	reason, choice, _ := r2RetireDecideForScope(c, scope, windows, picks.Choices)
	if reason != "nas_absence_unproven" || !p.delivery[key] {
		return
	}
	c.Facts.NASAbsent = true
	if _, _, ok := r2RetireDecideForScope(c, scope, windows, picks.Choices); ok {
		p.ListingPotential.add(c.SizeBytes)
		p.listingItems = append(p.listingItems, r2RetireItem{r2RetireCandidate: c, Reason: "conditional_on_future_complete_listing_absence", Choice: choice})
	}
}
func (p *r2RetireProjection) write(dir string, h r2RetireHeader) error {
	h.Phase = r2RetireBlocked
	h.Reason = "projection_only_not_deletion_authority"
	h.Items = p.AfterRelease.Objects
	h.Bytes = p.AfterRelease.Bytes
	var err error
	p.AfterReleaseSHA, err = writeR2RetireManifest(filepath.Join(dir, "after-hold-release.jsonl"), h, p.releaseItems)
	if err != nil {
		return err
	}
	h.Items = p.ListingPotential.Objects
	h.Bytes = p.ListingPotential.Bytes
	p.ListingSHA, err = writeR2RetireManifest(filepath.Join(dir, "listing-absence-upper-bound.jsonl"), h, p.listingItems)
	return err
}
