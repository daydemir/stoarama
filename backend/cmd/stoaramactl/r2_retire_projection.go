package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
)

// These manifests are deliberately non-executable. They explain the delta
// after explicit releases, and the conditional upper bound if a future fresh
// complete listing confirms absence. They never manufacture production proof.
type r2RetireProjection struct {
	Class                 r2RetireClass `json:"retirement_class,omitempty"`
	AfterRelease          r2RetireStat  `json:"after_hold_release"`
	AfterReleaseSHA       string        `json:"after_hold_release_sha256"`
	FenceListingPotential r2RetireStat  `json:"after_fence_and_listing_upper_bound"`
	FenceListingSHA       string        `json:"after_fence_and_listing_sha256"`

	fenceItems, releaseItems []r2RetireItem
	release, held            map[string]bool
	unsafePaths              map[string]bool
	rawAliases               map[int64]string
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
	p := &r2RetireProjection{Class: m.Class, release: map[string]bool{}, held: map[string]bool{}}
	itemIDs := []int64{}
	for _, i := range m.Items {
		p.release[retireIdentity(i.Candidate.Kind, i.Candidate.SourceID)] = true
		itemIDs = append(itemIDs, i.ItemID)
	}
	// Materialize protections after exactly these proposed item releases. Never
	// assume completion frees the rest of a plan or any incident identity.
	heldSQL := `SELECT e.value,i.id FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n) LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n WHERE p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL`
	if m.Class == r2RetireOwnerAccepted {
		heldSQL += ` OR i.state IN ('delete_issued','unverified','media_deleted_sidecar_failed')`
	}
	rows, err := q.Query(ctx, heldSQL)
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
	rows, err = q.Query(ctx, `WITH items AS MATERIALIZED (
 SELECT p.id plan_id,e->>'kind' kind,e->>'source_id' source_id,
 coalesce(nullif(e->'source_ids','null'::jsonb),'[]'::jsonb) sources
 FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) e
), membership AS MATERIALIZED (
 SELECT plan_id,source_id::bigint clip_id FROM items WHERE kind='raw'
 UNION SELECT plan_id,(source.value)::bigint FROM items CROSS JOIN LATERAL jsonb_array_elements_text(sources) source
)
SELECT DISTINCT h.clip_id FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
LEFT JOIN membership m ON m.plan_id=h.plan_id AND m.clip_id=h.clip_id
WHERE m.clip_id IS NULL AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id
 AND (i.delete_ever_issued OR i.state IN ('delete_issued','unverified','media_deleted_sidecar_failed'))))`)
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
	if m.Class == r2RetireOwnerAccepted {
		return p, nil
	}
	if err := p.loadFenceWriters(ctx, q); err != nil {
		return nil, err
	}
	return p, nil
}
func (p *r2RetireProjection) add(c r2RetireCandidate, scope r2RetireScope, picks r2RetirePicks, windows map[int64]api.Keep28Window, actualEligible bool) {
	if p.Class != scope.Class || actualEligible || !api.NASListingPathValid(c.RelativePath) {
		return
	}
	key := retireIdentity(c.Kind, c.SourceID)
	if p.release[key] {
		c.Facts.Held = p.held[key]
		if scope.Class != r2RetireOwnerAccepted {
			c.Facts.NASAbsent = true
			// The receipt supersedes prior inventory, subject to a live recheck when
			// actually applying retirement. The projection is not deletion authority.
			c.Facts.NASCopy = false
			c.Facts.ExactNAS = false
		}
		reason, choice, ok := r2RetireDecideForScope(c, scope, windows, picks.Choices)
		if ok {
			p.AfterRelease.add(c.SizeBytes)
			p.releaseItems = append(p.releaseItems, r2RetireItem{r2RetireCandidate: c, Reason: reason, Choice: choice})
			return
		}
	}
	if scope.Class == r2RetireOwnerAccepted {
		return
	}
	reason, choice, _ := r2RetireDecideForScope(c, scope, windows, picks.Choices)
	if reason == "nas_absence_unproven" && !p.unsafePaths[c.RelativePath] && !(c.Kind == "raw" && p.unsafePaths[p.rawAliases[c.SourceID]]) && !(c.Kind == "collated" && p.unsafePaths["joined/"+c.RelativePath]) {
		projected := c
		projected.Facts.NASAbsent = true
		if _, _, ok := r2RetireDecideForScope(projected, scope, windows, picks.Choices); ok {
			p.FenceListingPotential.add(c.SizeBytes)
			p.fenceItems = append(p.fenceItems, r2RetireItem{r2RetireCandidate: c, Reason: "conditional_on_delivery_fence_settlement_and_future_complete_listing_absence", Choice: choice})
		}
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
	h.Items = p.FenceListingPotential.Objects
	h.Bytes = p.FenceListingPotential.Bytes
	p.FenceListingSHA, err = writeR2RetireManifest(filepath.Join(dir, "after-fence-and-listing-upper-bound.jsonl"), h, p.fenceItems)
	return err
}
