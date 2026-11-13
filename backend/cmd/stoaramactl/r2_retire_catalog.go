package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// r2RetireQuerier is satisfied by *pgxpool.Pool, *pgxpool.Conn and pgx.Tx.
type r2RetireQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// r2RetireScope is what every catalog query is bound to.
type r2RetireScope struct {
	Class         r2RetireClass
	OwnerSchema   bool
	OwnerPlanning bool
	OwnerPolicy   []byte
	AccountID     int64
	ConnectionID  int64
	Bucket        string
	Grace         time.Duration
	// HasRetirements is false only before migration 0174 is applied (plan only).
	HasRetirements bool
	NASBacked      bool
	ReleaseSchema  bool
}

// Raw clip facts. $1 account, $2 NAS connection, $3 bucket, $4 grace seconds.
// NAS evidence is deliberately broad: any non-missing inventory row for the
// clip (any connection), or any non-missing inventory/unmatched file at its
// display path, counts as a NAS copy and keeps the clip.
const r2RetireRawSelect = `
SELECT c.id, c.recording_id, coalesce(r.stream_id,0), coalesce(c.bucket,''), coalesce(c.object_key,''), coalesce(c.size_bytes,0),
  lower(coalesce(c.sha256,'')), c.clip_start_at, c.clip_end_at, coalesce(nullif(s.local_timezone,''), r.cron_timezone, ''),
  1, CASE WHEN c.clip_start_at IS NOT NULL AND c.clip_end_at>c.clip_start_at THEN 1 ELSE 0 END, c.clip_start_at, c.clip_end_at,
  c.purged_at IS NOT NULL,
  {RETIRED},
  (EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.clip_id=c.id AND n.state<>'missing')
   OR EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.connection_id=$2 AND n.relative_path=c.display_path AND n.state<>'missing')
   OR EXISTS(SELECT 1 FROM nas_inventory_unmatched_files u WHERE u.connection_id=$2 AND u.relative_path=c.display_path AND u.state<>'missing')),
  EXISTS(SELECT 1 FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
    WHERE h.clip_id=c.id AND (p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.delete_ever_issued))),
  (EXISTS(SELECT 1 FROM recording_joined_source_snapshots js WHERE js.clip_id=c.id)
   OR EXISTS(SELECT 1 FROM recording_joined_snapshot_scopes sc JOIN recording_joined_batches b ON b.id=sc.batch_record_id AND b.state='snapshotting'
     WHERE sc.recording_id=c.recording_id AND sc.recording_job_id=c.recording_job_id AND c.id<=sc.high_water_clip_id)
   OR EXISTS(SELECT 1 FROM recording_joined_dry_run_scopes d
     WHERE d.recording_id=c.recording_id AND d.recording_job_id=c.recording_job_id AND c.id<=d.high_water_clip_id)),
  EXISTS(SELECT 1 FROM nas_restore_requests q WHERE q.clip_id=c.id AND q.state IN ('pending','leased')),
  false,
  coalesce(c.clip_end_at>now()-make_interval(secs => $4::bigint) OR c.created_at>now()-make_interval(secs => $4::bigint), true)
FROM recording_clips c
JOIN recordings r ON r.id=c.recording_id AND r.account_id=$1 AND $3::text<>''
LEFT JOIN streams s ON s.id=r.stream_id
`

const r2RetireRawRetired = `EXISTS(SELECT 1 FROM r2_object_retirements x WHERE (x.kind='raw' AND x.source_id=c.id) OR (x.bucket=c.bucket AND x.object_key=c.object_key))`

// Stitched output facts. Same parameters. The source hull covers every source
// clip; a missing, invalid or foreign source makes the timing unknown.
const r2RetireCollatedSelect = `
SELECT o.id, h.recording_id, coalesce(r.stream_id,0), $3::text, coalesce(o.object_key,''), coalesce(o.size_bytes,0),
  lower(coalesce(o.sha256,'')), o.start_at, o.end_at, coalesce(nullif(s.local_timezone,''), r.cron_timezone, ''),
  (SELECT count(DISTINCT x)::int FROM unnest(o.source_clip_ids) x), src.found, src.a, src.b,
  false,
  {RETIRED},
  (EXISTS(SELECT 1 FROM recording_collation_output_acks a WHERE a.output_id=o.id)
   OR EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.connection_id=$2
     AND n.relative_path IN (o.nas_relative_path, 'joined/'||o.nas_relative_path) AND n.state<>'missing')
   OR EXISTS(SELECT 1 FROM nas_inventory_unmatched_files u WHERE u.connection_id=$2
     AND u.relative_path IN (o.nas_relative_path, 'joined/'||o.nas_relative_path) AND u.state<>'missing')),
  false,
  false,
  false,
  EXISTS(SELECT 1 FROM recording_clips oc WHERE oc.bucket=$3 AND oc.object_key=o.object_key),
  coalesce(o.created_at>now()-make_interval(secs => $4::bigint), true)
FROM recording_collation_outputs o
JOIN recording_collation_hours h ON h.id=o.collation_hour_id
JOIN recordings r ON r.id=h.recording_id AND r.account_id=$1
LEFT JOIN streams s ON s.id=r.stream_id
LEFT JOIN LATERAL (
  SELECT count(*) FILTER (WHERE sc.recording_id=h.recording_id AND sc.clip_start_at IS NOT NULL AND sc.clip_end_at>sc.clip_start_at)::int found,
    min(sc.clip_start_at) a, max(sc.clip_end_at) b
  FROM recording_clips sc WHERE sc.id=ANY(o.source_clip_ids)) src ON true
`

const r2RetireCollatedRetired = `EXISTS(SELECT 1 FROM r2_object_retirements x WHERE (x.kind='collated' AND x.source_id=o.id) OR (x.bucket=$3 AND x.object_key=o.object_key))`

func r2RetireSQL(kind string, scope r2RetireScope, single bool) string {
	sel, retired, alias := r2RetireRawSelect, r2RetireRawRetired, "c"
	if kind == "collated" {
		sel, retired, alias = r2RetireCollatedSelect, r2RetireCollatedRetired, "o"
	}
	if !scope.HasRetirements {
		retired = "false"
	}
	sel = strings.Replace(sel, "{RETIRED}", retired, 1)
	if scope.Class == r2RetireDeliveryOnly && kind == "collated" {
		// Prevent a hashed subplan from enumerating the raw catalog for aliases.
		sel = strings.Replace(sel, `EXISTS(SELECT 1 FROM recording_clips oc WHERE oc.bucket=$3 AND oc.object_key=o.object_key)`,
			`EXISTS(SELECT 1 FROM LATERAL (SELECT 1 FROM recording_clips oc WHERE oc.bucket=$3 AND oc.object_key=o.object_key OFFSET 0) alias)`, 1)
	}
	ownerSnapshot := scope.Class == r2RetireOwnerAccepted && (!single || scope.OwnerPlanning)
	if kind == "raw" && (scope.ReleaseSchema || ownerSnapshot) {
		start := strings.Index(sel, "  EXISTS(SELECT 1 FROM nas_cleanup_source_holds")
		end := strings.Index(sel[start:], "\n  (EXISTS(SELECT 1 FROM recording_joined_source_snapshots") + start
		held := "nas_cleanup_clip_held(c.id)"
		if ownerSnapshot {
			held = "false"
		} // Exact item/source holds are materialized once below.
		sel = sel[:start] + "  " + held + "," + sel[end:]
	}
	if scope.Class.owner() {
		sel = strings.Replace(sel, "r.account_id=$1", "r.account_id=$1 AND $2::bigint>0", 1)
		start := strings.Index(sel, "  (EXISTS(")
		end := start + strings.Index(sel[start:], "),\n") + 2
		presence := `  (EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.state<>'missing' AND
          (n.clip_id=c.id OR n.relative_path=c.display_path OR (n.sha256=lower(c.sha256) AND n.size_bytes=c.size_bytes)))
          OR EXISTS(SELECT 1 FROM nas_inventory_unmatched_files n WHERE n.state<>'missing' AND n.relative_path=c.display_path)),`
		if kind == "collated" {
			presence = `  (EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.state<>'missing' AND
          (n.relative_path IN (o.nas_relative_path,'joined/'||o.nas_relative_path) OR (n.sha256=lower(o.sha256) AND n.size_bytes=o.size_bytes)))
          OR EXISTS(SELECT 1 FROM nas_inventory_unmatched_files n WHERE n.state<>'missing' AND n.relative_path IN (o.nas_relative_path,'joined/'||o.nas_relative_path))),`
		}
		if !single || scope.OwnerPlanning {
			presence = "  false,"
		}
		sel = sel[:start] + presence + sel[end:]
	}
	if scope.Class == r2RetireOwnerAccepted && scope.OwnerSchema {
		sel = strings.Replace(sel, "nas_cleanup_clip_held(c.id)", "nas_cleanup_owner_clip_held(c.id)", 1)
	}
	if scope.NASBacked {
		// Preserve the legacy query for old manifests. New plans require exact
		// inventory identities and the shared reservation view, never ACK alone.
		extra := r2RetireRawExactSQL
		if kind == "collated" {
			extra = r2RetireCollatedExactSQL
		}
		if !single || scope.OwnerPlanning {
			path := "c.display_path"
			if kind == "collated" {
				path = "o.nas_relative_path"
			}
			extra = ", false, coalesce(" + path + ",'')" // One exact-NAS planning snapshot in sets.
		}
		anchor := "\nFROM recording_clips c\nJOIN recordings"
		if kind == "collated" {
			anchor = "\nFROM recording_collation_outputs o\nJOIN recording_collation_hours"
		}
		sel = strings.Replace(sel, anchor, extra+anchor, 1)
	}
	if single {
		return sel + "WHERE " + alias + ".id=$5"
	}
	if scope.Class == r2RetireDeliveryOnly {
		where := "WHERE " + alias + ".id BETWEEN $5 AND $6"
		if kind == "raw" {
			where += " AND c.purged_at IS NULL"
		}
		return sel + where + " ORDER BY " + alias + ".id"
	}
	where := "WHERE " + alias + ".id>$5"
	if kind == "raw" {
		where += " AND c.purged_at IS NULL"
	}
	return sel + where + " ORDER BY " + alias + ".id LIMIT $6"
}

func r2RetireScanCandidate(kind string, rows pgx.Row, nasBacked bool) (r2RetireCandidate, error) {
	c := r2RetireCandidate{Kind: kind}
	var start, end, srcStart, srcEnd *time.Time
	var srcFound *int
	f := &c.Facts
	dest := []any{&c.SourceID, &c.RecordingID, &c.StreamID, &c.Bucket, &c.ObjectKey, &c.SizeBytes, &c.SHA256, &start, &end, &c.Timezone,
		&c.SourceCount, &srcFound, &srcStart, &srcEnd,
		&f.Purged, &f.Retired, &f.NASCopy, &f.Held, &f.JoinedRef, &f.RestorePending, &f.SharedKey, &f.Recent}
	if nasBacked {
		dest = append(dest, &f.ExactNAS, &c.RelativePath)
	}
	if err := rows.Scan(dest...); err != nil {
		return c, err
	}
	if start != nil {
		c.StartAt = start.UTC()
	}
	if end != nil {
		c.EndAt = end.UTC()
	}
	if srcStart != nil {
		c.SourceStart = srcStart.UTC()
	}
	if srcEnd != nil {
		c.SourceEnd = srcEnd.UTC()
	}
	if srcFound != nil {
		c.SourceFound = *srcFound
	}
	return c, nil
}

type r2RetireNASIdentity struct {
	Path, SHA256 string
	SizeBytes    int64
}

// r2RetireSets binds cleanup protections, shared/reference keys and the exact
// NAS planning snapshot. Apply always reads NAS identity and freshness live.
type r2RetireObjectKey struct{ Bucket, Key string }

type r2RetireSets struct {
	DeliveryKeys      map[r2RetireObjectKey]bool
	HeldKeys          map[r2RetireObjectKey]bool
	PresentRaw        map[int64]bool
	PresentPaths      map[string]bool
	PresentContent    map[r2RetireNASIdentity]bool
	OwnerSchema       bool
	Class             r2RetireClass
	ProtectedRaw      map[int64]bool
	ProtectedCollated map[int64]bool
	Evidence          *r2RetireEvidence
	ExactRaw          map[int64]r2RetireNASIdentity
	ExactPaths        map[r2RetireNASIdentity]bool
	HeldRaw           map[int64]bool
	HeldCollated      map[int64]bool
	ArtifactKeys      map[string]bool
	OutputKeyCount    map[string]int
}

func (s r2RetireSets) apply(c *r2RetireCandidate) {
	c.Facts.DeliveryHeld = s.DeliveryKeys[r2RetireObjectKey{c.Bucket, c.ObjectKey}]
	if s.HeldKeys[r2RetireObjectKey{c.Bucket, c.ObjectKey}] {
		c.Facts.Held = true
	}
	if s.Class.owner() {
		c.Facts.NASCopy = c.Facts.NASCopy || s.PresentPaths[c.RelativePath] || s.PresentContent[r2RetireNASIdentity{"", c.SHA256, c.SizeBytes}]
		if c.Kind == "raw" {
			c.Facts.NASCopy = c.Facts.NASCopy || s.PresentRaw[c.SourceID]
		} else {
			c.Facts.NASCopy = c.Facts.NASCopy || s.PresentPaths["joined/"+c.RelativePath]
		}
	}
	if s.ProtectedRaw[c.SourceID] && c.Kind == "raw" || s.ProtectedCollated[c.SourceID] && c.Kind == "collated" {
		c.Facts.JoinedRef = true
	}
	switch c.Kind {
	case "raw":
		if s.HeldRaw[c.SourceID] {
			c.Facts.Held = true
		}
		if s.OutputKeyCount[c.ObjectKey] > 0 {
			c.Facts.SharedKey = true
		}
	case "collated":
		if s.HeldCollated[c.SourceID] {
			c.Facts.Held = true
		}
		if s.OutputKeyCount[c.ObjectKey] > 1 {
			c.Facts.SharedKey = true
		}
	}
	if s.ArtifactKeys[c.ObjectKey] {
		c.Facts.JoinedRef = true
	}
}

func (s r2RetireSets) applyPlanNAS(c *r2RetireCandidate) {
	identity := r2RetireNASIdentity{c.RelativePath, c.SHA256, c.SizeBytes}
	if c.Kind == "raw" {
		got, ok := s.ExactRaw[c.SourceID]
		c.Facts.ExactNAS = ok && got == identity
		return
	}
	c.Facts.ExactNAS = s.ExactPaths[identity]
	identity.Path = "joined/" + identity.Path
	c.Facts.ExactNAS = c.Facts.ExactNAS || s.ExactPaths[identity]
}

const r2RetirePlanManifestSQL = `
SELECT i FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) i
WHERE p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items x WHERE x.plan_id=p.id AND x.delete_ever_issued)`

// r2RetireFingerprintSQL changes whenever a NAS cleanup plan is added or
// changes mode, any plan item first issues a NAS deletion, or a joined
// artifact or stitched output is registered. Apply stops if it moves during a
// run (the Go-side sets would be stale).
const r2RetireFingerprintSQL = `
SELECT md5(coalesce((SELECT string_agg(id||':'||mode, ',' ORDER BY id) FROM nas_cleanup_plans),'')
  ||'|'||(SELECT count(*) FROM nas_cleanup_items WHERE delete_ever_issued)
  ||'|'||(SELECT count(*)||':'||coalesce(max(id),0) FROM recording_joined_artifacts)
  ||'|'||(SELECT count(*)||':'||coalesce(max(id),0) FROM recording_collation_outputs))`

func loadR2RetireSets(ctx context.Context, q r2RetireQuerier, manifest string, nasBacked bool, classes ...r2RetireClass) (r2RetireSets, error) {
	s := r2RetireSets{HeldKeys: map[r2RetireObjectKey]bool{}, HeldRaw: map[int64]bool{}, HeldCollated: map[int64]bool{}, ArtifactKeys: map[string]bool{}, OutputKeyCount: map[string]int{}}
	if len(classes) > 0 {
		s.Class = classes[0]
	}
	var err error
	s.Evidence, err = loadR2RetireEvidence(ctx, q, s.Class, manifest != "" && s.Class == r2RetireDeliveryOnly)
	if err != nil {
		return s, err
	}
	if err := q.QueryRow(ctx, `SELECT to_regprocedure('nas_cleanup_owner_clip_held(bigint)') IS NOT NULL`).Scan(&s.OwnerSchema); err != nil {
		return s, err
	}
	manifestSQL := r2RetirePlanManifestSQL
	if s.Evidence.ReleaseSchema {
		manifestSQL = `SELECT e.value FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n) LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n WHERE (p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL) AND NOT EXISTS(SELECT 1 FROM nas_cleanup_r2_effective_releases x WHERE x.item_id=i.id)`
	}
	mappedSources := map[int64]map[int64]bool{}
	if s.Class == r2RetireOwnerAccepted {
		held := `(p.mode<>'retired' OR i.delete_ever_issued OR i.id IS NULL OR i.state IN ('delete_issued','unverified','media_deleted_sidecar_failed'))`
		releaseView := "nas_cleanup_r2_effective_releases"
		if s.OwnerSchema {
			releaseView = "nas_cleanup_owner_effective_releases"
		}
		if s.Evidence.ReleaseSchema {
			held += ` AND (p.mode<>'retired' OR NOT EXISTS(SELECT 1 FROM ` + releaseView + ` x WHERE x.item_id=i.id))`
		}
		manifestSQL = `SELECT p.id,e.value,(` + held + `) FROM nas_cleanup_plans p
        CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY e(value,n)
        LEFT JOIN nas_cleanup_items i ON i.plan_id=p.id AND i.ordinal=e.n`
	}
	rows, err := q.Query(ctx, manifestSQL)
	if err != nil {
		return s, fmt.Errorf("load NAS cleanup plan manifests: %w", err)
	}
	for rows.Next() {
		var raw []byte
		var planID int64
		held := true
		var scanErr error
		if s.Class == r2RetireOwnerAccepted {
			scanErr = rows.Scan(&planID, &raw, &held)
		} else {
			scanErr = rows.Scan(&raw)
		}
		if scanErr != nil {
			rows.Close()
			return s, scanErr
		}

		var item struct {
			Kind      string  `json:"kind"`
			SourceID  int64   `json:"source_id"`
			SourceIDs []int64 `json:"source_ids"`
		}
		if err := json.Unmarshal(raw, &item); err != nil || item.SourceID <= 0 {
			rows.Close()
			return s, errors.New("unreadable NAS cleanup plan item; refusing (fail closed)")
		}
		if item.Kind != "raw" && item.Kind != "collated" {
			rows.Close()
			return s, fmt.Errorf("unknown NAS cleanup plan item kind %q", item.Kind)
		}
		if s.Class == r2RetireOwnerAccepted {
			if mappedSources[planID] == nil {
				mappedSources[planID] = map[int64]bool{}
			}
			if item.Kind == "raw" {
				mappedSources[planID][item.SourceID] = true
			}
			for _, id := range item.SourceIDs {
				mappedSources[planID][id] = true
			}
			if !held {
				continue
			}
		}
		switch item.Kind {
		case "raw":
			s.HeldRaw[item.SourceID] = true
		case "collated":
			s.HeldCollated[item.SourceID] = true
		default:
			rows.Close()
			return s, fmt.Errorf("unknown NAS cleanup plan item kind %q; refusing (fail closed)", item.Kind)
		}
		for _, id := range item.SourceIDs {
			s.HeldRaw[id] = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return s, err
	}
	if s.Class == r2RetireOwnerAccepted {
		rows, err := q.Query(ctx, `SELECT h.plan_id,h.clip_id FROM nas_cleanup_source_holds h JOIN nas_cleanup_plans p ON p.id=h.plan_id
        WHERE p.mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id
        AND (i.delete_ever_issued OR i.state IN ('delete_issued','unverified','media_deleted_sidecar_failed')))`)
		if err != nil {
			return s, err
		}
		for rows.Next() {
			var planID, id int64
			if err := rows.Scan(&planID, &id); err != nil {
				rows.Close()
				return s, err
			}
			if !mappedSources[planID][id] {
				s.HeldRaw[id] = true
			}
		}
		rows.Close()
		if rows.Err() != nil {
			return s, rows.Err()
		}
	}

	for _, spec := range []struct {
		sql string
		fn  func(string)
	}{
		{`SELECT coalesce(object_key,'') FROM recording_joined_artifacts`, func(k string) { s.ArtifactKeys[k] = true }},
		{`SELECT coalesce(object_key,'') FROM recording_collation_outputs`, func(k string) { s.OutputKeyCount[k]++ }},
	} {
		rows, err := q.Query(ctx, spec.sql)
		if err != nil {
			return s, err
		}
		for rows.Next() {
			var k string
			if err := rows.Scan(&k); err != nil {
				rows.Close()
				return s, err
			}
			spec.fn(k)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return s, err
		}
	}
	if nasBacked {
		// Materialize this shared view once, not again for every catalog page.
		// It includes retired legacy items whose issuance state is uncertain.
		reservedView := "nas_cleanup_reserved_files"
		if s.Evidence.ReleaseSchema {
			reservedView = "nas_cleanup_r2_reserved_files"
		}
		if s.Class == r2RetireOwnerAccepted && s.OwnerSchema {
			reservedView = "nas_cleanup_owner_reserved_files"
		}
		rows, err := q.Query(ctx, `SELECT DISTINCT kind,source_id FROM `+reservedView)
		if err != nil {
			return s, err
		}
		for rows.Next() {
			var kind string
			var id int64
			if err := rows.Scan(&kind, &id); err != nil {
				rows.Close()
				return s, err
			}
			if kind == "raw" {
				s.HeldRaw[id] = true
			} else if kind == "collated" {
				s.HeldCollated[id] = true
			} else {
				rows.Close()
				return s, fmt.Errorf("unknown reserved kind %q", kind)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return s, err
		}
	}

	if nasBacked && manifest == "" {
		// PostgreSQL otherwise rebuilds a full fresh-inventory hash for every
		// page. Apply never uses this snapshot: its single-row query stays live.
		s.ExactRaw = map[int64]r2RetireNASIdentity{}
		s.ExactPaths = map[r2RetireNASIdentity]bool{}
		var id int64
		var identity r2RetireNASIdentity
		var sidecar bool
		err := r2RetireInventoryEach(ctx, q, `n.clip_id,n.relative_path,n.sha256,n.size_bytes,
 coalesce(n.sidecar_relative_path=n.relative_path||'.stoarama.json' AND n.sidecar_size_bytes>0 AND n.sidecar_sha256 ~ '^[a-f0-9]{64}$',false)`,
			`n.connection_id=13 AND n.state='present' AND n.verified_at BETWEEN now()-interval '24 hours' AND now()+interval '5 minutes' AND n.server_received_at>=now()-interval '24 hours'`, nil,
			[]any{&id, &identity.Path, &identity.SHA256, &identity.SizeBytes, &sidecar}, func() error {
				s.ExactPaths[identity] = true
				if sidecar {
					s.ExactRaw[id] = identity
				}
				return nil
			})
		if err != nil {
			return s, err
		}

	}

	s.DeliveryKeys = map[r2RetireObjectKey]bool{}
	var holdsInstalled bool
	if err := q.QueryRow(ctx, `SELECT to_regclass('delivery_keep_holds') IS NOT NULL`).Scan(&holdsInstalled); err != nil {
		return s, err
	}
	if holdsInstalled {
		rows, err := q.Query(ctx, `SELECT bucket,object_key FROM delivery_keep_holds`)
		if err != nil {
			return s, err
		}
		for rows.Next() {
			var key r2RetireObjectKey
			if err := rows.Scan(&key.Bucket, &key.Key); err != nil {
				rows.Close()
				return s, err
			}
			s.DeliveryKeys[key] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return s, err
		}
	}
	var has bool
	if err := q.QueryRow(ctx, `SELECT to_regclass('r2_retirement_reservations') IS NOT NULL`).Scan(&has); err != nil {
		return s, err
	}
	if has {
		exclude := manifest
		rows, err := q.Query(ctx, `SELECT kind,source_id,bucket,object_key FROM r2_retirement_reservations WHERE manifest_sha256<>$1`, exclude)
		if err != nil {
			return s, err
		}
		for rows.Next() {
			var kind string
			var id int64
			var key r2RetireObjectKey
			if err := rows.Scan(&kind, &id, &key.Bucket, &key.Key); err != nil {
				rows.Close()
				return s, err
			}
			s.HeldKeys[key] = true
			if kind == "raw" {
				s.HeldRaw[id] = true
			} else {
				s.HeldCollated[id] = true
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return s, err
		}
	}

	return s, nil
}

func r2RetireFingerprint(ctx context.Context, q r2RetireQuerier) (string, error) {
	var fp string
	err := q.QueryRow(ctx, r2RetireFingerprintSQL).Scan(&fp)
	if err != nil {
		return fp, err
	}
	var has bool
	if err = q.QueryRow(ctx, `SELECT to_regclass('nas_cleanup_r2_releases') IS NOT NULL`).Scan(&has); err != nil {
		return fp, err
	}
	if has {
		var released string
		if err = q.QueryRow(ctx, `SELECT md5(coalesce(string_agg(item_id::text,',' ORDER BY item_id),'')) FROM nas_cleanup_r2_effective_releases`).Scan(&released); err != nil {
			return fp, err
		}
		fp += ":" + released
	}
	return fp, err
}

func r2RetireEnabledPlans(ctx context.Context, q r2RetireQuerier) ([]int64, error) {
	var ids []int64
	err := q.QueryRow(ctx, `SELECT coalesce(array_agg(id ORDER BY id),'{}') FROM nas_cleanup_plans WHERE mode='enabled'`).Scan(&ids)
	return ids, err
}

func r2RetireHasRetirementsTable(ctx context.Context, q r2RetireQuerier) (bool, error) {
	var ok bool
	err := q.QueryRow(ctx, `SELECT to_regclass('r2_object_retirements') IS NOT NULL`).Scan(&ok)
	return ok, err
}

// r2RetireScan pages through one kind and calls fn for every candidate.
func r2RetireScan(ctx context.Context, q r2RetireQuerier, kind string, scope r2RetireScope, sets r2RetireSets, pageSize int, fn func(r2RetireCandidate) error) error {
	scope.ReleaseSchema = sets.Evidence != nil && sets.Evidence.ReleaseSchema
	scope.OwnerSchema = sets.OwnerSchema
	sql := r2RetireSQL(kind, scope, false)
	after := int64(0)
	for {
		first, last := after, int64(pageSize)
		bounded := scope.Class == r2RetireDeliveryOnly
		if bounded {
			var found bool
			var err error
			first, last, found, err = r2RetireIDRange(ctx, q, kind, after, pageSize)
			if err != nil {
				return err
			}
			if !found {
				return nil
			}
		}
		rows, err := q.Query(ctx, sql, scope.AccountID, scope.ConnectionID, scope.Bucket, int64(scope.Grace/time.Second), first, last)
		if err != nil {
			return fmt.Errorf("scan %s: %w", kind, err)
		}
		n := 0
		var page []r2RetireCandidate
		for rows.Next() {
			c, err := r2RetireScanCandidate(kind, rows, scope.NASBacked)
			if err != nil {
				rows.Close()
				return fmt.Errorf("scan %s row: %w", kind, err)
			}
			page = append(page, c)
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("scan %s: %w", kind, err)
		}
		for i := range page {
			sets.apply(&page[i])
			if scope.NASBacked {
				sets.applyPlanNAS(&page[i])
			}
			if scope.Class != r2RetireOwnerAccepted {
				presence := page[i].Facts.NASCopy
				sets.Evidence.apply(&page[i])
				if scope.Class == r2RetireDeliveryOnly {
					// Ordinary listing absence cannot clear broad owner inventory.
					page[i].Facts.NASCopy = page[i].Facts.NASCopy || presence
					sets.Evidence.applyOwner(&page[i])
				}
			} else {
				sets.Evidence.applyOwner(&page[i])
			}
			if err := fn(page[i]); err != nil {
				return err
			}
			after = page[i].SourceID
		}
		if bounded {
			after = last
		} else if n < pageSize {
			return nil
		}
	}
}

// r2RetireLoadOne re-reads one catalog row (purged or not) for apply.
func r2RetireLoadOne(ctx context.Context, q r2RetireQuerier, kind string, id int64, scope r2RetireScope, sets r2RetireSets) (r2RetireCandidate, error) {
	scope.ReleaseSchema = sets.Evidence != nil && sets.Evidence.ReleaseSchema
	scope.OwnerSchema = sets.OwnerSchema
	row := q.QueryRow(ctx, r2RetireSQL(kind, scope, true), scope.AccountID, scope.ConnectionID, scope.Bucket, int64(scope.Grace/time.Second), id)
	c, err := r2RetireScanCandidate(kind, row, scope.NASBacked)
	if err != nil {
		return c, err
	}
	sets.apply(&c)
	if scope.Class.owner() {
		if scope.Class == r2RetireDeliveryOnly {
			presence := c.Facts.NASCopy
			if err := r2RetireLiveEvidence(ctx, q, &c, sets.Evidence); err != nil {
				return c, err
			}
			c.Facts.NASCopy = c.Facts.NASCopy || presence
		}
		if err := r2RetireOwnerLive(ctx, q, &c, scope); err != nil {
			return c, err
		}
		err = r2RetireOwnerLiveEvidence(ctx, q, &c, sets.Evidence)
		return c, err
	}
	if err := r2RetireLiveEvidence(ctx, q, &c, sets.Evidence); err != nil {
		return c, err
	}
	return c, nil
}

// Extra columns are selected only for NAS-backed plans. Broad presence is a
// veto outside keep28; only this exact inventory match authorizes kept footage.
const r2RetireRawExactSQL = `,
 EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.connection_id=$2 AND n.clip_id=c.id
   AND n.relative_path=c.display_path AND n.state='present' AND n.sha256=lower(c.sha256) AND n.size_bytes=c.size_bytes
   AND n.verified_at BETWEEN now()-interval '24 hours' AND now()+interval '5 minutes'
   AND n.server_received_at>=now()-interval '24 hours' AND n.sidecar_relative_path=c.display_path||'.stoarama.json' AND n.sidecar_size_bytes>0 AND n.sidecar_sha256 ~ '^[a-f0-9]{64}$'),
 coalesce(c.display_path,'')
`
const r2RetireCollatedExactSQL = `,
 EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.connection_id=$2
   AND n.relative_path IN (o.nas_relative_path,'joined/'||o.nas_relative_path)
   AND n.state='present' AND n.sha256=lower(o.sha256) AND n.size_bytes=o.size_bytes
   AND n.verified_at BETWEEN now()-interval '24 hours' AND now()+interval '5 minutes'
   AND n.server_received_at>=now()-interval '24 hours'),
 coalesce(o.nas_relative_path,'')
`
