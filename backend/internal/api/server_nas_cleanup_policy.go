package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
)

const cleanupPolicySHA = "428620008349a12c468bd3663cf678a1047344c019ef9d53caa8adf6c6ad75a7"
const cleanupPolicyKey = "staging/best14-20261021/nas-retention-20261025/cleanup-policy-20261101/" + cleanupPolicySHA + "/policy.json.gz"

type cleanupWindow struct {
	StreamID int64     `json:"stream_id"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end_exclusive"`
}
type nasCleanupPolicy struct {
	ReportSHA string          `json:"report_sha256"`
	Windows   []cleanupWindow `json:"windows_utc"`
	Streams   []struct {
		ID       int64             `json:"stream_id"`
		Timezone string            `json:"timezone"`
		Rankings map[string]string `json:"rankings"`
	} `json:"streams"`
	Whole []int64 `json:"whole_stream_keep"`
}

const cleanupSourceMetadataSQL = `SELECT jsonb_agg(jsonb_build_array(c.id,c.recording_id,r.stream_id,(extract(epoch FROM c.clip_start_at)*1000000)::bigint,(extract(epoch FROM c.clip_end_at)*1000000)::bigint,coalesce(nullif(s.local_timezone,''),r.cron_timezone),c.size_bytes,lower(c.sha256),c.object_key,c.display_path,c.bucket) ORDER BY c.id) FROM recording_clips c JOIN recordings r ON r.id=c.recording_id JOIN streams s ON s.id=r.stream_id WHERE r.account_id=47 AND c.id=ANY($1)`

func (s *Server) cleanupPolicy(ctx context.Context) (*nasCleanupPolicy, error) {
	s.nasCleanupMu.Lock()
	defer s.nasCleanupMu.Unlock()
	if s.nasCleanupPolicyCache != nil {
		return s.nasCleanupPolicyCache, nil
	}
	b, err := s.r2.Get(ctx, cleanupPolicyKey)
	if err != nil || cleanupSHA(b) != cleanupPolicySHA {
		return nil, errors.New("cleanup guard hash unavailable")
	}
	z, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer z.Close()
	b, err = io.ReadAll(io.LimitReader(z, 1<<20))
	if err != nil {
		return nil, err
	}
	var p nasCleanupPolicy
	if json.Unmarshal(b, &p) != nil || p.ReportSHA != nasCleanupReportSHA || len(p.Windows) != 221 || len(p.Streams) != 109 || len(p.Whole) != 31 {
		return nil, errors.New("incomplete cleanup guards")
	}
	s.nasCleanupPolicyCache = &p
	return &p, nil
}
func (p *nasCleanupPolicy) outside(stream int64, a, b time.Time) bool {
	if a.IsZero() || !b.After(a) || a.Year() < 2020 || b.Year() >= 2028 {
		return false
	}
	for _, id := range p.Whole {
		if id == stream {
			return false
		}
	}
	for _, w := range p.Windows {
		if w.StreamID == stream && a.Before(w.End) && b.After(w.Start) {
			return false
		}
	}
	return true
}

// Replay all three registered/availability/quarantine sensitivity selections.
// Neither a changed winner nor an invalid interval can silently narrow KEEP.
const cleanupCurrentRankingsSQL = `WITH r AS (
 SELECT r.id,coalesce(nullif(s.local_timezone,''),r.cron_timezone) tz FROM recordings r JOIN streams s ON s.id=r.stream_id WHERE r.account_id=47 AND r.stream_id=$1
), excluded AS (
 SELECT DISTINCT (p.item->'clip'->>'clip_id')::bigint clip_id FROM recording_collation_hours h JOIN r ON r.id=h.recording_id CROSS JOIN LATERAL jsonb_array_elements(h.manifest->'clips') p(item) WHERE h.excluded_clip_count>0 AND p.item->>'disposition'='quarantined'
), c AS (
 SELECT c.clip_start_at a,c.clip_end_at b,r.tz,(c.purged_at IS NULL AND c.size_bytes>0 AND nullif(c.bucket,'') IS NOT NULL AND nullif(c.object_key,'') IS NOT NULL) available,e.clip_id IS NULL clean FROM recording_clips c JOIN r ON r.id=c.recording_id LEFT JOIN excluded e ON e.clip_id=c.id
), pieces AS (
 SELECT d::date local_date,tstzrange(greatest(c.a,d AT TIME ZONE c.tz),least(c.b,(d+interval '1 day') AT TIME ZONE c.tz),'[)') span,available,clean FROM c CROSS JOIN LATERAL generate_series(date_trunc('day',a AT TIME ZONE tz),date_trunc('day',(b-interval '1 microsecond') AT TIME ZONE tz),interval '1 day') d WHERE b>a AND a>='2020-01-01' AND b<'2028-01-01'
), unions AS (
 SELECT local_date,range_agg(span) all_spans,range_agg(span) FILTER(WHERE available) available_spans,range_agg(span) FILTER(WHERE available AND clean) clean_spans FROM pieces GROUP BY local_date
), daily AS (
 SELECT local_date,metric,(SELECT coalesce(sum(extract(epoch FROM upper(x)-lower(x))),0) FROM unnest(spans) x) seconds FROM unions CROSS JOIN LATERAL (VALUES('registered_seconds',all_spans),('catalog_available_seconds',available_spans),('not_known_quarantined_seconds',clean_spans)) m(metric,spans)
), candidates AS (
 SELECT d::date local_date FROM generate_series((SELECT min(local_date)-13 FROM daily)::timestamp,(SELECT max(local_date) FROM daily)::timestamp,interval '1 day') d
), scores AS (
 SELECT m.metric,candidates.local_date,(SELECT coalesce(sum(seconds),0) FROM daily WHERE daily.metric=m.metric AND daily.local_date>=candidates.local_date AND daily.local_date<candidates.local_date+14) score FROM candidates CROSS JOIN (VALUES('registered_seconds'),('catalog_available_seconds'),('not_known_quarantined_seconds')) m(metric)
), winners AS (
 SELECT DISTINCT ON(metric) metric,local_date FROM scores WHERE NOT EXISTS(SELECT 1 FROM c WHERE a IS NULL OR b IS NULL OR b<=a OR a<'2020-01-01' OR b>='2028-01-01') ORDER BY metric,score DESC,local_date
) SELECT coalesce(jsonb_object_agg(metric,local_date::text),'{}') FROM winners`

func (s *Server) cleanupCurrentPolicy(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, i nasCleanupItem) error {
	report, err := s.cleanupReport(ctx)
	if err != nil || !report.permits(i) {
		return errors.New("reviewed policy unavailable")
	}
	policy, err := s.cleanupPolicy(ctx)
	if err != nil {
		return err
	}
	if !policy.outside(i.StreamID, i.StartAt, i.EndAt) {
		return errors.New("current media bounds overlap KEEP")
	}
	var expected map[string]string
	for _, stream := range policy.Streams {
		if stream.ID == i.StreamID {
			if stream.Timezone != i.Timezone {
				return errors.New("canonical timezone changed")
			}
			expected = stream.Rankings
		}
	}
	var rankings []byte
	if err = q.QueryRow(ctx, cleanupCurrentRankingsSQL, i.StreamID).Scan(&rankings); err != nil {
		return err
	}
	encoded, _ := json.Marshal(expected)
	if !nasSpaceAuditJSONEqual(rankings, encoded) {
		return errors.New("current selection or sensitivity buffers changed; KEEP until reviewed")
	}
	// SourceMetadataSHA pins dates, source membership, canonical timezone and exact
	// catalog source identity. Recheck every source against all221 protected UTC
	// intervals, including the authoritative185 older windows.
	var raw []byte
	if err = q.QueryRow(ctx, cleanupSourceMetadataSQL, i.SourceIDs).Scan(&raw); err != nil || cleanupSHA(raw) != i.SourceManifestSHA {
		return errors.New("source manifest changed")
	}
	var sources [][]json.RawMessage
	if json.Unmarshal(raw, &sources) != nil || len(sources) != len(i.SourceIDs) {
		return errors.New("incomplete source metadata")
	}
	for _, row := range sources {
		if len(row) != 11 {
			return errors.New("invalid source metadata")
		}
		var sid, a, b int64
		var tz, bucket string
		if json.Unmarshal(row[2], &sid) != nil || json.Unmarshal(row[3], &a) != nil || json.Unmarshal(row[4], &b) != nil || json.Unmarshal(row[5], &tz) != nil || json.Unmarshal(row[10], &bucket) != nil || bucket != i.Bucket || sid != i.StreamID || tz != i.Timezone || !policy.outside(sid, time.UnixMicro(a), time.UnixMicro(b)) {
			return errors.New("current source intersects KEEP or is unknown")
		}
	}
	var protected bool
	if i.Kind == "raw" {
		err = q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_collation_hours h JOIN recordings r ON r.id=h.recording_id CROSS JOIN LATERAL jsonb_array_elements(h.manifest->'clips') p(item) WHERE r.account_id=47 AND r.stream_id=$1 AND (p.item->'clip'->>'clip_id')::bigint=$2) OR EXISTS(SELECT 1 FROM recording_joined_source_snapshots WHERE clip_id=$2)`, i.StreamID, i.SourceID).Scan(&protected)
	} else {
		err = q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_collation_hours h JOIN recordings r ON r.id=h.recording_id CROSS JOIN LATERAL jsonb_array_elements(h.manifest->'clips') p(item) WHERE r.account_id=47 AND r.stream_id=$1 AND p.item->>'disposition'='quarantined' AND (p.item->'clip'->>'clip_id')::bigint=ANY($2)) OR EXISTS(SELECT 1 FROM recording_joined_source_snapshots WHERE clip_id=ANY($2))`, i.StreamID, i.SourceIDs).Scan(&protected)
	}
	if err != nil || protected {
		return errors.New("current source/error protection; KEEP")
	}
	return s.cleanupPublicUnchanged(ctx)
}
func (s *Server) cleanupPublicUnchanged(ctx context.Context) error {
	s.nasCleanupMu.Lock()
	defer s.nasCleanupMu.Unlock()
	key := globalDeliveryPrefix + "public/current.json"
	if s.nasCleanupPublicETag != "" {
		_, err := s.r2.HeadExact(ctx, key, s.nasCleanupPublicETag, "")
		return err
	}
	head, err := s.r2.Head(ctx, key)
	if err != nil || head.ETag == "" {
		return errors.New("public delivery identity unavailable")
	}
	body, err := s.r2.Get(ctx, key)
	if err != nil {
		return err
	}
	decoded, err := decodeGlobalDeliveryRegistry(body)
	if err != nil || cleanupSHA(decoded) != "ae55a9fdb106c1e8e4d359320e4b3d6257432606bbf2fc5e8df47a9d16944054" {
		return errors.New("public delivery changed; KEEP")
	}
	if _, err = s.r2.HeadExact(ctx, key, head.ETag, ""); err != nil {
		return err
	}
	s.nasCleanupPublicETag = head.ETag
	return nil
}
