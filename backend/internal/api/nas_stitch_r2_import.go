package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type DurhamStitchHoldSet struct {
	Bucket      string             `json:"bucket"`
	EndpointSHA string             `json:"endpoint_sha256"`
	Hours       []DurhamStitchHour `json:"hours"`
}

type DurhamStitchHour struct {
	ScopeID int64               `json:"scope_id"`
	Hour    time.Time           `json:"hour_at"`
	Plan    json.RawMessage     `json:"plan"`
	Inputs  []DurhamStitchInput `json:"inputs"`
}

type DurhamStitchInput struct {
	ClipID int64  `json:"clip_id"`
	Key    string `json:"object_key"`
	ETag   string `json:"etag"`
	SHA    string `json:"sha256"`
	Size   int64  `json:"size_bytes"`
}

// One import holds one advisory lock per input until commit. Production has
// 64 locks per transaction x 103 connections, about 6,592 shared lock slots
// with ~550 in use. 1,000 inputs (about 16 hours of 60 clips) leaves most of
// the table to everyone else; plan larger sets in parts.
const durhamStitchMaxImportInputs = 1000

func durhamStitchHoldSetBounded(set DurhamStitchHoldSet) error {
	inputs := 0
	for _, hour := range set.Hours {
		inputs += len(hour.Inputs)
	}
	if set.Bucket == "" || !stitchSHA(set.EndpointSHA) || len(set.Hours) < 1 || inputs > durhamStitchMaxImportInputs {
		return errors.New("bounded exact Durham hold set required")
	}
	return nil
}

func ImportDurhamStitchHoldSet(ctx context.Context, tx pgx.Tx, set DurhamStitchHoldSet) (int, error) {
	if err := durhamStitchHoldSetBounded(set); err != nil {
		return 0, err
	}
	// Each hold keeps its own key lock until commit. Imports serialize on 1189;
	// retirement of other keys proceeds.
	var owns bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(7343226011189)`).Scan(&owns); err != nil || !owns {
		return 0, errors.New("another stitch import is active")
	}
	var safe bool
	if err := tx.QueryRow(ctx, `SELECT NOT c.enabled AND EXISTS(SELECT 1 FROM r2_retirement_writer_fences f
 WHERE f.bucket=$1 AND f.endpoint_sha256=$2) AND EXISTS(SELECT 1 FROM nas_stitch_corpus WHERE sha256=$3)
 FROM nas_stitch_r2_control c WHERE singleton FOR UPDATE`, set.Bucket, set.EndpointSHA, nasStitchCorpusSHA).
		Scan(&safe); err != nil || !safe {
		return 0, errors.New("Durham hold import requires the frozen R2 fence and corpus with the queue disabled")
	}
	policy, err := cleanupEmbeddedPolicy()
	if err != nil {
		return 0, err
	}
	count := 0
	for n, hour := range set.Hours {
		var plan nasStitchPlan
		if json.Unmarshal(hour.Plan, &plan) != nil || len(hour.Inputs) < 1 || len(hour.Inputs) > nasStitchMaxClips ||
			hour.ScopeID <= 0 || hour.Hour.IsZero() || !hour.Hour.Equal(hour.Hour.Truncate(time.Hour)) {
			return 0, fmt.Errorf("invalid Durham plan at ordinal %d", n)
		}
		keep, ok := policy.Streams[plan.StreamID]
		if !ok {
			return 0, errors.New("Durham scope not in pinned keep policy")
		}
		var start, end time.Time
		err := tx.QueryRow(ctx, `SELECT s.start_at,s.end_at FROM nas_stitch_scopes s
 JOIN recordings r ON r.stream_id=s.stream_id WHERE s.id=$1 AND s.stream_id=$2 AND r.id=$3 AND r.account_id=47
 AND NOT s.paused AND s.approved_until>clock_timestamp() FOR SHARE OF s`,
			hour.ScopeID, plan.StreamID, plan.RecordingID).Scan(&start, &end)
		if err != nil || start.Before(keep.start) || end.After(keep.end) || hour.Hour.Before(start) || hour.Hour.Add(time.Hour).After(end) {
			return 0, fmt.Errorf("Durham whole-hour scope unavailable at ordinal %d", n)
		}
		frozen := nasStitchTask{Plan: nasStitchPlan{StreamID: plan.StreamID, RecordingID: plan.RecordingID}}
		frozen, err = buildNASStitchPlan(ctx, tx, frozen, start, end, hour.Hour, set.Bucket, nasStitchOnDurham, true)
		if err != nil {
			return 0, err
		}
		body, err := json.Marshal(frozen.Plan)
		if err != nil || !nasSpaceAuditJSONEqual(body, hour.Plan) || len(hour.Inputs) != len(frozen.Plan.Clips) {
			return 0, fmt.Errorf("Durham plan differs from the canonical catalog plan at ordinal %d", n)
		}
		var inputBytes int64
		for i, clip := range frozen.Plan.Clips {
			input := hour.Inputs[i]
			inputBytes += clip.SizeBytes
			if input.ClipID != clip.SourceID || input.Key != clip.ObjectKey || input.SHA != clip.SHA256 || input.Size != clip.SizeBytes ||
				!strings.HasPrefix(input.Key, "managed/") || input.ETag == "" {
				return 0, fmt.Errorf("Durham frozen input differs at hour %d clip %d", n, i)
			}
		}
		var id int64
		if err := tx.QueryRow(ctx, `INSERT INTO nas_stitch_r2_hours(scope_id,recording_id,hour_at,plan,plan_sha256,input_bytes)
 VALUES($1,$2,$3,$4,$5,$6) RETURNING id`, hour.ScopeID, plan.RecordingID, hour.Hour, string(body), cleanupSHA(body), inputBytes).Scan(&id); err != nil {
			return 0, err
		}
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"nas_stitch_r2_input_holds"},
			[]string{"hour_id", "bucket", "object_key", "catalog_clip_id", "sha256", "size_bytes", "etag"},
			pgx.CopyFromSlice(len(hour.Inputs), func(i int) ([]any, error) {
				input := hour.Inputs[i]
				return []any{id, set.Bucket, input.Key, input.ClipID, input.SHA, input.Size, input.ETag}, nil
			}))
		if err != nil {
			return 0, err
		}
		var exact bool
		if err := tx.QueryRow(ctx, `SELECT nas_stitch_r2_has_exact_input_set($1)`, id).Scan(&exact); err != nil || !exact {
			return 0, fmt.Errorf("Durham hour has incomplete or changed R2 coverage at ordinal %d", n)
		}
		count += len(hour.Inputs)
	}
	return count, nil
}

// PlanDurhamStitchHoldSet records a read-only snapshot for controller review.
// Import revalidates the complete snapshot before publishing any holds.
func PlanDurhamStitchHoldSet(ctx context.Context, tx pgx.Tx, scopes []int64, bucket, endpoint string, limit int) (DurhamStitchHoldSet, error) {
	set := DurhamStitchHoldSet{Bucket: bucket, EndpointSHA: cleanupSHA([]byte(strings.TrimRight(endpoint, "/")))}
	if len(scopes) == 0 || len(scopes) > 400 || bucket == "" || endpoint == "" || limit < 1 || limit > 20000 {
		return set, errors.New("bounded selected Durham scopes required")
	}
	rows, err := tx.Query(ctx, `WITH hours AS (
 SELECT s.id AS scope,r.id AS recording,s.stream_id AS stream,s.start_at,s.end_at,
  date_trunc('hour',c.clip_start_at,'UTC') AS hour,sum(c.size_bytes) AS bytes
 FROM nas_stitch_scopes s JOIN recordings r ON r.stream_id=s.stream_id AND r.account_id=47
 JOIN recording_clips c ON c.recording_id=r.id AND c.clip_start_at>=s.start_at AND c.clip_start_at<s.end_at
 LEFT JOIN nas_inventory_files f ON f.connection_id=13 AND f.clip_id=c.id AND f.state='present'
  AND f.size_bytes=c.size_bytes AND f.sha256=c.sha256 AND f.relative_path=c.display_path
 WHERE s.id=ANY($1) AND NOT s.paused AND s.approved_until>clock_timestamp()
  AND NOT EXISTS(SELECT 1 FROM nas_stitch_r2_hours h WHERE h.recording_id=r.id
   AND h.hour_at=date_trunc('hour',c.clip_start_at,'UTC'))
 GROUP BY s.id,r.id,6
 HAVING date_trunc('hour',c.clip_start_at,'UTC')>=s.start_at
  AND date_trunc('hour',c.clip_start_at,'UTC')+interval '1 hour'<=s.end_at
  AND count(*) BETWEEN 1 AND 200 AND sum(c.size_bytes)<=2500000000
  AND bool_and(c.clip_end_at>c.clip_start_at AND c.clip_end_at<=s.end_at
   AND c.purged_at IS NULL AND c.bucket=$2 AND rtrim(c.endpoint,'/')=$3
   AND c.object_key LIKE 'managed/%' AND c.etag<>'' AND c.size_bytes>0 AND f.clip_id IS NOT NULL)
 ), chosen AS (
 SELECT DISTINCT ON (h.recording,h.hour) h.* FROM hours h
 ORDER BY h.recording,h.hour,h.scope
 )
 SELECT scope,recording,stream,start_at,end_at,hour FROM chosen ORDER BY bytes DESC,hour,recording LIMIT $4`,
		scopes, bucket, strings.TrimRight(endpoint, "/"), limit)
	if err != nil {
		return set, err
	}
	type slot struct {
		scope, recording, stream int64
		start, end, hour         time.Time
	}
	slots, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (slot, error) {
		var value slot
		err := row.Scan(&value.scope, &value.recording, &value.stream, &value.start, &value.end, &value.hour)
		return value, err
	})
	if err != nil {
		return set, err
	}
	for _, candidate := range slots {
		task := nasStitchTask{Plan: nasStitchPlan{StreamID: candidate.stream, RecordingID: candidate.recording}}
		task, err = buildNASStitchPlan(ctx, tx, task, candidate.start, candidate.end, candidate.hour, bucket, nasStitchOnDurham, false)
		if err != nil {
			return set, err
		}
		var hour DurhamStitchHour
		hour.ScopeID, hour.Hour = candidate.scope, candidate.hour.UTC()
		hour.Plan, err = json.Marshal(task.Plan)
		if err != nil {
			return set, err
		}
		for _, clip := range task.Plan.Clips {
			var etag string
			if err := tx.QueryRow(ctx, `SELECT etag FROM recording_clips WHERE id=$1`, clip.SourceID).Scan(&etag); err != nil {
				return set, err
			}
			hour.Inputs = append(hour.Inputs, DurhamStitchInput{ClipID: clip.SourceID, Key: clip.ObjectKey,
				ETag: etag, SHA: clip.SHA256, Size: clip.SizeBytes})
		}
		set.Hours = append(set.Hours, hour)
	}
	return set, nil
}
