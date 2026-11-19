package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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

type durhamInputRow struct {
	purged                                     bool
	id                                         int64
	bucket, key, endpoint, etag, sha, relative string
	size                                       int64
	start, end                                 time.Time
}

func ImportDurhamStitchHoldSet(ctx context.Context, tx pgx.Tx, set DurhamStitchHoldSet) (int, error) {
	if set.Bucket == "" || !stitchSHA(set.EndpointSHA) || len(set.Hours) < 1 || len(set.Hours) > 20000 {
		return 0, errors.New("bounded exact Durham hold set required")
	}
	for _, lock := range []int64{7343226011173, 7343226011189} {
		var owns bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, lock).Scan(&owns); err != nil || !owns {
			return 0, errors.New("retirement or another stitch import is active")
		}
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
		if json.Unmarshal(hour.Plan, &plan) != nil || len(plan.Clips) < 1 || len(plan.Clips) > nasStitchMaxClips ||
			len(hour.Inputs) != len(plan.Clips) || len(plan.FeatureInputs) != len(plan.Clips) ||
			len(plan.Eligible) != len(plan.Clips) || len(plan.Joins) != len(plan.Clips)-1 || len(plan.Frozen) != len(plan.Joins) ||
			hour.ScopeID <= 0 || hour.Hour.IsZero() || !hour.Hour.Equal(hour.Hour.Truncate(time.Hour)) ||
			plan.Policy != nasStitchPolicy || plan.Revision != nasStitchRevision || plan.CorpusSHA != nasStitchCorpusSHA || plan.KeepSHA != cleanupPolicySHA {
			return 0, fmt.Errorf("invalid Durham plan at ordinal %d", n)
		}
		expectedFolder := strings.Split(plan.Clips[0].RelativePath, "/")[0] + "_V85r_keep28/" +
			strconv.FormatInt(plan.RecordingID, 10) + "/" + hour.Hour.UTC().Format("2006-01-02T15-04-05Z")
		if !validCleanupPath(expectedFolder) || plan.OutputFolder != expectedFolder {
			return 0, fmt.Errorf("invalid Durham output namespace at ordinal %d", n)
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
		if err != nil || start.Before(keep.start) || end.After(keep.end) {
			return 0, fmt.Errorf("Durham scope unavailable at ordinal %d", n)
		}
		rows, err := tx.Query(ctx, `SELECT id,bucket,object_key,endpoint,etag,lower(sha256),size_bytes,display_path,clip_start_at,clip_end_at,purged_at IS NOT NULL
 FROM recording_clips WHERE recording_id=$1 AND clip_start_at>=$2 AND clip_start_at<$3
 AND clip_start_at>=$4 AND clip_end_at<=$5 ORDER BY clip_start_at,id LIMIT $6`,
			plan.RecordingID, hour.Hour, hour.Hour.Add(time.Hour), start, end, nasStitchMaxClips+1)
		if err != nil {
			return 0, err
		}
		catalog, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (durhamInputRow, error) {
			var c durhamInputRow
			err := row.Scan(&c.id, &c.bucket, &c.key, &c.endpoint, &c.etag, &c.sha, &c.size, &c.relative, &c.start, &c.end, &c.purged)
			return c, err
		})
		if err != nil || len(catalog) != len(plan.Clips) {
			return 0, fmt.Errorf("Durham hour has incomplete catalog coverage at ordinal %d", n)
		}
		var bytes int64
		for i, clip := range plan.Clips {
			c, input, feature := catalog[i], hour.Inputs[i], plan.FeatureInputs[i]
			if c.purged || c.id != clip.SourceID || c.id != input.ClipID || c.id != feature.ClipID || c.bucket != set.Bucket ||
				cleanupSHA([]byte(strings.TrimRight(c.endpoint, "/"))) != set.EndpointSHA ||
				c.key != input.Key || c.etag != input.ETag || c.sha != clip.SHA256 || c.sha != input.SHA ||
				c.size != clip.SizeBytes || c.size != input.Size || c.relative != clip.RelativePath ||
				clip.RecordingID != plan.RecordingID || feature.RecordingID != plan.RecordingID ||
				feature.SHA256 != c.sha || feature.SizeBytes != c.size || !c.start.Equal(clip.StartAt) || !c.end.Equal(clip.EndAt) ||
				!c.start.Equal(feature.StartUTC) || !c.end.Equal(feature.EndUTC) ||
				!validCleanupPath(c.relative) || !c.end.After(c.start) || !stitchSHA(c.sha) ||
				c.size <= 0 || c.size > nasStitchMaxBytes || !plan.Eligible[i] || feature.NominalSeconds <= 0 {
				return 0, fmt.Errorf("Durham frozen input changed at hour %d clip %d", n, i)
			}
			bytes += c.size
		}
		if bytes > nasStitchMaxBytes {
			return 0, fmt.Errorf("Durham hour exceeds bounded input size at ordinal %d", n)
		}
		if err := validateDurhamFrozenEdges(ctx, tx, plan); err != nil {
			return 0, err
		}
		body, _ := json.Marshal(plan)
		var id int64
		if err := tx.QueryRow(ctx, `INSERT INTO nas_stitch_r2_hours(scope_id,recording_id,hour_at,plan,plan_sha256)
 VALUES($1,$2,$3,$4,$5) RETURNING id`, hour.ScopeID, plan.RecordingID, hour.Hour, string(body), cleanupSHA(body)).Scan(&id); err != nil {
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
		count += len(hour.Inputs)
	}
	return count, nil
}

func validateDurhamFrozenEdges(ctx context.Context, tx pgx.Tx, plan nasStitchPlan) error {
	ids, hashes, sizes := make([]int64, 0, len(plan.Clips)), make([]string, 0, len(plan.Clips)), make([]int64, 0, len(plan.Clips))
	for _, clip := range plan.Clips {
		ids, hashes, sizes = append(ids, clip.SourceID), append(hashes, clip.SHA256), append(sizes, clip.SizeBytes)
	}
	rows, err := tx.Query(ctx, `SELECT e.witness FROM generate_series(1,cardinality($1::bigint[])-1) g(n)
 LEFT JOIN nas_stitch_edges e ON e.left_id=($1::bigint[])[n] AND e.left_sha=($2::text[])[n] AND e.left_size=($3::bigint[])[n]
  AND e.right_id=($1::bigint[])[n+1] AND e.right_sha=($2::text[])[n+1] AND e.right_size=($3::bigint[])[n+1] AND e.recording_id=$4
 ORDER BY n`, ids, hashes, sizes, plan.RecordingID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for n := 0; rows.Next(); n++ {
		var frozen []byte
		if err := rows.Scan(&frozen); err != nil {
			return err
		}
		join := plan.Clips[n].EndAt.Equal(plan.Clips[n+1].StartAt)
		if len(frozen) > 0 {
			edge, err := parseNASStitchEdge(frozen)
			if err != nil || !nasSpaceAuditJSONEqual(frozen, plan.Frozen[n]) {
				return errors.New("Durham frozen witness differs")
			}
			join = join && edge.Nomination
		} else if len(plan.Frozen[n]) > 0 && string(plan.Frozen[n]) != "null" {
			return errors.New("Durham plan contains an unrecognized frozen witness")
		}
		if join != plan.Joins[n] {
			return errors.New("Durham plan join metadata differs")
		}
	}
	return rows.Err()
}
