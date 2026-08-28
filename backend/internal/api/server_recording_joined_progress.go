package api

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type recordingJoinedProgress struct {
	SourceDurationMS int64 `json:"source_duration_ms"`
	JoinedReadyMS    int64 `json:"joined_ready_ms"`
	Percent          *int  `json:"joined_percent"`
}

const recordingJoinedReadyClipsSQL = `
	SELECT DISTINCT src.clip_id
	FROM recording_joined_sources src
	JOIN recording_joined_hours h
	  ON h.id=src.hour_record_id
	 AND h.recording_id=src.recording_id
	 AND h.account_id=src.account_id
	 AND h.batch_record_id=src.batch_record_id
	 AND h.state='sealed'
	JOIN recording_joined_media_sources ms ON ms.source_id=src.id
	JOIN recording_joined_artifacts media
	  ON media.id=ms.artifact_id
	 AND media.hour_record_id=h.id
	 AND media.batch_record_id=h.batch_record_id
	 AND media.account_id=h.account_id
	 AND media.artifact_kind='media'
	 AND media.published_at IS NOT NULL
	 AND media.etag IS NOT NULL AND media.etag<>''
	 AND media.version_id IS NOT NULL
	WHERE src.account_id=$1
	  AND src.recording_id=ANY($2::bigint[])
	  AND EXISTS (
	    SELECT 1
	    FROM recording_joined_artifacts manifest
	    WHERE manifest.hour_record_id=h.id
	      AND manifest.batch_record_id=h.batch_record_id
	      AND manifest.account_id=h.account_id
	      AND manifest.artifact_kind='hour_manifest'
	      AND manifest.publication_state='published'
	      AND manifest.published_at IS NOT NULL
	      AND manifest.etag IS NOT NULL AND manifest.etag<>''
	      AND manifest.version_id IS NOT NULL
	  )`

const recordingJoinedProgressSQL = `WITH requested AS (
	SELECT DISTINCT id AS recording_id
	FROM recordings
	WHERE account_id=$1 AND status<>'canceled' AND id=ANY($2::bigint[])
), ready_clip_ids AS (` + recordingJoinedReadyClipsSQL + `), source AS (
	SELECT c.recording_id,
		COALESCE(sum(EXTRACT(epoch FROM (c.clip_end_at-c.clip_start_at))*1000),0)::bigint AS duration_ms
	FROM recording_clips c
	JOIN requested req ON req.recording_id=c.recording_id
	WHERE c.purged_at IS NULL AND c.recording_id=ANY($2::bigint[])
	GROUP BY c.recording_id
), ready AS (
	SELECT c.recording_id,
		COALESCE(sum(EXTRACT(epoch FROM (c.clip_end_at-c.clip_start_at))*1000),0)::bigint AS duration_ms
	FROM recording_clips c
	JOIN requested req ON req.recording_id=c.recording_id
	JOIN ready_clip_ids ready_clip ON ready_clip.clip_id=c.id
	WHERE c.purged_at IS NULL AND c.recording_id=ANY($2::bigint[])
	GROUP BY c.recording_id
)
SELECT req.recording_id,COALESCE(source.duration_ms,0),COALESCE(ready.duration_ms,0)
FROM requested req
LEFT JOIN source ON source.recording_id=req.recording_id
LEFT JOIN ready ON ready.recording_id=req.recording_id
ORDER BY req.recording_id`

// recordingJoinedProgressForAccount returns duration-based joined coverage for
// the requested account-owned recordings. A mapped source counts only when its
// specific media artifact and containing hour manifest are both published.
func (s *Server) recordingJoinedProgressForAccount(ctx context.Context, accountID int64, recordingIDs []int64) (map[int64]recordingJoinedProgress, error) {
	out := make(map[int64]recordingJoinedProgress, len(recordingIDs))
	if accountID <= 0 || len(recordingIDs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, recordingJoinedProgressSQL, accountID, recordingIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var recordingID int64
		var progress recordingJoinedProgress
		if err := rows.Scan(&recordingID, &progress.SourceDurationMS, &progress.JoinedReadyMS); err != nil {
			return nil, err
		}
		if progress.JoinedReadyMS > progress.SourceDurationMS {
			return nil, fmt.Errorf("recording %d joined duration %d exceeds source duration %d", recordingID, progress.JoinedReadyMS, progress.SourceDurationMS)
		}
		if progress.SourceDurationMS > 0 {
			percent := int(progress.JoinedReadyMS * 100 / progress.SourceDurationMS)
			progress.Percent = &percent
		}
		out[recordingID] = progress
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func attachRecordingJoinedProgress(items []map[string]any, progress map[int64]recordingJoinedProgress) {
	for _, item := range items {
		value, ok := progress[item["id"].(int64)]
		if !ok {
			continue
		}
		item["source_duration_ms"] = value.SourceDurationMS
		item["joined_ready_ms"] = value.JoinedReadyMS
		item["joined_percent"] = value.Percent
	}
}

// recordingJoinedSortDirection maps the two supported list values to a stable
// numeric direction and leaves every other list order unchanged.
func recordingJoinedSortDirection(raw string) int {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "joined_asc":
		return 1
	case "joined_desc":
		return -1
	default:
		return 0
	}
}

// recordingJoinedComesFirst orders available percentages before unavailable
// values and uses descending recording ID as the deterministic final tie-breaker.
func recordingJoinedComesFirst(leftID, rightID int64, left, right recordingJoinedProgress, direction int) bool {
	leftAvailable := left.Percent != nil
	rightAvailable := right.Percent != nil
	if leftAvailable != rightAvailable {
		return leftAvailable
	}
	if leftAvailable && *left.Percent != *right.Percent {
		return direction*(*left.Percent-*right.Percent) < 0
	}
	return leftID > rightID
}

// sortRecordingMapsByJoinedProgress applies joined coverage ordering to the
// authenticated recordings DTO without changing default list order.
func sortRecordingMapsByJoinedProgress(items []map[string]any, progress map[int64]recordingJoinedProgress, direction int) {
	if direction == 0 {
		return
	}
	sort.SliceStable(items, func(i, j int) bool {
		leftID, leftOK := items[i]["id"].(int64)
		rightID, rightOK := items[j]["id"].(int64)
		if !leftOK || !rightOK {
			return leftOK
		}
		return recordingJoinedComesFirst(leftID, rightID, progress[leftID], progress[rightID], direction)
	})
}

// sortSharedRecordingsByJoinedProgress mirrors authenticated joined ordering on
// the public cohort DTO.
func sortSharedRecordingsByJoinedProgress(items []sharedRecording, direction int) {
	if direction == 0 {
		return
	}
	sort.SliceStable(items, func(i, j int) bool {
		left := recordingJoinedProgress{SourceDurationMS: items[i].SourceDurationMS, JoinedReadyMS: items[i].JoinedReadyMS, Percent: items[i].JoinedPercent}
		right := recordingJoinedProgress{SourceDurationMS: items[j].SourceDurationMS, JoinedReadyMS: items[j].JoinedReadyMS, Percent: items[j].JoinedPercent}
		return recordingJoinedComesFirst(items[i].ID, items[j].ID, left, right, direction)
	})
}

const recordingJoinedProgressBinsSQL = `WITH bins AS (
	SELECT bin_start,bin_end,ordinality
	FROM unnest($2::timestamptz[],$3::timestamptz[]) WITH ORDINALITY b(bin_start,bin_end,ordinality)
), clips AS (
	SELECT id,clip_start_at,clip_end_at
	FROM recording_clips
	WHERE recording_id=$1 AND purged_at IS NULL
), ready AS (
	SELECT DISTINCT src.clip_id
	FROM recording_joined_sources src
	JOIN recording_joined_hours h
	  ON h.id=src.hour_record_id
	 AND h.recording_id=src.recording_id
	 AND h.account_id=src.account_id
	 AND h.batch_record_id=src.batch_record_id
	 AND h.state='sealed'
	JOIN recording_joined_media_sources ms ON ms.source_id=src.id
	JOIN recording_joined_artifacts media
	  ON media.id=ms.artifact_id
	 AND media.hour_record_id=h.id
	 AND media.batch_record_id=h.batch_record_id
	 AND media.account_id=h.account_id
	 AND media.artifact_kind='media'
	 AND media.published_at IS NOT NULL
	 AND media.etag IS NOT NULL AND media.etag<>''
	 AND media.version_id IS NOT NULL
	WHERE src.recording_id=$1
	  AND EXISTS (
	    SELECT 1 FROM recording_joined_artifacts manifest
	    WHERE manifest.hour_record_id=h.id
	      AND manifest.batch_record_id=h.batch_record_id
	      AND manifest.account_id=h.account_id
	      AND manifest.artifact_kind='hour_manifest'
	      AND manifest.publication_state='published'
	      AND manifest.published_at IS NOT NULL
	      AND manifest.etag IS NOT NULL AND manifest.etag<>''
	      AND manifest.version_id IS NOT NULL
	  )
)
SELECT b.ordinality,
	COALESCE(sum(EXTRACT(epoch FROM (least(c.clip_end_at,b.bin_end)-greatest(c.clip_start_at,b.bin_start)))*1000),0)::bigint,
	COALESCE(sum(EXTRACT(epoch FROM (least(c.clip_end_at,b.bin_end)-greatest(c.clip_start_at,b.bin_start)))*1000)
		FILTER (WHERE ready.clip_id IS NOT NULL),0)::bigint
FROM bins b
LEFT JOIN clips c ON c.clip_start_at<b.bin_end AND c.clip_end_at>b.bin_start
LEFT JOIN ready ON ready.clip_id=c.id
GROUP BY b.ordinality
ORDER BY b.ordinality`

func (s *Server) populateRecordingJoinedProgressBins(ctx context.Context, recordingID int64, starts, ends []time.Time, bins []recordingHealthBin) error {
	rows, err := s.pool.Query(ctx, recordingJoinedProgressBinsSQL, recordingID, starts, ends)
	if err != nil {
		return fmt.Errorf("count joined source duration: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ordinal, sourceMS, readyMS int64
		if err := rows.Scan(&ordinal, &sourceMS, &readyMS); err != nil {
			return fmt.Errorf("scan joined source duration: %w", err)
		}
		if ordinal < 1 || ordinal > int64(len(bins)) {
			return fmt.Errorf("joined health bin ordinal %d out of range", ordinal)
		}
		if readyMS > sourceMS {
			return fmt.Errorf("joined health bin %d ready duration %d exceeds source duration %d", ordinal, readyMS, sourceMS)
		}
		bins[ordinal-1].SourceDurationMS = sourceMS
		bins[ordinal-1].JoinedReadyMS = readyMS
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("count joined source duration: %w", err)
	}
	return nil
}
