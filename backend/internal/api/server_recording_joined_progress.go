package api

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

type recordingJoinedProgress struct {
	SourceDurationMS int64 `json:"source_duration_ms"`
	JoinedReadyMS    int64 `json:"joined_ready_ms"`
	Percent          *int  `json:"joined_percent"`
}

const recordingJoinedReadyFromCandidateHoursSQL = `SELECT DISTINCT src.clip_id
	FROM candidate_hours h
	JOIN recording_joined_artifacts media
	  ON media.hour_record_id=h.id
	 AND media.batch_record_id=h.batch_record_id
	 AND media.account_id=h.account_id
	 AND media.artifact_kind='media'
	 AND media.published_at IS NOT NULL
	 AND media.etag IS NOT NULL AND media.etag<>''
	 AND media.version_id IS NOT NULL
	JOIN recording_joined_media_sources ms ON ms.artifact_id=media.id
	JOIN recording_joined_sources src
	  ON src.id=ms.source_id
	 AND src.hour_record_id=h.id
	 AND src.recording_id=h.recording_id
	 AND src.account_id=h.account_id
	 AND src.batch_record_id=h.batch_record_id
	WHERE EXISTS (
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

const recordingJoinedReadyClipsSQL = `WITH candidate_hours AS MATERIALIZED (
	SELECT id,batch_record_id,account_id,recording_id
	FROM recording_joined_hours
	WHERE account_id=$1 AND recording_id=ANY($2::bigint[]) AND state='sealed'
)
` + recordingJoinedReadyFromCandidateHoursSQL

const recordingJoinedProgressSQL = `WITH requested AS (
	SELECT DISTINCT id AS recording_id
	FROM recordings
	WHERE account_id=$1 AND status<>'canceled' AND id=ANY($2::bigint[])
), frozen_sources AS MATERIALIZED (
	SELECT DISTINCT ON (src.recording_id,src.clip_id)
		src.recording_id,src.clip_id,src.start_at,src.end_at
	FROM recording_joined_sources src
	JOIN requested req ON req.recording_id=src.recording_id
	WHERE src.account_id=$1 AND src.recording_id=ANY($2::bigint[])
	ORDER BY src.recording_id,src.clip_id,src.batch_record_id DESC,src.id DESC
), ready_clip_ids AS (` + recordingJoinedReadyClipsSQL + `), source AS (
	SELECT src.recording_id,
		COALESCE(sum(EXTRACT(epoch FROM (src.end_at-src.start_at))*1000),0)::bigint AS duration_ms
	FROM frozen_sources src
	GROUP BY src.recording_id
), ready AS (
	SELECT src.recording_id,
		COALESCE(sum(EXTRACT(epoch FROM (src.end_at-src.start_at))*1000),0)::bigint AS duration_ms
	FROM frozen_sources src
	JOIN ready_clip_ids ready_clip ON ready_clip.clip_id=src.clip_id
	GROUP BY src.recording_id
)
SELECT req.recording_id,COALESCE(source.duration_ms,0),COALESCE(ready.duration_ms,0)
FROM requested req
LEFT JOIN source ON source.recording_id=req.recording_id
LEFT JOIN ready ON ready.recording_id=req.recording_id
ORDER BY req.recording_id`

// recordingJoinedProgressForAccount returns duration-based joined coverage for
// the requested account-owned recordings. Its denominator is the immutable
// frozen source population selected for joining, not every raw clip ever
// recorded. Repeated generations are deduplicated by clip ID. A mapped source
// counts only when its specific media artifact and containing hour manifest are
// both published.
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
			log.Printf("recording joined progress omitted recording_id=%d ready_ms=%d source_ms=%d", recordingID, progress.JoinedReadyMS, progress.SourceDurationMS)
			continue
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

type recordingJoinedBinProgress struct {
	SourceDurationMS int64
	JoinedReadyMS    int64
}

const recordingJoinedProgressBinsSQL = `WITH bins AS (
	SELECT recording_id,bin_start,bin_end,ordinality
	FROM unnest($1::bigint[],$2::timestamptz[],$3::timestamptz[])
		WITH ORDINALITY b(recording_id,bin_start,bin_end,ordinality)
), candidate_hours AS MATERIALIZED (
	SELECT id,batch_record_id,account_id,recording_id
	FROM recording_joined_hours
	WHERE account_id=$4 AND recording_id=ANY($1::bigint[]) AND state='sealed'
), ready AS (` + recordingJoinedReadyFromCandidateHoursSQL + `)
SELECT b.ordinality,
	COALESCE(sum(EXTRACT(epoch FROM (least(c.clip_end_at,b.bin_end)-greatest(c.clip_start_at,b.bin_start)))*1000),0)::bigint,
	COALESCE(sum(EXTRACT(epoch FROM (least(c.clip_end_at,b.bin_end)-greatest(c.clip_start_at,b.bin_start)))*1000)
		FILTER (WHERE ready.clip_id IS NOT NULL),0)::bigint
FROM bins b
LEFT JOIN recording_clips c ON c.recording_id=b.recording_id AND c.purged_at IS NULL
	AND c.clip_start_at<b.bin_end AND c.clip_end_at>b.bin_start
LEFT JOIN ready ON ready.clip_id=c.id
GROUP BY b.ordinality
ORDER BY b.ordinality`

func (s *Server) recordingJoinedProgressForBins(ctx context.Context, accountID int64, recordingIDs []int64, starts, ends []time.Time) ([]recordingJoinedBinProgress, error) {
	if accountID <= 0 || len(recordingIDs) != len(starts) || len(starts) != len(ends) {
		return nil, fmt.Errorf("invalid joined health bin scope")
	}
	out := make([]recordingJoinedBinProgress, len(starts))
	if len(starts) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, recordingJoinedProgressBinsSQL, recordingIDs, starts, ends, accountID)
	if err != nil {
		return nil, fmt.Errorf("count joined source duration: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ordinal, sourceMS, readyMS int64
		if err := rows.Scan(&ordinal, &sourceMS, &readyMS); err != nil {
			return nil, fmt.Errorf("scan joined source duration: %w", err)
		}
		if ordinal < 1 || ordinal > int64(len(out)) {
			return nil, fmt.Errorf("joined health bin ordinal %d out of range", ordinal)
		}
		if readyMS > sourceMS {
			log.Printf("recording joined health bin omitted ordinal=%d ready_ms=%d source_ms=%d", ordinal, readyMS, sourceMS)
			continue
		}
		out[ordinal-1] = recordingJoinedBinProgress{SourceDurationMS: sourceMS, JoinedReadyMS: readyMS}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("count joined source duration: %w", err)
	}
	return out, nil
}

func (s *Server) populateRecordingJoinedProgressBins(ctx context.Context, accountID, recordingID int64, starts, ends []time.Time, bins []recordingHealthBin) error {
	recordingIDs := make([]int64, len(starts))
	for i := range recordingIDs {
		recordingIDs[i] = recordingID
	}
	progress, err := s.recordingJoinedProgressForBins(ctx, accountID, recordingIDs, starts, ends)
	if err != nil {
		return err
	}
	if len(progress) != len(bins) {
		return fmt.Errorf("joined health bins=%d want %d", len(progress), len(bins))
	}
	for i := range bins {
		bins[i].SourceDurationMS = progress[i].SourceDurationMS
		bins[i].JoinedReadyMS = progress[i].JoinedReadyMS
	}
	return nil
}
