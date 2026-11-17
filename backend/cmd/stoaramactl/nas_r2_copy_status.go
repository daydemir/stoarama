package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type nasCopyStatus struct {
	VerifiedInPlaceItemsPerMinute int64                  `json:"verified_in_place_items_per_minute"`
	DownloadedItemsPerMinute      int64                  `json:"downloaded_items_per_minute"`
	Schema                        string                 `json:"schema"`
	UpdatedAt                     time.Time              `json:"updated_at"`
	QueueDepth                    int64                  `json:"queue_depth"`
	Blocked                       int64                  `json:"blocked"`
	VerifiedInPlace               nasCopyStat            `json:"verified_in_place"`
	Downloaded                    nasCopyStat            `json:"downloaded"`
	DownloadedBytes               int64                  `json:"downloaded_bytes"`
	CommittedBytes                int64                  `json:"committed_bytes"`
	BlockedByReason               map[string]nasCopyStat `json:"blocked_by_reason"`
	MismatchLatched               int64                  `json:"mismatch_latched"`
	Attempts                      int64                  `json:"attempts"`
	ReLeasedItems                 int64                  `json:"re_leased_items"`
	MaxAttempts                   int64                  `json:"max_attempts"`
}

func loadNASCopyStatus(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}) (nasCopyStatus, error) {
	s := nasCopyStatus{Schema: "stoarama.nas-r2-copy-status.v1", UpdatedAt: time.Now().UTC(), BlockedByReason: map[string]nasCopyStat{}}
	rows, err := q.Query(ctx, `SELECT state,completion,blocked_reason,count(*),coalesce(sum((item->>'size_bytes')::bigint),0),sum(downloaded_bytes),sum(committed_bytes),sum(attempts),count(*) FILTER(WHERE attempts>1),max(attempts),count(*) FILTER(WHERE verified_at>=clock_timestamp()-interval '1 minute') FROM nas_r2_copy_items WHERE connection_id=13 GROUP BY state,completion,blocked_reason`)
	if err != nil {
		return s, err
	}
	defer rows.Close()
	for rows.Next() {
		var state, completion, reason string
		var stat nasCopyStat
		var downloaded, committed, attempts, released, max, recent int64
		if err = rows.Scan(&state, &completion, &reason, &stat.Objects, &stat.Bytes, &downloaded, &committed, &attempts, &released, &max, &recent); err != nil {
			return s, err
		}
		if state == "pending" || state == "leased" {
			s.QueueDepth += stat.Objects
		}
		if state == "blocked" {
			s.Blocked += stat.Objects
			old := s.BlockedByReason[reason]
			old.Objects += stat.Objects
			old.Bytes += stat.Bytes
			s.BlockedByReason[reason] = old
			if reason == "checksum_mismatch" || reason == "final_conflict" {
				s.MismatchLatched += stat.Objects
			}
		}
		if state == "verified" {
			if completion == "verified_in_place" {
				s.VerifiedInPlaceItemsPerMinute += recent
				s.VerifiedInPlace.Objects += stat.Objects
				s.VerifiedInPlace.Bytes += stat.Bytes
			}
			if completion == "downloaded" {
				s.DownloadedItemsPerMinute += recent
				s.Downloaded.Objects += stat.Objects
				s.Downloaded.Bytes += stat.Bytes
			}
		}
		s.DownloadedBytes += downloaded
		s.CommittedBytes += committed
		s.Attempts += attempts
		s.ReLeasedItems += released
		if max > s.MaxAttempts {
			s.MaxAttempts = max
		}
	}
	return s, rows.Err()
}
