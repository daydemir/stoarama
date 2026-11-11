package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/jackc/pgx/v5/pgxpool"
)

func hash(v string) string {
	s := sha256.Sum256([]byte(v))
	return hex.EncodeToString(s[:])
}

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	for _, id := range []int64{380} {
		var sid int64
		var provider, recURL, catURL, page, via, status, family, ctype, execClass string
		var revision *int64
		var next time.Time
		err = db.QueryRow(ctx, `SELECT r.stream_id,s.provider,r.stream_url,s.source_url,COALESCE(s.source_page_url,''),r.capture_via,r.status,s.source_family,s.capture_type,s.execution_class,(SELECT max(id) FROM stream_source_revisions WHERE stream_id=s.id),r.next_fire_at FROM recordings r JOIN streams s ON s.id=r.stream_id WHERE r.id=$1 AND r.account_id=47`, id).Scan(&sid, &provider, &recURL, &catURL, &page, &via, &status, &family, &ctype, &execClass, &revision, &next)
		if err != nil {
			panic(err)
		}
		var jobs []map[string]any
		rows, err := db.Query(ctx, `SELECT j.id,j.status,j.fire_at,j.window_end_at,j.attempt_count,COALESCE(j.error_text,''),COALESCE(j.lease_owner,''),(SELECT count(*) FROM recording_clips c WHERE c.recording_job_id=j.id) FROM recording_jobs j WHERE j.recording_id=$1 ORDER BY j.fire_at DESC LIMIT 5`, id)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var jid, attempt, clips int64
			var st, msg, owner string
			var fire time.Time
			var end *time.Time
			if err = rows.Scan(&jid, &st, &fire, &end, &attempt, &msg, &owner, &clips); err != nil {
				panic(err)
			}
			jobs = append(jobs, map[string]any{"id": jid, "status": st, "fire": fire, "end": end, "attempt": attempt, "error": msg, "lease_owner": owner, "clips": clips})
		}
		rows.Close()
		probeCtx, cancel := context.WithTimeout(ctx, 55*time.Second)
		resolved, isImage, headers, resolveErr := capture.ResolveCaptureInputWithHeaders(probeCtx, provider, catURL, page)
		probe := map[string]any{"resolved": resolveErr == nil, "image": isImage}
		if resolveErr != nil {
			probe["error"] = resolveErr.Error()
		} else {
			seg, capErr := capture.CaptureSegmentInDirWithHeadersNoThumbnail(probeCtx, resolved, 20*time.Second, "", "", headers)
			if seg.Path != "" {
				_ = os.RemoveAll(filepath.Dir(seg.Path))
			}
			probe["decode_ok"] = capErr == nil
			probe["duration_ms"] = seg.DurationMs
			probe["bytes"] = seg.SizeBytes
			probe["codec"] = seg.VideoCodec
			if capErr != nil {
				probe["error"] = capErr.Error()
			}
		}
		cancel()
		out := map[string]any{"recording": id, "stream": sid, "provider": provider, "capture_via": via, "status": status, "family": family, "capture_type": ctype, "execution_class": execClass, "source_url": catURL, "binding_equal": recURL == catURL, "recording_hash": hash(recURL), "catalog_hash": hash(catURL), "source_revision": revision, "next_fire": next, "probe": probe, "jobs": jobs}
		b, _ := json.Marshal(out)
		fmt.Println(string(b))
	}
}
