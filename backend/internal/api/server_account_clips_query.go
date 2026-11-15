package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/nasdelivery"
	"github.com/jackc/pgx/v5"
)

const accountClipsScopeSQL = `WITH delivery_fence AS MATERIALIZED (
 SELECT EXISTS(SELECT 1 FROM nas_delivery_retirement_fences
  WHERE connection_id=CASE WHEN $1::bigint=47 THEN 13 ELSE 0 END) AS active
), owned_windows AS (
 SELECT r.id AS recording_id,w.start_at,w.end_at
 FROM recordings r CROSS JOIN delivery_fence fence
 LEFT JOIN streams s ON s.id=r.stream_id
 JOIN nas_delivery_retirement_windows w ON w.connection_id=13 AND w.stream_id=r.stream_id
 WHERE fence.active AND r.account_id=$1 AND r.account_id=47 AND r.delivery='nas_pull'
  AND w.timezone=coalesce(nullif(s.local_timezone,''),r.cron_timezone)
)
SELECT active,coalesce((SELECT json_agg(w ORDER BY recording_id) FROM owned_windows w),'[]')
FROM delivery_fence`

type accountClipWindow struct {
	RecordingID int64     `json:"recording_id"`
	Start       time.Time `json:"start_at"`
	End         time.Time `json:"end_at"`
}

// Both statements use the caller's read-only repeatable-read snapshot: a changed
// owner, timezone, fence or recording cannot slip between scope and page reads.
func accountClipsQuery(ctx context.Context, tx pgx.Tx, accountID int64) (string, error) {
	var active bool
	var raw []byte
	if err := tx.QueryRow(ctx, accountClipsScopeSQL, accountID).Scan(&active, &raw); err != nil {
		return "", err
	}
	if !active {
		return accountClipsCursorSQL, nil
	}
	var windows []accountClipWindow
	if err := json.Unmarshal(raw, &windows); err != nil {
		return "", err
	}
	return accountClipsWindowQuery(windows), nil
}

func accountClipsWindowQuery(windows []accountClipWindow) string {
	if len(windows) == 0 {
		return `SELECT ` + accountClipsFieldsSQL + ` FROM recording_clips c WHERE false AND $1::bigint=47 AND c.id>$2 ORDER BY c.id ASC LIMIT $3`
	}
	branches := make([]string, len(windows))
	for n, w := range windows {
		// Only database int64 IDs and UTC timestamps are rendered, never user text.
		// Literal window selectivity and individual ordered LIMITs let Merge Append
		// request a page lazily under custom, generic and automatically cached plans.
		branches[n] = fmt.Sprintf(`(SELECT c.id FROM recording_clips c WHERE c.recording_id=%d
   AND %s AND c.clip_start_at IS NOT NULL AND c.clip_end_at IS NOT NULL
   AND c.clip_end_at>c.clip_start_at AND c.clip_start_at>='2020-01-01' AND c.clip_end_at<'2028-01-01'
   AND c.clip_start_at<'%s'::timestamptz AND c.clip_end_at>'%s'::timestamptz
   AND %s ORDER BY c.id ASC LIMIT $3)`, w.RecordingID, accountClipsAvailableSQL,
			w.End.UTC().Format(time.RFC3339Nano), w.Start.UTC().Format(time.RFC3339Nano), nasdelivery.NotHeldC)
	}
	return `SELECT ` + accountClipsFieldsSQL + ` FROM (SELECT id FROM (` + strings.Join(branches, " UNION ALL ") + `) recording_pages ORDER BY id ASC LIMIT $3) page
 JOIN recording_clips c ON c.id=page.id WHERE $1::bigint=47 ORDER BY c.id ASC LIMIT $3`
}
