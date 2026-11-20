package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5"
)

type deliveryNASWindow struct {
	StreamID int64     `json:"stream_id"`
	Start    time.Time `json:"ws"`
	End      time.Time `json:"we"`
}

type deliveryNASManifestRow struct {
	OutputID int64 `json:"output_id"`
	Bytes    int64 `json:"size_bytes"`
}

type deliveryNASManifestSummary struct {
	Rows              int    `json:"rows"`
	Bytes             int64  `json:"bytes"`
	RosterSHA         string `json:"roster_sha256"`
	PicksSHA          string `json:"picks_sha256"`
	Queued            bool   `json:"queued"`
	VerificationRound int64  `json:"verification_round,omitempty"`
}

func deliveryNASWindows(picks r2RetirePicks) ([]deliveryNASWindow, error) {
	windows := make([]deliveryNASWindow, 0, len(picks.Choices))
	for id, pick := range picks.Choices {
		if pick.Choice == "" || pick.Choice == "all" {
			continue
		}
		start, startOK := r2RetireParsePickTime(pick.KeepStartUTC)
		end, endOK := r2RetireParsePickTime(pick.KeepEndUTC)
		if !startOK || !endOK || !end.After(start) {
			return nil, fmt.Errorf("stream %d has an invalid owner window", id)
		}
		windows = append(windows, deliveryNASWindow{id, start, end})
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i].StreamID < windows[j].StreamID })
	if len(windows) == 0 {
		return nil, errors.New("owner window selection is empty")
	}
	return windows, nil
}

const deliveryNASRosterSQL = `
WITH picks AS (
 SELECT * FROM jsonb_to_recordset($1::jsonb) AS p(stream_id bigint,ws timestamptz,we timestamptz)
)
SELECT jsonb_build_object('bucket','stoarama','object_key',k.object_key,'size_bytes',k.size_bytes,
 'etag',k.etag,'output_id',o.id,'sha256',o.sha256,'start_at',o.start_at,'end_at',o.end_at,
 'recording_id',h.recording_id,'stream_id',r.stream_id)::text,
 o.nas_relative_path,
 CASE
  WHEN r.account_id<>47 OR r.delivery<>'nas_pull' THEN 'recording is outside NAS delivery on account 47'
  WHEN h.generation<>2 OR h.status<>'collated' OR o.size_bytes<>k.size_bytes
    OR k.sha256 NOT IN ('',o.sha256) OR o.sha256 !~ '^[a-f0-9]{64}$' THEN 'catalog and delivery hold identity differ'
  WHEN EXISTS(SELECT 1 FROM r2_object_retirements t WHERE t.kind='collated' AND t.source_id=o.id)
    THEN 'the R2 object is retired'
  ELSE '' END
FROM delivery_keep_holds k
JOIN recording_collation_outputs o ON o.object_key=k.object_key
JOIN recording_collation_hours h ON h.id=o.collation_hour_id
JOIN recordings r ON r.id=h.recording_id
JOIN picks p ON p.stream_id=r.stream_id
WHERE k.bucket='stoarama' AND k.kind='video_collated'
 AND (k.object_key LIKE 'joined/goodplus-%' OR k.object_key LIKE 'joined/nightly-collation-v2/%')
 AND o.start_at<p.we AND o.end_at>p.ws
ORDER BY k.object_key
`

func deliveryNASReadRoster(path, expected string) ([]byte, error) {
	if len(expected) != 64 || !isLowerHex(expected) {
		return nil, errors.New("expected SHA-256 must pin the reviewed roster")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	const maxBytes = 32 << 20
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	if len(raw) == 0 || len(raw) > maxBytes || hex.EncodeToString(sum[:]) != expected {
		return nil, errors.New("reviewed roster is empty, oversized or has changed")
	}
	return raw, nil
}

func queueDeliveryNASHandoff(ctx context.Context, tx pgx.Tx, approved []byte, windows []deliveryNASWindow,
	summary deliveryNASManifestSummary, mount *[2]int64) (deliveryNASManifestSummary, error) {
	if _, err := tx.Exec(ctx, "SET LOCAL TIME ZONE 'UTC'; SET LOCAL statement_timeout='45s'; SET LOCAL lock_timeout='2s'"); err != nil {
		return summary, err
	}
	body, err := json.Marshal(windows)
	if err != nil {
		return summary, err
	}
	rows, err := tx.Query(ctx, deliveryNASRosterSQL, string(body))
	if err != nil {
		return summary, err
	}
	defer rows.Close()
	read := bufio.NewReader(bytes.NewReader(approved))
	seen := map[int64]bool{}
	var outputIDs []int64
	pins := make([][]any, 0)
	for rows.Next() {
		var canonical, relative, blocker string
		if err := rows.Scan(&canonical, &relative, &blocker); err != nil {
			return summary, err
		}
		line, err := read.ReadString('\n')
		var want, row deliveryNASManifestRow
		if json.Unmarshal([]byte(canonical), &row) != nil {
			return summary, errors.New("live delivery roster produced invalid JSON")
		}
		if err != nil || json.Unmarshal([]byte(line), &want) != nil {
			return summary, fmt.Errorf("live roster contains unexpected delivery output %d", row.OutputID)
		}
		if line != canonical+"\n" {
			return summary, fmt.Errorf("approved delivery output %d differs from live output %d", want.OutputID, row.OutputID)
		}
		if blocker != "" {
			return summary, fmt.Errorf("delivery output %d is blocked: %s", row.OutputID, blocker)
		}
		if (!strings.HasSuffix(relative, ".mp4") || !api.ValidCollatedNASPath(relative)) || row.Bytes <= 0 || row.OutputID <= 0 || seen[row.OutputID] {
			return summary, fmt.Errorf("delivery output %d is ambiguous or outside the copy contract", row.OutputID)
		}
		seen[row.OutputID] = true
		outputIDs = append(outputIDs, row.OutputID)
		pins = append(pins, []any{canonical, relative, summary.RosterSHA, summary.PicksSHA})
		summary.Rows++
		summary.Bytes += row.Bytes
	}
	if err := rows.Err(); err != nil {
		return summary, err
	}
	if line, err := read.ReadString('\n'); !errors.Is(err, io.EOF) || line != "" {
		var expected deliveryNASManifestRow
		if json.Unmarshal([]byte(line), &expected) != nil {
			return summary, errors.New("approved delivery roster has an invalid final row")
		}
		return summary, fmt.Errorf("approved delivery output %d is absent from the live roster", expected.OutputID)
	}
	if summary.Rows == 0 {
		return summary, errors.New("live delivery roster is empty")
	}
	if err := checkDeliveryNASRosterFences(ctx, tx, outputIDs); err != nil {
		return summary, err
	}
	if mount != nil {
		if err := pinDeliveryNASHandoffMount(ctx, tx, summary.RosterSHA, *mount); err != nil {
			return summary, err
		}
		if err := insertDeliveryNASHandoff(ctx, tx, pins); err != nil {
			return summary, err
		}
		tag, err := tx.Exec(ctx, `UPDATE connections SET delivery_nas_handoff_enabled=true,delivery_nas_handoff_roster_sha256=$1
 WHERE id=13 AND account_id=47 AND kind='nas_pull'`, summary.RosterSHA)
		if err != nil {
			return summary, err
		}
		if tag.RowsAffected() != 1 {
			return summary, errors.New("the approved NAS connection is unavailable")
		}
		summary.Queued = true
	}
	return summary, nil
}

func checkDeliveryNASRosterFences(ctx context.Context, tx pgx.Tx, outputs []int64) error {
	// The same live predicate used by feed admission evaluates every source
	// clip. Bound each statement independently and keep one catalog snapshot.
	for batch := range slices.Chunk(outputs, 128) {
		var blocked []int64
		err := tx.QueryRow(ctx, `
 SELECT coalesce(array_agg(output_id ORDER BY output_id),'{}'::bigint[])
 FROM unnest($1::bigint[]) output_id
 WHERE NOT nas_delivery_allowed(13,'collated',output_id)`, batch).Scan(&blocked)
		if err != nil {
			return err
		}
		if len(blocked) > 0 {
			return fmt.Errorf("existing NAS delivery fence withholds outputs %v", blocked)
		}
	}
	return nil
}

func pinDeliveryNASHandoffMount(ctx context.Context, tx pgx.Tx, roster string, mount [2]int64) error {
	if mount[0] < 0 || mount[1] <= 0 {
		return errors.New("the controller-approved NAS device and root inode are required")
	}
	if _, err := tx.Exec(ctx, `
 INSERT INTO delivery_nas_handoff_mounts(roster_sha256,device,inode)
 VALUES($1,$2,$3) ON CONFLICT(roster_sha256) DO NOTHING`, roster, mount[0], mount[1]); err != nil {
		return err
	}
	var current [2]int64
	if err := tx.QueryRow(ctx, `SELECT device,inode FROM delivery_nas_handoff_mounts WHERE roster_sha256=$1`, roster).Scan(&current[0], &current[1]); err != nil {
		return err
	}
	if current != mount {
		return errors.New("the approved roster is already bound to a different NAS root")
	}
	return nil
}

func insertDeliveryNASHandoff(ctx context.Context, tx pgx.Tx, pins [][]any) error {
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE delivery_nas_handoff_import(
  identity jsonb, nas_relative_path text, roster_sha256 text, picks_sha256 text) ON COMMIT DROP`); err != nil {
		return err
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"delivery_nas_handoff_import"},
		[]string{"identity", "nas_relative_path", "roster_sha256", "picks_sha256"}, pgx.CopyFromRows(pins)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
 INSERT INTO delivery_nas_handoff_outputs(output_id,recording_id,roster_sha256,picks_sha256,bucket,
  object_key,etag,size_bytes,sha256,nas_relative_path)
 SELECT p.output_id,p.recording_id,i.roster_sha256,i.picks_sha256,p.bucket,p.object_key,p.etag,p.size_bytes,p.sha256,i.nas_relative_path
 FROM delivery_nas_handoff_import i CROSS JOIN LATERAL
  jsonb_to_record(i.identity) p(output_id bigint,recording_id bigint,bucket text,object_key text,etag text,size_bytes bigint,sha256 text)
 ON CONFLICT(output_id) DO NOTHING`); err != nil {
		return err
	}
	var complete bool
	err := tx.QueryRow(ctx, `
 SELECT NOT EXISTS(
  SELECT 1 FROM delivery_nas_handoff_import i CROSS JOIN LATERAL
   jsonb_to_record(i.identity) p(output_id bigint,recording_id bigint,bucket text,object_key text,etag text,size_bytes bigint,sha256 text)
  LEFT JOIN delivery_nas_handoff_outputs d ON d.output_id=p.output_id
   AND ROW(d.recording_id,d.bucket,d.object_key,d.etag,d.size_bytes,d.sha256,d.nas_relative_path,d.roster_sha256,d.picks_sha256)
       = ROW(p.recording_id,p.bucket,p.object_key,p.etag,p.size_bytes,p.sha256,i.nas_relative_path,i.roster_sha256,i.picks_sha256)
  WHERE d.output_id IS NULL
 )`).Scan(&complete)
	if err != nil {
		return err
	}
	if !complete {
		return errors.New("an existing approval has a different roster or output identity")
	}
	return nil
}

const deliveryNASCompleteRoundSQL = `
WITH prior AS (
 SELECT coalesce(max(round),0) AS round FROM delivery_nas_handoff_rounds WHERE roster_sha256=$1
), complete AS (
 SELECT count(*)=$3 AND coalesce(sum(p.size_bytes),0)=$4 AS ready
 FROM delivery_nas_handoff_outputs p
 JOIN delivery_nas_handoff_copyable live ON live.output_id=p.output_id
 JOIN delivery_nas_handoff_readbacks r ON r.output_id=p.output_id AND r.round=live.round
  AND r.connection_id=13 AND r.size_bytes=p.size_bytes AND r.device=live.device AND r.root_inode=live.inode
 JOIN recording_collation_output_acks a ON a.output_id=p.output_id AND a.connection_id=13
  AND a.nas_relative_path=p.nas_relative_path AND a.sha256=p.sha256 AND a.size_bytes=p.size_bytes
 WHERE p.roster_sha256=$1 AND p.picks_sha256=$2
)`

func startDeliveryNASFinalRecheck(ctx context.Context, tx pgx.Tx, summary deliveryNASManifestSummary) (int64, error) {
	var round int64
	err := tx.QueryRow(ctx, deliveryNASCompleteRoundSQL+`
 INSERT INTO delivery_nas_handoff_rounds(roster_sha256,round)
 SELECT $1,prior.round+1 FROM prior,complete WHERE complete.ready
  AND EXISTS(SELECT 1 FROM connections WHERE id=13 AND delivery_nas_handoff_enabled
    AND delivery_nas_handoff_roster_sha256=$1)
 RETURNING round`, summary.RosterSHA, summary.PicksSHA, summary.Rows, summary.Bytes).Scan(&round)
	if errors.Is(err, pgx.ErrNoRows) {
		err = errors.New("the active roster must finish its prior NAS readback round before final recheck")
	}
	return round, err
}

func requestDeliveryNASFinalCheck(ctx context.Context, tx pgx.Tx, summary deliveryNASManifestSummary) (int64, error) {
	if _, err := tx.Exec(ctx, `SELECT id FROM connections WHERE id=13 FOR SHARE`); err != nil {
		return 0, err
	}
	var round int64
	err := tx.QueryRow(ctx, deliveryNASCompleteRoundSQL+`,
 ready AS (
  SELECT prior.round,jsonb_agg(jsonb_build_array(p.output_id,p.nas_relative_path,
    jsonb_build_array(r.size_bytes,r.mtime_ns,r.ctime_ns,r.inode,r.device)) ORDER BY p.output_id) manifest
  FROM prior,complete,delivery_nas_handoff_outputs p
  JOIN delivery_nas_handoff_readbacks r ON r.output_id=p.output_id AND r.connection_id=13
  WHERE p.roster_sha256=$1 AND p.picks_sha256=$2 AND prior.round>0 AND r.round=prior.round AND complete.ready
   AND EXISTS(SELECT 1 FROM connections c WHERE c.id=13 AND c.delivery_nas_handoff_enabled
    AND c.delivery_nas_handoff_roster_sha256=$1)
  GROUP BY prior.round
 ), requested AS (
  INSERT INTO delivery_nas_handoff_checks(roster_sha256,round,manifest,manifest_sha256)
  SELECT $1,round,manifest,encode(sha256(convert_to(manifest::text,'UTF8')),'hex') FROM ready
  ON CONFLICT(roster_sha256,round) WHERE checked_at IS NULL AND cancelled_at IS NULL DO NOTHING RETURNING round
 )
 SELECT round FROM requested
 UNION ALL SELECT ready.round FROM ready JOIN delivery_nas_handoff_checks existing
  ON existing.roster_sha256=$1 AND existing.round=ready.round AND existing.manifest=ready.manifest
   AND existing.checked_at IS NULL AND existing.cancelled_at IS NULL
 `, summary.RosterSHA, summary.PicksSHA, summary.Rows, summary.Bytes).Scan(&round)
	if errors.Is(err, pgx.ErrNoRows) {
		err = errors.New("the complete final NAS readback round is required before requesting the path check")
	}
	return round, err
}

func pauseDeliveryNASHandoff(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "UPDATE connections SET delivery_nas_handoff_enabled=false WHERE id=13"); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE delivery_nas_handoff_checks SET cancelled_at=clock_timestamp()
 WHERE checked_at IS NULL AND cancelled_at IS NULL AND roster_sha256=(
 SELECT delivery_nas_handoff_roster_sha256 FROM connections WHERE id=13)`)
	return err
}

func runDeliveryNASHandoff(ctx context.Context, cfg config.Config, args []string) {
	if len(args) == 1 && args[0] == "pause-copy" {
		pool, err := r2RetireOpenPool(ctx, cfg, false)
		if err != nil {
			log.Fatal(err)
		}
		defer pool.Close()
		tx, err := pool.Begin(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if err := pauseDeliveryNASHandoff(ctx, tx); err != nil {
			log.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			log.Fatal(err)
		}
		fmt.Println("NAS delivery handoff copy paused")
		return
	}
	fs := flag.NewFlagSet("delivery-nas-handoff", flag.ExitOnError)
	roster := fs.String("roster", "", "reviewed in-window delivery JSONL")
	expected := fs.String("expected-roster-sha256", "", "SHA-256 of the reviewed roster")
	picksPath := fs.String("picks", "", "the owner picks used to build the reviewed roster")
	picksSHA := fs.String("expected-picks-sha256", "", "SHA-256 of the reviewed owner picks")
	apply := fs.Bool("apply", false, "queue the reviewed copies after live roster verification")
	rootDevice := fs.Int64("approved-root-device", -1, "device of the controller-verified NAS root")
	rootInode := fs.Int64("approved-root-inode", 0, "inode of the controller-verified NAS root")
	export := fs.String("export-file-list", "", "write the verified public file list to a new JSON file")
	recheck := fs.Bool("start-final-recheck", false, "start a fresh readback of every approved NAS path")
	check := fs.Bool("request-final-check", false, "check approved NAS paths after the final readback")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	actions := 0
	for _, selected := range []bool{*apply, *recheck, *check, *export != ""} {
		if selected {
			actions++
		}
	}
	if fs.NArg() != 0 || *roster == "" || *picksPath == "" || actions > 1 || (*apply && (*rootDevice < 0 || *rootInode <= 0)) {
		log.Fatal("delivery-nas-handoff --roster FILE --expected-roster-sha256 SHA --picks FILE --expected-picks-sha256 SHA [--apply --approved-root-device N --approved-root-inode N | --start-final-recheck | --request-final-check | --export-file-list FILE]")
	}
	approved, err := deliveryNASReadRoster(*roster, *expected)
	if err != nil {
		log.Fatal(err)
	}
	picks, err := loadR2RetirePicks(*picksPath)
	if err != nil {
		log.Fatal(err)
	}
	if picks.SHA256 != *picksSHA {
		log.Fatal("owner picks differ from the expected SHA-256")
	}
	windows, err := deliveryNASWindows(picks)
	if err != nil {
		log.Fatal(err)
	}
	pool, err := r2RetireOpenPool(ctx, cfg, !*apply && !*recheck && !*check)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	mode := pgx.ReadOnly
	if *apply || *recheck || *check {
		mode = pgx.ReadWrite
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: mode, IsoLevel: pgx.RepeatableRead})
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var mount *[2]int64
	if *apply {
		mount = &[2]int64{*rootDevice, *rootInode}
	}
	summary, err := queueDeliveryNASHandoff(ctx, tx, approved, windows,
		deliveryNASManifestSummary{RosterSHA: *expected, PicksSHA: *picksSHA}, mount)
	if err != nil {
		log.Fatal(err)
	}
	if *recheck {
		summary.VerificationRound, err = startDeliveryNASFinalRecheck(ctx, tx, summary)
		if err != nil {
			log.Fatal(err)
		}
	}
	if *check {
		summary.VerificationRound, err = requestDeliveryNASFinalCheck(ctx, tx, summary)
		if err != nil {
			log.Fatal(err)
		}
	}
	var list deliveryNASFileList
	if *export != "" {
		store := mustArchiveR2Client(ctx, cfg, pool)
		keep, err := api.ReadPublicDeliveryKeepWithLinks(ctx, store)
		if err != nil {
			log.Fatal(err)
		}
		list, err = deliveryNASCollectFileList(ctx, tx, summary, keep)
		if err != nil {
			log.Fatal(err)
		}
		if err := r2RetireDeliveryRegistryUnchanged(ctx, store, keep); err != nil {
			log.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	if *export != "" {
		if err := writeDeliveryNASFileList(*export, list); err != nil {
			log.Fatal(err)
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(summary); err != nil {
		log.Fatal(err)
	}
}
