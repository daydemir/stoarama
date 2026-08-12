package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"os"
	"sort"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5"
)

type campaignSeedManifest struct {
	AccountID, ActorUserID int64
	EvidenceObservedAt     time.Time
	EvidenceSHA256         string
	Tracks                 []campaignSeedTrack
}
type campaignSeedTrack struct {
	Key, Label, GradeFloor                  string
	DeadlineAt                              time.Time
	TargetCount, RequiredConsecutiveWindows int
	Entries                                 []campaignSeedEntry
}
type campaignSeedEntry struct {
	RecordingID  int64
	Role, Status string
	Rank         int
	ReasonCodes  []string
}

func runCampaignTrackSeed(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recordings campaign-tracks seed", flag.ExitOnError)
	file := fs.String("manifest", "", "exact reviewed manifest JSON")
	_ = fs.Parse(args)
	if *file == "" {
		log.Fatal("--manifest is required")
	}
	raw, err := os.ReadFile(*file)
	if err != nil {
		log.Fatal(err)
	}
	var m campaignSeedManifest
	if err = json.Unmarshal(raw, &m); err != nil {
		log.Fatal(err)
	}
	if m.AccountID <= 0 || m.ActorUserID <= 0 || m.EvidenceObservedAt.IsZero() || len(m.EvidenceSHA256) != 64 || len(m.Tracks) != 2 {
		log.Fatal("invalid manifest identity")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var authorized bool
	if err = tx.QueryRow(ctx, `SELECT u.is_operator OR EXISTS(SELECT 1 FROM memberships x WHERE x.user_id=u.id AND x.org_id=$1 AND x.accepted_at IS NOT NULL AND x.role IN('owner','admin')) FROM users u WHERE u.id=$2`, m.AccountID, m.ActorUserID).Scan(&authorized); err != nil || !authorized {
		log.Fatal("actor is not authorized for account")
	}
	for _, t := range m.Tracks {
		if t.Key == "" || t.Label == "" || t.GradeFloor != "GOOD" || t.TargetCount <= 0 || len(t.Entries) != t.TargetCount {
			log.Fatalf("invalid track %s", t.Key)
		}
		ids := map[int64]bool{}
		ranks := map[int]bool{}
		scenes := map[string]bool{}
		type resolved struct {
			campaignSeedEntry
			StreamID int64
			Scene    string
		}
		rr := []resolved{}
		for _, e := range t.Entries {
			if e.RecordingID <= 0 || e.Rank <= 0 || ids[e.RecordingID] || ranks[e.Rank] || len(e.ReasonCodes) == 0 || e.Role != "primary" || (e.Status != "protect" && e.Status != "probation") {
				log.Fatalf("invalid entry track=%s recording=%d", t.Key, e.RecordingID)
			}
			ids[e.RecordingID] = true
			ranks[e.Rank] = true
			var sid int64
			var provider, external string
			if err = tx.QueryRow(ctx, `SELECT r.stream_id,s.provider,s.external_id FROM recordings r JOIN streams s ON s.id=r.stream_id WHERE r.account_id=$1 AND r.id=$2`, m.AccountID, e.RecordingID).Scan(&sid, &provider, &external); err != nil {
				log.Fatalf("recording %d: %v", e.RecordingID, err)
			}
			sum := sha256.Sum256([]byte(provider + "\x00" + external))
			scene := hex.EncodeToString(sum[:])
			if scenes[scene] {
				log.Fatalf("duplicate scene %s", scene)
			}
			scenes[scene] = true
			sort.Strings(e.ReasonCodes)
			rr = append(rr, resolved{e, sid, scene})
		}
		var trackID int64
		err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_tracks(account_id,campaign_key,label,deadline_at,target_count,grade_floor,required_consecutive_windows,state,created_by_user_id) VALUES($1,$2,$3,$4,$5,$6,$7,'draft',$8) ON CONFLICT(account_id,campaign_key) DO UPDATE SET updated_at=recording_campaign_tracks.updated_at WHERE recording_campaign_tracks.state='draft' AND recording_campaign_tracks.label=EXCLUDED.label AND recording_campaign_tracks.deadline_at=EXCLUDED.deadline_at AND recording_campaign_tracks.target_count=EXCLUDED.target_count AND recording_campaign_tracks.grade_floor=EXCLUDED.grade_floor AND recording_campaign_tracks.required_consecutive_windows=EXCLUDED.required_consecutive_windows RETURNING id`, m.AccountID, t.Key, t.Label, t.DeadlineAt, t.TargetCount, t.GradeFloor, t.RequiredConsecutiveWindows, m.ActorUserID).Scan(&trackID)
		if err == pgx.ErrNoRows {
			var state string
			var matched int
			err = tx.QueryRow(ctx, `SELECT id,state FROM recording_campaign_tracks WHERE account_id=$1 AND campaign_key=$2 AND label=$3 AND deadline_at=$4 AND target_count=$5 AND grade_floor=$6 AND required_consecutive_windows=$7`, m.AccountID, t.Key, t.Label, t.DeadlineAt, t.TargetCount, t.GradeFloor, t.RequiredConsecutiveWindows).Scan(&trackID, &state)
			if err != nil || state != "active" {
				log.Fatalf("track %s exists with different definition/state", t.Key)
			}
			if err = tx.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_roster_entries WHERE track_id=$1 AND evidence_sha256=$2`, trackID, m.EvidenceSHA256).Scan(&matched); err != nil || matched != t.TargetCount {
				log.Fatalf("active track %s does not match manifest", t.Key)
			}
			continue
		}
		if err != nil {
			log.Fatalf("track %s: %v", t.Key, err)
		}
		var existing int
		_ = tx.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_roster_entries WHERE track_id=$1`, trackID).Scan(&existing)
		if existing == 0 {
			for _, e := range rr {
				_, err = tx.Exec(ctx, `INSERT INTO recording_campaign_roster_entries(track_id,recording_id,stream_id,scene_identity_sha256,role,rank,status,reason_codes,effective_at,decision_at,evidence_observed_at,evidence_sha256,updated_by_user_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$9,$9,$10,$11)`, trackID, e.RecordingID, e.StreamID, e.Scene, e.Role, e.Rank, e.Status, e.ReasonCodes, m.EvidenceObservedAt, m.EvidenceSHA256, m.ActorUserID)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
		if existing != 0 && existing != t.TargetCount {
			log.Fatal("existing draft roster differs")
		}
		if _, err = tx.Exec(ctx, `SELECT transition_recording_campaign_track($1,'active',ARRAY['reviewed_exact_manifest',$2],$3,$4)`, trackID, m.EvidenceSHA256, m.ActorUserID, m.EvidenceObservedAt); err != nil {
			log.Fatal(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	printJSON(map[string]any{"status": "activated", "manifest_sha256": m.EvidenceSHA256})
}
