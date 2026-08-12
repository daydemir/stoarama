package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	RecordingID         int64
	SceneIdentitySHA256 string
	Role, Status        string
	Rank                int
	ReasonCodes         []string
}

func validateCampaignSeedPolicy(m campaignSeedManifest) error {
	if len(m.Tracks) != 2 {
		return errors.New("exactly two tracks required")
	}
	want := map[string]struct {
		deadline time.Time
		count    int
	}{"delivery30": {time.Date(2026, 8, 19, 23, 59, 59, 0, time.UTC), 30}, "repair17": {time.Date(2026, 8, 26, 23, 59, 59, 0, time.UTC), 17}}
	seen := map[string]bool{}
	for _, t := range m.Tracks {
		p, ok := want[t.Key]
		if !ok || seen[t.Key] || !t.DeadlineAt.Equal(p.deadline) || t.TargetCount != p.count || len(t.Entries) != p.count || t.GradeFloor != "GOOD" || t.RequiredConsecutiveWindows != 0 {
			return errors.New("track policy mismatch")
		}
		seen[t.Key] = true
	}
	return nil
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
	if err = validateCampaignSeedPolicy(m); err != nil {
		log.Fatal(err)
	}
	claimed := m.EvidenceSHA256
	m.EvidenceSHA256 = ""
	canonical, err := json.Marshal(m)
	if err != nil {
		log.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	m.EvidenceSHA256 = claimed
	if hex.EncodeToString(digest[:]) != claimed {
		log.Fatal("manifest evidence_sha256 does not match canonical payload")
	}
	wantKeys := map[string]struct {
		Deadline time.Time
		Count    int
	}{"delivery30": {time.Date(2026, 8, 19, 23, 59, 59, 0, time.UTC), 30}, "repair17": {time.Date(2026, 8, 26, 23, 59, 59, 0, time.UTC), 17}}
	seenKeys := map[string]bool{}
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
		policy, known := wantKeys[t.Key]
		if !known || seenKeys[t.Key] || !t.DeadlineAt.Equal(policy.Deadline) || t.TargetCount != policy.Count || t.RequiredConsecutiveWindows != 0 || t.Label == "" || t.GradeFloor != "GOOD" || len(t.Entries) != t.TargetCount {
			log.Fatalf("invalid track %s", t.Key)
		}
		seenKeys[t.Key] = true
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
			var attestationCount int
			if err = tx.QueryRow(ctx, `SELECT r.stream_id,count(ev.id)::int FROM recordings r JOIN streams s ON s.id=r.stream_id LEFT JOIN recording_scene_frame_evidence ev ON ev.account_id=r.account_id AND ev.stream_id=r.stream_id AND ev.scene_identity_sha256=$3 AND ev.captured_at>=now()-interval '24 hours' WHERE r.account_id=$1 AND r.id=$2 AND r.status='active' GROUP BY r.stream_id`, m.AccountID, e.RecordingID, e.SceneIdentitySHA256).Scan(&sid, &attestationCount); err != nil {
				log.Fatalf("recording %d: %v", e.RecordingID, err)
			}
			if attestationCount != 1 {
				log.Fatalf("recording %d lacks exactly one current owned scene attestation", e.RecordingID)
			}
			scene := e.SceneIdentitySHA256
			if len(scene) != 64 {
				log.Fatalf("recording %d missing reviewed physical scene hash", e.RecordingID)
			}
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
			err = tx.QueryRow(ctx, `SELECT id,state FROM recording_campaign_tracks WHERE account_id=$1 AND campaign_key=$2 AND label=$3 AND deadline_at=$4 AND target_count=$5 AND grade_floor=$6 AND required_consecutive_windows=$7`, m.AccountID, t.Key, t.Label, t.DeadlineAt, t.TargetCount, t.GradeFloor, t.RequiredConsecutiveWindows).Scan(&trackID, &state)
			if err != nil || state != "active" {
				log.Fatalf("track %s exists with different definition/state", t.Key)
			}
			var total int
			if err = tx.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_roster_entries WHERE track_id=$1`, trackID).Scan(&total); err != nil || total != t.TargetCount {
				log.Fatalf("active track %s has extra or missing rows", t.Key)
			}
			for _, e := range rr {
				var matched int
				if err = tx.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_roster_entries WHERE track_id=$1 AND recording_id=$2 AND stream_id=$3 AND scene_identity_sha256=$4 AND role=$5 AND rank=$6 AND status=$7 AND reason_codes=$8 AND evidence_observed_at=$9 AND evidence_sha256=$10 AND updated_by_user_id=$11`, trackID, e.RecordingID, e.StreamID, e.Scene, e.Role, e.Rank, e.Status, e.ReasonCodes, m.EvidenceObservedAt, m.EvidenceSHA256, m.ActorUserID).Scan(&matched); err != nil || matched != 1 {
					log.Fatalf("active track %s differs", t.Key)
				}
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
		for _, e := range rr {
			var matched int
			if err = tx.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_roster_entries WHERE track_id=$1 AND recording_id=$2 AND stream_id=$3 AND scene_identity_sha256=$4 AND role=$5 AND rank=$6 AND status=$7 AND reason_codes=$8 AND evidence_observed_at=$9 AND evidence_sha256=$10 AND updated_by_user_id=$11`, trackID, e.RecordingID, e.StreamID, e.Scene, e.Role, e.Rank, e.Status, e.ReasonCodes, m.EvidenceObservedAt, m.EvidenceSHA256, m.ActorUserID).Scan(&matched); err != nil || matched != 1 {
				log.Fatalf("track %s recording %d differs from manifest", t.Key, e.RecordingID)
			}
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
