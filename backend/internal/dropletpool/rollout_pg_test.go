package dropletpool

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRecorderRolloutV2AtomicSwitchAndDirectSQLGuardsPostgres(t *testing.T) {
	pool, cleanup := testDropletPoolDB(t)
	defer cleanup()
	ctx := context.Background()
	var accountID int64
	if err := pool.QueryRow(ctx, `INSERT INTO accounts(email,name,role,status) VALUES($1,'Rollout operator','admin','active') RETURNING id`, fmt.Sprintf("rollout-%d@example.com", time.Now().UnixNano())).Scan(&accountID); err != nil {
		t.Fatal(err)
	}
	destinationID := insertForecastDestination(t, pool, accountID)
	insertForecastRecording(t, pool, accountID, destinationID, "cloud")
	var recordingID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM recordings WHERE account_id=$1 ORDER BY id DESC LIMIT 1`, accountID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	oldBuild := strings.Repeat("a", 40)
	newBuild := strings.Repeat("b", 40)
	rules := strings.Repeat("c", 64)
	providerIDs := make([]int64, 0, 8)
	for i := 1; i <= 8; i++ {
		name := fmt.Sprintf("stoarama-rec-old-%d", i)
		_, nodeID, _, err := NewStore(pool).MintNodeToken(ctx, accountID, name)
		if err != nil {
			t.Fatal(err)
		}
		id, err := NewStore(pool).InsertProvisioning(ctx, name, "nyc1", "s-2vcpu-4gb", 5, nodeID, oldBuild)
		if err != nil {
			t.Fatal(err)
		}
		doID := int64(100 + i)
		providerIDs = append(providerIDs, doID)
		if err = NewStore(pool).SetDropletID(ctx, id, doID, fmt.Sprintf("203.0.113.%d", i)); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET observed_build_sha=$2,desired_build_sha=$2,last_seen_at=now() WHERE id=$1`, id, oldBuild); err != nil {
			t.Fatal(err)
		}
		if ok, err := NewStore(pool).MarkActive(ctx, id); err != nil || !ok {
			t.Fatalf("mark old active ok=%v err=%v", ok, err)
		}
	}
	cfg := Config{OperatorAccountID: accountID, Capacity: 5, Min: 8, Max: 9, RolloutMaxSurge: 1, Region: "nyc1", Size: "s-2vcpu-4gb", BuildSHA: newBuild, FirewallID: "fw-v2", FirewallRulesSHA256: rules, FirewallTag: recorderFirewallTag}
	common, err := (CommonPolicyIdentity{BuildSHA: newBuild, Capacity: 5, HeartbeatSec: 15, PollSec: 5, BackendAPIURL: "https://api.stoarama.example", FirewallID: cfg.FirewallID, FirewallRulesSHA256: rules, FirewallTag: recorderFirewallTag}).Digest()
	if err != nil {
		t.Fatal(err)
	}
	facts := RolloutProviderFacts{AccountDropletLimit: 25, AccountDropletCount: 10, PoolDropletIDs: providerIDs, FirewallID: cfg.FirewallID, FirewallStatus: "succeeded", FirewallTags: []string{recorderFirewallTag}, FirewallRulesSHA256: rules}
	rollout, err := NewStore(pool).EnsureRollout(ctx, cfg, common, facts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET lease_admission_enabled=false WHERE id=(SELECT min(id) FROM recorder_droplets WHERE state='active')`); err == nil {
		t.Fatal("direct SQL shrank claimable rollout capacity")
	}
	name, runtimeSHA, allowlist, err := NewStore(pool).NextReplacementIdentity(ctx, rollout, "")
	if err != nil || allowlist != "" {
		t.Fatalf("identity name=%q allowlist=%q err=%v", name, allowlist, err)
	}
	replacement, err := NewStore(pool).ReserveRolloutReplacement(ctx, rollout, cfg, name, runtimeSHA)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.LeaseAdmissionEnabled || replacement.State != "provisioning" || replacement.PlainToken == "" {
		t.Fatalf("unsafe reservation: %+v", replacement)
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET lease_admission_enabled=true WHERE id=$1`, replacement.ID); err == nil {
		t.Fatal("direct SQL admitted provisioning worker")
	}
	if err = NewStore(pool).SetDropletID(ctx, replacement.ID, 109, "203.0.113.9"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET build_sha=$2,observed_build_sha=$2,observed_common_policy_sha256=$3,observed_runtime_config_sha256=$4,last_seen_at=now() WHERE id=$1`, replacement.ID, newBuild, common, runtimeSHA); err != nil {
		t.Fatal(err)
	}
	ready, err := NewStore(pool).MarkRolloutStandbyReady(ctx, replacement.ID, facts.FirewallRulesSHA256)
	if err != nil || !ready {
		t.Fatalf("standby ready=%v err=%v", ready, err)
	}
	var oldName string
	if err = pool.QueryRow(ctx, `SELECT name FROM recorder_droplets WHERE id=$1`, *replacement.ReplacementForID).Scan(&oldName); err != nil {
		t.Fatal(err)
	}
	var liveJobID int64
	if err = pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,lease_owner,lease_expires_at,idempotency_key) VALUES($1,now(),now(),30,'leased',$2,now()+interval '1 hour',$3) RETURNING id`, recordingID, oldName, fmt.Sprintf("rollout-busy:%d", replacement.ID)).Scan(&liveJobID); err != nil {
		t.Fatal(err)
	}
	if switched, err := NewStore(pool).SwitchRolloutReplacement(ctx, replacement.ID, 40); err != nil || switched {
		t.Fatalf("busy worker switched=%v err=%v", switched, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_jobs SET status='canceled',lease_owner=NULL,lease_expires_at=NULL WHERE id=$1`, liveJobID); err != nil {
		t.Fatal(err)
	}
	var before int
	if err = pool.QueryRow(ctx, `SELECT COALESCE(sum(capacity),0) FROM recorder_droplets WHERE state='active' AND lease_admission_enabled`).Scan(&before); err != nil || before != 40 {
		t.Fatalf("before capacity=%d err=%v", before, err)
	}
	switched, err := NewStore(pool).SwitchRolloutReplacement(ctx, replacement.ID, 40)
	if err != nil || !switched {
		t.Fatalf("switch=%v err=%v", switched, err)
	}
	var after, active, draining int
	if err = pool.QueryRow(ctx, `SELECT COALESCE(sum(capacity) FILTER(WHERE state='active' AND lease_admission_enabled),0),count(*) FILTER(WHERE state='active' AND lease_admission_enabled),count(*) FILTER(WHERE state='rollout_draining' AND NOT lease_admission_enabled) FROM recorder_droplets`).Scan(&after, &active, &draining); err != nil {
		t.Fatal(err)
	}
	if after != 40 || active != 8 || draining != 1 {
		t.Fatalf("after switch capacity=%d active=%d draining=%d", after, active, draining)
	}
	if _, err = pool.Exec(ctx, `TRUNCATE recorder_pool_rollouts`); err == nil {
		t.Fatal("rollout audit accepted TRUNCATE")
	}
	if _, err = pool.Exec(ctx, `TRUNCATE recorder_cloud_config_plans`); err == nil {
		t.Fatal("config authority accepted TRUNCATE")
	}
	plannedWorker := "stoarama-rec-future-allowlist"
	plannedRuntime, err := (RuntimeConfigIdentity{CommonPolicySHA256: common, WorkerID: plannedWorker, RecordingIDs: []int64{recordingID}}).Digest()
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	planSHA := typedDigest("stoarama-cloud-recorder-config-plan-v1", plannedWorker, fmt.Sprint(recordingID), common, plannedRuntime, expires.Format("2006-01-02T15:04:05.000000Z"))
	if _, err = pool.Exec(ctx, `INSERT INTO recorder_cloud_config_plans(revision,worker_id,recording_id,common_policy_sha256,runtime_config_sha256,plan_sha256,expires_at) VALUES(1,$1,$2,$3,$4,$5,$6)`, plannedWorker, recordingID, common, plannedRuntime, planSHA, expires); err != nil {
		t.Fatalf("insert exact config plan: %v", err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recorder_cloud_config_plans(revision,worker_id,recording_id,common_policy_sha256,runtime_config_sha256,plan_sha256,expires_at) VALUES(2,$1,$2,$3,$4,$5,$6)`, plannedWorker+"-forged", recordingID, common, strings.Repeat("d", 64), strings.Repeat("e", 64), expires); err == nil {
		t.Fatal("accepted forged self-consistent config authority")
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_cloud_config_plans SET expires_at=expires_at+interval '1 hour' WHERE plan_sha256=$1`, planSHA); err == nil {
		t.Fatal("config authority accepted mutation")
	}
}

func TestRecorderRolloutV2SharedClaimFenceBlocksExclusiveSwitchPostgres(t *testing.T) {
	pool, cleanup := testDropletPoolDB(t)
	defer cleanup()
	ctx := context.Background()
	claim, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Rollback(ctx)
	if _, err = claim.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended('stoarama-recorder-pool-rollout-v1',0))`); err != nil {
		t.Fatal(err)
	}
	probe, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Release()
	if _, err = probe.Exec(ctx, `SET lock_timeout='150ms'`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = probe.Exec(ctx, rolloutFenceSQL)
	if err == nil || time.Since(started) < 100*time.Millisecond {
		t.Fatalf("exclusive switch fence did not wait for shared claim: elapsed=%s err=%v", time.Since(started), err)
	}
	if err = claim.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = probe.Exec(ctx, `SET lock_timeout='1s'`); err != nil {
		t.Fatal(err)
	}
	if _, err = probe.Exec(ctx, rolloutFenceSQL); err != nil {
		t.Fatalf("switch fence unavailable after claim commit: %v", err)
	}
}
