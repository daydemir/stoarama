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
	cfg := Config{OperatorAccountID: accountID, Capacity: 5, Min: 8, Max: 9, RolloutMaxSurge: 1, Region: "nyc1", Size: "s-2vcpu-4gb", ProjectID: "project-v2", BuildSHA: newBuild, FirewallID: "fw-v2", FirewallRulesSHA256: rules, FirewallTag: recorderFirewallTag, HeartbeatSec: 15, PollSec: 5, BackendAPIURL: "https://api.stoarama.example"}
	hostSHA := ExpectedHostFirewallScriptSHA256()
	kernelSHA := ExpectedKernelFirewallPolicySHA256()
	cfg.HostFirewallSHA256 = hostSHA
	cfg.KernelPolicySHA256 = kernelSHA
	common, err := (CommonPolicyIdentity{BuildSHA: newBuild, Capacity: 5, HeartbeatSec: 15, PollSec: 5, BackendAPIURL: "https://api.stoarama.example", FirewallID: cfg.FirewallID, FirewallRulesSHA256: rules, FirewallTag: recorderFirewallTag, HostFirewallScriptSHA256: hostSHA, KernelFirewallPolicySHA256: kernelSHA}).Digest()
	if err != nil {
		t.Fatal(err)
	}
	facts := RolloutProviderFacts{AccountDropletLimit: 25, AccountDropletCount: 10, PoolDropletIDs: providerIDs, NonPoolDropletIDs: []int64{9001, 9002}, FirewallID: cfg.FirewallID, FirewallStatus: "succeeded", FirewallTags: []string{recorderFirewallTag}, FirewallRulesSHA256: rules}
	facts.NonPoolDroplets = []DODroplet{{ID: 9001, Name: "other-one", Region: "nyc1", Size: "s-1vcpu-1gb", Status: "active"}, {ID: 9002, Name: "other-two", Region: "nyc1", Size: "s-1vcpu-1gb", Status: "active"}}
	for i, id := range providerIDs {
		facts.PoolDroplets = append(facts.PoolDroplets, DODroplet{ID: id, Name: fmt.Sprintf("stoarama-rec-old-%d", i+1), Region: cfg.Region, Size: cfg.Size, Status: "active", ProjectMember: true, Tags: []string{recorderFirewallTag}})
	}
	rollout, err := NewStore(pool).EnsureRollout(ctx, cfg, common, facts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET lease_admission_enabled=false WHERE id=(SELECT min(id) FROM recorder_droplets WHERE state='active')`); err == nil {
		t.Fatal("direct SQL shrank claimable rollout capacity")
	}
	name, err := NewStore(pool).NextReplacementIdentity(ctx, rollout)
	if err != nil {
		t.Fatalf("identity name=%q err=%v", name, err)
	}
	replacement, err := NewStore(pool).ReserveRolloutReplacement(ctx, rollout, cfg, name)
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
	policy := strings.Join(sortedExpectedKernelFirewallRules(), "\n") + "\n"
	observed, err := ValidateWorkerRolloutFacts(WorkerRolloutFacts{ExecutableBuildSHA: newBuild, Capacity: 5, HeartbeatSec: 15, PollSec: 5, BackendAPIURL: "https://api.stoarama.example", HostFirewallScript: hostFirewallScriptV1, KernelPolicy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recorder_worker_rollout_observations(droplet_id,node_id,worker_id,executable_build_sha,capacity,heartbeat_sec,poll_sec,backend_api_origin,host_firewall_script_sha256,kernel_firewall_policy_sha256,observation_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, replacement.ID, *replacement.NodeID, replacement.Name, observed.ExecutableBuildSHA, observed.Capacity, observed.HeartbeatSec, observed.PollSec, observed.BackendAPIURL, hostSHA, kernelSHA, observed.ObservationSHA256); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recorder_worker_rollout_observations(droplet_id,node_id,worker_id,executable_build_sha,capacity,heartbeat_sec,poll_sec,backend_api_origin,host_firewall_script_sha256,kernel_firewall_policy_sha256,observation_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, replacement.ID, *replacement.NodeID, replacement.Name, observed.ExecutableBuildSHA, observed.Capacity, observed.HeartbeatSec, observed.PollSec, observed.BackendAPIURL, hostSHA, kernelSHA, observed.ObservationSHA256); err != nil {
		t.Fatalf("identical current worker observation was incorrectly deduplicated: %v", err)
	}
	var observationCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recorder_worker_rollout_observations WHERE droplet_id=$1 AND observation_sha256=$2`, replacement.ID, observed.ObservationSHA256).Scan(&observationCount); err != nil || observationCount != 2 {
		t.Fatalf("worker observation append count=%d err=%v", observationCount, err)
	}
	withReplacement := facts
	withReplacement.AccountDropletCount = 11
	withReplacement.PoolDropletIDs = append(append([]int64(nil), facts.PoolDropletIDs...), 109)
	withReplacement.PoolDroplets = append(append([]DODroplet(nil), facts.PoolDroplets...), DODroplet{ID: 109, Name: replacement.Name, Region: cfg.Region, Size: cfg.Size, Status: "active", ProjectMember: true, Tags: []string{recorderFirewallTag}})
	providerObservationID, err := NewStore(pool).AppendProviderObservation(ctx, rollout.Generation, "reconcile", withReplacement)
	if err != nil {
		t.Fatal(err)
	}
	forgedFingerprint := typedDigest("stoarama-recorder-provider-object-v1", "999", "stoarama-rec-forged", cfg.Region, cfg.Size, "active", "true", recorderFirewallTag)
	if _, err = pool.Exec(ctx, `INSERT INTO recorder_rollout_provider_objects(observation_id,provider_droplet_id,is_pool,name,region,size_slug,status,project_member,pool_tag_present,tags_csv,object_fingerprint_sha256) VALUES($1,999,true,'stoarama-rec-forged',$2,$3,'active',true,true,$4,$5)`, providerObservationID, cfg.Region, cfg.Size, recorderFirewallTag, forgedFingerprint); err == nil {
		t.Fatal("direct SQL appended an object outside the signed provider inventory")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recorder_rollout_provider_observations(rollout_generation,phase,account_droplet_limit,account_droplet_count,pool_droplet_count,frozen_nonpool_set_sha256,provider_inventory_sha256,firewall_id,firewall_rules_sha256,firewall_tag,firewall_status) VALUES($1,'reconcile',25,10,8,$2,$3,$4,$5,$6,'succeeded')`, rollout.Generation, nonPoolSetDigest(facts.NonPoolDropletIDs), strings.Repeat("d", 64), cfg.FirewallID, rules, recorderFirewallTag); err == nil {
		t.Fatal("direct SQL committed a provider observation without its exact object set")
	}
	ready, err := NewStore(pool).MarkRolloutStandbyReady(ctx, replacement.ID)
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
	preSwitchObservation, err := NewStore(pool).AppendProviderObservation(ctx, rollout.Generation, "pre_switch", withReplacement)
	if err != nil {
		t.Fatal(err)
	}
	if switched, err := NewStore(pool).SwitchRolloutReplacement(ctx, replacement.ID, preSwitchObservation, 40); err != nil || switched {
		t.Fatalf("busy worker switched=%v err=%v", switched, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_jobs SET status='canceled',lease_owner=NULL,lease_expires_at=NULL WHERE id=$1`, liveJobID); err != nil {
		t.Fatal(err)
	}
	var before int
	if err = pool.QueryRow(ctx, `SELECT COALESCE(sum(capacity),0) FROM recorder_droplets WHERE state='active' AND lease_admission_enabled`).Scan(&before); err != nil || before != 40 {
		t.Fatalf("before capacity=%d err=%v", before, err)
	}
	preSwitchObservation, err = NewStore(pool).AppendProviderObservation(ctx, rollout.Generation, "pre_switch", withReplacement)
	if err != nil {
		t.Fatal(err)
	}
	switched, err := NewStore(pool).SwitchRolloutReplacement(ctx, replacement.ID, preSwitchObservation, 40)
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
	withoutOld := withReplacement
	withoutOld.AccountDropletCount = 10
	withoutOld.PoolDropletIDs = nil
	withoutOld.PoolDroplets = nil
	for i, id := range withReplacement.PoolDropletIDs {
		if id != providerIDs[0] {
			withoutOld.PoolDropletIDs = append(withoutOld.PoolDropletIDs, id)
			withoutOld.PoolDroplets = append(withoutOld.PoolDroplets, withReplacement.PoolDroplets[i])
		}
	}
	if _, err = NewStore(pool).AppendProviderObservation(ctx, rollout.Generation, "reconcile", withoutOld); err != nil {
		t.Fatal(err)
	}
	if finalized, err := NewStore(pool).FinalizeAbsentRolloutWorker(ctx, *replacement.ReplacementForID, 0); err != nil || finalized {
		t.Fatalf("one absence observation finalized worker: finalized=%v err=%v", finalized, err)
	}
	if _, err = NewStore(pool).AppendProviderObservation(ctx, rollout.Generation, "reconcile", withoutOld); err != nil {
		t.Fatal(err)
	}
	if finalized, err := NewStore(pool).FinalizeAbsentRolloutWorker(ctx, *replacement.ReplacementForID, 0); err != nil || !finalized {
		t.Fatalf("two absence observations did not finalize worker: finalized=%v err=%v", finalized, err)
	}
	if _, err = pool.Exec(ctx, `TRUNCATE recorder_pool_rollouts`); err == nil {
		t.Fatal("rollout audit accepted TRUNCATE")
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

func TestRecorderRolloutV2DrainClaimFenceAndAppendOnlyFactsPostgres(t *testing.T) {
	pool, cleanup := testDropletPoolDB(t)
	defer cleanup()
	ctx := context.Background()
	claim, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Rollback(ctx)
	if _, err = claim.Exec(ctx, cloudRecorderPoolSharedFenceSQLForTest); err != nil {
		t.Fatal(err)
	}
	drain, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer drain.Release()
	if _, err = drain.Exec(ctx, `SET lock_timeout='150ms'`); err != nil {
		t.Fatal(err)
	}
	if _, err = drain.Exec(ctx, rolloutFenceSQL); err == nil {
		t.Fatal("exclusive drain fence crossed live claim")
	}
	if err = claim.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `TRUNCATE recorder_worker_rollout_observations`); err == nil {
		t.Fatal("worker observations accepted truncate")
	}
	if _, err = pool.Exec(ctx, `TRUNCATE recorder_rollout_provider_observations CASCADE`); err == nil {
		t.Fatal("provider observations accepted truncate")
	}
}

const cloudRecorderPoolSharedFenceSQLForTest = `SELECT pg_advisory_xact_lock_shared(hashtextextended('stoarama-recorder-pool-rollout-v1',0))`
