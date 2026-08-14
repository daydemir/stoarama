package dropletpool

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
	if _, err = pool.Exec(ctx, `DELETE FROM recorder_pool_rollouts WHERE generation=$1`, rollout.Generation); err == nil {
		t.Fatal("direct SQL deleted immutable rollout head")
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_pool_rollouts SET status='complete',completed_at=transaction_timestamp() WHERE generation=$1`, rollout.Generation); err == nil {
		t.Fatal("direct SQL completed an unsealed rollout")
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET lease_admission_enabled=false WHERE id=(SELECT min(id) FROM recorder_droplets WHERE state='active')`); err == nil {
		t.Fatal("direct SQL shrank claimable rollout capacity")
	}
	name, err := NewStore(pool).NextReplacementIdentity(ctx, rollout)
	if err != nil {
		t.Fatalf("identity name=%q err=%v", name, err)
	}
	type reserveResult struct {
		replacement ReservedReplacement
		err         error
	}
	startReserve := make(chan struct{})
	reserveResults := make(chan reserveResult, 2)
	for range 2 {
		go func() {
			<-startReserve
			r, reserveErr := NewStore(pool).ReserveRolloutReplacement(ctx, rollout, cfg, name)
			reserveResults <- reserveResult{replacement: r, err: reserveErr}
		}()
	}
	close(startReserve)
	var replacement ReservedReplacement
	var reserveSuccesses int
	for range 2 {
		result := <-reserveResults
		if result.err == nil {
			reserveSuccesses++
			replacement = result.replacement
		}
	}
	if reserveSuccesses != 1 {
		t.Fatalf("two controllers reserved %d replacement rows, want exactly one", reserveSuccesses)
	}
	if replacement.LeaseAdmissionEnabled || replacement.State != "provisioning" || replacement.PlainToken == "" {
		t.Fatalf("unsafe reservation: %+v", replacement)
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET name=name||'-forged' WHERE id=$1`, replacement.ID); err == nil {
		t.Fatal("direct SQL renamed a rollout worker")
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET node_id=(SELECT max(id) FROM nodes WHERE id<>recorder_droplets.node_id) WHERE id=$1`, replacement.ID); err == nil {
		t.Fatal("direct SQL rebound a rollout worker node")
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET do_droplet_id=99999 WHERE id=$1`, replacement.ID); err == nil {
		t.Fatal("direct SQL substituted rollout provider identity")
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET region='ams3' WHERE id=$1`, replacement.ID); err == nil {
		t.Fatal("direct SQL substituted rollout region")
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET lease_admission_enabled=true WHERE id=$1`, replacement.ID); err == nil {
		t.Fatal("direct SQL admitted provisioning worker")
	}
	if err = NewStore(pool).SetDropletID(ctx, replacement.ID, 109, "203.0.113.9"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET rollout_generation=NULL,rollout_ordinal=NULL WHERE id=$1`, replacement.ID); err == nil {
		t.Fatal("direct SQL removed rollout provenance")
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
	wrong, err := ValidateWorkerRolloutFacts(WorkerRolloutFacts{ExecutableBuildSHA: oldBuild, Capacity: 5, HeartbeatSec: 15, PollSec: 5, BackendAPIURL: "https://api.stoarama.example", HostFirewallScript: hostFirewallScriptV1, KernelPolicy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recorder_worker_rollout_observations(droplet_id,node_id,worker_id,executable_build_sha,capacity,heartbeat_sec,poll_sec,backend_api_origin,host_firewall_script_sha256,kernel_firewall_policy_sha256,observation_sha256,observed_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now()+interval '1 day')`, replacement.ID, *replacement.NodeID, replacement.Name, wrong.ExecutableBuildSHA, wrong.Capacity, wrong.HeartbeatSec, wrong.PollSec, wrong.BackendAPIURL, wrong.HostFirewallScriptSHA256, wrong.KernelFirewallPolicySHA256, wrong.ObservationSHA256); err != nil {
		t.Fatal(err)
	}
	var futureAuthored bool
	if err = pool.QueryRow(ctx, `SELECT observed_at<=transaction_timestamp() FROM recorder_worker_rollout_observations WHERE droplet_id=$1 ORDER BY id DESC LIMIT 1`, replacement.ID).Scan(&futureAuthored); err != nil || !futureAuthored {
		t.Fatalf("worker observed_at was not DB-authored: ok=%v err=%v", futureAuthored, err)
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
	if _, err = pool.Exec(ctx, `INSERT INTO recorder_rollout_provider_observations(rollout_generation,phase,account_droplet_limit,account_droplet_count,pool_droplet_count,frozen_nonpool_set_sha256,provider_inventory_sha256,firewall_id,firewall_rules_sha256,firewall_tag,firewall_status) VALUES($1,'reconcile',25,10,8,$2,$3,$4,$5,$6,'succeeded')`, rollout.Generation, nonPoolSetDigest(facts.NonPoolDroplets), strings.Repeat("d", 64), cfg.FirewallID, rules, recorderFirewallTag); err == nil {
		t.Fatal("direct SQL committed a provider observation without its exact object set")
	}
	ready, err := NewStore(pool).MarkRolloutStandbyReady(ctx, replacement.ID)
	if err != nil || ready {
		t.Fatalf("stale matching observation bypassed latest mismatch: ready=%v err=%v", ready, err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recorder_worker_rollout_observations(droplet_id,node_id,worker_id,executable_build_sha,capacity,heartbeat_sec,poll_sec,backend_api_origin,host_firewall_script_sha256,kernel_firewall_policy_sha256,observation_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, replacement.ID, *replacement.NodeID, replacement.Name, observed.ExecutableBuildSHA, observed.Capacity, observed.HeartbeatSec, observed.PollSec, observed.BackendAPIURL, hostSHA, kernelSHA, observed.ObservationSHA256); err != nil {
		t.Fatal(err)
	}
	ready, err = NewStore(pool).MarkRolloutStandbyReady(ctx, replacement.ID)
	if err != nil || !ready {
		t.Fatalf("standby ready from latest exact actual observation=%v err=%v", ready, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET state='active',lease_admission_enabled=true WHERE id=$1`, replacement.ID); err == nil {
		t.Fatal("direct SQL admitted standby without exact pre-switch provider evidence")
	}
	var oldName string
	if err = pool.QueryRow(ctx, `SELECT name FROM recorder_droplets WHERE id=$1`, *replacement.ReplacementForID).Scan(&oldName); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET state='rollout_draining',lease_admission_enabled=false,rollout_generation=$2,rollout_ordinal=0,desired_build_sha=$3,desired_common_policy_sha256=$4,do_droplet_id=99999 WHERE id=$1`, *replacement.ReplacementForID, rollout.Generation, newBuild, common); err == nil {
		t.Fatal("direct SQL attached a baseline while substituting provider identity")
	}
	var liveJobID int64
	if err = pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,lease_owner,lease_expires_at,idempotency_key) VALUES($1,now(),now(),30,'leased',$2,now()+interval '1 hour',$3) RETURNING id`, recordingID, oldName, fmt.Sprintf("rollout-busy:%d", replacement.ID)).Scan(&liveJobID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET state='rollout_draining',lease_admission_enabled=false,rollout_generation=$2,rollout_ordinal=0,replacement_for_id=NULL,desired_build_sha=$3,desired_common_policy_sha256=$4 WHERE id=$1`, *replacement.ReplacementForID, rollout.Generation, newBuild, common); err == nil {
		t.Fatal("direct SQL retired a rollout worker with a live lease")
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
	facts = withoutOld
	// Finish the remaining seven replacements through the same real store paths.
	// This is the full eight-worker crash-state spine needed for the completion
	// seal and ensures ordinal allocation never reuses retired rows.
	for ordinal := 2; ordinal <= 8; ordinal++ {
		name, err = NewStore(pool).NextReplacementIdentity(ctx, rollout)
		if err != nil {
			t.Fatal(err)
		}
		next, reserveErr := NewStore(pool).ReserveRolloutReplacement(ctx, rollout, cfg, name)
		if reserveErr != nil {
			t.Fatal(reserveErr)
		}
		providerID := int64(108 + ordinal)
		if err = NewStore(pool).SetDropletID(ctx, next.ID, providerID, fmt.Sprintf("203.0.113.%d", 8+ordinal)); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `INSERT INTO recorder_worker_rollout_observations(droplet_id,node_id,worker_id,executable_build_sha,capacity,heartbeat_sec,poll_sec,backend_api_origin,host_firewall_script_sha256,kernel_firewall_policy_sha256,observation_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, next.ID, *next.NodeID, next.Name, observed.ExecutableBuildSHA, observed.Capacity, observed.HeartbeatSec, observed.PollSec, observed.BackendAPIURL, hostSHA, kernelSHA, observed.ObservationSHA256); err != nil {
			t.Fatal(err)
		}
		var retiredProviderID int64
		if err = pool.QueryRow(ctx, `SELECT do_droplet_id FROM recorder_droplets WHERE id=$1`, *next.ReplacementForID).Scan(&retiredProviderID); err != nil {
			t.Fatal(err)
		}
		withNext := facts
		withNext.AccountDropletCount = 11
		withNext.PoolDropletIDs = append(append([]int64(nil), facts.PoolDropletIDs...), providerID)
		withNext.PoolDroplets = append(append([]DODroplet(nil), facts.PoolDroplets...), DODroplet{ID: providerID, Name: next.Name, Region: cfg.Region, Size: cfg.Size, Status: "active", ProjectMember: true, Tags: []string{recorderFirewallTag}})
		if _, err = NewStore(pool).AppendProviderObservation(ctx, rollout.Generation, "reconcile", withNext); err != nil {
			t.Fatal(err)
		}
		if ready, readyErr := NewStore(pool).MarkRolloutStandbyReady(ctx, next.ID); readyErr != nil || !ready {
			t.Fatalf("replacement %d ready=%v err=%v", ordinal, ready, readyErr)
		}
		preSwitch, observeErr := NewStore(pool).AppendProviderObservation(ctx, rollout.Generation, "pre_switch", withNext)
		if observeErr != nil {
			t.Fatal(observeErr)
		}
		if switched, switchErr := NewStore(pool).SwitchRolloutReplacement(ctx, next.ID, preSwitch, 40); switchErr != nil || !switched {
			t.Fatalf("replacement %d switch=%v err=%v", ordinal, switched, switchErr)
		}
		afterNext := withNext
		afterNext.AccountDropletCount = 10
		afterNext.PoolDropletIDs = nil
		afterNext.PoolDroplets = nil
		for i, id := range withNext.PoolDropletIDs {
			if id != retiredProviderID {
				afterNext.PoolDropletIDs = append(afterNext.PoolDropletIDs, id)
				afterNext.PoolDroplets = append(afterNext.PoolDroplets, withNext.PoolDroplets[i])
			}
		}
		for read := 0; read < 2; read++ {
			if _, err = NewStore(pool).AppendProviderObservation(ctx, rollout.Generation, "reconcile", afterNext); err != nil {
				t.Fatal(err)
			}
		}
		if finalized, finalizeErr := NewStore(pool).FinalizeAbsentRolloutWorker(ctx, *next.ReplacementForID, 0); finalizeErr != nil || !finalized {
			t.Fatalf("replacement %d finalize=%v err=%v", ordinal, finalized, finalizeErr)
		}
		facts = afterNext
	}
	if _, err = NewStore(pool).AppendProviderObservation(ctx, rollout.Generation, "post_delete", facts); err != nil {
		t.Fatal(err)
	}
	// Completion and a destroy path serialize on the same exclusive fence. The
	// destroy cannot enter while completion holds it, and after completion the DB
	// lifecycle guard rejects mutation of a target worker.
	completeBlock, beginErr := pool.Begin(ctx)
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	if _, err = completeBlock.Exec(ctx, rolloutFenceSQL); err != nil {
		t.Fatal(err)
	}
	var activeTargetID int64
	if err = pool.QueryRow(ctx, `SELECT min(id) FROM recorder_droplets WHERE state='active' AND lease_admission_enabled`).Scan(&activeTargetID); err != nil {
		t.Fatal(err)
	}
	destroyResult := make(chan error, 1)
	go func() { destroyResult <- NewStore(pool).MarkDestroying(ctx, activeTargetID) }()
	select {
	case err = <-destroyResult:
		t.Fatalf("destroy crossed completion fence: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err = completeBlock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-destroyResult; err == nil {
		t.Fatal("direct destroy mutated an active rollout target")
	}
	// The completion validator must run against commit-final state. Completing
	// first and then shrinking admission in the same transaction is rejected by
	// the deferred authority validator.
	forgedComplete, beginErr := pool.Begin(ctx)
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	if _, err = forgedComplete.Exec(ctx, `UPDATE recorder_pool_rollouts SET status='complete',completed_at=transaction_timestamp() WHERE generation=$1`, rollout.Generation); err != nil {
		forgedComplete.Rollback(ctx)
		t.Fatalf("valid pre-mutation completion setup: %v", err)
	}
	if _, err = forgedComplete.Exec(ctx, `UPDATE recorder_droplets SET lease_admission_enabled=false WHERE id=(SELECT min(id) FROM recorder_droplets WHERE state='active')`); err != nil {
		forgedComplete.Rollback(ctx)
		t.Fatalf("deferred hostile mutation setup: %v", err)
	}
	if err = forgedComplete.Commit(ctx); err == nil {
		t.Fatal("same-transaction completion escaped deferred final-state validation")
	}
	if completed, completeErr := NewStore(pool).CompleteRollout(ctx, rollout.Generation); completeErr != nil || !completed {
		t.Fatalf("DB-sealed completion=%v err=%v", completed, completeErr)
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_pool_rollouts SET status='running',completed_at=NULL WHERE generation=$1`, rollout.Generation); err == nil {
		t.Fatal("direct SQL reopened terminal rollout")
	}
	// Actual controller reconciliation never hands the permanently reserved
	// namespace to legacy one-read orphan deletion, even for a late object from a
	// terminal generation.
	provider := &fakeDOClient{fleet: append([]DODroplet(nil), facts.PoolDroplets...)}
	provider.fleet = append(provider.fleet,
		DODroplet{ID: 70001, Name: fmt.Sprintf("stoarama-rec-rollout-%d-999", rollout.Generation), Status: "active", CreatedAt: time.Now().Add(-time.Hour)},
		DODroplet{ID: 70004, Name: fmt.Sprintf("stoarama-rec-rollout-%d-1000", rollout.Generation), Status: "active", CreatedAt: time.Now()},
	)
	controller := NewController(pool, provider, Config{ProvisionTimeout: time.Minute, HeartbeatSec: 15})
	if err = controller.reconcile(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if len(provider.deleted) != 0 {
		t.Fatalf("legacy reconcile deleted terminal reserved provider object: %v", provider.deleted)
	}

	// Start a successor generation, then exercise response-loss adoption and an
	// exact-name/different-id duplicate. The first is bound to its preregistered
	// row; the second is retained and atomically stops convergence.
	nextBuild := strings.Repeat("d", 40)
	nextCfg := cfg
	nextCfg.BuildSHA = nextBuild
	nextCommon, digestErr := (CommonPolicyIdentity{BuildSHA: nextBuild, Capacity: 5, HeartbeatSec: 15, PollSec: 5, BackendAPIURL: cfg.BackendAPIURL, FirewallID: cfg.FirewallID, FirewallRulesSHA256: rules, FirewallTag: recorderFirewallTag, HostFirewallScriptSHA256: hostSHA, KernelFirewallPolicySHA256: kernelSHA}).Digest()
	if digestErr != nil {
		t.Fatal(digestErr)
	}
	nextRollout, ensureErr := NewStore(pool).EnsureRollout(ctx, nextCfg, nextCommon, facts)
	if ensureErr != nil {
		t.Fatal(ensureErr)
	}
	nextName, identityErr := NewStore(pool).NextReplacementIdentity(ctx, nextRollout)
	if identityErr != nil {
		t.Fatal(identityErr)
	}
	responseLoss, reserveErr := NewStore(pool).ReserveRolloutReplacement(ctx, nextRollout, nextCfg, nextName)
	if reserveErr != nil {
		t.Fatal(reserveErr)
	}
	provider.fleet = append(append([]DODroplet(nil), facts.PoolDroplets...), DODroplet{ID: 70002, Name: responseLoss.Name, IP: "203.0.113.200", Status: "active", CreatedAt: time.Now()})
	controller = NewController(pool, provider, Config{ProvisionTimeout: time.Minute, HeartbeatSec: 15})
	if err = controller.reconcile(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	var adoptedProviderID int64
	if err = pool.QueryRow(ctx, `SELECT do_droplet_id FROM recorder_droplets WHERE id=$1`, responseLoss.ID).Scan(&adoptedProviderID); err != nil || adoptedProviderID != 70002 {
		t.Fatalf("response-loss reservation adopted provider=%d err=%v", adoptedProviderID, err)
	}
	provider.fleet = append(provider.fleet, DODroplet{ID: 70003, Name: responseLoss.Name, Status: "active", CreatedAt: time.Now().Add(-time.Hour)})
	provider.fleet = append(provider.fleet, DODroplet{ID: 70002, Name: fmt.Sprintf("stoarama-rec-rollout-%d-9999", nextRollout.Generation), Status: "active", CreatedAt: time.Now()})
	if err = controller.reconcile(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	var stoppedStatus, stopReason string
	if err = pool.QueryRow(ctx, `SELECT status,stop_reason FROM recorder_pool_rollouts WHERE generation=$1`, nextRollout.Generation).Scan(&stoppedStatus, &stopReason); err != nil || stoppedStatus != "stopping" || stopReason != "duplicate_provider_object" {
		t.Fatalf("duplicate reserved object status=%q reason=%q err=%v", stoppedStatus, stopReason, err)
	}
	if len(provider.deleted) != 0 {
		t.Fatalf("controller deleted reserved response-loss/duplicate object: %v", provider.deleted)
	}
	if _, err = pool.Exec(ctx, `TRUNCATE recorder_pool_rollouts`); err == nil {
		t.Fatal("rollout audit accepted TRUNCATE")
	}
	var installSchema string
	if err = pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&installSchema); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `CREATE TEMP TABLE recorder_pool_rollouts(generation bigint); CREATE OR REPLACE FUNCTION pg_temp.recorder_rollout_typed_field(text) RETURNS bytea LANGUAGE sql AS 'SELECT decode(''00'',''hex'')'`); err != nil {
		t.Fatal(err)
	}
	qualifiedDelete := `DELETE FROM ` + pgx.Identifier{installSchema, "recorder_pool_rollouts"}.Sanitize() + ` WHERE generation=$1`
	if _, err = pool.Exec(ctx, qualifiedDelete, rollout.Generation); err == nil {
		t.Fatal("pg_temp shadows bypassed rollout head delete fence")
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
