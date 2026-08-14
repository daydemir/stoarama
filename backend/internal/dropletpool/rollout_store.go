package dropletpool

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const rolloutFenceSQL = `SELECT pg_advisory_xact_lock(hashtextextended('stoarama-recorder-pool-rollout-v1',0))`

const (
	recorderRolloutSteadyWorkers  = 8
	recorderRolloutWorkerCapacity = 5
	recorderRolloutMaxSurge       = 1
)

func AcquireRolloutExclusiveFence(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, rolloutFenceSQL)
	return err
}

type PoolRollout struct {
	Generation               int64
	Status                   string
	Kind                     string
	PredecessorGeneration    *int64
	RecoveryMissingDropletID *int64
	ToBuildSHA               string
	CommonPolicySHA256       string
	FirewallID               string
	FirewallRulesSHA256      string
	FirewallTag              string
	BaselineMin              int
	Capacity                 int
	MaxSurge                 int
	AccountDropletLimit      int
	BaselineAccountDroplets  int
	ProviderFactsSHA256      string
	HostFirewallSHA256       string
	KernelPolicySHA256       string
	TargetSHA256             string
	FrozenNonPoolSHA256      string
	HeartbeatSec             int
	PollSec                  int
	BackendAPIURL            string
}

type ReservedReplacement struct {
	Droplet
	NodeTokenID int64
	PlainToken  string
}

func providerFactsDigest(f RolloutProviderFacts) string {
	pool := append([]int64(nil), f.PoolDropletIDs...)
	nonpool := append([]int64(nil), f.NonPoolDropletIDs...)
	firewallIDs := append([]int64(nil), f.FirewallDropletIDs...)
	tags := append([]string(nil), f.FirewallTags...)
	sort.Slice(pool, func(i, j int) bool { return pool[i] < pool[j] })
	sort.Slice(nonpool, func(i, j int) bool { return nonpool[i] < nonpool[j] })
	sort.Slice(firewallIDs, func(i, j int) bool { return firewallIDs[i] < firewallIDs[j] })
	sort.Strings(tags)
	fields := []string{
		strconv.Itoa(f.AccountDropletLimit), strconv.Itoa(f.AccountDropletCount),
		f.FirewallID, f.FirewallStatus, f.FirewallRulesSHA256,
		strconv.Itoa(len(pool)),
	}
	for _, id := range pool {
		fields = append(fields, strconv.FormatInt(id, 10))
	}
	fields = append(fields, strconv.Itoa(len(nonpool)))
	for _, id := range nonpool {
		fields = append(fields, strconv.FormatInt(id, 10))
	}
	fields = append(fields, strconv.Itoa(len(f.PoolDroplets)))
	for _, d := range f.PoolDroplets {
		tags := append([]string(nil), d.Tags...)
		sort.Strings(tags)
		fields = append(fields, strconv.FormatInt(d.ID, 10), d.Name, d.Region, d.Size, d.Status, strconv.FormatBool(d.ProjectMember), strings.Join(tags, ","))
	}
	fields = append(fields, strconv.Itoa(len(f.NonPoolDroplets)))
	for _, d := range f.NonPoolDroplets {
		tags := append([]string(nil), d.Tags...)
		sort.Strings(tags)
		fields = append(fields, strconv.FormatInt(d.ID, 10), d.Name, d.Region, d.Size, d.Status, strconv.FormatBool(d.ProjectMember), strings.Join(tags, ","))
	}
	fields = append(fields, strconv.Itoa(len(tags)))
	fields = append(fields, tags...)
	fields = append(fields, strconv.Itoa(len(firewallIDs)))
	for _, id := range firewallIDs {
		fields = append(fields, strconv.FormatInt(id, 10))
	}
	return typedDigest("stoarama-recorder-pool-provider-facts-v1", fields...)
}

func validateRolloutProviderFacts(f RolloutProviderFacts, expectedFirewallID, expectedRules, expectedTag string, steady, maxSurge int) error {
	if f.AccountDropletLimit <= 0 || f.AccountDropletCount < 0 || f.AccountDropletCount > f.AccountDropletLimit {
		return fmt.Errorf("invalid DigitalOcean account capacity facts")
	}
	if f.FirewallID != strings.TrimSpace(expectedFirewallID) || f.FirewallStatus != "succeeded" || f.FirewallRulesSHA256 != expectedRules {
		return fmt.Errorf("firewall identity/status/rules mismatch")
	}
	if expectedTag != recorderFirewallTag || len(f.FirewallTags) != 1 || f.FirewallTags[0] != expectedTag {
		return fmt.Errorf("firewall must target only exact recorder pool tag")
	}
	if len(f.FirewallDropletIDs) != 0 {
		return fmt.Errorf("tag-scoped rollout firewall must not carry explicit droplet ids")
	}
	if len(f.PoolDropletIDs) < steady-1 || len(f.PoolDropletIDs) > steady+maxSurge || len(f.PoolDroplets) != len(f.PoolDropletIDs) {
		return fmt.Errorf("provider pool size %d outside loss/steady/surge bounds [%d,%d]", len(f.PoolDropletIDs), steady-1, steady+maxSurge)
	}
	if len(f.NonPoolDropletIDs) != 2 || len(f.NonPoolDroplets) != 2 || f.AccountDropletCount != len(f.PoolDropletIDs)+len(f.NonPoolDropletIDs) || f.AccountDropletCount < 9 || f.AccountDropletCount > 11 {
		return fmt.Errorf("account inventory must be exact pool plus frozen two non-pool objects")
	}
	if len(f.PoolDropletIDs) == steady && f.AccountDropletCount+maxSurge > f.AccountDropletLimit {
		return fmt.Errorf("account quota has no bounded surge slot")
	}
	if len(f.PoolDropletIDs) == steady-1 && f.AccountDropletCount+1 > f.AccountDropletLimit {
		return fmt.Errorf("account quota has no loss-recovery slot")
	}
	for i, id := range f.PoolDropletIDs {
		if id <= 0 || (i > 0 && f.PoolDropletIDs[i-1] >= id) {
			return fmt.Errorf("provider pool droplet ids must be positive, sorted, unique")
		}
	}
	for i, d := range f.PoolDroplets {
		if d.ID != f.PoolDropletIDs[i] || !d.ProjectMember || !validRecorderWorkerID(d.Name) || d.Region == "" || d.Size == "" || d.Status != "active" || !containsString(d.Tags, recorderFirewallTag) {
			return fmt.Errorf("provider pool object identity mismatch")
		}
		if i > 0 && d.Name == f.PoolDroplets[i-1].Name {
			return fmt.Errorf("duplicate provider pool name")
		}
	}
	for i, id := range f.NonPoolDropletIDs {
		if id <= 0 || (i > 0 && f.NonPoolDropletIDs[i-1] >= id) || f.NonPoolDroplets[i].ID != id || f.NonPoolDroplets[i].Name == "" || f.NonPoolDroplets[i].Region == "" || f.NonPoolDroplets[i].Size == "" || f.NonPoolDroplets[i].Status == "" || containsString(f.NonPoolDroplets[i].Tags, recorderFirewallTag) {
			return fmt.Errorf("provider non-pool ids must be positive, sorted, unique")
		}
	}
	return nil
}

func validateProviderTargetObjects(f RolloutProviderFacts, cfg Config) error {
	seen := map[string]struct{}{}
	for _, d := range f.PoolDroplets {
		if d.Region != cfg.Region || d.Size != cfg.Size {
			return fmt.Errorf("provider object target mismatch")
		}
		if _, ok := seen[d.Name]; ok {
			return fmt.Errorf("duplicate provider object name")
		}
		seen[d.Name] = struct{}{}
	}
	return nil
}

func baselineWorkerSetDigest(rows []Droplet) string {
	rows = append([]Droplet(nil), rows...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	fields := []string{strconv.Itoa(len(rows))}
	for _, row := range rows {
		fields = append(fields, strconv.FormatInt(row.ID, 10), row.Name, firstNonemptyBuild(row.ObservedBuildSHA, row.BuildSHA))
	}
	return typedDigest("stoarama-recorder-pool-baseline-v1", fields...)
}

func firstNonemptyBuild(values ...string) string {
	for _, value := range values {
		if value = strings.ToLower(strings.TrimSpace(value)); value != "" {
			return value
		}
	}
	return ""
}

func scanRollout(row pgx.Row) (PoolRollout, error) {
	var out PoolRollout
	err := row.Scan(&out.Generation, &out.Status, &out.ToBuildSHA, &out.CommonPolicySHA256,
		&out.FirewallID, &out.FirewallRulesSHA256, &out.FirewallTag,
		&out.BaselineMin, &out.Capacity, &out.MaxSurge, &out.HostFirewallSHA256,
		&out.KernelPolicySHA256, &out.TargetSHA256, &out.FrozenNonPoolSHA256,
		&out.HeartbeatSec, &out.PollSec, &out.BackendAPIURL, &out.Kind, &out.PredecessorGeneration, &out.RecoveryMissingDropletID)
	return out, err
}

const rolloutSelectFields = `generation,status,to_build_sha,common_policy_sha256,firewall_id,firewall_rules_sha256,firewall_tag,steady_workers,worker_capacity,max_surge,host_firewall_script_sha256,kernel_firewall_policy_sha256,target_sha256,frozen_nonpool_set_sha256,heartbeat_sec,poll_sec,backend_api_origin,rollout_kind,predecessor_generation,recovery_missing_droplet_id`

func (s *Store) ActiveRollout(ctx context.Context) (PoolRollout, bool, error) {
	out, err := scanRollout(s.pool.QueryRow(ctx, `SELECT `+rolloutSelectFields+` FROM recorder_pool_rollouts WHERE status IN ('running','stopping')`))
	if errors.Is(err, pgx.ErrNoRows) {
		return PoolRollout{}, false, nil
	}
	return out, err == nil, err
}

func (s *Store) RequestRolloutStop(ctx context.Context, generation int64, reason string) error {
	if !containsString([]string{"disabled", "forecast_exceeds_capacity", "provider_drift", "duplicate_provider_object", "target_mismatch", "operator_stop"}, reason) {
		return fmt.Errorf("invalid rollout stop reason")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE recorder_pool_rollouts SET status='stopping',stop_reason=$2,stopping_at=COALESCE(stopping_at,now()) WHERE generation=$1 AND status IN ('running','stopping')`, generation, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) CompleteRolloutStop(ctx context.Context, generation int64) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return false, err
	}
	var transitional, claimable int
	if err = tx.QueryRow(ctx, `SELECT count(*) FILTER(WHERE rollout_generation=$1 AND state IN ('provisioning','standby_ready','rollout_draining','destroying')),count(*) FILTER(WHERE state='active' AND lease_admission_enabled) FROM recorder_droplets`, generation).Scan(&transitional, &claimable); err != nil {
		return false, err
	}
	if transitional != 0 || claimable != 8 {
		return false, nil
	}
	var poolCount, accountCount int
	if err = tx.QueryRow(ctx, `SELECT pool_droplet_count,account_droplet_count FROM recorder_rollout_provider_observations WHERE rollout_generation=$1 AND phase='post_delete' AND observed_at>now()-interval '60 seconds' ORDER BY observed_at DESC,id DESC LIMIT 1`, generation).Scan(&poolCount, &accountCount); err != nil || poolCount != 8 || accountCount != 10 {
		return false, nil
	}
	result, err := tx.Exec(ctx, `UPDATE recorder_pool_rollouts SET status='stopped',completed_at=now() WHERE generation=$1 AND status='stopping'`)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, tx.Commit(ctx)
}

func (s *Store) BeginUnswitchedRolloutDestroy(ctx context.Context, id int64, minimumAge time.Duration) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return false, err
	}
	var generation int64
	var doID *int64
	var oldEnough bool
	if err = tx.QueryRow(ctx, `SELECT rollout_generation,do_droplet_id,created_at<=now()-$2::interval FROM recorder_droplets WHERE id=$1 AND state IN ('provisioning','standby_ready') AND NOT lease_admission_enabled FOR UPDATE`, id, minimumAge.String()).Scan(&generation, &doID, &oldEnough); err != nil {
		return false, nil
	}
	if doID == nil || !oldEnough {
		return false, nil
	}
	var reads int
	var separated bool
	if err = tx.QueryRow(ctx, `SELECT count(*),COALESCE(max(observed_at)-min(observed_at)>=$3::interval,false) FROM (SELECT p.observed_at FROM recorder_rollout_provider_observations p JOIN recorder_rollout_provider_objects po ON po.observation_id=p.id AND po.provider_droplet_id=$2 AND po.is_pool WHERE p.rollout_generation=$1 AND p.phase='pre_delete' ORDER BY p.observed_at DESC LIMIT 2) q`, generation, *doID, minimumAge.String()).Scan(&reads, &separated); err != nil {
		return false, err
	}
	if reads < 2 || !separated {
		return false, nil
	}
	if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET state='destroying',lease_admission_enabled=false WHERE id=$1`, id); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func nonPoolSetDigest(objects []DODroplet) string {
	objects = append([]DODroplet(nil), objects...)
	sort.Slice(objects, func(i, j int) bool { return objects[i].ID < objects[j].ID })
	fields := []string{strconv.Itoa(len(objects))}
	for _, object := range objects {
		tags := append([]string(nil), object.Tags...)
		sort.Strings(tags)
		fields = append(fields, strconv.FormatInt(object.ID, 10), object.Name, object.Region, object.Size,
			object.Status, strconv.FormatBool(object.ProjectMember), strings.Join(tags, ","))
	}
	return typedDigest("stoarama-recorder-pool-nonpool-objects-v2", fields...)
}

func rolloutTargetDigest(cfg Config, commonPolicySHA256, frozenNonPoolSHA256 string) string {
	return typedDigest("stoarama-recorder-pool-rollout-target-v1", cfg.BuildSHA, commonPolicySHA256,
		cfg.Region, cfg.Size, cfg.ProjectID, cfg.FirewallID, cfg.FirewallRulesSHA256,
		cfg.FirewallTag, cfg.HostFirewallSHA256, cfg.KernelPolicySHA256,
		strconv.Itoa(recorderRolloutSteadyWorkers), strconv.Itoa(cfg.Capacity), strconv.Itoa(cfg.RolloutMaxSurge), frozenNonPoolSHA256)
}

// EnsureRollout creates or returns the one active rollout under the exclusive
// claim/switch fence. It persists provider and baseline identities before any
// replacement token or provider object exists.
func (s *Store) EnsureRollout(ctx context.Context, cfg Config, commonPolicySHA256 string, facts RolloutProviderFacts) (PoolRollout, error) {
	if cfg.Capacity != recorderRolloutWorkerCapacity || cfg.RolloutMaxSurge != recorderRolloutMaxSurge || cfg.Max < recorderRolloutSteadyWorkers+recorderRolloutMaxSurge {
		return PoolRollout{}, fmt.Errorf("rollout dimensions must remain exact 8 steady + 1 surge at capacity 5")
	}
	if err := validateRolloutProviderFacts(facts, cfg.FirewallID, cfg.FirewallRulesSHA256, cfg.FirewallTag, recorderRolloutSteadyWorkers, cfg.RolloutMaxSurge); err != nil {
		return PoolRollout{}, err
	}
	if err := validateProviderTargetObjects(facts, cfg); err != nil {
		return PoolRollout{}, err
	}
	frozenNonPoolSHA := nonPoolSetDigest(facts.NonPoolDroplets)
	targetSHA := rolloutTargetDigest(cfg, commonPolicySHA256, frozenNonPoolSHA)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return PoolRollout{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return PoolRollout{}, err
	}
	const fields = rolloutSelectFields
	out, err := scanRollout(tx.QueryRow(ctx, `SELECT `+fields+` FROM recorder_pool_rollouts WHERE status IN ('running','stopping') FOR UPDATE`))
	if err == nil {
		expectedTarget := targetSHA
		if out.Kind == "loss_recovery" && out.PredecessorGeneration != nil && out.RecoveryMissingDropletID != nil {
			expectedTarget = typedDigest("stoarama-recorder-pool-loss-recovery-v1", targetSHA, strconv.FormatInt(*out.PredecessorGeneration, 10), strconv.FormatInt(*out.RecoveryMissingDropletID, 10))
		}
		if out.TargetSHA256 != expectedTarget {
			return PoolRollout{}, fmt.Errorf("running rollout identity differs from controller policy")
		}
		return out, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return PoolRollout{}, err
	}
	out, err = scanRollout(tx.QueryRow(ctx, `SELECT `+fields+` FROM recorder_pool_rollouts WHERE status='complete' AND to_build_sha=$1 AND common_policy_sha256=$2 AND region=$3 AND size_slug=$4 AND project_id=$5 AND frozen_nonpool_set_sha256=$6 ORDER BY generation DESC LIMIT 1`, cfg.BuildSHA, commonPolicySHA256, cfg.Region, cfg.Size, cfg.ProjectID, frozenNonPoolSHA))
	if err == nil {
		var memberCount int
		var missingIDs []int64
		if err = tx.QueryRow(ctx, `SELECT count(*),COALESCE(array_agg(m.droplet_id ORDER BY m.droplet_id) FILTER(WHERE NOT (m.provider_droplet_id=ANY($2))),ARRAY[]::bigint[]) FROM recorder_rollout_completion_members m WHERE m.rollout_generation=$1`, out.Generation, facts.PoolDropletIDs).Scan(&memberCount, &missingIDs); err != nil {
			return PoolRollout{}, err
		}
		if memberCount != recorderRolloutSteadyWorkers {
			return PoolRollout{}, fmt.Errorf("completed rollout membership is not sealed exact")
		}
		if len(missingIDs) == 0 && len(facts.PoolDropletIDs) == recorderRolloutSteadyWorkers {
			return out, tx.Commit(ctx)
		}
		if len(missingIDs) != 1 || len(facts.PoolDropletIDs) != recorderRolloutSteadyWorkers-1 {
			return PoolRollout{}, fmt.Errorf("completed rollout provider membership drift is not an exact single loss")
		}
		recoveryTarget := typedDigest("stoarama-recorder-pool-loss-recovery-v1", targetSHA, strconv.FormatInt(out.Generation, 10), strconv.FormatInt(missingIDs[0], 10))
		out, err = scanRollout(tx.QueryRow(ctx, `INSERT INTO recorder_pool_rollouts
			(rollout_kind,predecessor_generation,recovery_missing_droplet_id,status,baseline_worker_set_sha256,frozen_nonpool_set_sha256,target_sha256,to_build_sha,common_policy_sha256,host_firewall_script_sha256,kernel_firewall_policy_sha256,firewall_id,firewall_rules_sha256,firewall_tag,region,size_slug,project_id,steady_workers,worker_capacity,heartbeat_sec,poll_sec,backend_api_origin,max_surge,steady_account_droplets,surge_account_droplets)
			SELECT 'loss_recovery',generation,$2,'running',baseline_worker_set_sha256,frozen_nonpool_set_sha256,$3,to_build_sha,common_policy_sha256,host_firewall_script_sha256,kernel_firewall_policy_sha256,firewall_id,firewall_rules_sha256,firewall_tag,region,size_slug,project_id,steady_workers,worker_capacity,heartbeat_sec,poll_sec,backend_api_origin,max_surge,steady_account_droplets,surge_account_droplets FROM recorder_pool_rollouts WHERE generation=$1 RETURNING `+fields, out.Generation, missingIDs[0], recoveryTarget))
		if err != nil {
			return PoolRollout{}, err
		}
		return out, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return PoolRollout{}, err
	}
	rows, err := tx.Query(ctx, `SELECT id,name,COALESCE(observed_build_sha,build_sha,'') FROM recorder_droplets WHERE state='active' AND lease_admission_enabled ORDER BY id FOR UPDATE`)
	if err != nil {
		return PoolRollout{}, err
	}
	var baseline []Droplet
	for rows.Next() {
		var d Droplet
		if err = rows.Scan(&d.ID, &d.Name, &d.ObservedBuildSHA); err != nil {
			rows.Close()
			return PoolRollout{}, err
		}
		baseline = append(baseline, d)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return PoolRollout{}, err
	}
	if len(baseline) != recorderRolloutSteadyWorkers {
		return PoolRollout{}, fmt.Errorf("claimable baseline %d must equal rollout steady size %d", len(baseline), recorderRolloutSteadyWorkers)
	}
	var live int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM recorder_droplets WHERE state IN ('provisioning','standby_ready','active','rollout_draining','draining','destroying')`).Scan(&live); err != nil {
		return PoolRollout{}, err
	}
	if live != recorderRolloutSteadyWorkers {
		return PoolRollout{}, fmt.Errorf("rollout baseline contains %d live rows, want exact steady %d", live, recorderRolloutSteadyWorkers)
	}
	if len(facts.PoolDropletIDs) != recorderRolloutSteadyWorkers {
		return PoolRollout{}, fmt.Errorf("ordinary rollout baseline requires exact provider steady membership")
	}
	out, err = scanRollout(tx.QueryRow(ctx, `INSERT INTO recorder_pool_rollouts
		(status,baseline_worker_set_sha256,frozen_nonpool_set_sha256,target_sha256,to_build_sha,common_policy_sha256,host_firewall_script_sha256,kernel_firewall_policy_sha256,firewall_id,firewall_rules_sha256,firewall_tag,region,size_slug,project_id,steady_workers,worker_capacity,heartbeat_sec,poll_sec,backend_api_origin,max_surge,steady_account_droplets,surge_account_droplets)
		VALUES ('running',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,10,11) RETURNING `+fields,
		baselineWorkerSetDigest(baseline), frozenNonPoolSHA, targetSHA, cfg.BuildSHA, commonPolicySHA256,
		cfg.HostFirewallSHA256, cfg.KernelPolicySHA256, cfg.FirewallID, cfg.FirewallRulesSHA256,
		cfg.FirewallTag, cfg.Region, cfg.Size, cfg.ProjectID, recorderRolloutSteadyWorkers, cfg.Capacity, cfg.HeartbeatSec, cfg.PollSec, cfg.BackendAPIURL, cfg.RolloutMaxSurge))
	if err != nil {
		return PoolRollout{}, err
	}
	return out, tx.Commit(ctx)
}

// AppendProviderObservation persists one complete provider read after the
// provider call has returned. It never performs provider I/O and every child is
// committed with the exact generation/inventory parent.
func (s *Store) AppendProviderObservation(ctx context.Context, generation int64, phase string, facts RolloutProviderFacts) (int64, error) {
	if generation <= 0 || !containsString([]string{"baseline", "reconcile", "pre_switch", "pre_delete", "post_delete"}, phase) {
		return 0, fmt.Errorf("invalid provider observation binding")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return 0, err
	}
	var expectedNonPool string
	if err = tx.QueryRow(ctx, `SELECT frozen_nonpool_set_sha256 FROM recorder_pool_rollouts WHERE generation=$1`, generation).Scan(&expectedNonPool); err != nil {
		return 0, err
	}
	if expectedNonPool != nonPoolSetDigest(facts.NonPoolDroplets) {
		return 0, fmt.Errorf("non-pool inventory drift")
	}
	digest := providerFactsDigest(facts)
	var observationID int64
	err = tx.QueryRow(ctx, `INSERT INTO recorder_rollout_provider_observations(rollout_generation,phase,account_droplet_limit,account_droplet_count,pool_droplet_count,frozen_nonpool_set_sha256,provider_inventory_sha256,firewall_id,firewall_rules_sha256,firewall_tag,firewall_status) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id`, generation, phase, facts.AccountDropletLimit, facts.AccountDropletCount, len(facts.PoolDroplets), expectedNonPool, digest, facts.FirewallID, facts.FirewallRulesSHA256, recorderFirewallTag, facts.FirewallStatus).Scan(&observationID)
	if err != nil {
		return 0, err
	}
	for _, d := range facts.PoolDroplets {
		tags := append([]string(nil), d.Tags...)
		sort.Strings(tags)
		fingerprint := typedDigest("stoarama-recorder-provider-object-v1", strconv.FormatInt(d.ID, 10), d.Name, d.Region, d.Size, d.Status, strconv.FormatBool(d.ProjectMember), strings.Join(tags, ","))
		if _, err = tx.Exec(ctx, `INSERT INTO recorder_rollout_provider_objects(observation_id,provider_droplet_id,is_pool,name,region,size_slug,status,project_member,pool_tag_present,tags_csv,object_fingerprint_sha256) VALUES($1,$2,true,$3,$4,$5,$6,$7,$8,$9,$10)`, observationID, d.ID, d.Name, d.Region, d.Size, d.Status, d.ProjectMember, containsString(d.Tags, recorderFirewallTag), strings.Join(tags, ","), fingerprint); err != nil {
			return 0, err
		}
	}
	for _, d := range facts.NonPoolDroplets {
		tags := append([]string(nil), d.Tags...)
		sort.Strings(tags)
		fingerprint := typedDigest("stoarama-recorder-provider-object-v1", strconv.FormatInt(d.ID, 10), d.Name, d.Region, d.Size, d.Status, strconv.FormatBool(d.ProjectMember), strings.Join(tags, ","))
		if _, err = tx.Exec(ctx, `INSERT INTO recorder_rollout_provider_objects(observation_id,provider_droplet_id,is_pool,name,region,size_slug,status,project_member,pool_tag_present,tags_csv,object_fingerprint_sha256) VALUES($1,$2,false,$3,$4,$5,$6,$7,false,$8,$9)`, observationID, d.ID, d.Name, d.Region, d.Size, d.Status, d.ProjectMember, strings.Join(tags, ","), fingerprint); err != nil {
			return 0, err
		}
	}
	return observationID, tx.Commit(ctx)
}

// ReserveRolloutReplacement atomically preregisters the future worker identity,
// one nonrecoverable token hash, and one nonclaimable provisioning row. The
// plaintext token leaves this call only for its single cloud-init request.
func (s *Store) ReserveRolloutReplacement(ctx context.Context, rollout PoolRollout, cfg Config, name string) (ReservedReplacement, error) {
	if !validRecorderWorkerID(name) {
		return ReservedReplacement{}, fmt.Errorf("invalid replacement identity")
	}
	raw, err := generateNodeSecret(36)
	if err != nil {
		return ReservedReplacement{}, err
	}
	plain := "sin_" + raw
	prefix := plain
	if len(prefix) > 16 {
		prefix = prefix[:16]
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return ReservedReplacement{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return ReservedReplacement{}, err
	}
	var status, kind string
	var recoveryMissingID *int64
	if err = tx.QueryRow(ctx, `SELECT status,rollout_kind,recovery_missing_droplet_id FROM recorder_pool_rollouts WHERE generation=$1 FOR UPDATE`, rollout.Generation).Scan(&status, &kind, &recoveryMissingID); err != nil || status != "running" {
		return ReservedReplacement{}, fmt.Errorf("rollout is not running")
	}
	var ordinal int
	if err = tx.QueryRow(ctx, `SELECT COALESCE(max(rollout_ordinal),0)+1 FROM recorder_droplets WHERE rollout_generation=$1`, rollout.Generation).Scan(&ordinal); err != nil {
		return ReservedReplacement{}, err
	}
	if name != fmt.Sprintf("%srollout-%d-%d", NamePrefix, rollout.Generation, ordinal) {
		return ReservedReplacement{}, fmt.Errorf("replacement name does not match reserved generation ordinal")
	}
	var pending int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM recorder_droplets WHERE rollout_generation=$1 AND state IN ('provisioning','standby_ready')`, rollout.Generation).Scan(&pending); err != nil || pending != 0 {
		return ReservedReplacement{}, fmt.Errorf("rollout already has a pending replacement")
	}
	var live int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM recorder_droplets WHERE state IN ('provisioning','standby_ready','active','rollout_draining','draining','destroying')`).Scan(&live); err != nil {
		return ReservedReplacement{}, err
	}
	if live >= recorderRolloutSteadyWorkers+cfg.RolloutMaxSurge || live >= cfg.Max {
		return ReservedReplacement{}, fmt.Errorf("bounded surge slot unavailable")
	}
	var staleID int64
	if kind == "loss_recovery" {
		if recoveryMissingID == nil {
			return ReservedReplacement{}, fmt.Errorf("loss recovery lacks missing member")
		}
		if err = tx.QueryRow(ctx, `SELECT d.id FROM recorder_droplets d WHERE d.id=$1 AND d.state='active' AND d.lease_admission_enabled AND NOT EXISTS(SELECT 1 FROM recording_jobs j WHERE j.lease_owner=d.name AND j.status='leased' AND j.lease_expires_at>now()) AND EXISTS(SELECT 1 FROM (SELECT p.observed_at FROM recorder_rollout_provider_observations p WHERE p.rollout_generation=$2 AND p.phase='reconcile' AND p.pool_droplet_count=7 AND NOT EXISTS(SELECT 1 FROM recorder_rollout_provider_objects po WHERE po.observation_id=p.id AND (po.provider_droplet_id=d.do_droplet_id OR po.name=d.name)) ORDER BY p.observed_at DESC,p.id DESC LIMIT 2) q HAVING count(*)=2 AND max(observed_at)>min(observed_at) AND max(observed_at)>transaction_timestamp()-interval '60 seconds') FOR UPDATE`, *recoveryMissingID, rollout.Generation).Scan(&staleID); err != nil {
			return ReservedReplacement{}, fmt.Errorf("loss recovery missing member lacks two exact absence observations: %w", err)
		}
	} else if err = tx.QueryRow(ctx, `SELECT d.id FROM recorder_droplets d WHERE d.state='active' AND d.lease_admission_enabled AND (COALESCE(d.observed_build_sha,d.build_sha,'')<>$1 OR COALESCE(d.observed_common_policy_sha256,'')<>$2) AND NOT EXISTS (SELECT 1 FROM recording_jobs j WHERE j.lease_owner=d.name AND j.status='leased' AND j.lease_expires_at>now()) ORDER BY d.id LIMIT 1 FOR UPDATE`, cfg.BuildSHA, rollout.CommonPolicySHA256).Scan(&staleID); err != nil {
		return ReservedReplacement{}, fmt.Errorf("no idle stale worker available: %w", err)
	}
	var nodeID, tokenID, dropletID int64
	if err = tx.QueryRow(ctx, `INSERT INTO nodes(account_id,node_type,display_name,hostname,platform,status,enrolled_at,last_heartbeat_at) VALUES($1,'local_recorder',$2,'','digitalocean','active',now(),now()) RETURNING id`, cfg.OperatorAccountID, name).Scan(&nodeID); err != nil {
		return ReservedReplacement{}, err
	}
	if err = tx.QueryRow(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash,last_used_at) VALUES($1,$2,$3,now()) RETURNING id`, nodeID, prefix, hashNodeSecret(plain)).Scan(&tokenID); err != nil {
		return ReservedReplacement{}, err
	}
	if err = tx.QueryRow(ctx, `INSERT INTO recorder_droplets(name,node_id,region,size,capacity,state,build_sha,desired_build_sha,desired_common_policy_sha256,rollout_generation,rollout_ordinal,replacement_for_id,lease_admission_enabled) VALUES($1,$2,$3,$4,$5,'provisioning','',$6,$7,$8,$9,$10,$11,false) RETURNING id`, name, nodeID, cfg.Region, cfg.Size, cfg.Capacity, cfg.BuildSHA, rollout.CommonPolicySHA256, rollout.Generation, ordinal, staleID).Scan(&dropletID); err != nil {
		return ReservedReplacement{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ReservedReplacement{}, err
	}
	return ReservedReplacement{Droplet: Droplet{ID: dropletID, Name: name, NodeID: &nodeID, Region: cfg.Region, Size: cfg.Size, Capacity: cfg.Capacity, State: "provisioning", DesiredBuildSHA: cfg.BuildSHA, DesiredCommonPolicySHA256: rollout.CommonPolicySHA256, RolloutGeneration: &rollout.Generation, ReplacementForID: &staleID}, NodeTokenID: tokenID, PlainToken: plain}, nil
}

func (s *Store) MarkRolloutStandbyReady(ctx context.Context, id int64) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return false, err
	}
	result, err := tx.Exec(ctx, `WITH candidate AS (SELECT d.id,latest.executable_build_sha,r.common_policy_sha256 FROM recorder_droplets d JOIN recorder_pool_rollouts r ON r.generation=d.rollout_generation AND r.status='running' CROSS JOIN LATERAL (SELECT wo.* FROM recorder_worker_rollout_observations wo WHERE wo.droplet_id=d.id ORDER BY wo.observed_at DESC,wo.id DESC LIMIT 1) latest WHERE d.id=$1 AND d.state IN ('provisioning','standby_ready') AND d.do_droplet_id IS NOT NULL AND EXISTS(SELECT 1 FROM recorder_rollout_provider_observations p JOIN recorder_rollout_provider_objects po ON po.observation_id=p.id WHERE p.rollout_generation=d.rollout_generation AND p.observed_at>=transaction_timestamp()-interval '60 seconds' AND p.observed_at<=transaction_timestamp() AND po.is_pool AND po.provider_droplet_id=d.do_droplet_id AND po.name=d.name AND po.region=d.region AND po.size_slug=d.size AND po.status='active' AND po.project_member AND po.pool_tag_present) AND latest.observed_at>=transaction_timestamp()-interval '90 seconds' AND latest.observed_at<=transaction_timestamp() AND latest.executable_build_sha=d.desired_build_sha AND latest.capacity=r.worker_capacity AND latest.heartbeat_sec=r.heartbeat_sec AND latest.poll_sec=r.poll_sec AND latest.backend_api_origin=r.backend_api_origin AND latest.host_firewall_script_sha256=r.host_firewall_script_sha256 AND latest.kernel_firewall_policy_sha256=r.kernel_firewall_policy_sha256) UPDATE recorder_droplets d SET state='standby_ready',lease_admission_enabled=false,observed_build_sha=c.executable_build_sha,observed_common_policy_sha256=c.common_policy_sha256 FROM candidate c WHERE d.id=c.id`, id)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, tx.Commit(ctx)
}

// SwitchRolloutReplacement atomically removes the old idle worker from the
// claimable set and admits its exact ready replacement. The exclusive fence is
// acquired before droplet/job row locks, matching the cloud claim path.
func (s *Store) SwitchRolloutReplacement(ctx context.Context, replacementID, providerObservationID int64, minimumCapacity int) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return false, err
	}
	var generation, hintID int64
	var kind string
	var rolloutPredecessor, recoveryMissingID *int64
	if err = tx.QueryRow(ctx, `SELECT r.generation,d.replacement_for_id,r.rollout_kind,r.predecessor_generation,r.recovery_missing_droplet_id FROM recorder_droplets d JOIN recorder_pool_rollouts r ON r.generation=d.rollout_generation AND r.status='running' JOIN recorder_rollout_provider_observations p ON p.id=$2 AND p.rollout_generation=r.generation AND p.phase='pre_switch' AND p.observed_at>now()-interval '30 seconds' JOIN recorder_rollout_provider_objects po ON po.observation_id=p.id AND po.is_pool AND po.provider_droplet_id=d.do_droplet_id AND po.name=d.name AND po.region=d.region AND po.size_slug=d.size AND po.status='active' AND po.project_member AND po.pool_tag_present WHERE d.id=$1 AND d.state='standby_ready' AND NOT d.lease_admission_enabled`, replacementID, providerObservationID).Scan(&generation, &hintID, &kind, &rolloutPredecessor, &recoveryMissingID); err != nil {
		return false, nil
	}
	var oldID int64
	if kind == "loss_recovery" {
		if rolloutPredecessor == nil || recoveryMissingID == nil || *recoveryMissingID != hintID {
			return false, nil
		}
		oldID = hintID
	} else if err = tx.QueryRow(ctx, `SELECT d.id FROM recorder_droplets d JOIN recorder_pool_rollouts r ON r.generation=$1 WHERE d.state='active' AND d.lease_admission_enabled AND (COALESCE(d.observed_build_sha,d.build_sha,'')<>r.to_build_sha OR COALESCE(d.observed_common_policy_sha256,'')<>r.common_policy_sha256) AND NOT EXISTS(SELECT 1 FROM recording_jobs j WHERE j.lease_owner=d.name AND j.status='leased' AND j.lease_expires_at>now()) ORDER BY (d.id=$2) DESC,d.id LIMIT 1`, generation, hintID).Scan(&oldID); err != nil {
		return false, nil
	}
	ids := []int64{oldID, replacementID}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	rows, err := tx.Query(ctx, `SELECT id FROM recorder_droplets WHERE id=ANY($1) ORDER BY id FOR UPDATE`, ids)
	if err != nil {
		return false, err
	}
	for rows.Next() {
	}
	rows.Close()
	var oldName, oldState, newState, oldBuild, oldPolicy, targetBuild, targetPolicy, oldBefore, newBefore, oldAfter, newAfter string
	var oldAdmission, newAdmission bool
	var oldCapacity, newCapacity int
	var predecessorGeneration *int64
	var replacementOrdinal int
	if err = tx.QueryRow(ctx, `SELECT name,state,lease_admission_enabled,capacity,COALESCE(observed_build_sha,build_sha,''),COALESCE(observed_common_policy_sha256,'') FROM recorder_droplets WHERE id=$1`, oldID).Scan(&oldName, &oldState, &oldAdmission, &oldCapacity, &oldBuild, &oldPolicy); err != nil {
		return false, err
	}
	if err = tx.QueryRow(ctx, `SELECT state,lease_admission_enabled,capacity,rollout_ordinal FROM recorder_droplets WHERE id=$1`, replacementID).Scan(&newState, &newAdmission, &newCapacity, &replacementOrdinal); err != nil {
		return false, err
	}
	if err = tx.QueryRow(ctx, `SELECT to_build_sha,common_policy_sha256 FROM recorder_pool_rollouts WHERE generation=$1`, generation).Scan(&targetBuild, &targetPolicy); err != nil {
		return false, err
	}
	if oldState != "active" || !oldAdmission || (kind != "loss_recovery" && oldBuild == targetBuild && oldPolicy == targetPolicy) || newState != "standby_ready" || newAdmission || oldCapacity != newCapacity {
		return false, nil
	}
	if err = tx.QueryRow(ctx, `SELECT rollout_generation,recorder_rollout_droplet_head_digest(d) FROM recorder_droplets d WHERE id=$1`, oldID).Scan(&predecessorGeneration, &oldBefore); err != nil {
		return false, err
	}
	if err = tx.QueryRow(ctx, `SELECT recorder_rollout_droplet_head_digest(d) FROM recorder_droplets d WHERE id=$1`, replacementID).Scan(&newBefore); err != nil {
		return false, err
	}
	var jobIDs []int64
	jobRows, err := tx.Query(ctx, `SELECT id FROM recording_jobs WHERE lease_owner=$1 AND status='leased' AND lease_expires_at>now() ORDER BY id FOR UPDATE`, oldName)
	if err != nil {
		return false, err
	}
	for jobRows.Next() {
		var id int64
		if err = jobRows.Scan(&id); err != nil {
			jobRows.Close()
			return false, err
		}
		jobIDs = append(jobIDs, id)
	}
	jobRows.Close()
	if len(jobIDs) != 0 {
		return false, nil
	}
	var capacity int
	if err = tx.QueryRow(ctx, `SELECT COALESCE(sum(capacity),0) FROM recorder_droplets WHERE state='active' AND lease_admission_enabled`).Scan(&capacity); err != nil {
		return false, err
	}
	if capacity-oldCapacity+newCapacity < minimumCapacity {
		return false, fmt.Errorf("atomic switch would reduce claimable capacity below %d", minimumCapacity)
	}
	var workerObservationID, switchIntentID int64
	if err = tx.QueryRow(ctx, `SELECT id FROM recorder_worker_rollout_observations WHERE droplet_id=$1 ORDER BY observed_at DESC,id DESC LIMIT 1`, replacementID).Scan(&workerObservationID); err != nil {
		return false, err
	}
	if kind == "loss_recovery" {
		if rolloutPredecessor == nil || predecessorGeneration == nil || *rolloutPredecessor != *predecessorGeneration {
			return false, fmt.Errorf("loss recovery predecessor membership mismatch")
		}
		var absenceIDs []int64
		if err = tx.QueryRow(ctx, `SELECT array_agg(id ORDER BY observed_at,id) FROM (SELECT p.id,p.observed_at FROM recorder_rollout_provider_observations p WHERE p.rollout_generation=$1 AND p.phase='reconcile' AND p.pool_droplet_count=7 AND NOT EXISTS(SELECT 1 FROM recorder_rollout_provider_objects po,recorder_droplets d WHERE d.id=$2 AND po.observation_id=p.id AND (po.provider_droplet_id=d.do_droplet_id OR po.name=d.name)) ORDER BY p.observed_at DESC,p.id DESC LIMIT 2) q HAVING count(*)=2 AND max(observed_at)>min(observed_at)`, generation, oldID).Scan(&absenceIDs); err != nil || len(absenceIDs) != 2 {
			return false, fmt.Errorf("loss recovery lacks two exact separated absence observations: %w", err)
		}
		var lossIntentID int64
		if err = tx.QueryRow(ctx, `INSERT INTO recorder_rollout_loss_intents(recovery_generation,predecessor_generation,missing_droplet_id,replacement_droplet_id,first_absence_observation_id,second_absence_observation_id,recovery_provider_observation_id,worker_observation_id,missing_before_sha256,replacement_before_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`, generation, *rolloutPredecessor, oldID, replacementID, absenceIDs[0], absenceIDs[1], providerObservationID, workerObservationID, oldBefore, newBefore).Scan(&lossIntentID); err != nil {
			return false, err
		}
		var oldNodeID int64
		if err = tx.QueryRow(ctx, `SELECT node_id FROM recorder_droplets WHERE id=$1`, oldID).Scan(&oldNodeID); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `UPDATE node_tokens SET revoked_at=COALESCE(revoked_at,transaction_timestamp()) WHERE node_id=$1 AND revoked_at IS NULL`, oldNodeID); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `UPDATE nodes SET status='disabled' WHERE id=$1`, oldNodeID); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET state='destroyed',lease_admission_enabled=false,destroyed_at=transaction_timestamp(),rollout_generation=$2,rollout_ordinal=0,replacement_for_id=NULL,desired_build_sha=$3,desired_common_policy_sha256=$4 WHERE id=$1`, oldID, generation, targetBuild, targetPolicy); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET rollout_generation=$1,rollout_ordinal=0,replacement_for_id=NULL,desired_build_sha=$2,desired_common_policy_sha256=$3 WHERE state='active' AND lease_admission_enabled AND rollout_generation=$4`, generation, targetBuild, targetPolicy, *rolloutPredecessor); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET state='active',lease_admission_enabled=true,idle_since=NULL WHERE id=$1`, replacementID); err != nil {
			return false, err
		}
		if err = tx.QueryRow(ctx, `SELECT recorder_rollout_droplet_head_digest(d) FROM recorder_droplets d WHERE id=$1`, oldID).Scan(&oldAfter); err != nil {
			return false, err
		}
		if err = tx.QueryRow(ctx, `SELECT recorder_rollout_droplet_head_digest(d) FROM recorder_droplets d WHERE id=$1`, replacementID).Scan(&newAfter); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO recorder_rollout_loss_events(intent_id,recovery_generation,predecessor_generation,missing_droplet_id,replacement_droplet_id,missing_after_sha256,replacement_after_sha256) VALUES($1,$2,$3,$4,$5,$6,$7)`, lossIntentID, generation, *rolloutPredecessor, oldID, replacementID, oldAfter, newAfter); err != nil {
			return false, err
		}
		return true, tx.Commit(ctx)
	}
	if err = tx.QueryRow(ctx, `INSERT INTO recorder_rollout_switch_intents(successor_generation,replacement_ordinal,predecessor_generation,old_droplet_id,new_droplet_id,pre_switch_provider_observation_id,worker_observation_id,old_before_sha256,new_before_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`, generation, replacementOrdinal, predecessorGeneration, oldID, replacementID, providerObservationID, workerObservationID, oldBefore, newBefore).Scan(&switchIntentID); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET state='rollout_draining',lease_admission_enabled=false,drain_started_at=now(),rollout_generation=$2,rollout_ordinal=0,replacement_for_id=NULL,desired_build_sha=$3,desired_common_policy_sha256=$4 WHERE id=$1`, oldID, generation, targetBuild, targetPolicy); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET state='active',lease_admission_enabled=true,idle_since=NULL WHERE id=$1`, replacementID); err != nil {
		return false, err
	}
	if err = tx.QueryRow(ctx, `SELECT recorder_rollout_droplet_head_digest(d) FROM recorder_droplets d WHERE id=$1`, oldID).Scan(&oldAfter); err != nil {
		return false, err
	}
	if err = tx.QueryRow(ctx, `SELECT recorder_rollout_droplet_head_digest(d) FROM recorder_droplets d WHERE id=$1`, replacementID).Scan(&newAfter); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO recorder_rollout_switch_events(intent_id,successor_generation,replacement_ordinal,predecessor_generation,old_droplet_id,new_droplet_id,old_after_sha256,new_after_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, switchIntentID, generation, replacementOrdinal, predecessorGeneration, oldID, replacementID, oldAfter, newAfter); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (s *Store) BeginRolloutDestroy(ctx context.Context, id int64, minimumAge time.Duration) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return false, err
	}
	var name string
	var oldEnough bool
	var generation int64
	if err = tx.QueryRow(ctx, `SELECT name,rollout_generation,drain_started_at<=now()-$2::interval FROM recorder_droplets WHERE id=$1 AND state='rollout_draining' AND NOT lease_admission_enabled FOR UPDATE`, id, minimumAge.String()).Scan(&name, &generation, &oldEnough); err != nil {
		return false, nil
	}
	var providerReads int
	var separated bool
	if err = tx.QueryRow(ctx, `SELECT count(*),COALESCE(max(observed_at)-min(observed_at)>=$3::interval,false) FROM (SELECT p.observed_at FROM recorder_rollout_provider_observations p JOIN recorder_rollout_provider_objects po ON po.observation_id=p.id AND po.is_pool JOIN recorder_droplets d ON d.id=$1 AND po.provider_droplet_id=d.do_droplet_id WHERE p.rollout_generation=$2 AND p.phase='pre_delete' ORDER BY p.observed_at DESC LIMIT 2) q`, id, generation, minimumAge.String()).Scan(&providerReads, &separated); err != nil {
		return false, err
	}
	if !oldEnough || providerReads < 2 || !separated {
		return false, nil
	}
	var busy bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_jobs WHERE lease_owner=$1 AND status='leased' AND lease_expires_at>now())`, name).Scan(&busy); err != nil || busy {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET state='destroying',lease_admission_enabled=false WHERE id=$1`, id); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (s *Store) FailAbsentRolloutReservation(ctx context.Context, id int64, olderThan time.Duration) (nodeID, tokenID *int64, failed bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return nil, nil, false, err
	}
	var oldEnough bool
	var generation int64
	var name string
	if err = tx.QueryRow(ctx, `SELECT node_id,rollout_generation,name,created_at<=now()-$2::interval FROM recorder_droplets WHERE id=$1 AND state='provisioning' AND do_droplet_id IS NULL AND NOT lease_admission_enabled FOR UPDATE`, id, olderThan.String()).Scan(&nodeID, &generation, &name, &oldEnough); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, false, nil
		}
		return nil, nil, false, err
	}
	var reads int
	var separated bool
	if err = tx.QueryRow(ctx, `SELECT count(*),COALESCE(max(observed_at)-min(observed_at)>=$3::interval,false) FROM (SELECT p.observed_at FROM recorder_rollout_provider_observations p WHERE p.rollout_generation=$1 AND p.phase='reconcile' AND NOT EXISTS(SELECT 1 FROM recorder_rollout_provider_objects po WHERE po.observation_id=p.id AND po.name=$2) ORDER BY p.observed_at DESC LIMIT 2) q`, generation, name, olderThan.String()).Scan(&reads, &separated); err != nil {
		return nil, nil, false, err
	}
	if !oldEnough || reads < 2 || !separated {
		return nodeID, nil, false, nil
	}
	if nodeID != nil {
		if err = tx.QueryRow(ctx, `SELECT id FROM node_tokens WHERE node_id=$1 AND revoked_at IS NULL ORDER BY id DESC LIMIT 1 FOR UPDATE`, *nodeID).Scan(&tokenID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, false, err
		}
	}
	if tokenID != nil {
		if _, err = tx.Exec(ctx, `UPDATE node_tokens SET revoked_at=COALESCE(revoked_at,now()) WHERE id=$1`, *tokenID); err != nil {
			return nil, nil, false, err
		}
	}
	if nodeID != nil {
		if _, err = tx.Exec(ctx, `UPDATE nodes SET status='disabled' WHERE id=$1`, *nodeID); err != nil {
			return nil, nil, false, err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET state='failed',lease_admission_enabled=false,provision_error='provider object absent after bounded recovery' WHERE id=$1`, id); err != nil {
		return nil, nil, false, err
	}
	return nodeID, tokenID, true, tx.Commit(ctx)
}

func (s *Store) CompleteRollout(ctx context.Context, generation int64) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO recorder_rollout_completion_members(rollout_generation,droplet_id,node_id,provider_droplet_id,worker_name,region,size_slug,capacity,build_sha,common_policy_sha256,sealed_at) SELECT $1,d.id,d.node_id,d.do_droplet_id,d.name,d.region,d.size,d.capacity,COALESCE(d.observed_build_sha,d.build_sha,''),d.observed_common_policy_sha256,transaction_timestamp() FROM recorder_droplets d WHERE d.state='active' AND d.lease_admission_enabled AND d.rollout_generation=$1 ON CONFLICT DO NOTHING`, generation); err != nil {
		return false, err
	}
	result, err := tx.Exec(ctx, `UPDATE recorder_pool_rollouts r SET status='complete',completed_at=transaction_timestamp() WHERE generation=$1 AND status='running' AND EXISTS(SELECT 1 FROM recorder_rollout_provider_observations p WHERE p.rollout_generation=r.generation AND p.phase='post_delete' AND p.observed_at>=transaction_timestamp()-interval '60 seconds' AND p.observed_at<=transaction_timestamp() AND p.pool_droplet_count=r.steady_workers AND p.account_droplet_count=r.steady_account_droplets) AND (SELECT count(*) FROM recorder_droplets WHERE state IN ('provisioning','standby_ready','active','rollout_draining','draining','destroying'))=r.steady_workers AND NOT EXISTS(SELECT 1 FROM recorder_droplets d WHERE d.state IN ('provisioning','standby_ready','rollout_draining','draining','destroying') OR (d.state='active' AND (NOT d.lease_admission_enabled OR d.rollout_generation IS DISTINCT FROM r.generation OR d.observed_build_sha IS DISTINCT FROM r.to_build_sha OR d.observed_common_policy_sha256 IS DISTINCT FROM r.common_policy_sha256)) OR (d.state='active' AND d.lease_admission_enabled AND NOT EXISTS(SELECT 1 FROM LATERAL (SELECT o.* FROM recorder_worker_rollout_observations o WHERE o.droplet_id=d.id ORDER BY o.observed_at DESC,o.id DESC LIMIT 1) latest WHERE latest.observed_at>=transaction_timestamp()-interval '90 seconds' AND latest.observed_at<=transaction_timestamp() AND latest.executable_build_sha=r.to_build_sha AND latest.capacity=r.worker_capacity AND latest.heartbeat_sec=r.heartbeat_sec AND latest.poll_sec=r.poll_sec AND latest.backend_api_origin=r.backend_api_origin AND latest.host_firewall_script_sha256=r.host_firewall_script_sha256 AND latest.kernel_firewall_policy_sha256=r.kernel_firewall_policy_sha256)))`, generation)
	if err != nil {
		return false, err
	}
	if result.RowsAffected() != 1 {
		return false, nil
	}
	return true, tx.Commit(ctx)
}

func (s *Store) FinalizeRolloutDestroy(ctx context.Context, id int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return err
	}
	var name string
	var nodeID *int64
	if err = tx.QueryRow(ctx, `SELECT name,node_id FROM recorder_droplets WHERE id=$1 AND state='destroying' AND NOT lease_admission_enabled FOR UPDATE`, id).Scan(&name, &nodeID); err != nil {
		return err
	}
	jobRows, err := tx.Query(ctx, `SELECT id FROM recording_jobs WHERE lease_owner=$1 AND status='leased' AND lease_expires_at>now() ORDER BY id FOR UPDATE`, name)
	if err != nil {
		return err
	}
	if jobRows.Next() {
		jobRows.Close()
		return fmt.Errorf("rollout destroy regained a live lease")
	}
	jobRows.Close()
	if nodeID != nil {
		if _, err = tx.Exec(ctx, `SELECT id FROM nodes WHERE id=$1 FOR UPDATE`, *nodeID); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE node_tokens SET revoked_at=COALESCE(revoked_at,now()) WHERE node_id=$1 AND revoked_at IS NULL`, *nodeID); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE nodes SET status='disabled' WHERE id=$1`, *nodeID); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET state='destroyed',lease_admission_enabled=false,destroyed_at=now() WHERE id=$1`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// FinalizeAbsentRolloutWorker retires only a nonclaimable rollout worker whose
// exact provider object is absent from two complete, independently committed
// provider inventories separated by the configured dwell. It never handles an
// active/claimable worker: losing steady capacity is a provider-drift incident,
// not an implicit authorization to shrink or silently replace the fleet.
func (s *Store) FinalizeAbsentRolloutWorker(ctx context.Context, id int64, minimumAge time.Duration) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return false, err
	}
	var generation, providerID int64
	var name, state string
	var nodeID *int64
	var oldEnough bool
	if err = tx.QueryRow(ctx, `SELECT rollout_generation,do_droplet_id,name,state,node_id,
		CASE WHEN state IN ('provisioning','standby_ready') THEN created_at<=now()-$2::interval
		     WHEN state='rollout_draining' THEN drain_started_at<=now()-$2::interval
		     ELSE true END
		FROM recorder_droplets WHERE id=$1 AND rollout_generation IS NOT NULL
		  AND state IN ('provisioning','standby_ready','rollout_draining','destroying')
		  AND NOT lease_admission_enabled AND do_droplet_id IS NOT NULL FOR UPDATE`, id, minimumAge.String()).Scan(&generation, &providerID, &name, &state, &nodeID, &oldEnough); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	var reads int
	var separated, latestFresh bool
	if err = tx.QueryRow(ctx, `SELECT count(*),COALESCE(max(observed_at)-min(observed_at)>=$3::interval,false),COALESCE(max(observed_at)>now()-interval '60 seconds',false)
		FROM (SELECT p.observed_at FROM recorder_rollout_provider_observations p
		      WHERE p.rollout_generation=$1 AND p.phase IN ('reconcile','pre_delete')
		        AND NOT EXISTS(SELECT 1 FROM recorder_rollout_provider_objects po
		                       WHERE po.observation_id=p.id AND (po.provider_droplet_id=$2 OR po.name=$4))
		      ORDER BY p.observed_at DESC,id DESC LIMIT 2) q`, generation, providerID, minimumAge.String(), name).Scan(&reads, &separated, &latestFresh); err != nil {
		return false, err
	}
	if !oldEnough || reads < 2 || !separated || !latestFresh {
		return false, nil
	}
	var busy bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_jobs WHERE lease_owner=$1 AND status='leased' AND lease_expires_at>now())`, name).Scan(&busy); err != nil || busy {
		return false, err
	}
	if nodeID != nil {
		if _, err = tx.Exec(ctx, `SELECT id FROM nodes WHERE id=$1 FOR UPDATE`, *nodeID); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `UPDATE node_tokens SET revoked_at=COALESCE(revoked_at,now()) WHERE node_id=$1 AND revoked_at IS NULL`, *nodeID); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, `UPDATE nodes SET status='disabled' WHERE id=$1`, *nodeID); err != nil {
			return false, err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET state='destroyed',lease_admission_enabled=false,destroyed_at=now() WHERE id=$1`, id); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (s *Store) NextReplacementIdentity(ctx context.Context, rollout PoolRollout) (name string, err error) {
	var ordinal int
	if err = s.pool.QueryRow(ctx, `SELECT COALESCE(max(rollout_ordinal) FILTER(WHERE rollout_ordinal>0),0)+1 FROM recorder_droplets WHERE rollout_generation=$1`, rollout.Generation).Scan(&ordinal); err != nil {
		return "", err
	}
	name = fmt.Sprintf("%srollout-%d-%d", NamePrefix, rollout.Generation, ordinal)
	return name, nil
}
