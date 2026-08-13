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

type PoolRollout struct {
	Generation              int64
	Status                  string
	ToBuildSHA              string
	CommonPolicySHA256      string
	FirewallID              string
	FirewallRulesSHA256     string
	FirewallTag             string
	BaselineMin             int
	Capacity                int
	MaxSurge                int
	AccountDropletLimit     int
	BaselineAccountDroplets int
	ProviderFactsSHA256     string
}

type ReservedReplacement struct {
	Droplet
	NodeTokenID int64
	PlainToken  string
}

type CloudConfigPlan struct {
	WorkerID           string
	RecordingID        int64
	CommonPolicySHA256 string
	RuntimeSHA256      string
	PlanSHA256         string
	ExpiresAt          time.Time
}

func providerFactsDigest(f RolloutProviderFacts) string {
	pool := append([]int64(nil), f.PoolDropletIDs...)
	firewallIDs := append([]int64(nil), f.FirewallDropletIDs...)
	tags := append([]string(nil), f.FirewallTags...)
	sort.Slice(pool, func(i, j int) bool { return pool[i] < pool[j] })
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
	if len(f.PoolDropletIDs) < steady || len(f.PoolDropletIDs) > steady+maxSurge {
		return fmt.Errorf("provider pool size %d outside steady/surge bounds [%d,%d]", len(f.PoolDropletIDs), steady, steady+maxSurge)
	}
	if len(f.PoolDropletIDs) == steady && f.AccountDropletCount+maxSurge > f.AccountDropletLimit {
		return fmt.Errorf("account quota has no bounded surge slot")
	}
	for i, id := range f.PoolDropletIDs {
		if id <= 0 || (i > 0 && f.PoolDropletIDs[i-1] >= id) {
			return fmt.Errorf("provider pool droplet ids must be positive, sorted, unique")
		}
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
		&out.BaselineMin, &out.Capacity, &out.MaxSurge, &out.AccountDropletLimit,
		&out.BaselineAccountDroplets, &out.ProviderFactsSHA256)
	return out, err
}

// EnsureRollout creates or returns the one active rollout under the exclusive
// claim/switch fence. It persists provider and baseline identities before any
// replacement token or provider object exists.
func (s *Store) EnsureRollout(ctx context.Context, cfg Config, commonPolicySHA256 string, facts RolloutProviderFacts) (PoolRollout, error) {
	if err := validateRolloutProviderFacts(facts, cfg.FirewallID, cfg.FirewallRulesSHA256, cfg.FirewallTag, cfg.Min, cfg.RolloutMaxSurge); err != nil {
		return PoolRollout{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return PoolRollout{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return PoolRollout{}, err
	}
	const fields = `generation,status,to_build_sha,common_policy_sha256,firewall_id,firewall_rules_sha256,firewall_tag,baseline_min,capacity,max_surge,account_droplet_limit,baseline_account_droplets,provider_facts_sha256`
	out, err := scanRollout(tx.QueryRow(ctx, `SELECT `+fields+` FROM recorder_pool_rollouts WHERE status IN ('planned','running') FOR UPDATE`))
	if err == nil {
		if out.ToBuildSHA != cfg.BuildSHA || out.CommonPolicySHA256 != commonPolicySHA256 ||
			out.FirewallID != cfg.FirewallID || out.FirewallRulesSHA256 != cfg.FirewallRulesSHA256 ||
			out.FirewallTag != cfg.FirewallTag || out.BaselineMin != cfg.Min || out.Capacity != cfg.Capacity || out.MaxSurge != cfg.RolloutMaxSurge {
			return PoolRollout{}, fmt.Errorf("running rollout identity differs from controller policy")
		}
		if _, err = tx.Exec(ctx, `UPDATE recorder_pool_rollouts SET provider_observed_at=now(),provider_facts_sha256=$2,account_droplet_limit=$3 WHERE generation=$1`, out.Generation, providerFactsDigest(facts), facts.AccountDropletLimit); err != nil {
			return PoolRollout{}, err
		}
		out.ProviderFactsSHA256 = providerFactsDigest(facts)
		out.AccountDropletLimit = facts.AccountDropletLimit
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
	if len(baseline) != cfg.Min {
		return PoolRollout{}, fmt.Errorf("claimable baseline %d must equal steady minimum %d", len(baseline), cfg.Min)
	}
	var live int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM recorder_droplets WHERE state IN ('provisioning','standby_ready','active','rollout_draining','draining','destroying')`).Scan(&live); err != nil {
		return PoolRollout{}, err
	}
	if live != cfg.Min {
		return PoolRollout{}, fmt.Errorf("rollout baseline contains %d live rows, want exact steady %d", live, cfg.Min)
	}
	out, err = scanRollout(tx.QueryRow(ctx, `INSERT INTO recorder_pool_rollouts
		(status,baseline_worker_set_sha256,to_build_sha,common_policy_sha256,firewall_id,firewall_rules_sha256,firewall_tag,baseline_min,capacity,max_surge,account_droplet_limit,baseline_account_droplets,provider_observed_at,provider_facts_sha256)
		VALUES ('running',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now(),$12) RETURNING `+fields,
		baselineWorkerSetDigest(baseline), cfg.BuildSHA, commonPolicySHA256, cfg.FirewallID,
		cfg.FirewallRulesSHA256, cfg.FirewallTag, cfg.Min, cfg.Capacity, cfg.RolloutMaxSurge,
		facts.AccountDropletLimit, facts.AccountDropletCount, providerFactsDigest(facts)))
	if err != nil {
		return PoolRollout{}, err
	}
	return out, tx.Commit(ctx)
}

// ReserveRolloutReplacement atomically preregisters the future worker identity,
// one nonrecoverable token hash, and one nonclaimable provisioning row. The
// plaintext token leaves this call only for its single cloud-init request.
func (s *Store) ReserveRolloutReplacement(ctx context.Context, rollout PoolRollout, cfg Config, name, runtimeSHA string) (ReservedReplacement, error) {
	if !validRecorderWorkerID(name) || !isLowerHex(runtimeSHA, 64) {
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
	var status string
	if err = tx.QueryRow(ctx, `SELECT status FROM recorder_pool_rollouts WHERE generation=$1 FOR UPDATE`, rollout.Generation).Scan(&status); err != nil || status != "running" {
		return ReservedReplacement{}, fmt.Errorf("rollout is not running")
	}
	var pending int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM recorder_droplets WHERE rollout_generation=$1 AND state IN ('provisioning','standby_ready')`, rollout.Generation).Scan(&pending); err != nil || pending != 0 {
		return ReservedReplacement{}, fmt.Errorf("rollout already has a pending replacement")
	}
	var live int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM recorder_droplets WHERE state IN ('provisioning','standby_ready','active','rollout_draining','draining','destroying')`).Scan(&live); err != nil {
		return ReservedReplacement{}, err
	}
	if live >= cfg.Min+cfg.RolloutMaxSurge || live >= cfg.Max {
		return ReservedReplacement{}, fmt.Errorf("bounded surge slot unavailable")
	}
	var staleID int64
	if err = tx.QueryRow(ctx, `SELECT d.id FROM recorder_droplets d WHERE d.state='active' AND d.lease_admission_enabled AND (COALESCE(d.observed_build_sha,d.build_sha,'')<>$1 OR COALESCE(d.observed_common_policy_sha256,'')<>$2) AND NOT EXISTS (SELECT 1 FROM recording_jobs j WHERE j.lease_owner=d.name AND j.status='leased' AND j.lease_expires_at>now()) ORDER BY d.id LIMIT 1 FOR UPDATE`, cfg.BuildSHA, rollout.CommonPolicySHA256).Scan(&staleID); err != nil {
		return ReservedReplacement{}, fmt.Errorf("no idle stale worker available: %w", err)
	}
	var nodeID, tokenID, dropletID int64
	if err = tx.QueryRow(ctx, `INSERT INTO nodes(account_id,node_type,display_name,hostname,platform,status,enrolled_at,last_heartbeat_at) VALUES($1,'local_recorder',$2,'','digitalocean','active',now(),now()) RETURNING id`, cfg.OperatorAccountID, name).Scan(&nodeID); err != nil {
		return ReservedReplacement{}, err
	}
	if err = tx.QueryRow(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash,last_used_at) VALUES($1,$2,$3,now()) RETURNING id`, nodeID, prefix, hashNodeSecret(plain)).Scan(&tokenID); err != nil {
		return ReservedReplacement{}, err
	}
	if err = tx.QueryRow(ctx, `INSERT INTO recorder_droplets(name,node_id,region,size,capacity,state,build_sha,desired_build_sha,desired_common_policy_sha256,desired_runtime_config_sha256,rollout_generation,replacement_for_id,lease_admission_enabled) VALUES($1,$2,$3,$4,$5,'provisioning','',$6,$7,$8,$9,$10,false) RETURNING id`, name, nodeID, cfg.Region, cfg.Size, cfg.Capacity, cfg.BuildSHA, rollout.CommonPolicySHA256, runtimeSHA, rollout.Generation, staleID).Scan(&dropletID); err != nil {
		return ReservedReplacement{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ReservedReplacement{}, err
	}
	return ReservedReplacement{Droplet: Droplet{ID: dropletID, Name: name, NodeID: &nodeID, Region: cfg.Region, Size: cfg.Size, Capacity: cfg.Capacity, State: "provisioning", DesiredBuildSHA: cfg.BuildSHA, DesiredCommonPolicySHA256: rollout.CommonPolicySHA256, DesiredRuntimeConfigSHA256: runtimeSHA, RolloutGeneration: &rollout.Generation, ReplacementForID: &staleID}, NodeTokenID: tokenID, PlainToken: plain}, nil
}

func (s *Store) MarkRolloutStandbyReady(ctx context.Context, id int64, firewallDigest string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return false, err
	}
	result, err := tx.Exec(ctx, `UPDATE recorder_droplets SET state='standby_ready',firewall_fingerprint=$2,firewall_verified_at=now(),lease_admission_enabled=false WHERE id=$1 AND state IN ('provisioning','standby_ready') AND do_droplet_id IS NOT NULL AND last_seen_at>now()-interval '90 seconds' AND observed_build_sha=desired_build_sha AND observed_common_policy_sha256=desired_common_policy_sha256 AND observed_runtime_config_sha256=desired_runtime_config_sha256`, id, firewallDigest)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, tx.Commit(ctx)
}

// SwitchRolloutReplacement atomically removes the old idle worker from the
// claimable set and admits its exact ready replacement. The exclusive fence is
// acquired before droplet/job row locks, matching the cloud claim path.
func (s *Store) SwitchRolloutReplacement(ctx context.Context, replacementID int64, minimumCapacity int) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, rolloutFenceSQL); err != nil {
		return false, err
	}
	var oldID int64
	if err = tx.QueryRow(ctx, `SELECT d.replacement_for_id FROM recorder_droplets d JOIN recorder_pool_rollouts r ON r.generation=d.rollout_generation AND r.status='running' WHERE d.id=$1 AND d.state='standby_ready' AND NOT d.lease_admission_enabled AND d.firewall_fingerprint=r.firewall_rules_sha256`, replacementID).Scan(&oldID); err != nil {
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
	var oldName, oldState, newState string
	var oldAdmission, newAdmission bool
	var oldCapacity, newCapacity int
	if err = tx.QueryRow(ctx, `SELECT name,state,lease_admission_enabled,capacity FROM recorder_droplets WHERE id=$1`, oldID).Scan(&oldName, &oldState, &oldAdmission, &oldCapacity); err != nil {
		return false, err
	}
	if err = tx.QueryRow(ctx, `SELECT state,lease_admission_enabled,capacity FROM recorder_droplets WHERE id=$1`, replacementID).Scan(&newState, &newAdmission, &newCapacity); err != nil {
		return false, err
	}
	if oldState != "active" || !oldAdmission || newState != "standby_ready" || newAdmission || oldCapacity != newCapacity {
		return false, nil
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
	if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET state='rollout_draining',lease_admission_enabled=false,drain_started_at=now() WHERE id=$1`, oldID); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET state='active',lease_admission_enabled=true,idle_since=NULL WHERE id=$1`, replacementID); err != nil {
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
	var providerReads int
	var oldEnough bool
	if err = tx.QueryRow(ctx, `SELECT name,rollout_provider_read_count,drain_started_at<=now()-$2::interval FROM recorder_droplets WHERE id=$1 AND state='rollout_draining' AND NOT lease_admission_enabled FOR UPDATE`, id, minimumAge.String()).Scan(&name, &providerReads, &oldEnough); err != nil {
		return false, nil
	}
	if !oldEnough || providerReads < 2 {
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

func (s *Store) NoteRolloutDestroyProviderRead(ctx context.Context, id int64, factsSHA string, observedAt time.Time) error {
	if !isLowerHex(factsSHA, 64) {
		return fmt.Errorf("invalid provider facts digest")
	}
	_, err := s.pool.Exec(ctx, `UPDATE recorder_droplets SET
		rollout_provider_read_count=LEAST(2,rollout_provider_read_count+1),
		rollout_last_provider_facts_sha256=$2,rollout_last_provider_observed_at=$3
		WHERE id=$1 AND state IN ('provisioning','rollout_draining') AND NOT lease_admission_enabled
		  AND (rollout_last_provider_observed_at IS NULL OR rollout_last_provider_observed_at<$3)`, id, factsSHA, observedAt.UTC())
	return err
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
	var reads int
	var oldEnough bool
	if err = tx.QueryRow(ctx, `SELECT node_id,rollout_provider_read_count,created_at<=now()-$2::interval FROM recorder_droplets WHERE id=$1 AND state='provisioning' AND do_droplet_id IS NULL AND NOT lease_admission_enabled FOR UPDATE`, id, olderThan.String()).Scan(&nodeID, &reads, &oldEnough); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, false, nil
		}
		return nil, nil, false, err
	}
	if !oldEnough || reads < 2 {
		return nodeID, nil, false, nil
	}
	if nodeID != nil {
		if err = tx.QueryRow(ctx, `SELECT id FROM node_tokens WHERE node_id=$1 AND revoked_at IS NULL ORDER BY id DESC LIMIT 1 FOR UPDATE`, *nodeID).Scan(&tokenID); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, false, err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE recorder_droplets SET state='failed',lease_admission_enabled=false,provision_error='provider object absent after bounded recovery' WHERE id=$1`, id); err != nil {
		return nil, nil, false, err
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
	return nodeID, tokenID, true, tx.Commit(ctx)
}

func (s *Store) CompleteRollout(ctx context.Context, generation int64) (bool, error) {
	result, err := s.pool.Exec(ctx, `UPDATE recorder_pool_rollouts r SET status='complete',completed_at=now() WHERE generation=$1 AND status='running' AND NOT EXISTS(SELECT 1 FROM recorder_droplets d WHERE d.state IN ('provisioning','standby_ready','rollout_draining','destroying') OR (d.state='active' AND d.lease_admission_enabled AND (d.observed_build_sha<>r.to_build_sha OR d.observed_common_policy_sha256<>r.common_policy_sha256 OR d.observed_runtime_config_sha256 IS NULL)))`, generation)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
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

func (s *Store) LoadConfigPlan(ctx context.Context, planSHA, commonPolicySHA string) (CloudConfigPlan, error) {
	if planSHA == "" {
		return CloudConfigPlan{CommonPolicySHA256: commonPolicySHA}, nil
	}
	var p CloudConfigPlan
	err := s.pool.QueryRow(ctx, `SELECT worker_id,recording_id,common_policy_sha256,runtime_config_sha256,plan_sha256,expires_at FROM recorder_cloud_config_plans WHERE plan_sha256=$1 AND common_policy_sha256=$2 AND expires_at>now()`, planSHA, commonPolicySHA).Scan(&p.WorkerID, &p.RecordingID, &p.CommonPolicySHA256, &p.RuntimeSHA256, &p.PlanSHA256, &p.ExpiresAt)
	if err != nil {
		return CloudConfigPlan{}, fmt.Errorf("load exact cloud config plan: %w", err)
	}
	expectedRuntime := RuntimeConfigIdentity{CommonPolicySHA256: p.CommonPolicySHA256, WorkerID: p.WorkerID, RecordingIDs: []int64{p.RecordingID}}
	runtimeSHA, digestErr := expectedRuntime.Digest()
	if digestErr != nil || runtimeSHA != p.RuntimeSHA256 {
		return CloudConfigPlan{}, fmt.Errorf("cloud config runtime digest mismatch")
	}
	expected := typedDigest("stoarama-cloud-recorder-config-plan-v1", p.WorkerID, strconv.FormatInt(p.RecordingID, 10), p.CommonPolicySHA256, p.RuntimeSHA256, p.ExpiresAt.UTC().Format("2006-01-02T15:04:05.000000Z"))
	if expected != p.PlanSHA256 {
		return CloudConfigPlan{}, fmt.Errorf("cloud config plan digest mismatch")
	}
	return p, nil
}

func (s *Store) NextReplacementIdentity(ctx context.Context, rollout PoolRollout, configPlanSHA string) (name, runtimeSHA, allowlist string, err error) {
	if configPlanSHA != "" {
		plan, loadErr := s.LoadConfigPlan(ctx, configPlanSHA, rollout.CommonPolicySHA256)
		if loadErr != nil {
			return "", "", "", loadErr
		}
		var exists bool
		if err = s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recorder_droplets WHERE name=$1)`, plan.WorkerID).Scan(&exists); err != nil {
			return "", "", "", err
		}
		if !exists {
			identity := RuntimeConfigIdentity{CommonPolicySHA256: rollout.CommonPolicySHA256, WorkerID: plan.WorkerID, RecordingIDs: []int64{plan.RecordingID}}
			allowlist, err = identity.Allowlist()
			return plan.WorkerID, plan.RuntimeSHA256, allowlist, err
		}
	}
	var ordinal int
	if err = s.pool.QueryRow(ctx, `SELECT count(*)+1 FROM recorder_droplets WHERE rollout_generation=$1`, rollout.Generation).Scan(&ordinal); err != nil {
		return "", "", "", err
	}
	name = fmt.Sprintf("%srollout-%d-%d", NamePrefix, rollout.Generation, ordinal)
	runtimeSHA, err = (RuntimeConfigIdentity{CommonPolicySHA256: rollout.CommonPolicySHA256, WorkerID: name}).Digest()
	return name, runtimeSHA, "", err
}
