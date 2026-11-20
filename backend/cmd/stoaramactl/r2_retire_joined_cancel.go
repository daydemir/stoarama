package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type r2JoinedStopKind string

const (
	r2JoinedRenderStopped r2JoinedStopKind = "render_suspended"
	r2JoinedDODestroyed   r2JoinedStopKind = "do_destroyed"
)

type r2JoinedOwnerMapping struct {
	Owner                    string           `json:"owner"`
	Kind                     r2JoinedStopKind `json:"kind"`
	ResourceID               string           `json:"resource_id"`
	HistoricalResourceFile   string           `json:"historical_resource_file,omitempty"`
	HistoricalResourceSHA256 string           `json:"historical_resource_sha256,omitempty"`
}
type r2JoinedOwnerEvidence struct {
	Owner          string           `json:"owner"`
	Kind           r2JoinedStopKind `json:"kind"`
	ResourceID     string           `json:"resource_id"`
	EvidenceSHA256 string           `json:"evidence_sha256"`
	ObservedAt     time.Time        `json:"observed_at"`
}
type r2JoinedCancelOptions struct {
	batch                                                      int64
	out, ownerMap, ownerMapSHA, snapshot, reviewer, liveCommit string
	apply                                                      bool
}

func parseR2JoinedCancelArgs(args []string) (r2JoinedCancelOptions, error) {
	var o r2JoinedCancelOptions
	fs := flag.NewFlagSet("r2-retire cancel-joined-writers", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Int64Var(&o.batch, "batch-record-id", 0, "exact joined batch record")
	fs.StringVar(&o.out, "out", "", "new private append-only evidence file")
	fs.StringVar(&o.ownerMap, "owner-map", "", "reviewed exact owner/resource mapping JSON")
	fs.StringVar(&o.ownerMapSHA, "expected-owner-map-sha256", "", "reviewed mapping digest")
	fs.StringVar(&o.snapshot, "expected-snapshot-sha256", "", "exact dry-run snapshot digest")
	fs.StringVar(&o.reviewer, "reviewer-reference", "", "fresh independent production review reference")
	fs.StringVar(&o.liveCommit, "expected-live-commit", "", "controller-pinned live backend commit for the idle window")
	fs.BoolVar(&o.apply, "apply", false, "atomically retire only the reviewed, provider-verified joined writers")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() != 0 || o.batch <= 0 || o.out == "" || (o.ownerMap == "") != (o.ownerMapSHA == "") {
		return o, errors.New("require batch record, new evidence path and paired owner-map/digest")
	}
	if o.apply && (o.ownerMap == "" || len(o.snapshot) != 64 || len(o.liveCommit) != 40 || strings.TrimSpace(o.reviewer) == "") {
		return o, errors.New("apply requires reviewed owner map, exact snapshot, pinned live commit and independent reviewer reference")
	}
	return o, nil
}
func r2JoinedReadBoundFile(path, want string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("reviewed evidence file unavailable")
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != want {
		return nil, errors.New("reviewed evidence digest differs")
	}
	return b, nil
}

// Fresh provider reads prove stop only after an exact historical owner/resource
// association was independently reviewed. Neither an old lease nor a provider
// name search creates that association. No provider mutation is implemented.
func r2JoinedVerifyOwners(ctx context.Context, mappings []r2JoinedOwnerMapping, owners []string, read func(context.Context, string, string) (int, []byte, error), renderKey, doKey string) ([]r2JoinedOwnerEvidence, error) {
	if len(mappings) != len(owners) {
		return nil, errors.New("owner mapping roster differs")
	}
	required := make(map[string]bool, len(owners))
	for _, owner := range owners {
		required[owner] = true
	}
	evidence := make([]r2JoinedOwnerEvidence, 0, len(owners))
	doAccount := ""
	for _, m := range mappings {
		if !required[m.Owner] {
			return nil, errors.New("unknown or duplicate owner mapping")
		}
		delete(required, m.Owner)
		var canonical any
		switch m.Kind {
		case r2JoinedRenderStopped:
			if m.ResourceID != "srv-da4l03s9v7es738gshag" || !strings.HasPrefix(m.Owner, m.ResourceID+"-") || renderKey == "" || m.HistoricalResourceFile != "" || m.HistoricalResourceSHA256 != "" {
				return nil, errors.New("Render owner association differs")
			}
			status, b, err := read(ctx, "https://api.render.com/v1/services/"+m.ResourceID, renderKey)
			if err != nil || status != http.StatusOK {
				return nil, errors.New("Render stopped-state evidence unavailable")
			}
			var s struct {
				ID        string `json:"id"`
				Name      string `json:"name"`
				Suspended string `json:"suspended"`
			}
			if json.Unmarshal(b, &s) != nil || s.ID != m.ResourceID || s.Name != "stoarama-joined-worker" || s.Suspended != "suspended" {
				return nil, errors.New("exact Render owner is not suspended")
			}
			status, b, err = read(ctx, "https://api.render.com/v1/services/"+m.ResourceID+"/env-vars/STOARAMA_JOINED_WORK_SCOPE", renderKey)
			var scope struct {
				Value string `json:"value"`
			}
			if err != nil || status != http.StatusOK || json.Unmarshal(b, &scope) != nil || scope.Value != "disabled" {
				return nil, errors.New("Render joined admission is not disabled")
			}
			canonical = s
		case r2JoinedDODestroyed:
			id, err := strconv.ParseInt(m.ResourceID, 10, 64)
			if err != nil || id <= 0 || strconv.FormatInt(id, 10) != m.ResourceID || doKey == "" || !filepath.IsAbs(m.HistoricalResourceFile) {
				return nil, errors.New("DigitalOcean owner resource mapping incomplete")
			}
			b, err := r2JoinedReadBoundFile(m.HistoricalResourceFile, m.HistoricalResourceSHA256)
			if err != nil {
				return nil, err
			}
			var old struct {
				AccountUUID string `json:"account_uuid"`
				Droplet     struct {
					ID   int64  `json:"id"`
					Name string `json:"name"`
				} `json:"droplet"`
			}
			if json.Unmarshal(b, &old) != nil || old.AccountUUID == "" || old.Droplet.ID != id || old.Droplet.Name != m.Owner {
				return nil, errors.New("historical provider resource does not identify exact owner")
			}

			if doAccount == "" {
				status, b, e := read(ctx, "https://api.digitalocean.com/v2/account", doKey)
				var account struct {
					Account struct {
						UUID string `json:"uuid"`
					} `json:"account"`
				}
				if e != nil || status != http.StatusOK || json.Unmarshal(b, &account) != nil || account.Account.UUID == "" {
					return nil, errors.New("DigitalOcean account identity unavailable")
				}
				doAccount = account.Account.UUID
			}
			if old.AccountUUID != doAccount {
				return nil, errors.New("historical resource account differs from current provider account")
			}
			status, _, err := read(ctx, "https://api.digitalocean.com/v2/droplets/"+m.ResourceID, doKey)
			if err != nil || status != http.StatusNotFound {
				return nil, errors.New("exact DigitalOcean owner compute is not proved absent")
			}
			canonical = struct {
				ID                                  int64
				Name, HistoricalSHA256, AccountUUID string
				HTTPStatus                          int
			}{id, m.Owner, m.HistoricalResourceSHA256, doAccount, status}
		default:
			return nil, errors.New("unsupported owner proof kind")
		}
		b, err := json.Marshal(canonical)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(b)
		evidence = append(evidence, r2JoinedOwnerEvidence{Owner: m.Owner, Kind: m.Kind, ResourceID: m.ResourceID, EvidenceSHA256: hex.EncodeToString(sum[:]), ObservedAt: time.Now().UTC()})
	}
	return evidence, nil
}
func r2JoinedProviderRead(ctx context.Context, url, key string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, errors.New("provider request invalid")
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("User-Agent", "stoarama-operator/1")
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}
	r, err := client.Do(req)
	if err != nil {
		return 0, nil, errors.New("provider read failed")
	}
	defer r.Body.Close()
	b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return 0, nil, errors.New("provider response incomplete")
	}
	return r.StatusCode, b, nil
}
func r2JoinedRenderKey() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", errors.New("local Render credentials unavailable")
	}
	b, err := os.ReadFile(filepath.Join(home, ".render", "cli.yaml"))
	if err != nil {
		return "", errors.New("local Render credentials unavailable")
	}
	matches := regexp.MustCompile(`(?m)^\s*key:\s*(\S+)\s*$`).FindAllSubmatch(b, -1)
	if len(matches) != 1 {
		return "", errors.New("local Render credential identity ambiguous")
	}
	return string(matches[0][1]), nil
}

type r2JoinedWriterKind string

const (
	r2JoinedHour      r2JoinedWriterKind = "hour"
	r2JoinedArtifact  r2JoinedWriterKind = "artifact"
	r2JoinedAdmission r2JoinedWriterKind = "admission"
	r2JoinedBatch     r2JoinedWriterKind = "batch"
)

type r2JoinedWriterItem struct {
	Kind      r2JoinedWriterKind `json:"kind"`
	ID        int64              `json:"id"`
	State     string             `json:"state"`
	Owner     string             `json:"owner,omitempty"`
	RowSHA256 string             `json:"row_sha256"`
}
type r2JoinedWriterSnapshot struct {
	ObservedAt             time.Time            `json:"observed_at"`
	BatchID                int64                `json:"batch_record_id"`
	SHA256                 string               `json:"snapshot_sha256"`
	Owners                 []string             `json:"owners"`
	OwnerInventoryComplete bool                 `json:"owner_inventory_complete"`
	Items                  []r2JoinedWriterItem `json:"items"`
}

func r2JoinedWriterObserve(ctx context.Context, q r2RetireQuerier, batch int64) (r2JoinedWriterSnapshot, error) {
	s := r2JoinedWriterSnapshot{BatchID: batch}
	if err := q.QueryRow(ctx, `SELECT clock_timestamp(),r2_joined_writer_snapshot($1),r2_joined_writer_owner_inventory_complete($1),ARRAY(SELECT DISTINCT owner FROM(SELECT claimed_by owner FROM recording_joined_hours WHERE batch_record_id=$1 AND state='leased' UNION SELECT publication_claimed_by FROM recording_joined_artifacts WHERE batch_record_id=$1 AND publication_state='publishing') x WHERE owner IS NOT NULL ORDER BY owner)`, batch).Scan(&s.ObservedAt, &s.SHA256, &s.OwnerInventoryComplete, &s.Owners); err != nil {
		return s, errors.New("joined writer snapshot unavailable")
	}
	rows, err := q.Query(ctx, `SELECT 'hour',id,state,coalesce(claimed_by,''),r2_joined_writer_row_sha(to_jsonb(h)) FROM recording_joined_hours h WHERE batch_record_id=$1 AND state IN('pending','leased') UNION ALL SELECT 'artifact',id,publication_state,coalesce(publication_claimed_by,''),r2_joined_writer_row_sha(to_jsonb(a)) FROM recording_joined_artifacts a WHERE batch_record_id=$1 AND publication_state IN('sealed','publishing') UNION ALL SELECT 'admission',batch_record_id,claims_paused::text,'',r2_joined_writer_row_sha(to_jsonb(c)) FROM recording_joined_admission_controls c WHERE batch_record_id=$1 UNION ALL SELECT 'batch',id,state,'',r2_joined_writer_row_sha(to_jsonb(b)) FROM recording_joined_batches b WHERE id=$1 ORDER BY 1,2`, batch)
	if err != nil {
		return s, errors.New("joined writer rows unavailable")
	}
	defer rows.Close()
	for rows.Next() {
		var i r2JoinedWriterItem
		if err := rows.Scan(&i.Kind, &i.ID, &i.State, &i.Owner, &i.RowSHA256); err != nil {
			return s, errors.New("joined writer observation failed")
		}
		s.Items = append(s.Items, i)
	}
	if rows.Err() != nil {
		return s, errors.New("joined writer observation incomplete")
	}
	return s, nil
}
func runR2JoinedWriterCancel(ctx context.Context, cfg config.Config, args []string) {
	o, err := parseR2JoinedCancelArgs(args)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	pool, err := r2RetireOpenPool(ctx, cfg, !o.apply)
	if err != nil {
		log.Fatal("joined writer database unavailable")
	}
	defer pool.Close()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		log.Fatal("joined writer observation transaction failed")
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL statement_timeout='15s';SET LOCAL work_mem='16MB';SET LOCAL timezone='UTC'`); err != nil {
		log.Fatal("joined writer observation configuration failed")
	}
	snapshot, err := r2JoinedWriterObserve(ctx, tx, o.batch)
	if err != nil {
		log.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		log.Fatal("joined writer observation commit failed")
	}
	var mappings []r2JoinedOwnerMapping
	var evidence []r2JoinedOwnerEvidence
	if o.ownerMap != "" {
		b, e := r2JoinedReadBoundFile(o.ownerMap, o.ownerMapSHA)
		if e != nil {
			log.Fatal(e)
		}
		d := json.NewDecoder(bytes.NewReader(b))
		d.DisallowUnknownFields()
		if d.Decode(&mappings) != nil || d.Decode(new(any)) != io.EOF {
			log.Fatal("invalid owner mapping JSON")
		}
		renderKey := ""
		for _, mapping := range mappings {
			if mapping.Kind == r2JoinedRenderStopped || o.apply {
				renderKey, e = r2JoinedRenderKey()
				if e != nil {
					log.Fatal(e)
				}
				break
			}
		}
		evidence, err = r2JoinedVerifyOwners(ctx, mappings, snapshot.Owners, r2JoinedProviderRead, renderKey, os.Getenv("DIGITALOCEAN_TOKEN"))
		if err != nil {
			log.Fatal(err)
		}
	}
	f, err := os.OpenFile(o.out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		log.Fatal("new private writer evidence path unavailable")
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	run := uuid.New()
	if enc.Encode(struct {
		Type               string                  `json:"type"`
		RunID              uuid.UUID               `json:"run_id"`
		DryRun             bool                    `json:"dry_run"`
		Snapshot           r2JoinedWriterSnapshot  `json:"snapshot"`
		OwnerEvidence      []r2JoinedOwnerEvidence `json:"owner_evidence"`
		ProofSHA256        string                  `json:"proof_sha256,omitempty"`
		Reviewer           string                  `json:"reviewer_reference,omitempty"`
		ExpectedLiveCommit string                  `json:"expected_live_commit,omitempty"`
	}{"joined_writer_retirement", run, !o.apply, snapshot, evidence, o.ownerMapSHA, o.reviewer, o.liveCommit}) != nil || f.Sync() != nil {
		log.Fatal("writer evidence was not durable; no apply")
	}
	if !o.apply {
		log.Printf("dry run: %d exact joined writer rows; no transitions", len(snapshot.Items))
		return
	}
	if !snapshot.OwnerInventoryComplete {
		log.Fatal("historical joined owner inventory is incomplete; no transition")
	}
	if snapshot.SHA256 != o.snapshot {
		log.Fatal("joined writer snapshot differs from reviewed dry run")
	}

	renderKey, err := r2JoinedRenderKey()
	if err != nil {
		log.Fatal(err)
	}
	requireWindow := func() error {
		status, b, e := r2JoinedProviderRead(ctx, "https://api.render.com/v1/services/srv-d6usqn94tr6s73d94cd0/deploys?limit=100", renderKey)
		var deployments []struct {
			Deploy struct {
				ID     string `json:"id"`
				Status string `json:"status"`
				Commit struct {
					ID string `json:"id"`
				} `json:"commit"`
			} `json:"deploy"`
		}
		if e != nil || status != 200 || json.Unmarshal(b, &deployments) != nil {
			return errors.New("live deployment evidence unavailable")
		}
		live := 0
		for _, row := range deployments {
			switch row.Deploy.Status {
			case "live":
				live++
				if row.Deploy.Commit.ID != o.liveCommit {
					return errors.New("live backend differs from controller window")
				}
			case "deactivated", "canceled", "build_failed", "update_failed", "pre_deploy_failed":
			default:
				return errors.New("deployment is in progress or unknown")
			}
		}
		if live != 1 {
			return errors.New("live deployment identity ambiguous")
		}
		var masterOn bool
		if e = pool.QueryRow(ctx, `SELECT enabled FROM nas_cleanup_master`).Scan(&masterOn); e != nil || masterOn {
			return errors.New("configured cleanup master is not OFF")
		}
		var latest int64
		if e = pool.QueryRow(ctx, `SELECT max(id) FROM nas_cleanup_plans WHERE account_id=47 AND connection_id=13`).Scan(&latest); e != nil {
			return errors.New("cleanup runtime identity unavailable")
		}
		status, b, e = r2JoinedProviderRead(ctx, "https://stoarama-api.onrender.com/api/v1/service/nas-cleanup/plans/"+strconv.FormatInt(latest, 10), cfg.ServiceToken)
		var plan struct {
			ID     int64 `json:"id"`
			Master *bool `json:"master_deletion_enabled"`
		}
		if e != nil || status != 200 || json.Unmarshal(b, &plan) != nil || plan.ID != latest || plan.Master == nil || *plan.Master {
			return errors.New("runtime cleanup master is not OFF")
		}
		return nil
	}
	if err = requireWindow(); err != nil {
		log.Fatal(err)
	}
	applyTx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatal("begin joined writer retirement failed")
	}
	defer applyTx.Rollback(ctx)
	if _, err = applyTx.Exec(ctx, `SET LOCAL statement_timeout='30s';SET LOCAL lock_timeout='5s';SET LOCAL work_mem='16MB';SET LOCAL timezone='UTC'`); err != nil {
		log.Fatal("joined writer apply configuration failed")
	}
	if err = r2JoinedApplyIdleFence(ctx, applyTx); err != nil {
		log.Fatal(err)
	}
	body, err := json.Marshal(evidence)
	if err != nil {
		log.Fatal("owner evidence encoding failed")
	}
	var changed int
	if err = applyTx.QueryRow(ctx, `SELECT r2_retire_dead_joined_batch($1,$2,$3,$4,$5,$6::jsonb)`, run, o.batch, o.snapshot, o.ownerMapSHA, o.reviewer, string(body)).Scan(&changed); err != nil {
		log.Fatal("joined writer retirement refused; transaction rolls back")
	}
	if changed != len(snapshot.Items) {
		log.Fatal("joined writer audit/update count differs; transaction rolls back")
	}
	if err = applyTx.Commit(ctx); err != nil {
		log.Fatal("joined writer commit unconfirmed; inspect durable database run ", run)
	}
	if enc.Encode(struct {
		Type    string    `json:"type"`
		RunID   uuid.UUID `json:"run_id"`
		Changed int       `json:"changed"`
	}{"committed", run, changed}) != nil || f.Sync() != nil {
		log.Fatal("joined writer commit recorded in database; local receipt failed, inspect run ", run)
	}
	log.Printf("joined writer run %s committed %d audited terminal transitions; other writer gates unchanged", run, changed)
}

// r2JoinedApplyIdleFence is the apply transaction's authority gate. The idle
// check runs first (it only reads cleanup state), and the master is read FOR
// SHARE inside this same transaction, on this same connection, so the lock
// holds until commit: an emergency OFF waits for this retirement, and a flip
// that committed first is seen here. Reading the master through the pool
// instead would take the lock on another connection and hold nothing, so the
// write could proceed with the master ON.
func r2JoinedApplyIdleFence(ctx context.Context, tx pgx.Tx) error {
	var idle bool
	if err := tx.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM nas_cleanup_plans WHERE mode='enabled') AND NOT EXISTS(SELECT 1 FROM nas_cleanup_batches WHERE mode='enabled') AND NOT EXISTS(SELECT 1 FROM nas_cleanup_items WHERE state IN ('leased','delete_issued'))`).Scan(&idle); err != nil || !idle {
		return errors.New("joined writer retirement requires controller global-idle window")
	}
	var masterHeld bool
	if err := tx.QueryRow(ctx, `SELECT enabled FROM nas_cleanup_master FOR SHARE`).Scan(&masterHeld); err != nil || masterHeld {
		return errors.New("joined writer retirement requires the cleanup master OFF (locked for this transaction)")
	}
	return nil
}
