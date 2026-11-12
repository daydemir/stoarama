package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// nas-cleanup verify-recovery is read-only: it reads the plan report through the
// operator API and HEADs (never GETs, writes or deletes) the R2 recovery object
// of every item whose NAS deletion permit was ever issued, checking that it
// still exists at the recorded size. Exits non-zero when any copy is missing,
// differs or cannot be checked.

type nasRecoveryHeadStore interface {
	Bucket() string
	Head(ctx context.Context, key string) (r2.ObjectHead, error)
}

type nasRecoveryItem struct {
	ItemID    int64  `json:"item_id"`
	Ordinal   int    `json:"ordinal"`
	State     string `json:"state"`
	Kind      string `json:"kind"`
	SourceID  int64  `json:"source_id"`
	ObjectKey string `json:"object_key"`
	SizeBytes int64  `json:"size_bytes"`
	R2Bytes   int64  `json:"r2_size_bytes,omitempty"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

type nasRecoverySummary struct {
	PlanID      int64             `json:"plan_id"`
	PlanSHA     string            `json:"plan_sha256"`
	Items       int               `json:"items"`
	Issued      int               `json:"delete_ever_issued"`
	Verified    int               `json:"verified"`
	Problems    int               `json:"problems"`
	ByStatus    map[string]int    `json:"by_status"`
	ProblemList []nasRecoveryItem `json:"problem_items"`
}

func verifyNASCleanupRecovery(ctx context.Context, report map[string]any, store nasRecoveryHeadStore) (nasRecoverySummary, error) {
	raw, err := json.Marshal(report)
	if err != nil {
		return nasRecoverySummary{}, err
	}
	var plan struct {
		ID      int64  `json:"id"`
		PlanSHA string `json:"plan_sha256"`
		Items   []struct {
			Kind      string `json:"kind"`
			SourceID  int64  `json:"source_id"`
			Bucket    string `json:"bucket"`
			ObjectKey string `json:"object_key"`
			SizeBytes int64  `json:"size_bytes"`
		} `json:"items"`
		Results []struct {
			ItemID  int64  `json:"item_id"`
			Ordinal int    `json:"ordinal"`
			State   string `json:"state"`
			Issued  bool   `json:"delete_ever_issued"`
		} `json:"results"`
	}
	if err := json.Unmarshal(raw, &plan); err != nil || plan.ID <= 0 || len(plan.Results) != len(plan.Items) {
		return nasRecoverySummary{}, errors.New("cleanup plan report is incomplete")
	}
	sum := nasRecoverySummary{PlanID: plan.ID, PlanSHA: plan.PlanSHA, Items: len(plan.Items), ByStatus: map[string]int{}}
	for _, res := range plan.Results {
		if !res.Issued {
			continue
		}
		sum.Issued++
		if res.Ordinal < 1 || res.Ordinal > len(plan.Items) {
			return sum, fmt.Errorf("item %d has an invalid ordinal", res.ItemID)
		}
		it := plan.Items[res.Ordinal-1]
		row := nasRecoveryItem{ItemID: res.ItemID, Ordinal: res.Ordinal, State: res.State, Kind: it.Kind, SourceID: it.SourceID, ObjectKey: it.ObjectKey, SizeBytes: it.SizeBytes}
		switch {
		case it.ObjectKey == "" || it.SizeBytes <= 0:
			row.Status = "incomplete_identity"
		case it.Bucket != store.Bucket():
			row.Status, row.Error = "bucket_mismatch", "item bucket "+it.Bucket+" is not "+store.Bucket()
		default:
			head, err := store.Head(ctx, it.ObjectKey)
			switch {
			case r2.IsNotFound(err):
				row.Status = "missing"
			case err != nil:
				row.Status, row.Error = "head_error", err.Error()
			case head.SizeBytes != it.SizeBytes:
				row.Status, row.R2Bytes = "size_mismatch", head.SizeBytes
			default:
				row.Status = "ok"
			}
		}
		sum.ByStatus[row.Status]++
		if row.Status == "ok" {
			sum.Verified++
		} else {
			sum.Problems++
			sum.ProblemList = append(sum.ProblemList, row)
		}
	}
	return sum, nil
}

func runNASCleanupVerifyRecovery(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("nas-cleanup verify-recovery", flag.ExitOnError)
	id := fs.Int64("plan-id", 0, "exact plan id")
	base := fs.String("backend-api-url", defaultBackendAPIURL(), "backend URL")
	token := fs.String("api-token", cfg.APIToken, "service operator API token")
	_ = fs.Parse(args)
	if *id <= 0 || fs.NArg() != 0 {
		log.Fatal("usage: nas-cleanup verify-recovery --plan-id ID")
	}
	report := mustAPIGet(ctx, *base, *token, fmt.Sprintf("/api/v1/service/nas-cleanup/plans/%d", *id))
	summary, err := verifyNASCleanupRecovery(ctx, report, mustArchiveR2Client(ctx, cfg, nil))
	printJSON(summary)
	if err != nil {
		log.Fatalf("verify-recovery: %v", err)
	}
	if summary.Problems > 0 {
		fmt.Fprintf(os.Stderr, "verify-recovery: %d of %d issued items lack a verified R2 recovery copy\n", summary.Problems, summary.Issued)
		os.Exit(2)
	}
}
