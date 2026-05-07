package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/korea"
)

var loadKoreaInventoryCmd = korea.LoadInventory

func runKorea(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl korea <inventory|audit>")
	}
	switch args[0] {
	case "inventory":
		runKoreaInventory(ctx, cfg, false)
	case "audit":
		runKoreaInventory(ctx, cfg, true)
	default:
		log.Fatalf("unknown korea subcommand: %s", args[0])
	}
}

func runKoreaInventory(ctx context.Context, cfg config.Config, audit bool) {
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()

	inventory, err := loadKoreaInventoryCmd(ctx, pool)
	if err != nil {
		log.Fatalf("korea inventory: %v", err)
	}
	printJSON(inventory)
	if !audit || inventory.Summary.Complete {
		return
	}
	fmt.Fprintf(os.Stderr, "korea audit incomplete: unresolved=%d families=%d/%d\n",
		inventory.Summary.UnresolvedStreams, inventory.Summary.CoveredFamilies, inventory.Summary.TotalFamilies)
	os.Exit(1)
}
