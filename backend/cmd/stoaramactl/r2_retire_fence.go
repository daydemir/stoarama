package main

import (
	"context"
	"flag"
	"log"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
)

func runR2DeliveryFence(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-retire delivery-fence", flag.ExitOnError)
	activate := fs.Bool("activate", false, "activate connection 13 fence after all API instances run this gate")
	settle := fs.String("settle-drain-acknowledgment", "", "settle only after ALL URLs expire AND NAS workers are stopped/waited; record exact drain evidence")
	deployed := fs.Bool("all-api-instances-gated", false, "attest all old API instances retired; required for activation")
	fs.Parse(args)
	if fs.NArg() != 0 || (*activate && *settle != "") || (!*activate && *settle == "") || (*activate && !*deployed) {
		log.Fatal("choose --activate --all-api-instances-gated OR --settle-drain-acknowledgment <evidence>")
	}
	pool, err := r2RetireOpenPool(ctx, cfg, false)
	if err != nil {
		log.Fatal("delivery fence database unavailable")
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatal("begin delivery fence failed")
	}
	defer tx.Rollback(ctx)
	if *activate {
		err = api.ActivateNASDeliveryFence(ctx, tx)
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO nas_delivery_retirement_settlements(connection_id,drain_acknowledgment) VALUES(13,$1)`, *settle)
	}
	if err != nil {
		log.Fatal("delivery fence transition refused: ", err)
	}
	if err = tx.Commit(ctx); err != nil {
		log.Fatal("delivery fence commit failed")
	}
	log.Print("connection 13 delivery fence transition committed")
}
