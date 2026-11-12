package main

import (
	"context"
	"flag"
	"github.com/daydemir/stoarama/backend/internal/config"
	"log"
)

// Rollout is deliberately separate from apply, like the delivery fence. It
// installs durable admission, then apply independently checks queue/provider
// state and legacy TTL drain. The rollout switch never asserts quiescence.
func runR2WriterFence(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-retire writer-fence", flag.ExitOnError)
	activate := fs.Bool("activate", false, "install durable maintenance fence after complete writer rollout")
	deactivate := fs.Bool("deactivate", false, "remove maintenance fence only with no apply active")
	deployed := fs.Bool("all-writers-gated", false, "deployment prerequisite: retire every pre-gate API and DB worker before activation")
	fs.Parse(args)
	if fs.NArg() != 0 || *activate == *deactivate || (*activate && !*deployed) {
		log.Fatal("choose --activate --all-writers-gated OR --deactivate")
	}
	pool, err := r2RetireOpenPool(ctx, cfg, false)
	if err != nil {
		log.Fatal("writer fence database unavailable")
	}
	defer pool.Close()
	if *activate {
		store := mustArchiveR2Client(ctx, cfg, pool)
		_, err = pool.Exec(ctx, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES($1,$2)`, store.Bucket(), store.EndpointSHA256())
	} else {
		_, err = pool.Exec(ctx, `DELETE FROM r2_retirement_writer_fences`)
	}
	if err != nil {
		log.Fatal("writer fence transition refused: ", err)
	}
	log.Print("writer fence transition committed; activation is not drain or deletion authority")
}
