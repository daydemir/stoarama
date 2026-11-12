package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
)

// Import is additive and has no hold release operation. Default is read-only;
// it emits exact copies needed for the NAS cutover, never copy/delete work.
func runDeliveryKeep(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("delivery-keep", flag.ContinueOnError)
	file := fs.String("keep-file", "", "exact reviewed keep JSONL")
	sha := fs.String("expected-sha256", "", "reviewed JSONL digest")
	out := fs.String("out", "", "new output directory for read-only NAS-copy manifest")
	apply := fs.Bool("import", false, "add/verify durable holds; never release or delete")
	if fs.Parse(args) != nil || fs.NArg() != 0 || *file == "" || *out == "" {
		log.Fatal("delivery-keep --keep-file FILE --expected-sha256 SHA --out DIR [--import]")
	}
	pool, err := r2RetireOpenPool(ctx, cfg, !*apply)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	store := mustArchiveR2Client(ctx, cfg, pool)
	keep, err := api.ReadPublicDeliveryKeep(ctx, store)
	if err != nil {
		log.Fatal(err)
	}
	if err := r2RetireDeliveryPreview(*file, *sha, keep); err != nil {
		log.Fatal(err)
	}
	keys := make([]string, 0, len(keep.Objects))
	for key := range keep.Objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if *apply {
		tx, err := pool.Begin(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if err := api.LockNASCleanupR2Retirement(ctx, tx); err != nil {
			log.Fatal(err)
		}
		// Adding holds cannot overtake a signed 30s NAS deletion capability or
		// unresolved R2 intent. No capabilities are renewed by this operation.
		var busy bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_items WHERE state='delete_issued' AND permit_expires_at>clock_timestamp()) OR EXISTS(SELECT 1 FROM r2_retirement_reservations x WHERE NOT EXISTS(SELECT 1 FROM r2_object_retirements r WHERE r.bucket=x.bucket AND r.object_key=x.object_key))`).Scan(&busy)
		if err != nil {
			log.Fatal("delivery import fences unavailable")
		}
		if busy {
			log.Fatal("delivery import must wait for deletion authority to drain/reconcile")
		}
		for _, key := range keys {
			o := keep.Objects[key]
			_, err = tx.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,recording_id,kind,sha256,relative_path) VALUES($1,$2,$3,$4,nullif($5,0),$6,$7,$8)
 ON CONFLICT(bucket,object_key) DO UPDATE SET sha256=excluded.sha256,relative_path=excluded.relative_path
 WHERE delivery_keep_holds.etag=excluded.etag AND delivery_keep_holds.size_bytes=excluded.size_bytes`, store.Bucket(), o.Key, o.ETag, o.Size, o.RecordingID, o.Kind, o.SHA256, o.Path)
			if err != nil {
				log.Fatal("delivery hold identity/enrichment conflict")
			}
		}
		ids, recordings := []int64{}, []int64{}
		for id, recording := range keep.OutputDependencies {
			ids = append(ids, id)
			recordings = append(recordings, recording)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO delivery_keep_output_dependencies(output_id,recording_id) SELECT * FROM unnest($1::bigint[],$2::bigint[]) ON CONFLICT DO NOTHING`, ids, recordings); err != nil {
			log.Fatal("delivery assembly dependency import failed")
		}
		if _, err = tx.Exec(ctx, `SELECT refresh_delivery_nas_source_holds()`); err != nil {
			log.Fatal("delivery dependency import failed")
		}
		if err := api.RequireDeliveryKeep(ctx, tx, keep); err != nil {
			log.Fatal(err)
		}
		if err := r2RetireDeliveryRegistryUnchanged(ctx, store, keep); err != nil {
			log.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			log.Fatal("delivery hold commit failed")
		}
	}
	if err := os.Mkdir(*out, 0700); err != nil {
		log.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(*out, "delivery-copy-to-nas.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	var total, verified int64
	// One fresh inventory snapshot avoids one DB round trip per object.
	nas, err := loadDeliveryFreshNAS(ctx, pool)
	if err != nil {
		log.Fatal(err)
	}
	for _, key := range keys {
		o := keep.Objects[key]
		total += o.Size
		if nas[r2RetireNASIdentity{"", o.SHA256, o.Size}] {
			verified++
			continue
		}
		if err := enc.Encode(struct {
			api.DeliveryKeepIdentity
			Action  string `json:"action"`
			NASPath string `json:"nas_relative_path"`
		}{o, "copy_to_nas_then_verify", "delivery-keep/objects/" + o.SHA256}); err != nil {
			log.Fatal(err)
		}
	}
	all, err := os.OpenFile(filepath.Join(*out, "delivery-identities.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		log.Fatal(err)
	}
	for _, key := range keys {
		if err := json.NewEncoder(all).Encode(keep.Objects[key]); err != nil {
			log.Fatal(err)
		}
	}
	if err := all.Sync(); err != nil {
		log.Fatal(err)
	}
	all.Close()

	if err := f.Sync(); err != nil {
		log.Fatal(err)
	}
	b, _ := json.MarshalIndent(map[string]any{"objects": len(keys), "bytes": total, "verified_nas_objects": verified, "copy_first_objects": int64(len(keys)) - verified, "delivery_keep_sha256": keep.SHA256, "holds_imported": *apply, "r2_release": "OUT OF SCOPE: verified NAS copies + completed stitching + page validated serving from NAS + separately reviewed release required"}, "", "  ")
	if err := os.WriteFile(filepath.Join(*out, "summary.json"), append(b, '\n'), 0600); err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(b))
}

func loadDeliveryFreshNAS(ctx context.Context, q r2RetireQuerier) (map[r2RetireNASIdentity]bool, error) {
	rows, err := q.Query(ctx, `SELECT n.sha256,n.size_bytes FROM nas_inventory_files n WHERE n.connection_id=13 AND n.state='present'
 AND n.verified_at BETWEEN now()-interval '24 hours' AND now()+interval '5 minutes' AND n.server_received_at>=now()-interval '24 hours'
 AND NOT EXISTS(SELECT 1 FROM nas_listing_files f JOIN nas_listing_generations g USING(connection_id,generation)
 WHERE f.connection_id=13 AND f.relative_path=n.relative_path AND g.started_at>=n.verified_at
 AND ROW(f.size_bytes,f.mtime_ns,f.ctime_ns,f.inode,f.device) IS DISTINCT FROM ROW(n.size_bytes,n.file_mtime_ns,n.file_ctime_ns,n.file_inode,n.file_device))`)
	if err != nil {
		return nil, fmt.Errorf("fresh delivery NAS evidence unavailable")
	}
	defer rows.Close()
	out := map[r2RetireNASIdentity]bool{}
	for rows.Next() {
		var x r2RetireNASIdentity
		if err := rows.Scan(&x.SHA256, &x.SizeBytes); err != nil {
			return nil, err
		}
		out[x] = true
	}
	return out, rows.Err()
}
