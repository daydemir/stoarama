package main

import (
	"context"
	"log"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// mustArchiveR2Client builds an R2 client from the process config, exiting on a
// missing/invalid R2 configuration. Used by the survey commands to read and
// delete survey objects.
func mustArchiveR2Client(ctx context.Context, cfg config.Config) *r2.Client {
	if err := cfg.ValidateR2(); err != nil {
		log.Fatalf("R2 config required: %v", err)
	}
	r2c, err := r2.New(ctx, r2.Config{
		AccountID: cfg.R2AccountID,
		AccessKey: cfg.R2AccessKeyID,
		SecretKey: cfg.R2SecretAccessKey,
		Region:    cfg.R2Region,
		Bucket:    cfg.R2Bucket,
		Endpoint:  cfg.R2Endpoint,
	})
	if err != nil {
		log.Fatalf("open R2 client: %v", err)
	}
	return r2c
}
