package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"golang.org/x/sys/unix"
)

func durhamCredentialPipe(fd int) (*os.File, error) {
	const invalid = "Durham credential output must be an inherited writable pipe descriptor above 2"
	if fd < 3 {
		return nil, errors.New(invalid)
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil || flags&unix.O_ACCMODE != unix.O_WRONLY {
		return nil, errors.New(invalid)
	}
	file := os.NewFile(uintptr(fd), "durham-credential-pipe")
	if file == nil {
		return nil, errors.New(invalid)
	}
	stat, err := file.Stat()
	if err != nil || stat.Mode()&os.ModeType != os.ModeNamedPipe {
		file.Close()
		return nil, errors.New(invalid)
	}
	return file, nil
}

func runMintDurhamStitchKey(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("nas-stitch mint-durham-key", flag.ExitOnError)
	fd := fs.Int("secret-fd", -1, "inherited write pipe to the approved 1Password sink")
	authorization := fs.String("authorization", "", "nonsecret owner approval reference")
	fs.Parse(args)
	if fs.NArg() != 0 || strings.TrimSpace(*authorization) == "" {
		log.Fatal("an owner approval reference and private pipe are required")
	}
	sink, err := durhamCredentialPipe(*fd)
	if err != nil {
		log.Fatal(err)
	}
	defer sink.Close()
	host, hostErr := os.Hostname()
	operator, userErr := user.Current()
	if hostErr != nil || userErr != nil || host == "" || operator.Username == "" {
		log.Fatal("local operator identity unavailable")
	}
	pool, err := r2RetireOpenPool(ctx, cfg, false)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	mintCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	id, err := api.MintDurhamStitchKeyTo(mintCtx, pool, operator.Username+"@"+host, *authorization, sink)
	if err != nil {
		if id > 0 {
			log.Fatalf("Durham key %d issuance or delivery is uncertain; reconcile key and vault before retrying", id)
		}
		log.Fatal(err)
	}
	fmt.Printf("Durham key ID %d delivered to private pipe\n", id)
}
