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
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sys/unix"
)

func durhamCredentialPipe(fd int) (*os.File, error) {
	const invalid = "Durham credential output must be an inherited writable pipe separate from standard output and error"
	if fd < 3 || int64(fd) > 1<<31-1 {
		return nil, errors.New(invalid)
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil || flags&unix.O_ACCMODE != unix.O_WRONLY {
		return nil, errors.New(invalid)
	}
	var pipe unix.Stat_t
	if err := unix.Fstat(fd, &pipe); err != nil || pipe.Mode&unix.S_IFMT != unix.S_IFIFO {
		return nil, errors.New(invalid)
	}
	for _, standard := range []int{unix.Stdout, unix.Stderr} {
		var stream unix.Stat_t
		err := unix.Fstat(standard, &stream)
		if errors.Is(err, unix.EBADF) {
			continue
		}
		if err != nil || (pipe.Dev == stream.Dev && pipe.Ino == stream.Ino) {
			return nil, errors.New(invalid)
		}
	}
	return os.NewFile(uintptr(fd), "durham-credential-pipe"), nil
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
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatal("operator database configuration is invalid")
	}
	poolCfg.MaxConns, poolCfg.MinConns = 1, 0
	poolCfg.ConnConfig.RuntimeParams["application_name"] = "stoarama-durham-key-issuer"
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatal("operator database pool unavailable")
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
