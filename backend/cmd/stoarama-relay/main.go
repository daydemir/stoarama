// Command stoarama-relay is the account-owned relay agent: a small headless binary
// a user installs on their own Mac or Linux machine so Stoarama can capture streams
// (notably YouTube) through the user's residential IP and Chrome cookies. It enrolls
// as a node_type='relay' node, runs the shared recordingworker loop against the prod
// API, and reports liveness plus YouTube cookie health via the node heartbeat. It
// ships no server credentials, and cookies never leave the machine: only resolved
// CDN segments flow out via presigned uploads.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	args := os.Args[2:]
	switch os.Args[1] {
	case "enroll":
		if err := runEnroll(args); err != nil {
			fatal(err)
		}
	case "run":
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		if err := runRelay(ctx); err != nil && ctx.Err() == nil {
			fatal(err)
		}
	case "install-launchd":
		if err := installLaunchd(); err != nil {
			fatal(err)
		}
	case "install-systemd":
		if err := installSystemd(); err != nil {
			fatal(err)
		}
	case "uninstall":
		if err := uninstall(); err != nil {
			fatal(err)
		}
	case "link-youtube":
		if err := runLinkYouTube(args); err != nil {
			fatal(err)
		}
	case "self-update":
		if err := runSelfUpdate(args); err != nil {
			fatal(err)
		}
	case "version", "--version", "-v":
		fmt.Printf("stoarama-relay %s\n", version)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "stoarama-relay %s\n\nusage: stoarama-relay <command>\n\n", version)
	fmt.Fprint(os.Stderr, strings.Join([]string{
		"  enroll --token sie_... [--api-url URL] [--name NAME]  enroll this computer as a relay",
		"  run                                                   run the relay worker + heartbeat (service entrypoint)",
		"  install-launchd                                       write + load the macOS launchd user agent",
		"  install-systemd                                       write + enable the systemd user unit",
		"  uninstall                                             stop the service and remove the unit",
		"  link-youtube                                          export Chrome YouTube cookies for private/members streams (run in Terminal)",
		"  self-update [--api-url URL]                           update the relay binary + yt-dlp from latest.json",
		"  version                                               print the relay version",
		"",
	}, "\n"))
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "stoarama-relay: %v\n", err)
	os.Exit(1)
}
