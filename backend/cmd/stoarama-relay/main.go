// Command stoarama-relay is the account-owned relay agent: a small headless binary
// a user installs on their own Mac or Linux machine so Stoarama can capture generally
// PUBLIC streams (notably YouTube) through the user's residential IP. It enrolls as a
// node_type='relay' node, runs the shared recordingworker loop against the prod API,
// and reports liveness plus youtube_ready via the node heartbeat. It ships no server
// credentials; only resolved CDN segments flow out via presigned uploads.
//
// YouTube resolves COOKIELESS by default (decision 2026-07-04): yt-dlp's android
// client resolves public YouTube from a residential IP with no cookies and no JS
// runtime. The with-cookies path for private/members streams (link-youtube +
// cookie-file resolve) is present but DORMANT behind the STOARAMA_RELAY_YT_COOKIES
// opt-in, because a cookie'd resolve uses yt-dlp's web client and needs a bundled JS
// runtime (Deno) we do not ship. Revisit + bundle Deno only if the cookieless
// android-client bypass stops working. See config.go experimentalCookieMode.
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
		"  enroll --token sie_... [--api-url URL] [--name NAME] [--update-manifest NAME]",
		"  run                                                   run the relay worker + heartbeat (service entrypoint)",
		"  install-launchd                                       write + load the macOS launchd user agent",
		"  install-systemd                                       write + enable the systemd user unit",
		"  uninstall                                             stop the service and remove the unit",
		"  link-youtube                                          [experimental] export Chrome cookies for private/members YouTube; needs STOARAMA_RELAY_YT_COOKIES=1 + a bundled JS runtime",
		"  self-update [--api-url URL] [--manifest NAME]          update from a release manifest",
		"  self-update --rollback                                 restore the previous relay binary",
		"  version                                               print the relay version",
		"",
	}, "\n"))
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "stoarama-relay: %v\n", err)
	os.Exit(1)
}
