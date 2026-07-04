package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
)

// linkExportTimeout bounds the interactive export so the installer (or a standalone
// run) can never hang forever on the macOS Keychain "Always Allow" prompt. It is
// generous enough for a human to read the dialog and click, but bounded.
const linkExportTimeout = 120 * time.Second

// runLinkYouTube exports the user's Chrome YouTube cookies to ~/.stoarama/cookies.txt
// so the background relay can resolve private/members/age-gated YouTube without ever
// touching the browser cookie store itself (which fails headlessly on macOS: the
// Chrome Safe Storage key is bound to the interactive GUI login session).
//
// It MUST be run interactively in the user's GUI Terminal session: that is what makes
// the one-time macOS "Always Allow" Keychain prompt appear so the export can decrypt
// the cookies. It is intentionally the same code the installer invokes right after a
// successful enroll, while the user is still at the Terminal.
//
// On failure (Chrome absent, not logged into YouTube, prompt declined, zero cookies
// decrypted) it prints an honest note that public streams still work and returns an
// error; callers that treat linking as best-effort (the installer) ignore that error.
func runLinkYouTube(_ []string) error {
	bd, err := binDir()
	if err != nil {
		return err
	}
	ytdlp := filepath.Join(bd, "yt-dlp")
	if !fileExists(ytdlp) {
		// Fall back to a PATH yt-dlp for a bare `link-youtube` before install.
		ytdlp = "yt-dlp"
	}
	cookiePath, err := cookiesFilePath()
	if err != nil {
		return err
	}
	dir := filepath.Dir(cookiePath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}

	// Export the full jar to a private temp file (0600) in ~/.stoarama first, so the
	// UNFILTERED jar (yt-dlp dumps EVERY Chrome cookie, all sites) never persists at
	// cookies.txt. We filter it down to YouTube/Google sign-in domains and write only
	// that to cookiePath, then securely shred the temp no matter how we exit.
	rawFile, err := os.CreateTemp(dir, ".cookies-raw-*.txt")
	if err != nil {
		return fmt.Errorf("create temp cookie file: %w", err)
	}
	rawPath := rawFile.Name()
	_ = rawFile.Close()
	if err := os.Chmod(rawPath, 0o600); err != nil {
		secureRemove(rawPath)
		return fmt.Errorf("secure %s: %w", rawPath, err)
	}
	defer secureRemove(rawPath)

	fmt.Println("Linking YouTube: exporting your YouTube and Google sign-in cookies for private/members streams.")
	if runtime.GOOS == "darwin" {
		fmt.Println("macOS will now ask to allow access to \"Chrome Safe Storage\" in your Keychain.")
		fmt.Println("Click \"Always Allow\" so the background relay can keep using these cookies.")
	}
	fmt.Println("Nothing leaves your machine; only YouTube and Google sign-in cookies are kept, at", cookiePath)
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), linkExportTimeout)
	defer cancel()
	// Passing BOTH --cookies-from-browser and --cookies makes yt-dlp load the jar from
	// Chrome and write it to the temp file. --skip-download keeps it to a lightweight
	// info-extract against a stable public video. No --no-warnings: we want the
	// "Extracted N cookies" / "could not be decrypted" lines for validation.
	args := []string{"--cookies-from-browser", "chrome", "--cookies", rawPath, "--skip-download", probeURL}
	out, runErr := exec.CommandContext(ctx, ytdlp, args...).CombinedOutput()

	// Filter the exported jar down to ONLY YouTube/Google sign-in domains before it
	// lands at the path the background relay reads. Everything else (banking, trackers,
	// unrelated logins in the same Chrome profile) is dropped and never written to disk.
	raw, readErr := os.ReadFile(rawPath)
	if readErr != nil {
		return linkFailure(ctx.Err(), runErr, string(out), 0)
	}
	filtered, kept := filterCookieLines(raw)

	// Gate success on a genuine YouTube login cookie, not raw line count. On macOS the
	// login cookies are v10-encrypted and only decrypt with the Keychain grant; the
	// non-auth preference cookies (CONSENT, YSC, VISITOR_INFO1_LIVE, ...) decrypt with
	// no grant at all. A jar with only those would let public streams resolve but
	// silently fail every private/members stream, so it must NOT read as "linked".
	total, hasAuth := inspectCookieBytes(filtered)
	if !hasAuth {
		// Do not leave a login-less jar behind for the run loop to trust.
		_ = os.Remove(cookiePath)
		return linkFailure(ctx.Err(), runErr, string(out), total)
	}
	if err := os.WriteFile(cookiePath, filtered, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", cookiePath, err)
	}
	fmt.Printf("YouTube linked: kept %d YouTube and Google sign-in cookies (only these; all other sites dropped) at %s\n", kept, cookiePath)
	fmt.Println("The relay will use these for private/members YouTube. Public streams never needed them.")
	return nil
}

// linkFailure prints an honest, non-alarming explanation of why no cookies were
// exported and returns an error so a standalone `link-youtube` exits non-zero. The
// installer ignores the error and continues. It classifies the yt-dlp output so a
// Keychain/decrypt failure never reads as a login or resolver problem.
func linkFailure(ctxErr error, runErr error, out string, exportedNonAuth int) error {
	reason := "no YouTube cookies were exported"
	switch {
	case ctxErr == context.DeadlineExceeded:
		reason = "timed out waiting for the Keychain prompt (nothing was clicked)"
	case capture.ClassifyYTDLPOutput(out) == capture.YTDLPClassCookiesUnavailable:
		reason = "Chrome cookies could not be decrypted (Keychain access was not granted)"
	case strings.Contains(strings.ToLower(out), "could not find") && strings.Contains(strings.ToLower(out), "chrome"):
		reason = "Chrome was not found on this computer"
	case exportedNonAuth > 0:
		reason = "no YouTube login cookie was found (log into YouTube in Chrome first, or the Keychain grant was declined)"
	case runErr != nil && strings.TrimSpace(out) != "":
		reason = strings.TrimSpace(firstLine(out))
	}
	fmt.Println()
	fmt.Printf("YouTube not linked: %s.\n", reason)
	fmt.Println("This is fine: public YouTube streams record without any cookies.")
	fmt.Println("To record private/members/age-restricted YouTube later, log into YouTube in Chrome,")
	fmt.Println("then run:  stoarama-relay link-youtube   (and click \"Always Allow\" when macOS asks).")
	return fmt.Errorf("link-youtube: %s", reason)
}

// youTubeAuthCookieNames are Google/YouTube session cookies whose presence proves the
// user is logged in AND the value was actually decrypted. On macOS these are the
// v10-encrypted cookies that need the Chrome Safe Storage Keychain grant, so their
// presence is exactly the signal that the interactive export succeeded (as opposed to
// only the unencrypted preference cookies coming through).
var youTubeAuthCookieNames = map[string]bool{
	"SID": true, "HSID": true, "SSID": true, "APISID": true, "SAPISID": true,
	"__Secure-1PSID": true, "__Secure-3PSID": true,
	"__Secure-1PAPISID": true, "__Secure-3PAPISID": true,
	"LOGIN_INFO": true,
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
