package capture

import "strings"

// YTDLPClass classifies a yt-dlp resolve outcome for relay cookie diagnostics and
// the worker's job-fail error mapping. The class strings are reported verbatim in
// the relay node heartbeat as nodes.capabilities_jsonb.yt_cookie_error.
type YTDLPClass string

const (
	// YTDLPClassOK is a successful resolve (a playable URL was returned).
	YTDLPClassOK YTDLPClass = "ok"
	// YTDLPClassSignInRequired is a genuine login/cookie-expiry failure: the user
	// must sign into YouTube in Chrome (age-restricted, private, members-only, ...).
	YTDLPClassSignInRequired YTDLPClass = "sign_in_required"
	// YTDLPClassChromeCookieDBLocked is a cookie-store access failure: Chrome is
	// running / the SQLite cookie DB is busy / Keychain access was denied or timed
	// out. This is NOT a login problem and must never present as sign-in required.
	YTDLPClassChromeCookieDBLocked YTDLPClass = "chrome_cookie_db_locked"
	// YTDLPClassResolverOutdated is a stale-extractor failure (bot check, nsig /
	// player extraction breakage): the fix is a relay/yt-dlp self-update, not a
	// re-login.
	YTDLPClassResolverOutdated YTDLPClass = "resolver_outdated"
	// YTDLPClassOther is any other failure (network, geo-block, offline stream).
	YTDLPClassOther YTDLPClass = "other"
)

// ClassifyYTDLPOutput maps yt-dlp's combined stdout+stderr for a FAILED resolve
// into a diagnostic class. Precedence is deliberate: a locked/inaccessible Chrome
// cookie database is checked FIRST so a transient cookie-DB lock (Chrome running,
// SQLite busy, Keychain denied) never presents as "log into YouTube"; a stale
// extractor / bot check is checked before the sign-in bucket so it is not mistaken
// for a real login prompt. Callers classify only non-success output; a successful
// resolve is YTDLPClassOK by construction.
func ClassifyYTDLPOutput(output string) YTDLPClass {
	s := strings.ToLower(output)
	switch {
	case containsAny(s, cookieDBLockSignals):
		return YTDLPClassChromeCookieDBLocked
	case containsAny(s, resolverOutdatedSignals):
		return YTDLPClassResolverOutdated
	case containsAny(s, signInSignals):
		return YTDLPClassSignInRequired
	default:
		return YTDLPClassOther
	}
}

// IsYouTubeSignInError reports whether an error string is a genuine YouTube
// sign-in / cookie-expiry failure (not a cookie-DB lock, not a stale extractor),
// so the worker can map it to the "youtube_cookie_expired" job-fail sentinel.
func IsYouTubeSignInError(errText string) bool {
	return ClassifyYTDLPOutput(errText) == YTDLPClassSignInRequired
}

var (
	cookieDBLockSignals = []string{
		"could not copy",
		"database is locked",
		"unable to open database file",
		"failed to decrypt",
		"chrome is running",
		"safe storage",
		"keychain",
		"cookie database",
		"could not find chrome cookies database",
		"permission denied",
		"operation not permitted",
	}
	resolverOutdatedSignals = []string{
		"confirm you're not a bot",
		"confirm you’re not a bot",
		"unable to extract",
		"nsig",
		"please report this issue",
		"update to the latest version",
		"yt-dlp is out of date",
		"failed to extract any player response",
	}
	signInSignals = []string{
		"sign in to confirm your age",
		"confirm your age",
		"private video",
		"members-only",
		"join this channel",
		"sign in to view",
		"video is only available",
		"login required",
		"log in to",
	}
)

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
