package capture

import "strings"

// YTDLPClass classifies a yt-dlp resolve outcome for relay cookie diagnostics and
// the worker's job-fail error mapping. The class strings are reported verbatim in
// the relay node heartbeat as nodes.capabilities_jsonb.youtube_error.
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
	// YTDLPClassCookiesUnavailable is a cookie-decrypt / keychain-grant failure: the
	// Chrome "Safe Storage" key is not available to this process, so yt-dlp extracts
	// zero usable cookies ("Extracted 0 cookies ... could not be decrypted"). On macOS
	// the decrypt key is bound to the interactive GUI login session with a one-time
	// "Always Allow" Keychain grant, so a background/headless process can never read
	// it. The fix is to export cookies to a file from the GUI session (link-youtube),
	// NOT a re-login and NOT a resolver self-update. Also used to report that no linked
	// cookie file exists yet.
	YTDLPClassCookiesUnavailable YTDLPClass = "cookies_unavailable"
	// YTDLPClassResolverOutdated is a stale-extractor failure (bot check, nsig /
	// player extraction breakage): the fix is a relay/yt-dlp self-update, not a
	// re-login.
	YTDLPClassResolverOutdated YTDLPClass = "resolver_outdated"
	// YTDLPClassOther is any other failure (network, geo-block, offline stream).
	YTDLPClassOther YTDLPClass = "other"
)

// ClassifyYTDLPOutput maps yt-dlp's combined stdout+stderr for a FAILED resolve
// into a diagnostic class. Precedence is deliberate: a cookie-decrypt / keychain
// failure is checked FIRST, then a locked/inaccessible Chrome cookie database, so a
// cookie-source problem (which yt-dlp masks by falling back to a cookie-less request
// that then trips a bot check) never presents as "resolver outdated" or "log into
// YouTube"; a stale extractor / bot check is checked before the sign-in bucket so it
// is not mistaken for a real login prompt. Callers classify only non-success output;
// a successful resolve is YTDLPClassOK by construction.
func ClassifyYTDLPOutput(output string) YTDLPClass {
	s := strings.ToLower(output)
	switch {
	case containsAny(s, cookiesUnavailableSignals):
		return YTDLPClassCookiesUnavailable
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
	// cookiesUnavailableSignals identify a cookie-decrypt / keychain-grant failure:
	// yt-dlp reached the cookie store but could not decrypt it, yielding zero usable
	// cookies. On macOS this is the headless/background case where the Chrome Safe
	// Storage key is not granted to this process. The fix is a file export from the
	// GUI session (link-youtube), not a re-login or a self-update.
	cookiesUnavailableSignals = []string{
		"could not be decrypted",
		"extracted 0 cookies",
		"failed to decrypt",
		"no key found",
	}
	cookieDBLockSignals = []string{
		"could not copy",
		"database is locked",
		"unable to open database file",
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
