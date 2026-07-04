package capture

import "testing"

func TestClassifyYTDLPOutput(t *testing.T) {
	cases := []struct {
		name string
		out  string
		want YTDLPClass
	}{
		{"age-restricted sign in", "ERROR: [youtube] xyz: Sign in to confirm your age. This video may be inappropriate for some users.", YTDLPClassSignInRequired},
		{"private video", "ERROR: [youtube] xyz: Private video. Sign in if you've been granted access to this video", YTDLPClassSignInRequired},
		{"members only", "ERROR: [youtube] xyz: Join this channel to get access to members-only content", YTDLPClassSignInRequired},
		{"cookie db locked chrome running", "ERROR: Could not copy Chrome cookie database. See https://... Chrome is running", YTDLPClassChromeCookieDBLocked},
		{"cookie db sqlite locked", "ERROR: database is locked", YTDLPClassChromeCookieDBLocked},
		{"keychain safe storage no key", "WARNING: Failed to decrypt cookie: cannot decrypt v10 cookies: no key found", YTDLPClassCookiesUnavailable},
		// The proven macOS background/headless failure: reached the cookie store but the
		// Safe Storage key is not granted, so zero cookies decrypt. yt-dlp then silently
		// falls back to a cookie-less request that trips a bot check in the SAME output;
		// cookies_unavailable must win over that resolver_outdated bot-check line.
		{"extracted 0 cookies could not be decrypted", "WARNING: [Cookies] Extracted 0 cookies from chrome (530 could not be decrypted)\nERROR: [youtube] xyz: Sign in to confirm you're not a bot.", YTDLPClassCookiesUnavailable},
		{"bot check is resolver not signin", "ERROR: [youtube] xyz: Sign in to confirm you're not a bot. This helps protect our community.", YTDLPClassResolverOutdated},
		{"nsig extraction outdated", "WARNING: [youtube] Some formats may be missing; nsig extraction failed: Some players", YTDLPClassResolverOutdated},
		{"unable to extract player", "ERROR: [youtube] xyz: Unable to extract yt initial data; please report this issue", YTDLPClassResolverOutdated},
		{"generic network", "ERROR: Unable to download webpage: HTTP Error 503: Service Unavailable", YTDLPClassOther},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClassifyYTDLPOutput(c.out); got != c.want {
				t.Fatalf("ClassifyYTDLPOutput(%q) = %q, want %q", c.out, got, c.want)
			}
		})
	}
}

func TestIsYouTubeSignInError(t *testing.T) {
	if !IsYouTubeSignInError("ERROR: Sign in to confirm your age") {
		t.Fatal("expected genuine sign-in error to be classified as sign-in")
	}
	// A cookie-DB lock must never be treated as a re-login prompt.
	if IsYouTubeSignInError("ERROR: Could not copy Chrome cookie database. Chrome is running") {
		t.Fatal("cookie-DB lock must not classify as sign-in required")
	}
	// A bot check is a resolver-outdated case, not a re-login prompt.
	if IsYouTubeSignInError("ERROR: Sign in to confirm you're not a bot") {
		t.Fatal("bot check must not classify as sign-in required")
	}
}
