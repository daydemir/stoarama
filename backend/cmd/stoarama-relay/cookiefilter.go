package main

import (
	"bufio"
	"bytes"
	"os"
	"strings"
)

// allowedCookieSuffixes are the ONLY registrable domains kept in the exported
// cookie jar. yt-dlp's --cookies-from-browser dumps the ENTIRE Chrome cookie jar
// (every site the profile has ever visited: banking, trackers, unrelated logins),
// so after export we filter down to just the YouTube/Google sign-in domains yt-dlp
// needs to resolve private/members/age-gated YouTube. Everything else is dropped
// and never persisted. googlevideo.com serves the media segments; ytimg.com the
// thumbnails; accounts.google.com and the rest are covered by the google.com suffix.
var allowedCookieSuffixes = []string{
	"youtube.com",
	"youtu.be",
	"google.com",
	"googlevideo.com",
	"ytimg.com",
}

// cookieDomainAllowed reports whether a Netscape cookie domain belongs to one of
// the allowed YouTube/Google domains. The match rule: lowercase the domain, strip a
// single leading "." (Netscape wildcard marker), then keep it only if it equals an
// allowed suffix or is a subdomain of one (ends with "."+suffix). So youtube.com,
// .youtube.com, m.youtube.com, accounts.google.com, .google.com, googlevideo.com,
// i.ytimg.com are kept; media.net, yandex.ru, boston.gov, etc. are dropped.
func cookieDomainAllowed(domain string) bool {
	d := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(domain)), ".")
	for _, suffix := range allowedCookieSuffixes {
		if d == suffix || strings.HasSuffix(d, "."+suffix) {
			return true
		}
	}
	return false
}

// filterCookieLines returns a copy of a Netscape cookie jar containing only the
// header/comment lines and the cookie lines whose domain is allowed by
// cookieDomainAllowed, and the count of cookie lines kept. It handles yt-dlp's
// "#HttpOnly_" cookie lines: those begin with "#" but are real cookies, not
// comments, so their domain is unwrapped and matched like any other line.
func filterCookieLines(input []byte) (out []byte, kept int) {
	var b strings.Builder
	sc := bufio.NewScanner(bytes.NewReader(input))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		trimmed := strings.TrimSpace(line)
		httpOnly := strings.HasPrefix(trimmed, "#HttpOnly_")
		if trimmed == "" {
			// Preserve blank separator lines from the Netscape header block.
			b.WriteString(line)
			b.WriteByte('\n')
			continue
		}
		if strings.HasPrefix(trimmed, "#") && !httpOnly {
			// Genuine header/comment line: keep it.
			b.WriteString(line)
			b.WriteByte('\n')
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 7 {
			// Not a well-formed cookie line; drop it.
			continue
		}
		domain := fields[0]
		if httpOnly {
			domain = strings.TrimPrefix(domain, "#HttpOnly_")
		}
		if !cookieDomainAllowed(domain) {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
		kept++
	}
	return []byte(b.String()), kept
}

// inspectCookieBytes parses a Netscape cookie jar (already filtered) and reports the
// total cookie count and whether it contains at least one YouTube login cookie.
// Netscape format is tab-separated: domain, flag, path, secure, expiration, name,
// value. "#HttpOnly_" lines are real cookies (SID/HSID/SSID are HttpOnly), so they
// are counted, not skipped as comments.
func inspectCookieBytes(input []byte) (total int, hasAuth bool) {
	sc := bufio.NewScanner(bytes.NewReader(input))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		trimmed := strings.TrimSpace(line)
		httpOnly := strings.HasPrefix(trimmed, "#HttpOnly_")
		if trimmed == "" || (strings.HasPrefix(trimmed, "#") && !httpOnly) {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 6 {
			continue
		}
		total++
		if youTubeAuthCookieNames[strings.TrimSpace(fields[5])] {
			hasAuth = true
		}
	}
	return total, hasAuth
}

// secureRemove overwrites a file with zeros before unlinking it, so the unfiltered
// cookie jar (which briefly held every Chrome cookie) does not linger in freed disk
// blocks. Best-effort: a failed overwrite still proceeds to the unlink.
func secureRemove(path string) {
	if fi, err := os.Stat(path); err == nil && fi.Mode().IsRegular() && fi.Size() > 0 {
		if f, err := os.OpenFile(path, os.O_WRONLY, 0o600); err == nil {
			_, _ = f.Write(bytes.Repeat([]byte{0}, int(fi.Size())))
			_ = f.Sync()
			_ = f.Close()
		}
	}
	_ = os.Remove(path)
}
