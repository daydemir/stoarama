package api

import (
	"net/http/httptest"
	"testing"
)

func TestSanitizeAccountRedirectPath(t *testing.T) {
	if got := sanitizeAccountRedirectPath(""); got != "/account" {
		t.Fatalf("blank redirect=%q want /account", got)
	}
	if got := sanitizeAccountRedirectPath("https://example.com"); got != "/account" {
		t.Fatalf("absolute redirect=%q want /account", got)
	}
	if got := sanitizeAccountRedirectPath("/account/console"); got != "/account/console" {
		t.Fatalf("path redirect=%q want /account/console", got)
	}
}

func TestBuildAccountMagicLinkFallsBackToRequestHost(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest("GET", "https://api.example.test/account", nil)
	got := s.buildAccountMagicLink(req, "abc123")
	want := "https://api.example.test/auth/complete?token=abc123"
	if got != want {
		t.Fatalf("magic link=%q want %q", got, want)
	}
}

func TestHashSecretStable(t *testing.T) {
	if got := hashSecret("test-token"); got != hashSecret("test-token") {
		t.Fatalf("hash should be stable")
	}
	if got := hashSecret("test-token"); got == hashSecret("other-token") {
		t.Fatalf("hashes should differ")
	}
}
