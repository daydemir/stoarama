package api

import (
	"net/http/httptest"
	"testing"
)

func TestSanitizeResearchRedirectPath(t *testing.T) {
	if got := sanitizeResearchRedirectPath(""); got != "/account" {
		t.Fatalf("blank redirect=%q want /account", got)
	}
	if got := sanitizeResearchRedirectPath("https://example.com"); got != "/account" {
		t.Fatalf("absolute redirect=%q want /account", got)
	}
	if got := sanitizeResearchRedirectPath("/account/console"); got != "/account/console" {
		t.Fatalf("path redirect=%q want /account/console", got)
	}
}

func TestBuildResearchMagicLinkFallsBackToRequestHost(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest("GET", "https://api.example.test/account", nil)
	got := s.buildResearchMagicLink(req, "abc123")
	want := "https://api.example.test/auth/complete?token=abc123"
	if got != want {
		t.Fatalf("magic link=%q want %q", got, want)
	}
}

func TestHashResearchSecretStable(t *testing.T) {
	if got := hashResearchSecret("test-token"); got != hashResearchSecret("test-token") {
		t.Fatalf("hash should be stable")
	}
	if got := hashResearchSecret("test-token"); got == hashResearchSecret("other-token") {
		t.Fatalf("hashes should differ")
	}
}
