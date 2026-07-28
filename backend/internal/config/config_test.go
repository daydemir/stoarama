package config

import (
	"net/netip"
	"strings"
	"testing"
	"time"
)

func testCookieSigningKey() string {
	return strings.Repeat("test", 8)
}

func TestDefaultMagicLinkTTLIsOneHour(t *testing.T) {
	t.Setenv("MAGIC_LINK_TTL", "")
	t.Setenv("RESEARCH_MAGIC_LINK_TTL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MagicLinkTTL != time.Hour {
		t.Fatalf("MagicLinkTTL=%s want %s", cfg.MagicLinkTTL, time.Hour)
	}
}

func TestMITSharedRecordingsConfiguration(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", "47")
	t.Setenv("MIT_SCL_RECORDINGS_READ_SLUG", "mit-scl")
	t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", "manywalks")
	t.Setenv("MIT_SCL_RECORDINGS_COOKIE_SIGNING_KEY", testCookieSigningKey())
	t.Setenv("MIT_SCL_RECORDINGS_TRUSTED_PROXY_CIDRS", "10.0.0.0/8,2001:db8::/32")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SharedRecordingsAccountID != 47 {
		t.Fatalf("shared recordings account id=%d", cfg.SharedRecordingsAccountID)
	}
	if cfg.SharedRecordingsPassword != "manywalks" {
		t.Fatalf("shared recordings password=%q", cfg.SharedRecordingsPassword)
	}
	if cfg.SharedRecordingsSlug != "mit-scl" {
		t.Fatalf("shared recordings slug=%q", cfg.SharedRecordingsSlug)
	}
	if cfg.SharedRecordingsCookieSigningKey != testCookieSigningKey() {
		t.Fatal("shared recordings cookie signing key was not loaded")
	}
	want := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("2001:db8::/32")}
	if len(cfg.SharedRecordingsProxyCIDRs) != len(want) || cfg.SharedRecordingsProxyCIDRs[0] != want[0] || cfg.SharedRecordingsProxyCIDRs[1] != want[1] {
		t.Fatalf("trusted proxy CIDRs=%v", cfg.SharedRecordingsProxyCIDRs)
	}
}

func TestMITSharedRecordingsTrustedProxyCIDRsMustBeValid(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_TRUSTED_PROXY_CIDRS", "not-a-cidr")
	if _, err := Load(); err == nil {
		t.Fatal("Load succeeded with an invalid trusted proxy CIDR")
	}
}

func TestMITSharedRecordingsPasswordMustContainEightCharacters(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", "47")
	t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", "seven77")
	t.Setenv("MIT_SCL_RECORDINGS_COOKIE_SIGNING_KEY", testCookieSigningKey())
	t.Setenv("MIT_SCL_RECORDINGS_TRUSTED_PROXY_CIDRS", "10.0.0.0/8")
	if _, err := Load(); err == nil {
		t.Fatal("Load succeeded with a seven-character shared recordings password")
	}
}

func TestMITSharedRecordingsAccountIDCannotBeNegative(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", "-1")
	t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", "")
	if _, err := Load(); err == nil {
		t.Fatal("Load succeeded with a negative shared recordings account id")
	}
}

func TestMITSharedRecordingsConfigurationMustBePaired(t *testing.T) {
	for _, tc := range []struct {
		name, accountID, password string
	}{
		{name: "account only", accountID: "47"},
		{name: "password only", password: "team-password"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", tc.accountID)
			t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", tc.password)
			if _, err := Load(); err == nil {
				t.Fatal("Load succeeded with incomplete shared recordings configuration")
			}
		})
	}
}

func TestMITSharedRecordingsAllowsEmptyTrustedProxyCIDRs(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", "47")
	t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", "team-password")
	t.Setenv("MIT_SCL_RECORDINGS_COOKIE_SIGNING_KEY", testCookieSigningKey())
	t.Setenv("MIT_SCL_RECORDINGS_TRUSTED_PROXY_CIDRS", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.SharedRecordingsProxyCIDRs) != 0 {
		t.Fatalf("trusted proxy CIDRs=%v want none", cfg.SharedRecordingsProxyCIDRs)
	}
}

func TestMITSharedRecordingsRequiresCookieSigningKey(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", "47")
	t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", "team-password")
	t.Setenv("MIT_SCL_RECORDINGS_COOKIE_SIGNING_KEY", "")
	if _, err := Load(); err == nil {
		t.Fatal("Load succeeded without a shared recordings cookie signing key")
	}
}

func TestMITSharedRecordingsSlugMustBeURLSafe(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_SLUG", "MIT SCL")
	if _, err := Load(); err == nil {
		t.Fatal("Load succeeded with an invalid shared recordings slug")
	}
}
