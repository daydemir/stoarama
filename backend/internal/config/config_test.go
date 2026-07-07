package config

import (
	"testing"
	"time"
)

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
