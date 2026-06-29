package api

import (
	"testing"
	"time"
)

func TestAuthLinkLimiterAllowsUpToMaxThenBlocks(t *testing.T) {
	l := newAuthLinkLimiter()
	for i := 0; i < l.max; i++ {
		if !l.allow("1.2.3.4") {
			t.Fatalf("attempt %d should be allowed", i+1)
		}
	}
	if l.allow("1.2.3.4") {
		t.Fatalf("attempt %d should be blocked", l.max+1)
	}
	// A different key is independent.
	if !l.allow("5.6.7.8") {
		t.Fatalf("different key should be allowed")
	}
}

func TestAuthLinkLimiterWindowExpiry(t *testing.T) {
	l := newAuthLinkLimiter()
	now := time.Now()
	l.now = func() time.Time { return now }
	for i := 0; i < l.max; i++ {
		l.allow("k")
	}
	if l.allow("k") {
		t.Fatalf("should be blocked at max")
	}
	now = now.Add(l.window + time.Second)
	if !l.allow("k") {
		t.Fatalf("should be allowed after window expiry")
	}
}

func TestAuthLinkLimiterEmptyKeyAlwaysAllowed(t *testing.T) {
	l := newAuthLinkLimiter()
	for i := 0; i < l.max+3; i++ {
		if !l.allow("") {
			t.Fatalf("empty key should always be allowed")
		}
	}
}
