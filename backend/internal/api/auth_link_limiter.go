package api

import (
	"sync"
	"time"
)

// authLinkLimiter is a small in-memory sliding-window limiter for the
// request-a-sign-in-link endpoint. It is basic abuse protection (not enterprise
// rate limiting): it caps how many links a single key (requester IP, or email)
// can trigger inside a short window, so the endpoint cannot be used to spray
// sign-in emails. It never reveals whether an email exists; over-limit requests
// are simply dropped and the caller still gets the same neutral OK response.
type authLinkLimiter struct {
	mu     sync.Mutex
	hits   map[string][]time.Time
	window time.Duration
	max    int
	now    func() time.Time
}

func newAuthLinkLimiter() *authLinkLimiter {
	return &authLinkLimiter{
		hits:   map[string][]time.Time{},
		window: 15 * time.Minute,
		max:    5,
		now:    time.Now,
	}
}

// allow records an attempt for key and reports whether it is within the window
// budget. An empty key is always allowed (nothing to throttle on).
func (l *authLinkLimiter) allow(key string) bool {
	if l == nil || key == "" {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	cutoff := now.Add(-l.window)
	kept := l.hits[key][:0]
	for _, t := range l.hits[key] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	// Opportunistically drop stale keys so the map does not grow unbounded.
	if len(kept) == 0 {
		delete(l.hits, key)
	}
	if len(kept) >= l.max {
		l.hits[key] = kept
		return false
	}
	l.hits[key] = append(kept, now)
	return true
}
