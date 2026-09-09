package main

import (
	"errors"
	"strings"
	"time"
)

const (
	joinedAdmissionRetryMaxAttempts = 8
	joinedAdmissionRetryWindow      = 45 * time.Minute
	// The control plane leases both publication and preflight claims for five
	// minutes. A lost claim response is ambiguous until that lease plus margin.
	joinedClaimLeaseTTL    = 5 * time.Minute
	joinedClaimLeaseMargin = 5 * time.Second
)

type joinedAdmissionRetryState struct {
	attempts int
	started  time.Time
}

func (s *joinedAdmissionRetryState) reset() {
	s.attempts = 0
	s.started = time.Time{}
}

func (s *joinedAdmissionRetryState) next(err error, idlePoll time.Duration, now time.Time) (time.Duration, bool) {
	path, ok := joinedRetryableAdmissionPath(err)
	if !ok {
		return 0, false
	}
	s.attempts++
	if s.attempts > joinedAdmissionRetryMaxAttempts {
		return 0, false
	}
	if s.started.IsZero() {
		s.started = now
	}
	delay := joinedClaimLeaseTTL + joinedClaimLeaseMargin
	if path != "/api/v1/recording/joined/publication/claim" && path != "/api/v1/recording/joined/claim" {
		delay = idlePoll
		for i := 1; i < s.attempts && delay < 30*time.Second; i++ {
			delay *= 2
		}
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
	}
	if now.Add(delay).After(s.started.Add(joinedAdmissionRetryWindow)) {
		return 0, false
	}
	return delay, true
}

func joinedRetryableAdmissionPath(err error) (string, bool) {
	path, valid := "", true
	visitJoinedError(err, func(candidate error) {
		candidatePath := ""
		switch candidate := candidate.(type) {
		case *joinedAPIResponseError:
			if joinedTransientHTTPStatus(candidate.status) {
				candidatePath = candidate.path
			}
		case *joinedAPITransportError:
			if joinedTransientTransportCause(candidate.cause) {
				candidatePath = candidate.path
			}
		}
		if !joinedAdmissionPath(candidatePath) || path != "" && path != candidatePath {
			valid = false
			return
		}
		path = candidatePath
	})
	return path, valid && path != "" && !errors.Is(err, errJoinedWorkerTaskDeadline)
}

func joinedAdmissionPath(path string) bool {
	base, _, _ := strings.Cut(path, "?")
	switch base {
	case "/api/v1/recording/joined/token", "/api/v1/recording/joined/publication/claim",
		"/api/v1/recording/joined/claim", "/api/v1/recording/joined/leases/status",
		"/api/v1/recording/joined/status":
		return true
	default:
		return false
	}
}
