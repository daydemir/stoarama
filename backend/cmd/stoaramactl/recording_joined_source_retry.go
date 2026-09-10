package main

import (
	"context"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

const (
	joinedSourceCapabilityAttempts = 5
	joinedSourceCapabilityTimeout = time.Minute
	joinedSourceCapabilityPath = "/api/v1/recording/joined/capabilities/source"
)

// Only frozen preflight retries this read-capability mint. Publication rebuilds,
// seal/finalize calls, and exact-hour canaries retain their existing behavior.
func (s *remoteJoinedOperatorService) preflightSourceCapability(ctx context.Context, token string, request joinedrecording.SourceCapabilityRequest) (joinedrecording.SourceReadCapability, error) {
	scope, err := s.cfg.JoinedWorkScope()
	if err != nil {
		return joinedrecording.SourceReadCapability{}, err
	}
	if scope != config.JoinedWorkScopeFrozenBatch {
		return s.api.sourceCapability(ctx, token, request)
	}
	ctx, cancel := context.WithTimeout(ctx, joinedSourceCapabilityTimeout)
	defer cancel()
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return joinedrecording.SourceReadCapability{}, err
		}
		response, err := s.api.sourceCapability(ctx, token, request)
		if err == nil || attempt+1 >= joinedSourceCapabilityAttempts || !joinedSourceCapabilityRetryable(err) {
			return response, err
		}
		timer := time.NewTimer(time.Second << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return joinedrecording.SourceReadCapability{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func joinedSourceCapabilityRetryable(err error) bool {
	// postJSON returns these exact typed errors. Do not unwrap an unknown or
	// mixed error tree into a retry: identity, auth, and schema errors stop.
	switch err := err.(type) {
	case *joinedAPIResponseError:
		return err.path == joinedSourceCapabilityPath && joinedTransientHTTPStatus(err.status)
	case *joinedAPITransportError:
		return err.path == joinedSourceCapabilityPath && joinedTransientTransportCause(err.cause)
	default:
		return false
	}
}
