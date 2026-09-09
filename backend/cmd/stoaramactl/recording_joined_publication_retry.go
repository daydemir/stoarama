package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

const joinedPublicationRetryMaxWait = 30 * time.Minute

var errJoinedPublicationRetryAcknowledged = errors.New("joined publication retry acknowledged")

func joinedPublicationFailureRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, joinedrecording.ErrWorkerHeartbeatFailed) {
		return false
	}
	sawRetryable, sawUnknown, sawForbidden := false, false, false
	visitJoinedError(err, func(candidate error) {
		switch candidate := candidate.(type) {
		case *joinedrecording.StorageCapabilityError:
			operationOK := candidate.Operation == "put" || candidate.Operation == "create_capability" ||
				candidate.Operation == "reread_capability" || candidate.Operation == "reread"
			if operationOK && (candidate.Reason == "transport" && joinedTransientTransportCause(candidate.Cause) ||
				candidate.Reason == "status" && joinedTransientHTTPStatus(candidate.StatusCode)) {
				sawRetryable = true
			} else {
				sawForbidden = true
			}
		case *joinedAPIResponseError:
			if joinedTransientHTTPStatus(candidate.status) {
				sawRetryable = true
			} else {
				sawForbidden = true
			}
		case *joinedAPITransportError:
			if joinedTransientTransportCause(candidate.cause) {
				sawRetryable = true
			} else {
				sawForbidden = true
			}
		default:
			if candidate == errJoinedWorkerTaskDeadline || candidate == context.DeadlineExceeded && errors.Is(err, errJoinedWorkerTaskDeadline) {
				sawRetryable = true
			} else {
				sawUnknown = true
			}
		}
	})
	return sawRetryable && !sawForbidden && !sawUnknown
}

func joinedTransientTransportCause(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	for _, candidate := range []error{syscall.ECONNABORTED, syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.EPIPE, syscall.ETIMEDOUT} {
		if errors.Is(err, candidate) {
			return true
		}
	}
	return false
}

func visitJoinedError(err error, visit func(error)) {
	switch err.(type) {
	case *joinedrecording.StorageCapabilityError, *joinedAPIResponseError, *joinedAPITransportError:
		visit(err)
		return
	}
	if many, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range many.Unwrap() {
			visitJoinedError(child, visit)
		}
		return
	}
	if one, ok := err.(interface{ Unwrap() error }); ok {
		visitJoinedError(one.Unwrap(), visit)
		return
	}
	visit(err)
}

func joinedTransientHTTPStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests || status >= 500 && status <= 599
}

func (s *remoteJoinedOperatorService) reportJoinedPublicationFailure(taskCtx context.Context, token, kind, id string, taskErr error) error {
	if taskErr == nil {
		return nil
	}
	retryable := joinedPublicationFailureRetryable(taskErr)
	class, reason := joinedFailureClassification(taskErr)
	if !retryable {
		class, reason = "deterministic", "publication_failure_not_retryable"
	}
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(taskCtx), 30*time.Second)
	defer cancel()
	response, reportErr := s.api.reportFailure(reportCtx, token, joinedrecording.WorkFailureRequest{
		ProtocolVersion: joinedrecording.JoinedProtocolVersion, ScopeKind: kind, ScopeID: id,
		FailureClass: class, ReasonCode: reason,
	})
	if reportErr != nil {
		return errors.Join(taskErr, fmt.Errorf("report joined publication failure: %w", reportErr))
	}
	log.Printf("joined publication failure recorded scope_kind=%s scope_id=%s class=%s reason=%s state=%s", kind, id, class, reason, response.State)
	if response.State != "retry" || !retryable {
		return errJoinedTaskFailureReported
	}
	delay := time.Until(*response.NextAttemptAt)
	if delay > joinedPublicationRetryMaxWait {
		return errors.Join(taskErr, fmt.Errorf("joined publication retry wait exceeds bound"))
	}
	return errors.Join(errJoinedTaskFailureReported, errJoinedPublicationRetryAcknowledged)
}
